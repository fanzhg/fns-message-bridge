"""Text capture for Fast Note Sync 3.6.1. Python 3.11+."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import queue
from pathlib import Path, PurePosixPath
import sqlite3
import threading
import time
import tomllib
from datetime import datetime
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

LOG = logging.getLogger("bridge")
SOURCES = ("telegram", "dingtalk", "feishu")


class BridgeError(Exception):
    pass


class ApiError(BridgeError):
    def __init__(self, code):
        self.code = code
        super().__init__(f"API error code={code}")


def http_json(method, url, **kwargs):
    # Do not log requests exceptions: Telegram/session URLs contain credentials.
    try:
        response = requests.request(method, url, timeout=(10, 60),
                                    allow_redirects=False, **kwargs)
    except requests.RequestException as exc:
        raise BridgeError(f"Network error: {type(exc).__name__}") from None
    try:
        body = response.json()
    except ValueError:
        raise BridgeError(f"Non-JSON response, HTTP {response.status_code}") from None
    if not isinstance(body, dict):
        raise BridgeError("Invalid JSON response")
    if not 200 <= response.status_code < 300:
        if isinstance(body.get("code"), int):
            raise ApiError(body["code"])
        raise BridgeError(f"HTTP {response.status_code}")
    return body


def load_config(path):
    with open(path, "rb") as stream:
        config = tomllib.load(stream)
    fns = config["fns"]
    for key in ("url", "vault", "token"):
        if not fns.get(key):
            raise BridgeError(f"Missing fns.{key}")
    parsed = urlparse(fns["url"])
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.query or parsed.fragment:
        raise BridgeError("Invalid fns.url")
    if parsed.username or parsed.password:
        raise BridgeError("Do not embed credentials in fns.url")
    folder = config.get("capture", {}).get("folder", "Inbox")
    parts = folder.split("/")
    if not folder or any(p in ("", ".", "..") for p in parts) or any(c in folder for c in '\\:*?"<>|'):
        raise BridgeError("capture.folder must be a relative vault folder")
    ZoneInfo(config.get("capture", {}).get("timezone", "Asia/Shanghai"))
    return config


def make_note(config, source, message_id, sender, text, timestamp):
    if source not in SOURCES or not message_id or not text.strip():
        raise BridgeError("Invalid text message")
    key = hashlib.sha256(f"{source}:{message_id}".encode()).hexdigest()
    capture = config.get("capture", {})
    local = datetime.fromtimestamp(timestamp, ZoneInfo(capture.get("timezone", "Asia/Shanghai")))
    path = str(PurePosixPath(capture.get("folder", "Inbox"), source,
                            local.strftime("%Y-%m"), local.strftime("%Y-%m-%d_%H-%M-%S.md")))
    marker = f'bridge_id: "{key}"'
    content = (f"---\n{marker}\nsource: {source}\n"
               f"sender: {json.dumps(str(sender), ensure_ascii=False)}\n"
               f"message_id: {json.dumps(str(message_id), ensure_ascii=False)}\n"
               f"created: {json.dumps(local.isoformat())}\ntags: [inbox]\n---\n\n"
               f"{text.strip()}\n")
    return key, path, content


class Store:
    """One DB and one running consumer per source; SQLite commits before ACK."""
    def __init__(self, path):
        self.db = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
              key TEXT PRIMARY KEY, path TEXT NOT NULL, content TEXT NOT NULL,
              reply TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
              attempts INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL DEFAULT 0,
              last_error TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS jobs_path_idx ON jobs(path);
        """)
        self.db.commit()

    def enqueue(self, key, path, content, reply):
        with self.lock, self.db:
            if self.db.execute("SELECT 1 FROM jobs WHERE key=?", (key,)).fetchone():
                return False
            # Reserve date names durably; same-second messages get numeric suffixes.
            base = PurePosixPath(path)
            suffix = 2
            while self.db.execute("SELECT 1 FROM jobs WHERE path=?", (path,)).fetchone():
                path = str(base.with_name(f"{base.stem}-{suffix}{base.suffix}"))
                suffix += 1
            result = self.db.execute(
                "INSERT OR IGNORE INTO jobs(key,path,content,reply) VALUES(?,?,?,?)",
                (key, path, content, json.dumps(reply, ensure_ascii=False)))
            return result.rowcount == 1

    def next_job(self):
        with self.lock:
            row = self.db.execute(
                "SELECT * FROM jobs WHERE state != 'done' AND next_at <= ? ORDER BY rowid LIMIT 1",
                (time.time(),)).fetchone()
            return dict(row) if row else None

    def state(self, key, state):
        with self.lock, self.db:
            self.db.execute("UPDATE jobs SET state=?,attempts=0,next_at=0,last_error='' WHERE key=?",
                            (state, key))
            if state == "done":
                # Keep the dedupe key, discard captured text and transient reply credentials.
                self.db.execute("UPDATE jobs SET content='',reply='{}' WHERE key=?", (key,))

    def fail(self, job, error):
        attempts = job["attempts"] + 1
        with self.lock, self.db:
            self.db.execute("UPDATE jobs SET attempts=?,next_at=?,last_error=? WHERE key=?",
                            (attempts, time.time() + min(300, 2 ** min(attempts, 8)),
                             str(error), job["key"]))

    def get_meta(self, key, default="0"):
        with self.lock:
            row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row[0] if row else default

    def set_meta(self, key, value):
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, str(value)))

    def summary(self):
        with self.lock:
            return dict(self.db.execute("SELECT state,count(*) FROM jobs GROUP BY state").fetchall())


class FNS:
    def __init__(self, config):
        self.config = config
        self.url = config["url"].rstrip("/")
        if self.url.endswith("/api"):
            self.url = self.url[:-4]

    def call(self, method, route, **kwargs):
        headers = {"Token": self.config["token"],
                   "X-Client": self.config.get("client", "fns-message-bridge"),
                   "X-Client-Name": "FNS Message Bridge", "X-Client-Version": "1.0.0",
                   "User-Agent": "fns-message-bridge/1.0"}
        body = http_json(method, self.url + "/api/" + route, headers=headers, **kwargs)
        if body.get("status") is not True or body.get("code") not in range(1, 7):
            raise ApiError(body.get("code"))
        return body.get("data")

    def create(self, job):
        try:
            self.call("POST", "note", json={"vault": self.config["vault"],
                      "path": job["path"], "content": job["content"], "createOnly": True})
        except ApiError as exc:
            if exc.code != 431:
                raise
            note = self.call("GET", "note", params={"vault": self.config["vault"], "path": job["path"]})
            # An earlier POST may have committed even if its response was lost.
            # Preserve later manual edits; never overwrite a foreign note.
            marker = f'bridge_id: "{job["key"]}"'
            if not isinstance(note, dict) or marker not in str(note.get("content", "")).splitlines()[:12]:
                raise BridgeError("Existing note has a different bridge_id")

    def check(self):
        return self.call("GET", "notes", params={"vault": self.config["vault"], "pageSize": 1})


class Bridge:
    def __init__(self, config, source, store, sender):
        self.config, self.source, self.store, self.sender = config, source, store, sender
        self.fns = FNS(config["fns"])
        self.allowed = {str(value) for value in config[source].get("allowed_users", [])}

    def receive(self, message_id, user_id, text, timestamp, reply):
        if text.strip() in ("/whoami", "身份"):
            self.sender(reply, f"你的用户 ID：{user_id}")
            return
        if str(user_id) not in self.allowed:
            LOG.warning("Ignored sender outside %s allowlist", self.source)
            return
        key, path, content = make_note(self.config, self.source, message_id, user_id, text, timestamp)
        if self.store.enqueue(key, path, content, reply):
            LOG.info("Queued %s %s", self.source, key[:12])

    def deliver_one(self):
        job = self.store.next_job()
        if not job:
            return False
        try:
            if job["state"] == "pending":
                self.fns.create(job)
                self.store.state(job["key"], "saved")
                LOG.info("Saved %s %s", self.source, job["key"][:12])
                job = dict(job, state="saved", attempts=0)
            self.sender(json.loads(job["reply"]), "已保存到 Obsidian：" + job["path"])
            self.store.state(job["key"], "done")
        except Exception as exc:
            # Keep the durable job even if SDK or network delivery fails.
            safe = exc if isinstance(exc, BridgeError) else BridgeError(type(exc).__name__)
            LOG.warning("Delivery failed %s %s: %s", self.source, job["key"][:12], safe)
            if job["state"] == "saved" and job["attempts"] >= 9:
                LOG.warning("Note saved; acknowledgement abandoned after 10 attempts")
                self.store.state(job["key"], "done")
            else:
                self.store.fail(job, safe)
        return True

    def worker(self):
        while True:
            try:
                if not self.deliver_one():
                    time.sleep(1)
            except Exception as exc:
                LOG.error("Queue worker failure: %s", type(exc).__name__)
                time.sleep(5)


def start_worker(bridge):
    threading.Thread(target=bridge.worker, name="fns-worker", daemon=True).start()


def quiet_sdk(name):
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.ERROR)


def telegram_message(data):
    message = data.get("message", {})
    if not message.get("text") or message.get("from", {}).get("is_bot"):
        return None
    chat = message["chat"]["id"]
    return (f'{chat}:{message["message_id"]}', str(message["from"]["id"]),
            message["text"], message["date"], {"chat_id": chat, "message_id": message["message_id"]})


def dingtalk_message(data):
    if data.get("msgtype") != "text" or not data.get("text", {}).get("content", "").strip():
        return None
    return (data["msgId"], str(data.get("senderStaffId") or data["senderId"]),
            data["text"]["content"], data["createAt"] / 1000,
            {"webhook": data["sessionWebhook"], "expires": data["sessionWebhookExpiredTime"]})


def feishu_message(event):
    message, sender = event["message"], event["sender"]
    if message.get("message_type") != "text" or sender.get("sender_type") != "user":
        return None
    text = json.loads(message["content"]).get("text", "")
    for mention in message.get("mentions") or []:
        text = text.replace(mention["key"], "@" + mention.get("name", ""))
    if not text.strip():
        return None
    return (message["message_id"], sender["sender_id"]["open_id"], text,
            int(message["create_time"]) / 1000, {"message_id": message["message_id"]})


def run_telegram(config, store):
    token = config["telegram"]["bot_token"]
    root = "https://api.telegram.org/bot" + token + "/"

    def call(method, **kwargs):
        result = http_json("POST", root + method, json=kwargs)
        if result.get("ok") is not True:
            raise BridgeError(f'Telegram error {result.get("error_code")}')
        return result["result"]

    def send(reply, text):
        call("sendMessage", chat_id=reply["chat_id"], text=text,
             reply_parameters={"message_id": reply["message_id"], "allow_sending_without_reply": True})

    me = call("getMe")
    if call("getWebhookInfo").get("url"):
        raise BridgeError("Telegram bot already uses a webhook; use a separate bot or remove it yourself")
    bridge = Bridge(config, "telegram", store, send)
    start_worker(bridge)
    offset_key = f'telegram_offset_{me["id"]}'
    offset = int(store.get_meta(offset_key))
    LOG.info("Telegram polling started")
    while True:
        try:
            updates = call("getUpdates", offset=offset, timeout=40, allowed_updates=["message"])
            for update in updates:
                parsed = telegram_message(update)
                if parsed:
                    bridge.receive(*parsed)
                # The next poll confirms receipt only after the durable queue commit.
                offset = update["update_id"] + 1
                store.set_meta(offset_key, offset)
        except BridgeError as exc:
            LOG.warning("Telegram polling failed: %s", exc)
            time.sleep(5)


def run_dingtalk(config, store):
    import dingtalk_stream as ding

    def send(reply, text):
        parsed = urlparse(reply["webhook"])
        if parsed.scheme != "https" or parsed.hostname != "oapi.dingtalk.com" or parsed.port not in (None, 443):
            raise BridgeError("Invalid DingTalk session webhook")
        if time.time() * 1000 >= reply["expires"]:
            LOG.warning("DingTalk reply window expired; note remains saved")
            return
        result = http_json("POST", reply["webhook"], json={"msgtype": "text", "text": {"content": text}})
        if result.get("errcode") != 0:
            raise BridgeError(f'DingTalk reply error {result.get("errcode")}')

    bridge = Bridge(config, "dingtalk", store, send)

    class Handler(ding.ChatbotHandler):
        async def process(self, callback):
            parsed = dingtalk_message(callback.data)
            if parsed:
                bridge.receive(*parsed)
            return ding.AckMessage.STATUS_OK, "OK"

    options = config["dingtalk"]
    client = ding.DingTalkStreamClient(ding.Credential(options["client_id"], options["client_secret"]))
    client.register_callback_handler(ding.ChatbotMessage.TOPIC, Handler())
    quiet_sdk("dingtalk_stream.client")
    quiet_sdk("dingtalk_stream.handler")
    start_worker(bridge)
    client.start_forever()


def run_feishu(config, store):
    from feishu_lite import Client, FeishuError

    def send(reply, text):
        try:
            client.reply(reply, text)
        except FeishuError as exc:
            raise BridgeError(str(exc)) from None

    bridge = Bridge(config, "feishu", store, send)

    def handler(event):
        parsed = feishu_message(event)
        if parsed:
            bridge.receive(*parsed)

    client = Client(config["feishu"], http_json, handler)
    start_worker(bridge)
    asyncio.run(client.run())


def prepare_source(config, source, state_dir):
    options = config.get(source, {})
    required = {"telegram": ("bot_token",), "dingtalk": ("client_id", "client_secret"),
                "feishu": ("app_id", "app_secret")}[source]
    if not options.get("enabled", False):
        raise BridgeError(f"Source disabled: {source}")
    if any(not options.get(key) for key in required):
        raise BridgeError(f"Missing source credentials: {source}")
    if not options.get("allowed_users"):
        LOG.warning("%s allowlist empty: only /whoami or 身份 will be handled", source)
    process_lock = None
    if os.name == "posix":
        import fcntl
        process_lock = open(state_dir / (source + ".lock"), "a")
        try:
            fcntl.flock(process_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            process_lock.close()
            raise BridgeError(f"Another process already handles {source}") from None
    return Store(state_dir / (source + ".sqlite3")), process_lock


def run_all(config, state_dir):
    sources = [source for source in SOURCES if config.get(source, {}).get("enabled", False)]
    if not sources:
        raise BridgeError("Enable at least one source in config")
    prepared = {source: prepare_source(config, source, state_dir) for source in sources}
    failures = queue.Queue()

    def run(source, store):
        try:
            globals()["run_" + source](config, store)
            failures.put((source, "connection stopped"))
        except BaseException as exc:
            failures.put((source, str(exc) if isinstance(exc, BridgeError) else type(exc).__name__))

    for source, (store, _) in prepared.items():
        threading.Thread(target=run, args=(source, store), name=source, daemon=True).start()
    LOG.info("Shared process started: %s", ", ".join(sources))
    health_path = os.environ.get("BRIDGE_HEALTH_FILE")
    while True:
        if health_path:
            Path(health_path).touch()
        try:
            source, error = failures.get(timeout=20)
        except queue.Empty:
            continue
        raise BridgeError(f"{source} stopped: {error}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.toml")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--source", choices=SOURCES)
    mode.add_argument("--all", action="store_true", help="Run all enabled channels in one process")
    parser.add_argument("--check", action="store_true", help="Check FNS read access; never creates notes")
    parser.add_argument("--status", action="store_true", help="Inspect local queue")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Official SDKs can log session URLs; redact configured secrets from all log messages.
    config = load_config(args.config)
    secrets = [config["fns"]["token"]]
    for source in SOURCES:
        secrets.extend(config.get(source, {}).get(k, "") for k in ("bot_token", "client_secret", "app_secret"))
    class Redact(logging.Filter):
        def filter(self, record):
            text = record.getMessage()
            for secret in secrets:
                if secret:
                    text = text.replace(secret, "[redacted]")
            text = re.sub(r'(https?://[^\s?]+)\?[^\s]+', r'\1?[redacted]', text)
            record.msg, record.args = text, ()
            return True
    for handler in logging.getLogger().handlers:
        handler.addFilter(Redact())
    if args.check:
        FNS(config["fns"]).check()
        LOG.info("FNS read access OK; vault=%s (write permission not tested)", config["fns"]["vault"])
        return
    state_dir = Path(config.get("capture", {}).get("state_dir", "state"))
    state_dir.mkdir(parents=True, exist_ok=True)
    if args.all:
        if args.status:
            parser.error("--status requires --source")
        run_all(config, state_dir)
        return
    if not args.source:
        parser.error("--source is required unless --check is specified")
    if args.status:
        store = Store(state_dir / (args.source + ".sqlite3"))
        print(json.dumps(store.summary()))
        return
    store, process_lock = prepare_source(config, args.source, state_dir)
    globals()["run_" + args.source](config, store)
    raise BridgeError("Source connection stopped; restart required")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logging.error("Stopped: %s", exc if isinstance(exc, BridgeError) else type(exc).__name__)
        raise SystemExit(1)
