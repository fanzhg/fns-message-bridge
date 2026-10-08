"""Text and image capture for Fast Note Sync 3.6.1. Python 3.11+."""
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
from requests_toolbelt.multipart.encoder import MultipartEncoder
from media import DownloadError, ImageDownloader, MediaRejected, inspect_image, file_digest, remote_digest

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
    try:
        with open(path, "rb") as stream:
            config = tomllib.load(stream)
    except tomllib.TOMLDecodeError as exc:
        raise BridgeError(f"Invalid config.toml: {exc}") from None
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
    image_options = config.get("images", {})
    if type(image_options.get("enabled", True)) is not bool:
        raise BridgeError("images.enabled must be true or false")
    for key, default, maximum in (("max_size_mb", 5, 20), ("max_per_message", 10, 20)):
        value = image_options.get(key, default)
        if type(value) is not int or not 1 <= value <= maximum:
            raise BridgeError(f"images.{key} must be an integer between 1 and {maximum}")
    return config


def make_note(config, source, message_id, sender, text, timestamp, attachments=None):
    if source not in SOURCES or not message_id or not (text.strip() or attachments):
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
        self.path = Path(path)
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
        if "attachments" not in {row[1] for row in self.db.execute("PRAGMA table_info(jobs)")}:
            self.db.execute("ALTER TABLE jobs ADD COLUMN attachments TEXT NOT NULL DEFAULT '[]'")
        self.db.commit()

    def enqueue(self, key, path, content, reply, attachments=None):
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
                "INSERT OR IGNORE INTO jobs(key,path,content,reply,attachments) VALUES(?,?,?,?,?)",
                (key, path, content, json.dumps(reply, ensure_ascii=False), json.dumps(attachments or [])))
            return result.rowcount == 1

    def next_job(self):
        with self.lock:
            row = self.db.execute(
                "SELECT * FROM jobs WHERE state IN ('pending','saved','rejected') AND next_at <= ? ORDER BY rowid LIMIT 1",
                (time.time(),)).fetchone()
            return dict(row) if row else None

    def state(self, key, state):
        with self.lock, self.db:
            self.db.execute("UPDATE jobs SET state=?,attempts=0,next_at=0,last_error='' WHERE key=?",
                            (state, key))
            if state in ("done", "rejected_done"):
                # Keep the dedupe key, discard captured text and transient reply credentials.
                self.db.execute("UPDATE jobs SET content='',reply='{}',attachments='[]' WHERE key=?", (key,))

    def update_media(self, job):
        with self.lock, self.db:
            self.db.execute("UPDATE jobs SET attachments=?,content=? WHERE key=?",
                            (json.dumps(job["media"]), job["content"], job["key"]))

    def reject(self, key, reason):
        with self.lock, self.db:
            self.db.execute("UPDATE jobs SET state='rejected',attempts=0,next_at=0,last_error=? WHERE key=?",
                            (reason, key))

    def fail(self, job, error):
        attempts = job["attempts"] + 1
        with self.lock, self.db:
            self.db.execute("UPDATE jobs SET attempts=?,next_at=?,last_error=CASE WHEN state='rejected' THEN last_error ELSE ? END WHERE key=?",
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
        headers = self.headers()
        headers.update(kwargs.pop("headers", {}))
        body = http_json(method, self.url + "/api/" + route, headers=headers, **kwargs)
        if body.get("status") is not True or body.get("code") not in range(1, 7):
            raise ApiError(body.get("code"))
        return body.get("data")

    def headers(self):
        return {"Token": self.config["token"],
                   "X-Client": self.config.get("client", "fns-message-bridge"),
                   "X-Client-Name": "FNS Message Bridge", "X-Client-Version": "0.2.0",
                   "User-Agent": "fns-message-bridge/0.2.0"}

    def upload_image(self, path, local, mime, limit):
        # FNS 3.6.1 has no createOnly for files; inspect existing content before POST.
        for page in range(1, 101):
            data = self.call("GET", "files", params={"vault": self.config["vault"],
                "keyword": path, "page": page, "pageSize": 50})
            if not isinstance(data, dict) or "list" not in data or not isinstance(data["list"], (list, type(None))):
                raise BridgeError("Invalid FNS file list")
            rows = data.get("list") or []
            if any(row.get("path") == path for row in rows):
                from urllib.parse import urlencode
                url = self.url + "/api/file?" + urlencode({"vault": self.config["vault"], "path": path})
                if remote_digest(url, limit, self.headers()) != file_digest(local):
                    raise MediaRejected("附件路径已被其他内容占用，未覆盖原文件")
                return
            if len(rows) < 50:
                break
        else:
            raise BridgeError("FNS file lookup exceeded page limit")
        with local.open("rb") as stream:
            body = MultipartEncoder(fields={"vault": self.config["vault"], "path": path,
                "file": (PurePosixPath(path).name, stream, mime)})
            self.call("POST", "file", data=body, headers={"Content-Type": body.content_type})

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
    def __init__(self, config, source, store, sender, downloader=None):
        self.config, self.source, self.store, self.sender = config, source, store, sender
        self.fns = FNS(config["fns"])
        self.allowed = {str(value) for value in config[source].get("allowed_users", [])}
        self.downloader = downloader
        self.image_options = config.get("images", {})
        self.image_limit = self.image_options.get("max_size_mb", 5) * 1024 * 1024
        self.cache = store.path.parent / "media" / source

    def receive(self, message_id, user_id, text, timestamp, reply, attachments=None):
        if not attachments and text.strip() in ("/whoami", "身份"):
            self.sender(reply, f"你的用户 ID：{user_id}")
            return
        if str(user_id) not in self.allowed:
            LOG.warning("Ignored sender outside %s allowlist", self.source)
            return
        attachments = attachments or []
        key, path, content = make_note(self.config, self.source, message_id, user_id, text, timestamp, attachments)
        if self.store.enqueue(key, path, content, reply, attachments):
            LOG.info("Queued %s %s", self.source, key[:12])

    def cache_file(self, job, index):
        return self.cache / f'{job["key"]}-{index}.bin'

    def clean_cache(self, job):
        for index, _ in enumerate(json.loads(job.get("attachments", "[]"))):
            path = self.cache_file(job, index)
            path.unlink(missing_ok=True)
            path.with_suffix(".part").unlink(missing_ok=True)

    def prepare_images(self, job):
        job["media"] = json.loads(job.get("attachments", "[]"))
        if not job["media"]:
            return
        if not self.image_options.get("enabled", True):
            raise MediaRejected("图片收集已关闭")
        if len(job["media"]) > self.image_options.get("max_per_message", 10):
            raise MediaRejected("单条消息的图片数量超过限制")
        if any(int(item.get("size") or 0) > self.image_limit for item in job["media"]):
            raise MediaRejected("图片超过大小限制")
        self.cache.mkdir(parents=True, exist_ok=True, mode=0o700)
        note = PurePosixPath(job["path"])
        # Validate every image before uploading any, preventing partial invalid albums.
        for index, item in enumerate(job["media"]):
            if item.get("uploaded"):
                self.cache_file(job, index).unlink(missing_ok=True)
                continue
            local = self.cache_file(job, index)
            if not local.exists():
                if self.downloader is None:
                    raise BridgeError("Image downloader unavailable")
                self.downloader(item, local, self.image_limit)
            extension, mime = inspect_image(local, self.image_limit)
            item["path"] = str(note.parent / "assets" / note.stem / f"{note.stem}-{index + 1:02d}{extension}")
            self.store.update_media(job)
        for index, item in enumerate(job["media"]):
            if item.get("uploaded"):
                continue
            local = self.cache_file(job, index)
            _, mime = inspect_image(local, self.image_limit)
            self.fns.upload_image(item["path"], local, mime, self.image_limit)
            item["uploaded"] = True
            self.store.update_media(job)
            local.unlink(missing_ok=True)
        links = "\n".join("![](" + str(PurePosixPath(item["path"]).relative_to(note.parent)) + ")"
                          for item in job["media"])
        # Render from the stored original only once, even after failed note POSTs.
        if not job["media"][0].get("embedded"):
            job["content"] = job["content"].rstrip() + "\n\n" + links + "\n"
            job["media"][0]["embedded"] = True
            self.store.update_media(job)

    def deliver_one(self):
        job = self.store.next_job()
        if not job:
            return False
        try:
            if job["state"] == "pending":
                self.prepare_images(job)
                self.fns.create(job)
                self.store.state(job["key"], "saved")
                LOG.info("Saved %s %s", self.source, job["key"][:12])
                job = dict(job, state="saved", attempts=0)
            rejected = job["state"] == "rejected"
            self.sender(json.loads(job["reply"]), "图片未保存：" + job["last_error"] if rejected
                        else "已保存到 Obsidian：" + job["path"])
            self.clean_cache(job)
            self.store.state(job["key"], "rejected_done" if rejected else "done")
        except MediaRejected as exc:
            LOG.warning("Image rejected %s %s: %s", self.source, job["key"][:12], exc)
            self.clean_cache(job)
            self.store.reject(job["key"], str(exc))
        except Exception as exc:
            # Keep the durable job even if SDK or network delivery fails.
            safe = exc if isinstance(exc, (BridgeError, DownloadError)) else BridgeError(type(exc).__name__)
            LOG.warning("Delivery failed %s %s: %s", self.source, job["key"][:12], safe)
            if job["state"] in ("saved", "rejected") and job["attempts"] >= 9:
                LOG.warning("Acknowledgement abandoned after 10 attempts")
                self.clean_cache(job)
                self.store.state(job["key"], "rejected_done" if job["state"] == "rejected" else "done")
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
    if not message or message.get("from", {}).get("is_bot"):
        return None
    attachments = []
    text = message.get("text") or message.get("caption") or ""
    if message.get("photo"):
        photo = max(message["photo"], key=lambda image: image.get("width", 0) * image.get("height", 0))
        attachments.append({"file_id": photo["file_id"], "size": photo.get("file_size", 0)})
    elif message.get("document", {}).get("mime_type", "").startswith("image/"):
        document = message["document"]
        attachments.append({"file_id": document["file_id"], "size": document.get("file_size", 0)})
    elif not message.get("text"):
        return None
    chat = message["chat"]["id"]
    result = (f'{chat}:{message["message_id"]}', str(message["from"]["id"]),
              text, message["date"], {"chat_id": chat, "message_id": message["message_id"]})
    return result + (attachments,) if attachments else result


def dingtalk_message(data):
    kind = data.get("msgtype")
    if kind not in ("text", "picture", "richText"):
        return None
    content = data.get("content") or {}
    if isinstance(content, str):
        content = json.loads(content)
    attachments = []
    text = data.get("text", {}).get("content", "")
    parts = [content] if kind == "picture" else content.get("richText", []) if kind == "richText" else []
    if kind == "richText":
        text = "\n".join(str(part["text"]) for part in parts if part.get("text"))
    for part in parts:
        code = part.get("downloadCode") or part.get("pictureDownloadCode")
        if code and (kind == "picture" or part.get("type") == "picture" or part.get("pictureDownloadCode")):
            attachments.append({"download_code": code, "robot_code": data.get("robotCode", "")})
    if not (text.strip() or attachments):
        return None
    result = (data["msgId"], str(data.get("senderStaffId") or data["senderId"]),
              text, data["createAt"] / 1000,
              {"webhook": data["sessionWebhook"], "expires": data["sessionWebhookExpiredTime"]})
    return result + (attachments,) if attachments else result


def feishu_message(event):
    message, sender = event["message"], event["sender"]
    kind = message.get("message_type")
    if kind not in ("text", "image", "post") or sender.get("sender_type") != "user":
        return None
    content = json.loads(message["content"])
    attachments = []
    text = content.get("text", "")
    if kind == "image":
        attachments.append({"image_key": content["image_key"], "message_id": message["message_id"]})
    elif kind == "post":
        if "content" not in content:
            content = content.get("zh_cn") or content.get("en_us") or next(iter(content.values()), {})
        paragraphs = [content["title"]] if content.get("title") else []
        for row in content.get("content", []):
            paragraphs.append("".join(str(part.get("text", "")) for part in row if part.get("tag") in ("text", "a")))
            for part in row:
                if part.get("tag") == "img" and part.get("image_key"):
                    attachments.append({"image_key": part["image_key"], "message_id": message["message_id"]})
        text = "\n".join(paragraphs)
    for mention in message.get("mentions") or []:
        text = text.replace(mention["key"], "@" + mention.get("name", ""))
    if not (text.strip() or attachments):
        return None
    result = (message["message_id"], sender["sender_id"]["open_id"], text,
              int(message["create_time"]) / 1000, {"message_id": message["message_id"]})
    return result + (attachments,) if attachments else result


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
    bridge = Bridge(config, "telegram", store, send,
                    ImageDownloader("telegram", config["telegram"], http_json))
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

    bridge = Bridge(config, "dingtalk", store, send,
                    ImageDownloader("dingtalk", config["dingtalk"], http_json))

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
    bridge.downloader = ImageDownloader("feishu", config["feishu"], http_json, client)
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
        fns = FNS(config["fns"])
        fns.check()
        if config.get("images", {}).get("enabled", True):
            fns.call("GET", "files", params={"vault": config["fns"]["vault"], "pageSize": 1})
        LOG.info("FNS read access OK; vault=%s (note/file write permissions not tested)", config["fns"]["vault"])
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
