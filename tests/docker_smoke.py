"""Exercise all three real adapters with local simulated platforms, no secrets."""
import asyncio
import base64
from email import policy
from email.parser import BytesParser
import json
import logging
from pathlib import Path
import tempfile
import threading
import time
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, "/app")
import bridge
import media
import dingtalk_stream as ding
from feishu_lite import Client, Frame
from websockets.asyncio.server import serve

logging.basicConfig(level=logging.INFO)
notes, replies, callbacks, failures = {}, set(), set(), []
images = {}
PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
lock = threading.Lock()


class FNSHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def do_POST(self):
        if self.path == "/api/file":
            assert self.headers["Token"] == "dummy-token"
            assert self.headers["X-Client"] == "fns-message-bridge"
            body = self.rfile.read(int(self.headers["Content-Length"]))
            message = BytesParser(policy=policy.default).parsebytes(
                ("Content-Type: " + self.headers["Content-Type"] + "\r\n\r\n").encode() + body)
            fields = {part.get_param("name", header="content-disposition"): part.get_payload(decode=True)
                      for part in message.iter_parts()}
            assert fields["vault"] == b"test-vault" and fields["file"] == PNG
            with lock:
                images[fields["path"].decode()] = fields["file"]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"code":1,"status":true,"data":{}}')
            return
        data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert self.path == "/api/note" and data["createOnly"]
        assert self.headers["Token"] == "dummy-token"
        assert self.headers["X-Client"] == "fns-message-bridge"
        with lock:
            assert "![](assets/" in data["content"]
            assert len(images) >= 1
            notes[data["path"]] = data["content"]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"code":1,"status":true,"data":{}}')

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        if parsed.path == "/api/files":
            assert self.headers["Token"] == "dummy-token"
            query = parse_qs(parsed.query)
            with lock:
                matches = [{"path": path} for path in images if query["keyword"][0] in path]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps({"code": 1, "status": True, "data": {"list": matches}}).encode())
        elif parsed.path == "/image":
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(PNG)))
            self.end_headers()
            self.wfile.write(PNG)
        else:
            self.send_response(404)
            self.end_headers()


server = ThreadingHTTPServer(("127.0.0.1", 0), FNSHandler)
threading.Thread(target=server.serve_forever, daemon=True).start()
ready = threading.Event()
ws_port = []


async def ws_handler(ws):
    if ws.request.path.startswith("/ding"):
        await ws.send(json.dumps({"specVersion": "1.0", "type": "CALLBACK",
            "headers": {"topic": ding.ChatbotMessage.TOPIC, "messageId": "callback1", "time": "1791100000000"},
            "data": json.dumps({"msgtype": "richText", "msgId": "ding1", "senderStaffId": "42", "senderId": "42",
                "content": {"richText": [{"text": "钉钉模拟图文"},
                    {"type": "picture", "downloadCode": "ding-image"}]}, "robotCode": "dummy", "createAt": 1791100000000,
                "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?token=dummy",
                "sessionWebhookExpiredTime": int(time.time() * 1000) + 7200000})}))
        ack = json.loads(await asyncio.wait_for(ws.recv(), 10))
        assert ack["code"] == 200
        with lock:
            callbacks.add("dingtalk")
    else:
        frame = Frame(SeqID=1, LogID=2, service=7, method=1)
        for key, value in (("type", "event"), ("message_id", "f1"), ("sum", "1"), ("seq", "0")):
            frame.headers.add(key=key, value=value)
        frame.payload = json.dumps({"header": {"event_type": "im.message.receive_v1"}, "event": {
            "sender": {"sender_type": "user", "sender_id": {"open_id": "ou_42"}}, "message": {
                "message_type": "post", "content": json.dumps({"title": "飞书模拟图文", "content": [[
                    {"tag": "text", "text": "配文"}, {"tag": "img", "image_key": "img1"}]]}),
                "message_id": "om_1", "create_time": "1791100000000"}}}).encode()
        await ws.send(frame.SerializeToString())
        while True:
            ack = Frame.FromString(await asyncio.wait_for(ws.recv(), 10))
            if ack.method == 1:
                assert json.loads(ack.payload)["code"] == 200
                with lock:
                    callbacks.add("feishu")
                break
    await ws.wait_closed()


async def ws_server():
    async with serve(ws_handler, "127.0.0.1", 0) as ws:
        ws_port.append(ws.sockets[0].getsockname()[1])
        ready.set()
        await asyncio.Future()


threading.Thread(target=lambda: asyncio.run(ws_server()), daemon=True).start()
assert ready.wait(10)
Client.endpoint = lambda self: (f"ws://127.0.0.1:{ws_port[0]}/feishu?service_id=7", 7)
ding.DingTalkStreamClient.open_connection = lambda self: {"endpoint": f"ws://127.0.0.1:{ws_port[0]}/ding", "ticket": "dummy"}
real_http = bridge.http_json
real_get = media.requests.get
tg_polled = False


def simulated_http(method, url, **kwargs):
    global tg_polled
    if url.startswith("http://127.0.0.1"):
        return real_http(method, url, **kwargs)
    if "api.telegram.org" in url:
        action = url.rsplit("/", 1)[1]
        if action == "getMe": result = {"id": 123}
        elif action == "getWebhookInfo": result = {"url": ""}
        elif action == "sendMessage":
            with lock: replies.add("telegram")
            result = {}
        elif action == "getUpdates":
            if tg_polled:
                time.sleep(1)
                result = []
            else:
                tg_polled = True
                result = [{"update_id": 1, "message": {"from": {"id": 42}, "chat": {"id": 1},
                    "message_id": 1, "date": 1791100000, "caption": "TG模拟图文",
                    "photo": [{"file_id": "tg-image", "width": 1, "height": 1, "file_size": len(PNG)}]}}]
        elif action == "getFile":
            assert kwargs["json"] == {"file_id": "tg-image"}
            result = {"file_path": "photos/image.png", "file_size": len(PNG)}
        else: raise AssertionError(action)
        return {"ok": True, "result": result}
    if "oapi.dingtalk.com/robot/sendBySession" in url:
        with lock: replies.add("dingtalk")
        return {"errcode": 0}
    if url == "https://api.dingtalk.com/v1.0/oauth2/accessToken":
        return {"accessToken": "dummy", "expireIn": 7200}
    if url == "https://api.dingtalk.com/v1.0/robot/messageFiles/download":
        assert kwargs["json"] == {"downloadCode": "ding-image", "robotCode": "dummy"}
        assert kwargs["headers"]["x-acs-dingtalk-access-token"] == "dummy"
        return {"downloadUrl": "https://images.example.test/ding"}
    if "open.feishu.cn/open-apis/auth" in url:
        return {"code": 0, "tenant_access_token": "dummy", "expire": 7200}
    if "open.feishu.cn/open-apis/im" in url:
        with lock: replies.add("feishu")
        return {"code": 0}
    raise AssertionError("Unexpected network request")


bridge.http_json = simulated_http


def simulated_get(url, **kwargs):
    if url.startswith("https://api.telegram.org/file/"):
        assert url.endswith("/photos/image.png")
    elif "/messages/om_1/resources/img1?type=image" in url:
        assert kwargs["headers"] == {"Authorization": "Bearer dummy"}
    elif url != "https://images.example.test/ding":
        raise AssertionError("Unexpected image network request")
    return real_get(f"http://127.0.0.1:{server.server_port}/image", **kwargs)


media.requests.get = simulated_get
config = {"fns": {"url": f"http://127.0.0.1:{server.server_port}", "token": "dummy-token", "vault": "test-vault"},
    "capture": {"folder": "Inbox", "timezone": "Asia/Shanghai"},
    "telegram": {"enabled": True, "bot_token": "dummy", "allowed_users": ["42"]},
    "dingtalk": {"enabled": True, "client_id": "dummy", "client_secret": "dummy", "allowed_users": ["42"]},
    "feishu": {"enabled": True, "app_id": "dummy", "app_secret": "dummy", "allowed_users": ["ou_42"]}}
state = Path(tempfile.mkdtemp())


def run():
    try:
        bridge.run_all(config, state)
    except BaseException as exc:
        failures.append(str(exc))


threading.Thread(target=run, daemon=True).start()
deadline = time.monotonic() + 20
while time.monotonic() < deadline:
    if failures:
        raise RuntimeError(failures)
    with lock:
        complete = len(notes) == 3 and len(images) == 3 and len(replies) == 3 and len(callbacks) == 2
    if complete:
        break
    time.sleep(0.1)
else:
    raise RuntimeError(f"Smoke incomplete: notes={len(notes)}, replies={replies}, callbacks={callbacks}")
time.sleep(1)
assert set(notes) == {
    f"Inbox/{source}/2026-10/2026-10-04_15-46-40.md" for source in bridge.SOURCES
}, set(notes)
assert set(images) == {f"Inbox/{source}/2026-10/assets/2026-10-04_15-46-40/2026-10-04_15-46-40-01.png"
                       for source in bridge.SOURCES}, set(images)
for source in bridge.SOURCES:
    store = bridge.Store(state / (source + ".sqlite3"))
    assert store.summary() == {"done": 1}, (source, store.summary())
    store.db.close()
rss = next(line for line in Path("/proc/self/status").read_text().splitlines() if line.startswith("VmRSS:"))
print(json.dumps({"sources": 3, "notes": len(notes), "images": len(images), "replies": len(replies), "callbacks": len(callbacks),
                  "memory_rss": rss, "full_feishu_sdk_loaded": "lark_oapi" in sys.modules}), flush=True)
