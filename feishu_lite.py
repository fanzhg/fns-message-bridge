"""Feishu text-only WebSocket transport, matching the official SDK wire format.

Protocol reference: larksuite/oapi-sdk-python/lark_oapi/ws/{client.py,pb/pbbp2_pb2.py}.
No full SDK or business API model tree is imported.
"""
import asyncio
import contextlib
import json
import logging
import random
import threading
import time
from urllib.parse import parse_qs, quote, urlparse

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from websockets.asyncio.client import connect

LOG = logging.getLogger("bridge.feishu")


def frame_class():
    spec = descriptor_pb2.FileDescriptorProto(name="feishu_bridge.proto", package="fnsfeishu", syntax="proto2")
    header = spec.message_type.add(name="Header")
    for number, name in ((1, "key"), (2, "value")):
        header.field.add(name=name, number=number, label=2, type=9)
    frame = spec.message_type.add(name="Frame")
    for number, name, kind, label in ((1, "SeqID", 4, 2), (2, "LogID", 4, 2),
            (3, "service", 5, 2), (4, "method", 5, 2), (5, "headers", 11, 3),
            (6, "payload_encoding", 9, 1), (7, "payload_type", 9, 1),
            (8, "payload", 12, 1), (9, "LogIDNew", 9, 1)):
        field = frame.field.add(name=name, number=number, label=label, type=kind)
        if kind == 11:
            field.type_name = ".fnsfeishu.Header"
    pool = descriptor_pool.DescriptorPool()
    pool.Add(spec)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("fnsfeishu.Frame"))


Frame = frame_class()
MAX_PAYLOAD = 1024 * 1024


class FeishuError(Exception):
    pass


class Fragments:
    def __init__(self):
        self.pending = {}

    def combine(self, message_id, count, seq, payload):
        now = time.monotonic()
        self.pending = {k: v for k, v in self.pending.items() if now - v[0] < 5}
        if not 1 <= count <= 64 or not 0 <= seq < count or len(payload) > MAX_PAYLOAD:
            raise FeishuError("Invalid frame fragments")
        if count == 1:
            return payload
        if message_id not in self.pending:
            if len(self.pending) >= 16:
                raise FeishuError("Too many incomplete messages")
            self.pending[message_id] = (now, count, {})
        _, expected, chunks = self.pending[message_id]
        if expected != count:
            raise FeishuError("Fragment count changed")
        chunks[seq] = payload
        if sum(len(v) for _, _, chunk_map in self.pending.values() for v in chunk_map.values()) > 2 * MAX_PAYLOAD:
            self.pending.clear()
            raise FeishuError("Fragment memory limit exceeded")
        if sum(map(len, chunks.values())) > MAX_PAYLOAD:
            del self.pending[message_id]
            raise FeishuError("Message too large")
        if len(chunks) != count:
            return None
        del self.pending[message_id]
        return b"".join(chunks[i] for i in range(count))


class Client:
    def __init__(self, options, http, on_event):
        self.options, self.http, self.on_event = options, http, on_event
        self.ping_interval = 120
        self.token, self.token_until = "", 0
        self.token_lock = threading.Lock()

    def configure(self, values):
        self.ping_interval = max(1, min(600, int(values.get("PingInterval", self.ping_interval))))

    def endpoint(self):
        result = self.http("POST", "https://open.feishu.cn/callback/ws/endpoint",
                           json={"AppID": self.options["app_id"], "AppSecret": self.options["app_secret"]},
                           headers={"locale": "zh", "User-Agent": "fns-message-bridge/1.1"})
        if result.get("code") != 0:
            raise FeishuError(f'Endpoint error {result.get("code")}')
        data = result["data"]
        self.configure(data.get("ClientConfig") or {})
        parsed = urlparse(data["URL"])
        if parsed.scheme != "wss" or not parsed.hostname:
            raise FeishuError("Invalid WebSocket endpoint")
        service = int(parse_qs(parsed.query)["service_id"][0])
        return data["URL"], service

    async def ping(self, ws, service):
        while True:
            frame = Frame(SeqID=0, LogID=0, service=service, method=0)
            frame.headers.add(key="type", value="ping")
            await ws.send(frame.SerializeToString())
            await asyncio.sleep(self.ping_interval)

    async def handle(self, raw, ws, fragments):
        frame = Frame.FromString(raw)
        if not frame.IsInitialized():
            raise FeishuError("Missing required frame fields")
        headers = {header.key: header.value for header in frame.headers}
        if frame.method == 0:
            if headers.get("type") == "pong" and frame.payload:
                self.configure(json.loads(frame.payload))
            return
        if frame.method != 1 or headers.get("type") != "event":
            return
        start = time.monotonic()
        code = 200
        try:
            payload = fragments.combine(headers["message_id"], int(headers["sum"]), int(headers["seq"]), frame.payload)
            if payload is None:
                return
            envelope = json.loads(payload)
            if envelope.get("header", {}).get("event_type") == "im.message.receive_v1":
                await asyncio.to_thread(self.on_event, envelope["event"])
        except Exception as exc:
            # The platform may retry a failed callback; no success ACK before queue commit.
            LOG.warning("Feishu callback failed: %s", type(exc).__name__)
            code = 500
        frame.headers.add(key="biz_rt", value=str(round((time.monotonic() - start) * 1000)))
        frame.payload = json.dumps({"code": code}).encode()
        await ws.send(frame.SerializeToString())

    async def session(self, url, service):
        fragments = Fragments()
        async with connect(url, max_size=MAX_PAYLOAD, max_queue=4,
                           ping_interval=30, ping_timeout=30) as ws:
            LOG.info("Feishu connected")
            ping = asyncio.create_task(self.ping(ws, service))
            try:
                async for raw in ws:
                    await self.handle(raw, ws, fragments)
            finally:
                ping.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping

    async def run(self):
        while True:
            try:
                url, service = await asyncio.to_thread(self.endpoint)
                await self.session(url, service)
            except Exception as exc:
                LOG.warning("Feishu connection failed: %s", type(exc).__name__)
            await asyncio.sleep(10 + random.random() * 5)

    def tenant_token(self):
        with self.token_lock:
            if time.monotonic() < self.token_until:
                return self.token
            result = self.http("POST", "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                               json={"app_id": self.options["app_id"], "app_secret": self.options["app_secret"]})
            if result.get("code") != 0:
                raise FeishuError(f'Token error {result.get("code")}')
            self.token = result["tenant_access_token"]
            self.token_until = time.monotonic() + max(0, int(result["expire"]) - 60)
            return self.token

    def reply(self, reply, text):
        for attempt in range(2):
            token = self.tenant_token()
            result = self.http("POST", "https://open.feishu.cn/open-apis/im/v1/messages/" +
                               quote(reply["message_id"], safe="") + "/reply",
                               headers={"Authorization": "Bearer " + token},
                               json={"msg_type": "text", "content": json.dumps({"text": text}, ensure_ascii=False)})
            if result.get("code") == 0:
                return
            if result.get("code") not in (99991663, 99991664) or attempt:
                raise FeishuError(f'Reply error {result.get("code")}')
            with self.token_lock:
                self.token_until = 0
