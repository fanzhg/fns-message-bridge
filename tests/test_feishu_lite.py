import asyncio
import json
import unittest
from unittest.mock import patch

from feishu_lite import Client, Frame, FeishuError, Fragments, MAX_PAYLOAD


def event_frame(payload, count=1, seq=0):
    frame = Frame(SeqID=123, LogID=456, service=7, method=1)
    for key, value in (("type", "event"), ("message_id", "msg1"), ("sum", str(count)), ("seq", str(seq))):
        frame.headers.add(key=key, value=value)
    frame.payload = payload
    return frame


class ProtocolTests(unittest.TestCase):
    def test_frame_matches_official_sdk_fixture(self):
        # Generated with lark-oapi 1.7.3 Frame.SerializeToString(), not our encoder.
        fixture = bytes.fromhex("087b10c803180720012a0d0a047479706512056576656e742a120a0a6d6573736167655f696412046d7367312a080a0373756d1201312a080a0373657112013042027b7d")
        self.assertEqual(event_frame(b"{}").SerializeToString(), fixture)
        parsed = Frame.FromString(fixture)
        self.assertEqual((parsed.SeqID, parsed.LogID, parsed.service, parsed.method, parsed.payload), (123, 456, 7, 1, b"{}"))

    def test_fragment_reordering_duplicates_and_cleanup(self):
        fragments = Fragments()
        self.assertIsNone(fragments.combine("m", 3, 2, b"c"))
        self.assertIsNone(fragments.combine("m", 3, 2, b"c"))
        self.assertIsNone(fragments.combine("m", 3, 0, b"a"))
        self.assertEqual(fragments.combine("m", 3, 1, b"b"), b"abc")
        self.assertFalse(fragments.pending)

    def test_fragment_limits_and_expiration(self):
        fragments = Fragments()
        with self.assertRaises(FeishuError):
            fragments.combine("m", 65, 0, b"a")
        with self.assertRaises(FeishuError):
            fragments.combine("m", 2, 2, b"a")
        fragments.combine("m", 2, 0, b"a")
        with patch("feishu_lite.time.monotonic", return_value=999999999):
            self.assertEqual(fragments.combine("n", 1, 0, b"a"), b"a")
            self.assertFalse(fragments.pending)
        with self.assertRaises(FeishuError):
            fragments.combine("x", 1, 0, b"a" * (MAX_PAYLOAD + 1))

    def test_endpoint_server_config(self):
        calls = []
        def http(method, url, **kwargs):
            calls.append(kwargs)
            return {"code": 0, "data": {"URL": "wss://example.test/ws?service_id=9&device_id=1",
                    "ClientConfig": {"PingInterval": 42}}}
        client = Client({"app_id": "cli_1", "app_secret": "secret"}, http, lambda e: None)
        url, service = client.endpoint()
        self.assertEqual(service, 9)
        self.assertEqual(client.ping_interval, 42)
        self.assertEqual(calls[0]["json"], {"AppID": "cli_1", "AppSecret": "secret"})

    def test_reply_token_cache_and_refresh(self):
        calls = []
        reply_count = 0
        def http(method, url, **kwargs):
            nonlocal reply_count
            calls.append(url)
            if url.endswith("/internal"):
                return {"code": 0, "tenant_access_token": "t", "expire": 7200}
            reply_count += 1
            return {"code": 99991663 if reply_count == 1 else 0}
        client = Client({"app_id": "a", "app_secret": "s"}, http, lambda e: None)
        client.reply({"message_id": "om_1"}, "中文")
        client.reply({"message_id": "om_2"}, "中文")
        self.assertEqual(sum(url.endswith("/internal") for url in calls), 2)
        self.assertEqual(reply_count, 3)


class FakeSocket:
    def __init__(self):
        self.frames = []
    async def send(self, frame):
        self.frames.append(Frame.FromString(frame))


class AckTests(unittest.IsolatedAsyncioTestCase):
    async def test_ack_after_callback_commit_and_failure_ack(self):
        committed = []
        socket = FakeSocket()
        def receive(event):
            committed.append(event)
        client = Client({}, None, receive)
        envelope = json.dumps({"header": {"event_type": "im.message.receive_v1"}, "event": {"test": 1}}).encode()
        await client.handle(event_frame(envelope).SerializeToString(), socket, Fragments())
        self.assertEqual(committed, [{"test": 1}])
        self.assertEqual(json.loads(socket.frames[0].payload), {"code": 200})
        def fail(event):
            raise RuntimeError("disk full")
        client.on_event = fail
        await client.handle(event_frame(envelope).SerializeToString(), socket, Fragments())
        self.assertEqual(json.loads(socket.frames[1].payload), {"code": 500})

    async def test_fragment_ack_only_on_completed_payload(self):
        socket = FakeSocket()
        received = []
        client = Client({}, None, received.append)
        fragments = Fragments()
        envelope = b'{"header":{"event_type":"im.message.receive_v1"},"event":{}}'
        half = len(envelope) // 2
        await client.handle(event_frame(envelope[:half], 2, 0).SerializeToString(), socket, fragments)
        self.assertFalse(socket.frames)
        await client.handle(event_frame(envelope[half:], 2, 1).SerializeToString(), socket, fragments)
        self.assertEqual(len(socket.frames), 1)
        self.assertEqual(received, [{}])

    async def test_nonmessage_event_ignored_and_pong_updates_config(self):
        socket = FakeSocket()
        client = Client({}, None, lambda event: self.fail("unexpected event"))
        await client.handle(event_frame(b'{"header":{"event_type":"other"},"event":{}}').SerializeToString(), socket, Fragments())
        self.assertEqual(json.loads(socket.frames[0].payload)["code"], 200)
        pong = Frame(SeqID=0, LogID=0, service=7, method=0, payload=b'{"PingInterval":60}')
        pong.headers.add(key="type", value="pong")
        await client.handle(pong.SerializeToString(), socket, Fragments())
        self.assertEqual(client.ping_interval, 60)
