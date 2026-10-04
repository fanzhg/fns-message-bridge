import asyncio
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import bridge


def config():
    return {"fns": {"url": "https://example.test", "token": "test-token", "vault": "test-vault"},
            "capture": {"folder": "Inbox", "timezone": "Asia/Shanghai"},
            "telegram": {"allowed_users": ["42"]},
            "dingtalk": {"allowed_users": ["42"]}, "feishu": {"allowed_users": ["ou_42"]}}


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = bridge.Store(Path(self.tmp.name) / "queue.sqlite3")
        self.replies = []
        self.bridge = bridge.Bridge(config(), "telegram", self.store,
                                    lambda reply, text: self.replies.append(text))

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def enqueue(self):
        self.bridge.receive("chat:1", "42", "中文\n第二行", 1791100000, {"chat_id": 1})

    def test_unauthorized_and_empty_allowlist_do_not_store(self):
        self.bridge.receive("1", "99", "secret", 1791100000, {})
        self.bridge.allowed.clear()
        self.enqueue()
        self.assertIsNone(self.store.next_job())

    def test_identity_does_not_write_notes(self):
        self.bridge.receive("1", "99", "/whoami", 1791100000, {})
        self.assertEqual(self.replies, ["你的用户 ID：99"])
        self.assertIsNone(self.store.next_job())

    def test_duplicate_message_and_restart(self):
        self.enqueue()
        first = self.store.next_job()
        self.enqueue()
        self.assertEqual(self.store.summary(), {"pending": 1})
        self.store.db.close()
        self.store = bridge.Store(Path(self.tmp.name) / "queue.sqlite3")
        self.assertEqual(self.store.next_job()["content"], first["content"])

    def test_retry_path_independent_of_text_and_source_separated(self):
        key, path, content = bridge.make_note(config(), "telegram", "../../bad", "42", "中文", 1791100000)
        key2, path2, _ = bridge.make_note(config(), "telegram", "../../bad", "42", "edited", 1791100000)
        self.assertEqual((key, path), (key2, path2))
        self.assertNotIn("..", path)
        self.assertIn("+08:00", content)
        self.assertIn("中文", content)
        self.assertNotEqual(key, bridge.make_note(config(), "feishu", "../../bad", "42", "中文", 1791100000)[0])

    def test_date_filename_uses_message_time_and_configured_timezone(self):
        for source in bridge.SOURCES:
            key, path, _ = bridge.make_note(config(), source, "message-id", "42", "文本", 0)
            self.assertEqual(path, f"Inbox/{source}/1970-01/1970-01-01_08-00-00.md")
            self.assertNotIn(key, path)
            self.assertNotIn("message-id", path)
        cfg = config()
        cfg["capture"]["timezone"] = "UTC"
        self.assertEqual(bridge.make_note(cfg, "feishu", "id", "42", "文本", 0)[1],
                         "Inbox/feishu/1970-01/1970-01-01_00-00-00.md")

    def test_same_second_filenames_survive_restart_and_dedupe(self):
        self.bridge.receive("first", "42", "第一条", 0, {})
        first = self.store.next_job()
        self.store.state(first["key"], "done")
        self.store.db.close()
        self.store = bridge.Store(Path(self.tmp.name) / "queue.sqlite3")
        self.bridge = bridge.Bridge(config(), "telegram", self.store, lambda r, t: None)
        self.bridge.receive("second", "42", "第二条", 0, {})
        self.bridge.receive("second", "42", "重复投递", 0, {})
        self.bridge.receive("third", "42", "第三条", 0, {})
        rows = self.store.db.execute("SELECT path,content FROM jobs ORDER BY rowid").fetchall()
        self.assertEqual([row["path"] for row in rows], [
            "Inbox/telegram/1970-01/1970-01-01_08-00-00.md",
            "Inbox/telegram/1970-01/1970-01-01_08-00-00-2.md",
            "Inbox/telegram/1970-01/1970-01-01_08-00-00-3.md"])
        self.assertIn("第二条", rows[1]["content"])
        self.assertEqual(self.store.summary(), {"done": 1, "pending": 2})

    def test_concurrent_same_second_messages_get_distinct_names(self):
        failures = []
        def receive(index):
            try:
                self.bridge.receive(str(index), "42", str(index), 0, {})
            except Exception as exc:
                failures.append(exc)
        threads = [threading.Thread(target=receive, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(failures, [])
        paths = {row[0] for row in self.store.db.execute("SELECT path FROM jobs")}
        self.assertEqual(len(paths), 8)
        self.assertEqual(self.store.summary(), {"pending": 8})

    def test_existing_queued_hash_path_is_preserved(self):
        key, _, content = bridge.make_note(config(), "telegram", "legacy", "42", "旧消息", 0)
        legacy_path = f"Inbox/telegram/1970-01/{key}.md"
        self.store.enqueue(key, legacy_path, content, {})
        self.bridge.receive("legacy", "42", "再次投递", 0, {})
        self.assertEqual(self.store.next_job()["path"], legacy_path)
        self.assertEqual(self.store.summary(), {"pending": 1})

    def test_no_success_reply_before_save_and_retry_pending(self):
        self.enqueue()
        self.bridge.fns.create = lambda job: (_ for _ in ()).throw(bridge.ApiError(310))
        self.bridge.deliver_one()
        self.assertEqual(self.replies, [])
        row = self.store.db.execute("SELECT state,attempts,next_at FROM jobs").fetchone()
        self.assertEqual(tuple(row)[:2], ("pending", 1))
        self.assertGreater(row[2], 0)

    def test_save_and_ack_retry_do_not_rewrite_note(self):
        self.enqueue()
        calls = []
        self.bridge.fns.create = lambda job: calls.append(job["path"])
        def fail(reply, text):
            raise bridge.BridgeError("reply unavailable")
        self.bridge.sender = fail
        self.bridge.deliver_one()
        self.assertEqual(self.store.summary(), {"saved": 1})
        self.store.db.execute("UPDATE jobs SET next_at=0")
        self.store.db.commit()
        self.bridge.sender = lambda reply, text: self.replies.append(text)
        self.bridge.deliver_one()
        self.assertEqual(len(calls), 1)
        self.assertIn("已保存", self.replies[0])
        self.assertEqual(self.store.summary(), {"done": 1})
        row = self.store.db.execute("SELECT content,reply FROM jobs").fetchone()
        self.assertEqual(tuple(row), ("", "{}"))
        self.enqueue()
        self.assertEqual(self.store.summary(), {"done": 1})

    def test_saved_job_survives_restart(self):
        self.enqueue()
        job = self.store.next_job()
        self.store.state(job["key"], "saved")
        self.store.db.close()
        self.store = bridge.Store(Path(self.tmp.name) / "queue.sqlite3")
        new_bridge = bridge.Bridge(config(), "telegram", self.store, lambda r,t: self.replies.append(t))
        new_bridge.fns.create = lambda j: self.fail("saved job must not be posted again")
        new_bridge.deliver_one()
        self.assertEqual(self.store.summary(), {"done": 1})

    def test_duplicate_post_preserves_edited_note(self):
        self.enqueue()
        job = self.store.next_job()
        fns = bridge.FNS(config()["fns"])
        calls = []
        def call(method, route, **kwargs):
            calls.append(method)
            if method == "POST":
                self.assertTrue(kwargs["json"]["createOnly"])
                self.assertNotIn("contentHash", kwargs["json"])
                raise bridge.ApiError(431)
            return {"content": f'---\nbridge_id: "{job["key"]}"\n---\nEdited by user'}
        fns.call = call
        fns.create(job)
        self.assertEqual(calls, ["POST", "GET"])

    def test_foreign_note_not_overwritten_or_reported_saved(self):
        self.enqueue()
        fns = bridge.FNS(config()["fns"])
        def call(method, route, **kwargs):
            if method == "POST":
                raise bridge.ApiError(431)
            return {"content": "foreign content"}
        fns.call = call
        with self.assertRaises(bridge.BridgeError):
            fns.create(self.store.next_job())

    def test_tg_text_and_nontext_bot_filter(self):
        update = {"message": {"from": {"id": 42}, "chat": {"id": 5}, "message_id": 9,
                              "date": 1791100000, "text": "原文"}}
        self.assertEqual(bridge.telegram_message(update)[:3], ("5:9", "42", "原文"))
        update["message"]["from"]["is_bot"] = True
        self.assertIsNone(bridge.telegram_message(update))
        self.assertIsNone(bridge.telegram_message({"edited_message": update["message"]}))

    def test_dingtalk_text_and_sender_fallback(self):
        data = {"msgtype": "text", "msgId": "msg1", "senderStaffId": "42", "senderId": "fallback",
                "text": {"content": "速记"}, "createAt": 1791100000000,
                "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?token=secret",
                "sessionWebhookExpiredTime": 1791107200000}
        self.assertEqual(bridge.dingtalk_message(data)[:4], ("msg1", "42", "速记", 1791100000))
        del data["senderStaffId"]
        self.assertEqual(bridge.dingtalk_message(data)[1], "fallback")
        data["msgtype"] = "image"
        self.assertIsNone(bridge.dingtalk_message(data))

    def test_feishu_text_and_bot_filter(self):
        event = {"sender": {"sender_type": "user", "sender_id": {"open_id": "ou_42"}},
                 "message": {"message_type": "text", "content": '{"text":"速记"}',
                             "message_id": "om_1", "create_time": "1791100000000"}}
        result = bridge.feishu_message(event)
        self.assertEqual(result[:4], ("om_1", "ou_42", "速记", 1791100000))
        event["sender"]["sender_type"] = "app"
        self.assertIsNone(bridge.feishu_message(event))

    def test_sdk_clients_register_without_network(self):
        cfg = config()
        cfg["dingtalk"].update(client_id="dummy", client_secret="dummy")
        cfg["feishu"].update(app_id="dummy", app_secret="dummy")
        with patch("bridge.start_worker"), patch("dingtalk_stream.DingTalkStreamClient.start_forever") as ding_start:
            bridge.run_dingtalk(cfg, self.store)
            ding_start.assert_called_once()
        with patch("bridge.start_worker"), patch("feishu_lite.Client.run") as feishu_start:
            bridge.run_feishu(cfg, self.store)
            feishu_start.assert_awaited_once()

    def test_tg_offset_advances_after_queue_commit(self):
        cfg = config()
        cfg["telegram"]["bot_token"] = "dummy"
        offset_seen = []
        def http(method, url, **kwargs):
            action = url.rsplit("/", 1)[1]
            if action == "getMe": return {"ok": True, "result": {"id": 123}}
            if action == "getWebhookInfo": return {"ok": True, "result": {"url": ""}}
            if action == "getUpdates":
                offset_seen.append(kwargs["json"]["offset"])
                if len(offset_seen) == 2:
                    self.assertEqual(self.store.summary(), {"pending": 1})
                    raise KeyboardInterrupt()
                return {"ok": True, "result": [{"update_id": 10, "message": {
                    "from": {"id": 42}, "chat": {"id": 1}, "message_id": 1,
                    "date": 1791100000, "text": "速记"}}]}
        with patch("bridge.http_json", side_effect=http), patch("bridge.start_worker"):
            with self.assertRaises(KeyboardInterrupt):
                bridge.run_telegram(cfg, self.store)
        self.assertEqual(offset_seen, [0, 11])
        self.assertEqual(self.store.get_meta("telegram_offset_123"), "11")

    def test_tg_existing_webhook_refused(self):
        cfg = config()
        cfg["telegram"]["bot_token"] = "dummy"
        with patch("bridge.http_json", side_effect=[{"ok": True, "result": {"id": 1}},
                                                     {"ok": True, "result": {"url": "https://other"}}]):
            with self.assertRaises(bridge.BridgeError):
                bridge.run_telegram(cfg, self.store)

    def test_no_ack_on_queue_failure(self):
        self.store.enqueue = lambda *a: (_ for _ in ()).throw(sqlite_error())
        with self.assertRaises(Exception):
            self.enqueue()
        self.assertEqual(self.replies, [])


def sqlite_error():
    import sqlite3
    return sqlite3.OperationalError("disk full")


class HTTPContractTests(unittest.TestCase):
    def test_fns_contract_real_http(self):
        received = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                received.append((self.path, dict(self.headers), body))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"code":1,"status":true,"data":{}}')
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            cfg = config()["fns"]
            cfg["url"] = f"http://127.0.0.1:{server.server_port}/api/"
            bridge.FNS(cfg).create({"path": "Inbox/test.md", "content": "文本", "key": "key"})
            path, headers, body = received[0]
            self.assertEqual(path, "/api/note")
            self.assertEqual(headers["Token"], "test-token")
            self.assertEqual(headers["X-Client"], "fns-message-bridge")
            self.assertEqual(body, {"vault": "test-vault", "path": "Inbox/test.md", "content": "文本", "createOnly": True})
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_business_failure_even_http_200(self):
        with patch("bridge.http_json", return_value={"code": 310, "status": False}):
            with self.assertRaises(bridge.ApiError) as context:
                bridge.FNS(config()["fns"]).check()
            self.assertEqual(context.exception.code, 310)

    def test_network_error_does_not_expose_token(self):
        import requests
        with patch("bridge.requests.request", side_effect=requests.ConnectionError("url token SECRET")):
            with self.assertRaises(bridge.BridgeError) as context:
                bridge.http_json("GET", "https://example.test/SECRET")
            self.assertNotIn("SECRET", str(context.exception))


if __name__ == "__main__":
    unittest.main()
