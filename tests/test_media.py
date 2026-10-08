import base64
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import bridge
import media
from test_bridge import config

PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.destination = Path(self.tmp.name) / "image.bin"

    def tearDown(self):
        self.tmp.cleanup()

    def response(self, content=PNG, headers=None, status=200):
        response = Mock(status_code=status, headers=headers or {})
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.iter_content.return_value = [content[:16], content[16:]]
        return response

    def test_stream_download_and_detect_real_extension(self):
        response = self.response(headers={"Content-Length": str(len(PNG))})
        with patch("media.requests.get", return_value=response) as get:
            media.download_image("https://images.example.test/signed?token=secret", self.destination, 1024)
        self.assertEqual(self.destination.read_bytes(), PNG)
        self.assertEqual(media.inspect_image(self.destination, 1024), (".png", "image/png"))
        self.assertTrue(get.call_args.kwargs["stream"])
        self.assertFalse(get.call_args.kwargs["allow_redirects"])
        self.assertFalse(self.destination.with_suffix(".part").exists())

    def test_oversize_unknown_length_and_partial_cleanup(self):
        with patch("media.requests.get", return_value=self.response()) as get:
            with self.assertRaises(media.MediaRejected):
                media.download_image("https://images.example.test/a", self.destination, 20)
        self.assertFalse(self.destination.exists())
        self.assertFalse(self.destination.with_suffix(".part").exists())

    def test_declared_size_and_truncated_response(self):
        for length, exception in ((2000, media.MediaRejected), (100, media.DownloadError)):
            with patch("media.requests.get", return_value=self.response(headers={"Content-Length": str(length)})):
                with self.assertRaises(exception):
                    media.download_image("https://images.example.test/a", self.destination, 1024)
            self.assertFalse(self.destination.exists())

    def test_nonimage_is_not_saved(self):
        with patch("media.requests.get", return_value=self.response(b"<html>error</html>")):
            with self.assertRaises(media.MediaRejected):
                media.download_image("https://images.example.test/a", self.destination, 1024)
        self.assertFalse(self.destination.exists())

    def test_resource_json_error_is_retryable_and_does_not_expose_message(self):
        response = self.response(b'{"code":99991672,"msg":"secret credential"}',
                                 headers={"Content-Type": "application/json"})
        response.iter_content.return_value = iter([b'{"code":99991672,"msg":"secret credential"}'])
        with patch("media.requests.get", return_value=response):
            with self.assertRaises(media.DownloadError) as caught:
                media.download_image("https://images.example.test/a", self.destination, 1024)
        self.assertIn("99991672", str(caught.exception))
        self.assertNotIn("secret", str(caught.exception))

    def test_authenticated_redirect_does_not_leak_token(self):
        response = self.response(status=302, headers={"Location": "https://other.example.test/image"})
        with patch("media.requests.get", return_value=response) as get:
            with self.assertRaises(media.DownloadError):
                media.download_image("https://open.feishu.cn/image", self.destination, 1024,
                                     {"Authorization": "Bearer secret"})
        self.assertEqual(get.call_count, 1)

    def test_http_private_ip_and_url_credentials_rejected(self):
        for url in ("http://images.example.test/a", "https://127.0.0.1/a", "https://user:secret@host.test/a"):
            with self.assertRaises(media.DownloadError):
                media.validate_url(url)

    def test_provider_lookup_and_token_header(self):
        cases = [("telegram", {"bot_token": "secret"}, {"file_id": "file1"},
                  {"ok": True, "result": {"file_path": "photos/image.jpg"}}, None),
                 ("dingtalk", {"client_id": "id", "client_secret": "secret"},
                  {"download_code": "code", "robot_code": "robot"},
                  {"downloadUrl": "https://images.example.test/signed"}, None),
                 ("feishu", {}, {"image_key": "img1", "message_id": "om1"}, {},
                  Mock(tenant_token=Mock(return_value="tenant")))]
        for source, options, item, result, client in cases:
            http = Mock(side_effect=[{"accessToken": "ding", "expireIn": 7200}, result] if source == "dingtalk" else [result])
            downloader = media.ImageDownloader(source, options, http, client)
            with patch("media.download_image") as download:
                downloader(item, self.destination, 1024)
            if source == "feishu":
                self.assertIn("/messages/om1/resources/img1?type=image", download.call_args.args[0])
                self.assertEqual(download.call_args.args[3], {"Authorization": "Bearer tenant"})
            elif source == "dingtalk":
                self.assertEqual(http.call_args.kwargs["json"], {"robotCode": "robot", "downloadCode": "code"})
            else:
                self.assertIn("/file/botsecret/photos/image.jpg", download.call_args.args[0])


class ImageQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = bridge.Store(Path(self.tmp.name) / "queue.sqlite3")
        self.replies, self.downloads, self.uploads, self.notes = [], [], [], []
        self.bridge = bridge.Bridge(config(), "telegram", self.store,
                                   lambda _, text: self.replies.append(text), self.download)
        self.bridge.fns.upload_image = lambda *args: self.uploads.append(args[0])
        self.bridge.fns.create = lambda job: self.notes.append(job["content"])

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def download(self, item, destination, limit):
        self.downloads.append(item["file_id"])
        destination.write_bytes(PNG)

    def enqueue(self, text="配文", items=None, message_id="photo1"):
        self.bridge.receive(message_id, "42", text, 0, {}, items or [{"file_id": "one"}])

    def test_image_embedded_after_upload_then_success_and_cache_removed(self):
        self.enqueue()
        self.assertEqual(self.replies, [])
        self.bridge.deliver_one()
        self.assertEqual(self.uploads, ["Inbox/telegram/1970-01/assets/1970-01-01_08-00-00/1970-01-01_08-00-00-01.png"])
        self.assertIn("配文", self.notes[0])
        self.assertIn("![](assets/1970-01-01_08-00-00/1970-01-01_08-00-00-01.png)", self.notes[0])
        self.assertEqual(self.store.summary(), {"done": 1})
        self.assertEqual(list(self.bridge.cache.glob("*")), [])
        self.assertEqual(self.store.db.execute("SELECT attachments FROM jobs").fetchone()[0], "[]")

    def test_note_failure_retry_does_not_download_upload_or_embed_twice(self):
        self.enqueue(text="")
        self.bridge.fns.create = Mock(side_effect=bridge.ApiError(310))
        self.bridge.deliver_one()
        self.assertEqual(self.replies, [])
        self.store.db.execute("UPDATE jobs SET next_at=0")
        self.store.db.commit()
        self.bridge.fns.create = lambda job: self.notes.append(job["content"])
        self.bridge.deliver_one()
        self.assertEqual(self.downloads, ["one"])
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(self.notes[0].count("![]("), 1)

    def test_upload_failure_keeps_disk_copy_and_restart_resumes(self):
        self.enqueue()
        self.bridge.fns.upload_image = Mock(side_effect=bridge.ApiError(315))
        self.bridge.deliver_one()
        self.assertEqual(self.replies, [])
        self.assertEqual(len(list(self.bridge.cache.glob("*.bin"))), 1)
        self.store.db.close()
        self.store = bridge.Store(Path(self.tmp.name) / "queue.sqlite3")
        self.store.db.execute("UPDATE jobs SET next_at=0")
        self.store.db.commit()
        self.bridge = bridge.Bridge(config(), "telegram", self.store,
                                   lambda _, text: self.replies.append(text), self.download)
        self.bridge.fns.upload_image = lambda *args: self.uploads.append(args[0])
        self.bridge.fns.create = lambda job: self.notes.append(job["content"])
        self.bridge.deliver_one()
        self.assertEqual(self.downloads, ["one"])
        self.assertEqual(self.store.summary(), {"done": 1})

    def test_rejection_reply_and_queue_continue(self):
        self.enqueue(items=[{"file_id": "one", "size": 6 * 1024 * 1024}])
        self.bridge.deliver_one()
        self.assertEqual(self.store.summary(), {"rejected": 1})
        self.bridge.deliver_one()
        self.assertIn("图片未保存", self.replies[0])
        self.assertEqual(self.downloads, [])
        self.assertEqual(self.notes, [])
        self.enqueue(message_id="photo2")
        self.bridge.deliver_one()
        self.assertEqual(self.store.summary(), {"rejected_done": 1, "done": 1})

    def test_rejection_reason_survives_reply_failure(self):
        self.enqueue(items=[{"file_id": "one", "size": 6 * 1024 * 1024}])
        self.bridge.deliver_one()
        self.bridge.sender = Mock(side_effect=bridge.BridgeError("reply error"))
        self.bridge.deliver_one()
        self.assertEqual(self.store.next_job(), None)
        row = self.store.db.execute("SELECT last_error FROM jobs").fetchone()
        self.assertEqual(row[0], "图片超过大小限制")

    def test_unauthorized_image_and_identity_caption_are_not_identity_commands(self):
        self.bridge.receive("no", "99", "身份", 0, {}, [{"file_id": "one"}])
        self.assertEqual(self.replies, [])
        self.assertIsNone(self.store.next_job())
        self.enqueue(text="身份")
        self.assertIsNotNone(self.store.next_job())

    def test_multiple_images_and_same_second_paths_are_distinct(self):
        self.enqueue(items=[{"file_id": "one"}, {"file_id": "two"}])
        self.enqueue(message_id="photo2")
        self.bridge.deliver_one()
        self.bridge.deliver_one()
        self.assertEqual(len(set(self.uploads)), 3)
        self.assertEqual(self.notes[0].count("![]("), 2)
        self.assertIn("08-00-00-2/", self.uploads[2])

    def test_old_queue_schema_migrates_without_losing_jobs(self):
        path = Path(self.tmp.name) / "old.sqlite3"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE jobs (key TEXT PRIMARY KEY,path TEXT NOT NULL,content TEXT NOT NULL,reply TEXT NOT NULL,state TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,next_at REAL DEFAULT 0,last_error TEXT DEFAULT '')")
            db.execute("INSERT INTO jobs(key,path,content,reply) VALUES('old','old.md','旧内容','{}')")
        db.close()
        old = bridge.Store(path)
        try:
            job = old.next_job()
            self.assertEqual((job["content"], job["path"], job["attachments"]), ("旧内容", "old.md", "[]"))
        finally:
            old.db.close()


class ImageParserTests(unittest.TestCase):
    def test_telegram_photo_chooses_largest_and_keeps_caption(self):
        message = {"from": {"id": 42}, "chat": {"id": 1}, "message_id": 2, "date": 0,
                   "caption": "配文", "photo": [{"file_id": "small", "width": 10, "height": 10},
                   {"file_id": "large", "width": 100, "height": 100, "file_size": 50}]}
        parsed = bridge.telegram_message({"message": message})
        self.assertEqual(parsed[2], "配文")
        self.assertEqual(parsed[5], [{"file_id": "large", "size": 50}])
        del message["photo"]
        message["document"] = {"mime_type": "image/png", "file_id": "original"}
        self.assertEqual(bridge.telegram_message({"message": message})[5][0]["file_id"], "original")

    def test_ding_richtext_downloadcode_and_robotcode(self):
        data = {"msgtype": "richText", "msgId": "1", "senderStaffId": "42", "createAt": 0,
                "robotCode": "robot", "sessionWebhook": "https://oapi.dingtalk.com/a", "sessionWebhookExpiredTime": 1,
                "content": json.dumps({"richText": [{"text": "配文"},
                    {"type": "picture", "pictureDownloadCode": "legacy"}]})}
        result = bridge.dingtalk_message(data)
        self.assertEqual(result[2], "配文")
        self.assertEqual(result[5], [{"download_code": "legacy", "robot_code": "robot"}])

    def test_feishu_image_and_localized_post(self):
        event = {"sender": {"sender_type": "user", "sender_id": {"open_id": "ou_42"}},
                 "message": {"message_type": "image", "content": '{"image_key":"img1"}',
                             "message_id": "om1", "create_time": "0"}}
        self.assertEqual(bridge.feishu_message(event)[5], [{"image_key": "img1", "message_id": "om1"}])
        event["message"]["message_type"] = "post"
        event["message"]["content"] = json.dumps({"zh_cn": {"title": "标题", "content": [[
            {"tag": "text", "text": "配文"}, {"tag": "img", "image_key": "img1"}]]}})
        result = bridge.feishu_message(event)
        self.assertEqual(result[2], "标题\n配文")
        self.assertEqual(result[5][0]["image_key"], "img1")


class FNSImageTests(unittest.TestCase):
    def test_streaming_multipart_and_existing_file_protection(self):
        with tempfile.TemporaryDirectory() as directory:
            local = Path(directory) / "photo.png"
            local.write_bytes(PNG)
            fns = bridge.FNS(config()["fns"])
            fns.call = Mock(side_effect=[{"list": []}, {}])
            fns.upload_image("assets/photo.png", local, "image/png", 1024)
            body = fns.call.call_args.kwargs["data"]
            self.assertIsInstance(body, bridge.MultipartEncoder)
            self.assertIn("multipart/form-data", fns.call.call_args.kwargs["headers"]["Content-Type"])
            fns.call = Mock(return_value={"list": [{"path": "assets/photo.png"}]})
            with patch("bridge.remote_digest", return_value=media.file_digest(local)):
                fns.upload_image("assets/photo.png", local, "image/png", 1024)
            self.assertEqual(fns.call.call_count, 1)
            with patch("bridge.remote_digest", return_value="foreign"):
                with self.assertRaises(media.MediaRejected):
                    fns.upload_image("assets/photo.png", local, "image/png", 1024)


if __name__ == "__main__":
    unittest.main()
