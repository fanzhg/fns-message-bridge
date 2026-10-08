"""Bounded image downloads and platform resource lookup; no image decoding."""
import hashlib
import ipaddress
import json
import os
import time
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urljoin, urlparse

import requests


class MediaRejected(Exception):
    """Permanent, user-readable rejection; messages must contain no credentials."""


class DownloadError(Exception):
    pass


def validate_url(url, allow_http=False, allow_private=False):
    parsed = urlparse(url)
    if parsed.scheme not in (("https", "http") if allow_http else ("https",)) or not parsed.hostname:
        raise DownloadError("Invalid download URL")
    if parsed.username or parsed.password or parsed.fragment:
        raise DownloadError("Invalid download URL")
    if not allow_private:
        if parsed.hostname == "localhost" or parsed.hostname.endswith(".local"):
            raise DownloadError("Invalid download host")
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            pass
        else:
            if not address.is_global:
                raise DownloadError("Invalid download host")


def chunks(url, limit, headers=None, allow_http=False, allow_private=False):
    # Validate redirects ourselves so a bearer token cannot escape to another host.
    original = urlparse(url)
    try:
        for redirect in range(4):
            validate_url(url, allow_http, allow_private)
            parsed = urlparse(url)
            if headers and (parsed.netloc != original.netloc or
                            (original.scheme == "https" and parsed.scheme != "https")):
                raise DownloadError("Authenticated download redirected to another host")
            with requests.get(url, headers=headers, stream=True, timeout=(10, 60),
                              allow_redirects=False) as response:
                if response.status_code in (301, 302, 303, 307, 308):
                    url = urljoin(url, response.headers.get("Location", ""))
                    continue
                if "json" in response.headers.get("Content-Type", "").lower():
                    block = next(response.iter_content(chunk_size=4096), b"")
                    try:
                        code = json.loads(block).get("code")
                    except (ValueError, AttributeError):
                        code = None
                    code = code if type(code) is int else "unknown"
                    raise DownloadError(f"Image resource API code={code}")
                if response.status_code != 200:
                    raise DownloadError(f"Image HTTP {response.status_code}")
                declared = response.headers.get("Content-Length", "")
                if declared.isdigit() and int(declared) > limit:
                    raise MediaRejected("图片超过大小限制")
                size = 0
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    size += len(chunk)
                    if size > limit:
                        raise MediaRejected("图片超过大小限制")
                    if chunk:
                        yield chunk
                if declared.isdigit() and not response.headers.get("Content-Encoding") and size != int(declared):
                    raise DownloadError("Incomplete image download")
                return
        raise DownloadError("Too many image redirects")
    except requests.RequestException as exc:
        raise DownloadError(f"Image network error: {type(exc).__name__}") from None


def inspect_image(path, limit):
    size = path.stat().st_size
    if size > limit:
        raise MediaRejected("图片超过大小限制")
    with path.open("rb") as data:
        head = data.read(16)
    if head.startswith(b"\xff\xd8\xff"):
        return ".jpg", "image/jpeg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png", "image/png"
    if head.startswith((b"GIF87a", b"GIF89a")):
        return ".gif", "image/gif"
    if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
        return ".webp", "image/webp"
    if head.startswith(b"BM"):
        return ".bmp", "image/bmp"
    raise MediaRejected("仅支持 JPEG、PNG、GIF、WebP、BMP 图片")


def download_image(url, destination, limit, headers=None, *, allow_http=False):
    destination = Path(destination)
    partial = destination.with_suffix(".part")
    try:
        with partial.open("wb") as output:
            for chunk in chunks(url, limit, headers, allow_http=allow_http):
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        inspect_image(partial, limit)
        partial.replace(destination)
    finally:
        partial.unlink(missing_ok=True)


def file_digest(path):
    with path.open("rb") as data:
        return hashlib.file_digest(data, "sha256").hexdigest()


def remote_digest(url, limit, headers):
    digest = hashlib.sha256()
    # FNS is configured by the operator and may be hosted on a private network.
    for chunk in chunks(url, limit, headers, allow_http=True, allow_private=True):
        digest.update(chunk)
    return digest.hexdigest()


class ImageDownloader:
    def __init__(self, source, options, http, feishu_client=None):
        self.source, self.options, self.http = source, options, http
        self.feishu_client = feishu_client
        self.token, self.token_until = "", 0

    def ding_token(self):
        if time.monotonic() >= self.token_until:
            result = self.http("POST", "https://api.dingtalk.com/v1.0/oauth2/accessToken", json={
                "appKey": self.options["client_id"], "appSecret": self.options["client_secret"]})
            if not result.get("accessToken"):
                raise DownloadError("DingTalk image token unavailable")
            self.token = result["accessToken"]
            self.token_until = time.monotonic() + max(0, int(result.get("expireIn", 7200)) - 60)
        return self.token

    def __call__(self, item, destination, limit):
        if int(item.get("size") or 0) > limit:
            raise MediaRejected("图片超过大小限制")
        headers = None
        if self.source == "telegram":
            token = self.options["bot_token"]
            result = self.http("POST", f"https://api.telegram.org/bot{token}/getFile",
                               json={"file_id": item["file_id"]})
            if result.get("ok") is not True:
                raise DownloadError(f'Telegram image error {result.get("error_code")}')
            file = result["result"]
            if int(file.get("file_size") or 0) > limit:
                raise MediaRejected("图片超过大小限制")
            path = PurePosixPath(file["file_path"])
            if path.is_absolute() or ".." in path.parts:
                raise DownloadError("Invalid Telegram image path")
            url = f"https://api.telegram.org/file/bot{token}/" + quote(str(path), safe="/")
        elif self.source == "dingtalk":
            result = self.http("POST", "https://api.dingtalk.com/v1.0/robot/messageFiles/download",
                headers={"x-acs-dingtalk-access-token": self.ding_token()}, json={
                    "robotCode": item.get("robot_code") or self.options.get("robot_code") or self.options["client_id"],
                    "downloadCode": item["download_code"]})
            if not result.get("downloadUrl"):
                raise DownloadError("DingTalk image URL unavailable")
            url = result["downloadUrl"]
        else:
            url = "https://open.feishu.cn/open-apis/im/v1/messages/" + quote(item["message_id"], safe="")
            url += "/resources/" + quote(item["image_key"], safe="") + "?type=image"
            headers = {"Authorization": "Bearer " + self.feishu_client.tenant_token()}
        # DingTalk's authenticated lookup can return an HTTP temporary CDN URL.
        # Send no application credentials to that URL; keep host checks enabled.
        download_image(url, destination, limit, headers, allow_http=self.source == "dingtalk")
