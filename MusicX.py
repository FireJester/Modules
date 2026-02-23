# -*- coding: utf-8 -*-

__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

import os
import re
import time
import logging
import tempfile
import shutil
import asyncio
import subprocess
import sys

from telethon.tl.types import Message

from aiogram.types import (
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultCachedAudio,
    InputTextMessageContent,
    BufferedInputFile,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

INLINE_QUERY_BANNER = "https://github.com/FireJester/Media/raw/main/Banner_for_inline_query_in_musicX.jpeg"


def _ensure_all_deps():
    for mod, pip in {
        "aiohttp": "aiohttp",
        "mutagen": "mutagen",
        "Crypto": "pycryptodome",
        "m3u8": "m3u8",
        "yandex_music": "yandex-music",
        "yt_dlp": "yt-dlp",
    }.items():
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pip, "-q"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


_ensure_all_deps()

import aiohttp
from yandex_music import ClientAsync
from yandex_music.exceptions import UnauthorizedError

try:
    import m3u8 as m3u8_lib
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
except ImportError:
    m3u8_lib = None

try:
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, ID3NoHeaderError
except ImportError:
    ID3 = None

try:
    import yt_dlp as yt_dlp_lib
except ImportError:
    yt_dlp_lib = None


VK_AUDIO_RE = re.compile(
    r"https?://(?:www\.)?(?:vk\.com|vk\.ru)/audio(-?\d+)_(\d+)(?:_([a-f0-9]+))?"
)
VK_TOKEN_RE = re.compile(r"access_token=([A-Za-z0-9._-]+)")

YM_ALBUM_TRACK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/album/\d+/track/(\d+)"
)
YM_DIRECT_TRACK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/track/(\d+)"
)
YM_TOKEN_PATTERN = re.compile(r"access_token=([^&]+)")

YT_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/|music\.youtube\.com/watch\?v=)([A-Za-z0-9_-]{11})"
)

VK_KATE_APP_ID = 2685278
VK_REDIRECT = "https://oauth.vk.com/blank.html"
VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.131"
YM_CLIENT_ID = "23cabbbdc6cd418abb4b39c32c41195d"

VK_USER_AGENTS = {
    "kate": "KateMobileAndroid/100.1 lite-530 (Android 13; SDK 33; arm64-v8a; Xiaomi; Mi 9T Pro; cepheus; ru; 320)",
    "vk_android": "VKAndroidApp/8.31-17556 (Android 13; SDK 33; arm64-v8a; Xiaomi; Mi 9T Pro; cepheus; ru; 320)",
    "vk_iphone": "com.vk.vkclient/1032 (iPhone, iOS 16.0, iPhone14,5, Scale/3.0)",
    "chrome": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

OG_IMAGE_RE = re.compile(
    r'<meta\s+(?:property|name)=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
OG_IMAGE_RE2 = re.compile(
    r'<meta\s+content=["\']([^"\']+)["\']\s+(?:property|name)=["\']og:image["\']',
    re.IGNORECASE,
)

MAX_FILE_SIZE = 50 * 1024 * 1024
REQUEST_OK = 200
MAX_CONCURRENT = 15
SEG_TIMEOUT = 30
CACHE_TTL = 600

STUB_URLS = ["audio_api_unavailable.mp3", "audio_api_unavailable"]
STUB_TITLES = [
    "\u0410\u0443\u0434\u0438\u043e \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e \u043d\u0430 vk.com",
    "Audio is available on vk.com",
]

SOURCE_VK = "vk"
SOURCE_YM = "ym"
SOURCE_YT = "yt"


def escape_html(t):
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sanitize_fn(n):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"


def fmt_dur(s):
    return f"{s // 60}:{s % 60:02d}" if s and s > 0 else "0:00"


def detect_source(text):
    if not text:
        return None
    if VK_AUDIO_RE.search(text):
        return SOURCE_VK
    if YM_ALBUM_TRACK_RE.search(text) or YM_DIRECT_TRACK_RE.search(text):
        return SOURCE_YM
    if YT_URL_RE.search(text):
        return SOURCE_YT
    return None


def parse_vk_link(text):
    m = VK_AUDIO_RE.search(text or "")
    if m:
        return int(m.group(1)), int(m.group(2)), m.group(3)
    return None, None, None


def parse_ym_track_id(text):
    if not text:
        return None
    m = YM_ALBUM_TRACK_RE.search(text)
    if m:
        return m.group(1)
    m = YM_DIRECT_TRACK_RE.search(text)
    if m:
        return m.group(1)
    return None


def parse_yt_url(text):
    if not text:
        return None
    m = YT_URL_RE.search(text)
    if m:
        return m.group(0)
    return None


def parse_yt_video_id(text):
    if not text:
        return None
    m = YT_URL_RE.search(text)
    if m:
        return m.group(1)
    return None


def extract_vk_token(text):
    if not text:
        return None
    m = VK_TOKEN_RE.search(text)
    if m:
        return m.group(1)
    return None


def extract_ym_token(text):
    if not text:
        return None
    m = YM_TOKEN_PATTERN.search(text)
    if m:
        return m.group(1)
    return None


def _is_stub_url(url):
    if not url:
        return True
    return any(s in url for s in STUB_URLS)


def _is_stub_title(title):
    if not title:
        return False
    return any(s.lower() in title.lower() for s in STUB_TITLES)


def _build_vk_auth_url():
    return (
        f"https://oauth.vk.com/authorize?client_id={VK_KATE_APP_ID}"
        f"&display=page&redirect_uri={VK_REDIRECT}"
        f"&scope=audio,offline&response_type=token&v={VK_API_VERSION}"
    )


class VKThumbFetcher:
    @staticmethod
    async def fetch_og_image(owner_id, audio_id):
        url = f"https://vk.com/audio{owner_id}_{audio_id}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url,
                    headers={
                        "User-Agent": VK_USER_AGENTS["chrome"],
                        "Accept-Language": "ru-RU,ru;q=0.9",
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=True,
                ) as r:
                    if r.status != REQUEST_OK:
                        return ""
                    html = await r.text(errors="replace")
            for pat in [OG_IMAGE_RE, OG_IMAGE_RE2]:
                m = pat.search(html)
                if m:
                    u = m.group(1).replace("&amp;", "&")
                    if u and "userapi.com" in u:
                        return u
        except Exception:
            pass
        return ""

    @staticmethod
    async def download_image(url):
        if not url:
            return None
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status != REQUEST_OK:
                        return None
                    data = await r.read()
                    return data if len(data) > 1000 else None
        except Exception:
            return None

    @staticmethod
    async def try_bigger(url):
        if not url:
            return url
        bigger = re.sub(r'size=\d+x\d+', 'size=1200x1200', url)
        if bigger == url:
            return url
        try:
            async with aiohttp.ClientSession() as s:
                async with s.head(
                    bigger, timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    if r.status == REQUEST_OK:
                        return bigger
        except Exception:
            pass
        return url


class YMCoverFetcher:
    @staticmethod
    async def download_cover_bytes(cover_uri, size="600x600"):
        if not cover_uri:
            return None
        url = f"https://{cover_uri.replace('%%', size)}"
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    url, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != REQUEST_OK:
                        return None
                    data = await resp.read()
                    return data if len(data) > 500 else None
        except Exception:
            return None


class VKAPIClient:
    def __init__(self):
        self._token = None
        self._ok = False
        self._session = None
        self._user_id = None

    @property
    def ok(self):
        return self._ok and self._token is not None

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _api(self, method, app="kate", **params):
        params["access_token"] = self._token
        params["v"] = VK_API_VERSION
        s = await self._get_session()
        ua = VK_USER_AGENTS.get(app, VK_USER_AGENTS["kate"])
        async with s.post(
            f"{VK_API_BASE}/{method}",
            data=params,
            headers={"User-Agent": ua},
        ) as r:
            if r.status != REQUEST_OK:
                return None
            data = await r.json()
        if "error" in data:
            return None
        return data.get("response")

    async def auth(self, token):
        self._token = token
        try:
            r = await self._api("users.get")
            if r and isinstance(r, list) and r:
                self._user_id = r[0].get("id")
                self._ok = True
                return True
        except Exception:
            pass
        self._ok = False
        self._token = None
        self._user_id = None
        return False

    async def get_audio(self, oid, aid):
        for app in ["kate", "vk_android", "vk_iphone"]:
            try:
                r = await self._api(
                    "audio.getById", app=app, audios=f"{oid}_{aid}"
                )
                if r and isinstance(r, list) and r:
                    p = self._parse(r[0])
                    if (
                        p
                        and not _is_stub_url(p.get("url", ""))
                        and not _is_stub_title(p.get("title", ""))
                    ):
                        return p
            except Exception:
                pass
        for code in [
            'return API.audio.getById({{audios:"{a}"}});',
            (
                'var a=API.audio.getById({{audios:"{a}"}});'
                "if(a.length>0){{return a[0];}}return null;"
            ),
        ]:
            try:
                r = await self._api(
                    "execute", app="kate",
                    code=code.format(a=f"{oid}_{aid}"),
                )
                if r:
                    items = r if isinstance(r, list) else [r]
                    if items and isinstance(items[0], dict):
                        p = self._parse(items[0])
                        if (
                            p
                            and not _is_stub_url(p.get("url", ""))
                            and not _is_stub_title(p.get("title", ""))
                        ):
                            return p
            except Exception:
                pass
        try:
            r = await self._api("audio.get", app="kate", owner_id=oid, count=100)
            if r:
                items = r.get("items", []) if isinstance(r, dict) else r
                for it in items:
                    if it.get("id") == aid:
                        p = self._parse(it)
                        if p and not _is_stub_url(p.get("url", "")):
                            return p
        except Exception:
            pass
        return None

    def _parse(self, a):
        if not a:
            return None
        url = a.get("url", "")
        title = a.get("title", "Unknown") or "Unknown"
        artist = a.get("artist", "Unknown") or "Unknown"
        if not url:
            return None
        thumb = ""
        album = a.get("album", {})
        if album:
            th = album.get("thumb", {})
            if th:
                for k in ["photo_1200", "photo_600", "photo_300", "photo_270"]:
                    if th.get(k):
                        thumb = th[k]
                        break
        return {
            "id": a.get("id"),
            "owner_id": a.get("owner_id"),
            "url": url,
            "artist": artist,
            "title": title,
            "duration": int(a.get("duration", 0) or 0),
            "thumbnail": thumb,
        }


class YMApiClient:
    def __init__(self):
        self._token = None
        self._client = None
        self._ok = False
        self._uid = None
        self._login = None

    @property
    def ok(self):
        return self._ok and self._token is not None

    async def auth(self, token):
        self._token = token
        try:
            self._client = ClientAsync(token)
            await self._client.init()
            me = self._client.me
            self._uid = me.account.uid
            self._login = me.account.login
            self._ok = True
            return True
        except Exception:
            self._ok = False
            self._token = None
            self._client = None
            self._uid = None
            self._login = None
            return False

    async def fetch_track(self, track_id):
        if not self._client:
            return None
        try:
            tracks = await self._client.tracks(track_id, with_positions=False)
            if not tracks:
                return None
            return tracks[0]
        except Exception:
            return None

    async def download_track_file(self, track, filepath):
        try:
            await track.download_async(filepath)
            return os.path.exists(filepath) and os.path.getsize(filepath) > 0
        except Exception:
            return False

    async def download_cover_file(self, track, filepath):
        try:
            if not track.cover_uri:
                return False
            await track.download_cover_async(filepath, size="600x600")
            return os.path.exists(filepath) and os.path.getsize(filepath) > 500
        except Exception:
            return False

    def logout(self):
        self._token = None
        self._client = None
        self._ok = False
        self._uid = None
        self._login = None


class VKDownloader:
    def __init__(self, tmp):
        self.tmp = tmp

    async def dl(self, url, out):
        return (
            await self._m3u8(url, out)
            if ".m3u8" in url
            else await self._direct(url, out)
        )

    async def _direct(self, url, out):
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=120)
            ) as s:
                async with s.get(url) as r:
                    if r.status != REQUEST_OK:
                        return False
                    tot = 0
                    with open(out, "wb") as f:
                        async for ch in r.content.iter_chunked(65536):
                            tot += len(ch)
                            if tot > MAX_FILE_SIZE:
                                return False
                            f.write(ch)
            return os.path.exists(out) and os.path.getsize(out) > 0
        except Exception:
            return False

    async def _m3u8(self, url, out):
        if not m3u8_lib:
            return False
        try:
            conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
            async with aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=120, connect=15),
            ) as s:
                async with s.get(url) as r:
                    if r.status != REQUEST_OK:
                        return False
                    txt = await r.text()
                pl = m3u8_lib.loads(txt)
                if pl.playlists:
                    best = max(
                        pl.playlists,
                        key=lambda p: (
                            p.stream_info.bandwidth if p.stream_info else 0
                        ),
                    )
                    su = best.uri
                    if not su.startswith("http"):
                        su = f"{url.rsplit('/', 1)[0]}/{su}"
                    async with s.get(su) as r2:
                        if r2.status != REQUEST_OK:
                            return False
                        txt = await r2.text()
                    pl = m3u8_lib.loads(txt)
                    url = su
                segs = pl.segments
                if not segs:
                    return False
                base = url.rsplit("/", 1)[0]
                sem = asyncio.Semaphore(MAX_CONCURRENT)
                chunks = [None] * len(segs)
                keys = {}

                async def gk(ku):
                    if ku in keys:
                        return keys[ku]
                    async with s.get(
                        ku, timeout=aiohttp.ClientTimeout(total=15)
                    ) as kr:
                        if kr.status == REQUEST_OK:
                            kd = await kr.read()
                            keys[ku] = kd
                            return kd
                    return None

                async def ds(i, seg):
                    async with sem:
                        uri = seg.uri
                        if not uri.startswith("http"):
                            uri = f"{base}/{uri}"
                        try:
                            async with s.get(
                                uri,
                                timeout=aiohttp.ClientTimeout(total=SEG_TIMEOUT),
                            ) as rr:
                                if rr.status != REQUEST_OK:
                                    chunks[i] = b""
                                    return
                                data = await rr.read()
                        except Exception:
                            chunks[i] = b""
                            return
                        if (
                            seg.key
                            and seg.key.method == "AES-128"
                            and seg.key.uri
                        ):
                            ku = seg.key.uri
                            if not ku.startswith("http"):
                                ku = f"{base}/{ku}"
                            key = await gk(ku)
                            if key:
                                data = self._aes(data, key)
                        chunks[i] = data

                await asyncio.gather(*[ds(i, seg) for i, seg in enumerate(segs)])
                result = b"".join(c for c in chunks if c)
                if not result:
                    return False
                with open(out, "wb") as f:
                    f.write(result)
                return True
        except Exception:
            return False

    @staticmethod
    def _aes(data, key):
        try:
            if len(data) < 16:
                return data
            iv, ct = data[:16], data[16:]
            if not ct:
                return data
            dec = AES.new(key, AES.MODE_CBC, iv=iv).decrypt(ct)
            try:
                dec = unpad(dec, AES.block_size)
            except ValueError:
                pass
            return dec
        except Exception:
            return data

    async def to_mp3(self, inp, out):
        try:
            p = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-y", "-i", inp,
                "-vn", "-acodec", "libmp3lame", "-ab", "320k", out,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(p.communicate(), timeout=60)
            if p.returncode != 0:
                p2 = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-y", "-i", inp,
                    "-vn", "-acodec", "copy", out,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(p2.communicate(), timeout=60)
                return p2.returncode == 0
            return True
        except FileNotFoundError:
            try:
                shutil.copy2(inp, out)
                return True
            except Exception:
                return False
        except Exception:
            return False

    async def embed_cover(self, mp3, cover, out):
        try:
            p = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-i", mp3, "-i", cover,
                "-map", "0:a", "-map", "1:0",
                "-c:a", "copy", "-id3v2_version", "3",
                "-metadata:s:v", "title=Cover",
                "-metadata:s:v", "comment=Cover (front)",
                "-disposition:v", "attached_pic", out,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(p.communicate(), timeout=30)
            return (
                p.returncode == 0
                and os.path.exists(out)
                and os.path.getsize(out) > 0
            )
        except Exception:
            return False


class TagWriter:
    @staticmethod
    def write_tags(filepath, title, artist, album=None, cover_data=None):
        if not ID3:
            return
        try:
            try:
                tags = ID3(filepath)
            except ID3NoHeaderError:
                tags = ID3()
            tags.add(TIT2(encoding=3, text=[title or "Unknown"]))
            tags.add(TPE1(encoding=3, text=[artist or "Unknown"]))
            if album:
                tags.add(TALB(encoding=3, text=[album]))
            if cover_data and len(cover_data) > 500:
                tags.add(APIC(
                    encoding=3, mime="image/jpeg",
                    type=3, desc="Cover", data=cover_data,
                ))
            tags.save(filepath)
        except Exception:
            pass


class AudioConverter:
    @staticmethod
    async def to_mp3(inp, out):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-y", "-i", inp,
                "-vn", "-acodec", "libmp3lame", "-ab", "320k", out,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=60)
            if (
                proc.returncode == 0
                and os.path.exists(out)
                and os.path.getsize(out) > 0
            ):
                return True
            proc2 = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-y", "-i", inp,
                "-vn", "-acodec", "copy", out,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc2.communicate(), timeout=60)
            return proc2.returncode == 0
        except FileNotFoundError:
            try:
                shutil.copy2(inp, out)
                return True
            except Exception:
                return False
        except Exception:
            return False

    @staticmethod
    async def embed_cover_ffmpeg(mp3_path, cover_path, out_path):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-i", mp3_path, "-i", cover_path,
                "-map", "0:a", "-map", "1:0",
                "-c:a", "copy", "-id3v2_version", "3",
                "-metadata:s:v", "title=Cover",
                "-metadata:s:v", "comment=Cover (front)",
                "-disposition:v", "attached_pic", out_path,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=30)
            return (
                proc.returncode == 0
                and os.path.exists(out_path)
                and os.path.getsize(out_path) > 0
            )
        except Exception:
            return False


class YTDownloader:
    def __init__(self, tmp):
        self.tmp = tmp

    async def download_audio(self, url, ddir):
        if not yt_dlp_lib:
            return None
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(None, self._extract_info, url)
        if not info:
            return None
        title = info.get("title", "Unknown") or "Unknown"
        uploader = info.get("uploader", "Unknown") or "Unknown"
        duration = int(info.get("duration", 0) or 0)
        thumbnail_url = info.get("thumbnail", "")
        safe_name = sanitize_fn(f"{uploader} - {title}")
        out_template = os.path.join(ddir, f"{safe_name}.%(ext)s")
        dl_result = await loop.run_in_executor(
            None, self._download_audio_sync, url, out_template
        )
        if not dl_result:
            return None
        audio_extensions = (".mp3", ".m4a", ".opus", ".ogg", ".wav", ".webm")
        found_file = None
        for f in os.listdir(ddir):
            if f.endswith(audio_extensions) and os.path.isfile(os.path.join(ddir, f)):
                found_file = os.path.join(ddir, f)
                break
        if not found_file:
            return None
        final_mp3 = os.path.join(ddir, f"{safe_name}.mp3")
        if not found_file.endswith(".mp3"):
            conv_ok = await AudioConverter.to_mp3(found_file, final_mp3)
            if conv_ok and os.path.exists(final_mp3) and os.path.getsize(final_mp3) > 0:
                try:
                    os.remove(found_file)
                except Exception:
                    pass
            else:
                final_mp3 = found_file
        else:
            if found_file != final_mp3:
                try:
                    os.rename(found_file, final_mp3)
                except Exception:
                    final_mp3 = found_file
        if not os.path.exists(final_mp3) or os.path.getsize(final_mp3) == 0:
            return None
        if os.path.getsize(final_mp3) > MAX_FILE_SIZE:
            return {"error": "too_big"}
        cover_data = None
        cover_path = None
        if thumbnail_url:
            cover_data = await self._download_thumbnail(thumbnail_url)
            if cover_data:
                cover_path = os.path.join(ddir, "cover.jpg")
                with open(cover_path, "wb") as cf:
                    cf.write(cover_data)
        if cover_path and os.path.exists(cover_path) and final_mp3.endswith(".mp3"):
            covered_mp3 = os.path.join(ddir, f"{safe_name}_cover.mp3")
            embed_ok = await AudioConverter.embed_cover_ffmpeg(
                final_mp3, cover_path, covered_mp3
            )
            if embed_ok:
                try:
                    os.remove(final_mp3)
                except Exception:
                    pass
                final_mp3 = covered_mp3
            elif ID3:
                TagWriter.write_tags(final_mp3, title, uploader, cover_data=cover_data)
        elif ID3 and final_mp3.endswith(".mp3"):
            TagWriter.write_tags(final_mp3, title, uploader)
        return {
            "file": final_mp3,
            "track": {
                "title": title,
                "artist": uploader,
                "duration": duration,
                "duration_str": fmt_dur(duration),
                "thumbnail": thumbnail_url,
                "thumb_path": cover_path if cover_path and os.path.exists(cover_path) else None,
                "thumb_data": cover_data,
            },
        }

    def _extract_info(self, url):
        try:
            opts = {
                "quiet": True,
                "no_warnings": True,
                "socket_timeout": 30,
            }
            with yt_dlp_lib.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        except Exception:
            return None

    def _download_audio_sync(self, url, out_template):
        try:
            opts = {
                "outtmpl": out_template,
                "quiet": True,
                "no_warnings": True,
                "restrictfilenames": True,
                "format": "bestaudio/best",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "320",
                }],
            }
            with yt_dlp_lib.YoutubeDL(opts) as ydl:
                ydl.download([url])
            return True
        except Exception:
            return False

    @staticmethod
    async def _download_thumbnail(url):
        if not url:
            return None
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    url, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != REQUEST_OK:
                        return None
                    data = await resp.read()
                    if len(data) < 1000:
                        return None
                    if not data[:3] in (b"\xff\xd8\xff", b"\x89PN", b"GIF", b"RIF"):
                        try:
                            import io
                            from PIL import Image
                            img = Image.open(io.BytesIO(data))
                            buf = io.BytesIO()
                            img.convert("RGB").save(buf, format="JPEG", quality=95)
                            return buf.getvalue()
                        except Exception:
                            pass
                    return data
        except Exception:
            return None


@loader.tds
class MusicX(loader.Module):
    """VK + Yandex Music + YouTube audio downloader via inline."""

    strings = {
        "name": "MusicX",
        "vk_need_auth": "<b>VK not authorized!</b>\nUse <code>.vkauth</code>",
        "vk_auth_link": (
            '<b><a href="{}">Authorize via Kate Mobile</a></b>\n\n'
            "1. Open the link and log in\n"
            "2. Grant permissions and allow access\n"
            "3. Copy the <b>full URL</b> from the address bar\n"
            "4. <code>.vktoken URL</code>"
        ),
        "vk_auth_success": "<b>VK authorized successfully!</b>",
        "vk_auth_already": "<b>VK is already authorized.</b> Use <code>.vklogout</code> to reset.",
        "vk_auth_bad_url": "<b>Failed to extract VK token from the URL!</b>",
        "vk_auth_fail": "<b>VK token is invalid!</b>",
        "vk_deauth": "<b>VK logged out.</b>",
        "vk_status_ok": "<b>VK: Authorized</b> | id <code>{}</code>",
        "vk_status_no": "<b>VK: Not authorized</b> | Use <code>.vkauth</code>",
        "ym_need_auth": "<b>Yandex Music not authorized!</b>\nUse <code>.ymauth</code>",
        "ym_auth_link": (
            "<b>Yandex Music Authorization</b>\n\n"
            "1. <a href='https://oauth.yandex.ru/authorize?"
            "response_type=token&client_id={}'>Open this link</a>\n"
            "2. Log in with your Yandex account\n"
            "3. Copy the <b>full URL</b> from the address bar\n"
            "4. <code>.ymtoken URL</code>"
        ),
        "ym_auth_success": "<b>YM authorized!</b>\nUID: <code>{}</code> | Login: <code>{}</code>",
        "ym_auth_already": "<b>YM is already authorized.</b> Use <code>.ymlogout</code> to reset.",
        "ym_auth_bad": "<b>Failed to extract YM token from the URL!</b>",
        "ym_auth_fail": "<b>YM token is invalid!</b>",
        "ym_deauth": "<b>YM logged out.</b>",
        "ym_status_ok": "<b>YM: Authorized</b> | UID <code>{}</code> | Login <code>{}</code>",
        "ym_status_no": "<b>YM: Not authorized</b> | Use <code>.ymauth</code>",
        "help": (
            "<b>MusicX - Audio Downloader</b>\n\n"
            "<b>Download music via inline:</b>\n"
            "<code>@{} LINK</code>\n\n"
            "<b>Supported sources:</b>\n"
            "- VK Audio\n"
            "- Yandex Music\n"
            "- YouTube / YouTube Music\n\n"
            "<b>VK Auth:</b>\n"
            "<code>.vkauth</code> - get auth link\n"
            "<code>.vktoken URL</code> - submit token\n"
            "<code>.vkstatus</code> - check status\n"
            "<code>.vklogout</code> - log out\n\n"
            "<b>Yandex Music Auth:</b>\n"
            "<code>.ymauth</code> - get auth link\n"
            "<code>.ymtoken URL</code> - submit token\n"
            "<code>.ymstatus</code> - check status\n"
            "<code>.ymlogout</code> - log out"
        ),
        "too_big": "<b>File exceeds 50 MB limit!</b>",
        "no_audio": "<b>No audio found.</b>",
        "bad_link": (
            "<b>Invalid or unsupported link!</b>\n"
            "VK: <code>https://vk.com/audio-123_456</code>\n"
            "YM: <code>https://music.yandex.ru/album/123/track/456</code>\n"
            "YT: <code>https://youtube.com/watch?v=xxx</code>"
        ),
        "vk_stub": "<b>VK blocked the audio.</b>\nTry <code>.vklogout</code> and re-authorize.",
        "yt_no_ytdlp": "<b>yt-dlp is not available!</b>",
    }

    def __init__(self):
        super().__init__()
        self.inline_bot = None
        self.inline_bot_username = None
        self._tmp = None
        self._vk = None
        self._ym = None
        self._vk_dl = None
        self._yt_dl = None
        self._pending_futures = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._tmp = os.path.join(tempfile.gettempdir(), "musicx")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)
        self._vk = VKAPIClient()
        self._ym = YMApiClient()
        self._vk_dl = VKDownloader(self._tmp)
        self._yt_dl = YTDownloader(self._tmp)
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
            try:
                bi = await self.inline_bot.get_me()
                self.inline_bot_username = bi.username
            except Exception:
                pass
        vk_token = self._db.get("MusicX", "vk_token", "")
        if vk_token:
            await self._vk.auth(vk_token)
        ym_token = self._db.get("MusicX", "ym_token", "")
        if ym_token:
            await self._ym.auth(ym_token)
        self._cleanup_cache_db()

    async def _ensure_vk_auth(self):
        if self._vk.ok:
            return True
        token = self._db.get("MusicX", "vk_token", "")
        return await self._vk.auth(token) if token else False

    async def _ensure_ym_auth(self):
        if self._ym.ok:
            return True
        token = self._db.get("MusicX", "ym_token", "")
        return await self._ym.auth(token) if token else False

    def _get_cache_db(self):
        return self._db.get("MusicX", "inline_cache", {})

    def _save_cache_db(self, cache):
        self._db.set("MusicX", "inline_cache", cache)

    def _cache_get(self, key):
        cache = self._get_cache_db()
        entry = cache.get(key)
        if not entry:
            return None
        if time.time() - entry.get("ts", 0) > CACHE_TTL:
            cache.pop(key, None)
            self._save_cache_db(cache)
            return None
        entry["ts"] = time.time()
        self._save_cache_db(cache)
        return entry.get("data")

    def _cache_set(self, key, data):
        cache = self._get_cache_db()
        cache[key] = {"data": data, "ts": time.time()}
        self._save_cache_db(cache)

    def _cleanup_cache_db(self):
        cache = self._get_cache_db()
        if not cache:
            return
        now = time.time()
        dead = [k for k, v in cache.items() if now - v.get("ts", 0) > CACHE_TTL]
        if dead:
            for k in dead:
                cache.pop(k, None)
            self._save_cache_db(cache)

    def _make_cache_key(self, text):
        source = detect_source(text)
        if source == SOURCE_VK:
            owner, aid, _ = parse_vk_link(text)
            if owner is None:
                return None
            return f"vk_{owner}_{aid}"
        elif source == SOURCE_YM:
            tid = parse_ym_track_id(text)
            if not tid:
                return None
            return f"ym_{tid}"
        elif source == SOURCE_YT:
            vid = parse_yt_video_id(text)
            if not vid:
                return None
            return f"yt_{vid}"
        return None

    @loader.command()
    async def vkauth(self, message: Message):
        """Start VK authorization"""
        if self._vk.ok:
            await utils.answer(message, self.strings["vk_auth_already"])
            return
        await utils.answer(
            message,
            self.strings["vk_auth_link"].format(_build_vk_auth_url()),
        )

    @loader.command()
    async def vktoken(self, message: Message):
        """Submit VK token from redirect URL"""
        args = utils.get_args_raw(message).strip()
        if not args:
            rr = await message.get_reply_message()
            if rr and rr.text:
                args = rr.text.strip()
        if not args:
            await utils.answer(message, self.strings["vk_auth_bad_url"])
            return
        token = extract_vk_token(args)
        if not token:
            await utils.answer(message, self.strings["vk_auth_bad_url"])
            return
        try:
            await message.delete()
        except Exception:
            pass
        ok = await self._vk.auth(token)
        if ok:
            self._db.set("MusicX", "vk_token", token)
        await self._client.send_message(
            message.chat_id,
            self.strings["vk_auth_success"] if ok else self.strings["vk_auth_fail"],
            parse_mode="html",
        )

    @loader.command()
    async def vkstatus(self, message: Message):
        """Check VK authorization status"""
        if self._vk.ok:
            await utils.answer(
                message,
                self.strings["vk_status_ok"].format(self._vk._user_id or "?"),
            )
        else:
            await utils.answer(message, self.strings["vk_status_no"])

    @loader.command()
    async def vklogout(self, message: Message):
        """Log out from VK"""
        self._db.set("MusicX", "vk_token", "")
        await self._vk.close()
        self._vk = VKAPIClient()
        await utils.answer(message, self.strings["vk_deauth"])

    @loader.command()
    async def ymauth(self, message: Message):
        """Start Yandex Music authorization"""
        if self._ym.ok:
            await utils.answer(message, self.strings["ym_auth_already"])
            return
        await utils.answer(
            message,
            self.strings["ym_auth_link"].format(YM_CLIENT_ID),
        )

    @loader.command()
    async def ymtoken(self, message: Message):
        """Submit Yandex Music token from redirect URL"""
        args = utils.get_args_raw(message).strip()
        if not args:
            rr = await message.get_reply_message()
            if rr and rr.text:
                args = rr.text.strip()
        if not args:
            await utils.answer(message, self.strings["ym_auth_bad"])
            return
        token = extract_ym_token(args)
        if not token:
            await utils.answer(message, self.strings["ym_auth_bad"])
            return
        try:
            await message.delete()
        except Exception:
            pass
        ok = await self._ym.auth(token)
        if ok:
            self._db.set("MusicX", "ym_token", token)
            text = self.strings["ym_auth_success"].format(
                self._ym._uid or "?", self._ym._login or "?"
            )
        else:
            text = self.strings["ym_auth_fail"]
        await self._client.send_message(
            message.chat_id, text, parse_mode="html",
        )

    @loader.command()
    async def ymstatus(self, message: Message):
        """Check Yandex Music authorization status"""
        if self._ym.ok:
            await utils.answer(
                message,
                self.strings["ym_status_ok"].format(
                    self._ym._uid or "?", self._ym._login or "?"
                ),
            )
        else:
            await utils.answer(message, self.strings["ym_status_no"])

    @loader.command()
    async def ymlogout(self, message: Message):
        """Log out from Yandex Music"""
        self._db.set("MusicX", "ym_token", "")
        self._ym.logout()
        await utils.answer(message, self.strings["ym_deauth"])

    @loader.command()
    async def musicx(self, message: Message):
        """Show MusicX help and usage info"""
        await utils.answer(
            message,
            self.strings["help"].format(self.inline_bot_username or "bot"),
        )

    async def _vk_full_dl(self, owner, aid):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            if not await self._ensure_vk_auth():
                return {"error": "vk_not_auth", "dir": ddir}
            try:
                info = await self._vk.get_audio(owner, aid)
            except Exception as e:
                return {"error": f"VK API: {e}", "dir": ddir}
            if not info:
                return {"error": "vk_stub", "dir": ddir}
            url = info.get("url", "")
            if not url or _is_stub_url(url) or _is_stub_title(info.get("title", "")):
                return {"error": "vk_stub", "dir": ddir}
            artist = info.get("artist", "Unknown") or "Unknown"
            title = info.get("title", "Unknown") or "Unknown"
            dur = int(info.get("duration", 0) or 0)
            thumb_url = info.get("thumbnail", "")
            if not thumb_url:
                thumb_url = await VKThumbFetcher.fetch_og_image(owner, aid)
            if thumb_url:
                bigger = await VKThumbFetcher.try_bigger(thumb_url)
                if bigger != thumb_url:
                    thumb_url = bigger
            thumb_data = None
            thumb_path = None
            if thumb_url:
                thumb_data = await VKThumbFetcher.download_image(thumb_url)
                if thumb_data:
                    thumb_path = os.path.join(ddir, "cover.jpg")
                    with open(thumb_path, "wb") as f:
                        f.write(thumb_data)
            track = {
                "title": title, "artist": artist, "duration": dur,
                "thumbnail": thumb_url, "thumb_path": thumb_path,
                "thumb_data": thumb_data, "duration_str": fmt_dur(dur),
            }
            ext = "ts" if ".m3u8" in url else "mp3"
            raw = os.path.join(ddir, f"raw.{ext}")
            if not await self._vk_dl.dl(url, raw):
                return {"error": "dl_fail", "dir": ddir}
            if os.path.getsize(raw) == 0:
                return {"error": "empty", "dir": ddir}
            if os.path.getsize(raw) > MAX_FILE_SIZE:
                return {"error": "too_big", "dir": ddir}
            name = sanitize_fn(f"{artist} - {title}")
            mp3 = os.path.join(ddir, f"{name}.mp3")
            if ext != "mp3":
                if await self._vk_dl.to_mp3(raw, mp3) and os.path.exists(mp3):
                    try:
                        os.remove(raw)
                    except Exception:
                        pass
                else:
                    mp3 = raw
            else:
                try:
                    os.rename(raw, mp3)
                except Exception:
                    mp3 = raw
            if os.path.getsize(mp3) > MAX_FILE_SIZE:
                return {"error": "too_big", "dir": ddir}
            if thumb_path and os.path.exists(thumb_path) and mp3.endswith(".mp3"):
                mp3c = os.path.join(ddir, f"{name}_cover.mp3")
                if await self._vk_dl.embed_cover(mp3, thumb_path, mp3c):
                    try:
                        os.remove(mp3)
                    except Exception:
                        pass
                    mp3 = mp3c
                elif ID3:
                    TagWriter.write_tags(mp3, title, artist, cover_data=thumb_data)
            elif ID3 and mp3.endswith(".mp3"):
                TagWriter.write_tags(mp3, title, artist)
            return {"file": mp3, "track": track, "dir": ddir}
        except Exception as e:
            return {"error": str(e), "dir": ddir}

    async def _ym_full_dl(self, track_id_str):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            if not await self._ensure_ym_auth():
                return {"error": "ym_not_auth", "dir": ddir}
            track = await self._ym.fetch_track(track_id_str)
            if not track:
                return {"error": "no_track", "dir": ddir}
            artist = "Unknown"
            if track.artists:
                artist = ", ".join(a.name for a in track.artists if a.name) or "Unknown"
            title = track.title or "Unknown"
            album_title = ""
            if track.albums:
                album_title = track.albums[0].title or ""
            dur = int((track.duration_ms or 0) / 1000)
            has_cover = bool(track.cover_uri)
            cover_data = None
            cover_path = None
            if has_cover:
                cover_path = os.path.join(ddir, "cover.jpg")
                cover_ok = await self._ym.download_cover_file(track, cover_path)
                if cover_ok and os.path.exists(cover_path):
                    with open(cover_path, "rb") as cf:
                        cover_data = cf.read()
                else:
                    cover_data = await YMCoverFetcher.download_cover_bytes(track.cover_uri)
                    if cover_data:
                        cover_path = os.path.join(ddir, "cover.jpg")
                        with open(cover_path, "wb") as cf:
                            cf.write(cover_data)
                    else:
                        cover_path = None
            raw_path = os.path.join(ddir, "raw_track.mp3")
            dl_ok = await self._ym.download_track_file(track, raw_path)
            if not dl_ok:
                return {"error": "dl_fail", "dir": ddir}
            if os.path.getsize(raw_path) == 0:
                return {"error": "empty", "dir": ddir}
            if os.path.getsize(raw_path) > MAX_FILE_SIZE:
                return {"error": "too_big", "dir": ddir}
            clean_name = sanitize_fn(f"{artist} - {title}")
            final_mp3 = os.path.join(ddir, f"{clean_name}.mp3")
            try:
                with open(raw_path, "rb") as rf:
                    header = rf.read(4)
                is_mp3 = (
                    header[:3] == b"ID3"
                    or header[:2] == b"\xff\xfb"
                    or header[:2] == b"\xff\xf3"
                )
            except Exception:
                is_mp3 = True
            if not is_mp3:
                conv_ok = await AudioConverter.to_mp3(raw_path, final_mp3)
                if conv_ok and os.path.exists(final_mp3) and os.path.getsize(final_mp3) > 0:
                    try:
                        os.remove(raw_path)
                    except Exception:
                        pass
                else:
                    final_mp3 = raw_path
            else:
                try:
                    os.rename(raw_path, final_mp3)
                except Exception:
                    final_mp3 = raw_path
            if os.path.getsize(final_mp3) > MAX_FILE_SIZE:
                return {"error": "too_big", "dir": ddir}
            if cover_path and os.path.exists(cover_path) and final_mp3.endswith(".mp3"):
                covered_mp3 = os.path.join(ddir, f"{clean_name}_cover.mp3")
                ffmpeg_ok = await AudioConverter.embed_cover_ffmpeg(
                    final_mp3, cover_path, covered_mp3
                )
                if ffmpeg_ok:
                    try:
                        os.remove(final_mp3)
                    except Exception:
                        pass
                    final_mp3 = covered_mp3
                else:
                    TagWriter.write_tags(final_mp3, title, artist, album_title, cover_data)
            elif final_mp3.endswith(".mp3"):
                TagWriter.write_tags(final_mp3, title, artist, album_title, cover_data)
            thumb_url = ""
            if track.cover_uri:
                thumb_url = f"https://{track.cover_uri.replace('%%', '200x200')}"
            return {
                "file": final_mp3,
                "track": {
                    "title": title, "artist": artist,
                    "duration": dur, "duration_str": fmt_dur(dur),
                    "thumbnail": thumb_url,
                    "thumb_path": cover_path if cover_path and os.path.exists(cover_path) else None,
                    "thumb_data": cover_data,
                },
                "dir": ddir,
            }
        except Exception as e:
            return {"error": str(e), "dir": ddir}

    async def _yt_full_dl(self, url):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            if not yt_dlp_lib:
                return {"error": "yt_no_ytdlp", "dir": ddir}
            result = await self._yt_dl.download_audio(url, ddir)
            if result is None:
                return {"error": "dl_fail", "dir": ddir}
            if isinstance(result, dict) and "error" in result:
                return {"error": result["error"], "dir": ddir}
            return {
                "file": result["file"],
                "track": result["track"],
                "dir": ddir,
            }
        except Exception as e:
            return {"error": str(e), "dir": ddir}

    async def _unified_dl(self, text):
        source = detect_source(text)
        if source == SOURCE_VK:
            owner, aid, _ = parse_vk_link(text)
            if owner is None:
                return {"error": "bad_link"}
            return await self._vk_full_dl(owner, aid)
        elif source == SOURCE_YM:
            tid = parse_ym_track_id(text)
            if not tid:
                return {"error": "bad_link"}
            return await self._ym_full_dl(tid)
        elif source == SOURCE_YT:
            yt_url = parse_yt_url(text)
            if not yt_url:
                return {"error": "bad_link"}
            return await self._yt_full_dl(yt_url)
        else:
            return {"error": "bad_link"}

    async def _inline_dl_and_upload(self, text, user_id, cache_key):
        ddir = None
        try:
            res = await self._unified_dl(text)
            ddir = res.get("dir")
            if res.get("error"):
                err_map = {
                    "too_big": "File > 50 MB",
                    "no_audio": "No audio",
                    "no_track": "Track not found",
                    "vk_not_auth": "VK not authorized",
                    "ym_not_auth": "YM not authorized",
                    "empty": "Empty audio",
                    "dl_fail": "Download error",
                    "vk_stub": "VK blocks this audio",
                    "bad_link": "Invalid link",
                    "yt_no_ytdlp": "yt-dlp not available",
                }
                result = {"error": err_map.get(res["error"], str(res["error"])[:80])}
                self._cache_set(cache_key, result)
                return result
            fp = res["file"]
            t = res["track"]
            thumb_data = t.get("thumb_data")
            if not thumb_data and t.get("thumbnail"):
                thumb_data = await VKThumbFetcher.download_image(t["thumbnail"])
            with open(fp, "rb") as f:
                audio_bytes = f.read()
            audio_inp = BufferedInputFile(audio_bytes, filename=os.path.basename(fp))
            thumb_inp = (
                BufferedInputFile(thumb_data, filename="cover.jpg")
                if thumb_data
                else None
            )
            sent = await self.inline_bot.send_audio(
                chat_id=user_id,
                audio=audio_inp,
                title=t["title"],
                performer=t["artist"],
                duration=t["duration"],
                thumbnail=thumb_inp,
            )
            if sent and sent.audio:
                try:
                    await self.inline_bot.delete_message(
                        chat_id=user_id, message_id=sent.message_id,
                    )
                except Exception:
                    pass
                result = {
                    "file_id": sent.audio.file_id,
                    "title": t["title"],
                    "artist": t["artist"],
                    "duration": t["duration"],
                }
                self._cache_set(cache_key, result)
                return result
            result = {"error": "Telegram upload failed"}
            self._cache_set(cache_key, result)
            return result
        except Exception as e:
            result = {"error": str(e)[:80]}
            self._cache_set(cache_key, result)
            return result
        finally:
            if ddir and os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    @loader.inline_handler(ru_doc="VK / Yandex Music / YouTube")
    async def music_inline_handler(self, query: InlineQuery):
        text = query.query.strip()
        if not text:
            await self._inline_hint(query)
            return
        source = detect_source(text)
        if not source:
            await self._inline_hint(query)
            return
        cache_key = self._make_cache_key(text)
        if not cache_key:
            await self._inline_hint(query)
            return
        if source == SOURCE_VK and not await self._ensure_vk_auth():
            await self._inline_msg(query, "VK not authorized", "Use .vkauth")
            return
        if source == SOURCE_YM and not await self._ensure_ym_auth():
            await self._inline_msg(query, "YM not authorized", "Use .ymauth")
            return

        self._cleanup_cache_db()

        cached = self._cache_get(cache_key)
        if cached:
            if "error" in cached:
                await self._inline_msg(query, "Error", cached["error"])
                return
            if "file_id" in cached:
                try:
                    await self.inline_bot.answer_inline_query(
                        inline_query_id=query.id,
                        results=[
                            InlineQueryResultCachedAudio(
                                id=f"{cache_key}_{int(time.time())}",
                                audio_file_id=cached["file_id"],
                            )
                        ],
                        cache_time=0,
                        is_personal=True,
                    )
                except Exception:
                    pass
                return

        if source == SOURCE_YT:
            await self._handle_yt_inline(query, text, cache_key)
        else:
            await self._handle_vk_ym_inline(query, text, cache_key)

    async def _handle_vk_ym_inline(self, query, text, cache_key):
        if cache_key not in self._pending_futures:
            self._pending_futures[cache_key] = asyncio.ensure_future(
                self._inline_dl_and_upload(text, query.from_user.id, cache_key)
            )
        fut = self._pending_futures[cache_key]
        try:
            result = await asyncio.wait_for(asyncio.shield(fut), timeout=25)
        except asyncio.TimeoutError:
            await self._inline_hint(query)
            return
        self._pending_futures.pop(cache_key, None)
        if "error" in result:
            await self._inline_msg(query, "Error", result["error"])
            return
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultCachedAudio(
                        id=f"{cache_key}_{int(time.time())}",
                        audio_file_id=result["file_id"],
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _handle_yt_inline(self, query, text, cache_key):
        if cache_key in self._pending_futures:
            fut = self._pending_futures[cache_key]
            if fut.done():
                self._pending_futures.pop(cache_key, None)
                try:
                    result = fut.result()
                except Exception:
                    result = {"error": "Internal error"}
                if "error" in result:
                    await self._inline_msg(query, "Error", result["error"])
                elif "file_id" in result:
                    try:
                        await self.inline_bot.answer_inline_query(
                            inline_query_id=query.id,
                            results=[
                                InlineQueryResultCachedAudio(
                                    id=f"{cache_key}_{int(time.time())}",
                                    audio_file_id=result["file_id"],
                                )
                            ],
                            cache_time=0,
                            is_personal=True,
                        )
                    except Exception:
                        pass
                return
            await self._inline_yt_wait(query, text)
            return

        self._pending_futures[cache_key] = asyncio.ensure_future(
            self._inline_dl_and_upload(text, query.from_user.id, cache_key)
        )
        await self._inline_yt_wait(query, text)

    async def _inline_yt_wait(self, query, text):
        try:
            vid = parse_yt_video_id(text)
            thumb = f"https://img.youtube.com/vi/{vid}/hqdefault.jpg" if vid else None
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"ytwait_{int(time.time())}",
                        title="YouTube: downloading track...",
                        description="Please wait ~15 sec and repeat the query",
                        input_message_content=InputTextMessageContent(
                            message_text=(
                                "<b>MusicX:</b> YouTube track is being downloaded. "
                                "Please wait and try again."
                            ),
                            parse_mode="HTML",
                        ),
                        thumbnail_url=thumb,
                        thumbnail_width=320,
                        thumbnail_height=180,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _inline_hint(self, query):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"hint_{int(time.time())}",
                        title="MusicX",
                        description="Paste a VK, Yandex Music or YouTube link",
                        input_message_content=InputTextMessageContent(
                            message_text=(
                                "<b>MusicX:</b> "
                                "Paste a link to VK audio, Yandex Music or YouTube"
                            ),
                            parse_mode="HTML",
                        ),
                        thumbnail_url=INLINE_QUERY_BANNER,
                        thumbnail_width=640,
                        thumbnail_height=360,
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _inline_msg(self, query, title, desc):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"msg_{int(time.time())}",
                        title=title,
                        description=desc,
                        input_message_content=InputTextMessageContent(
                            message_text=f"<b>MusicX:</b> {escape_html(desc)}",
                            parse_mode="HTML",
                        ),
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        for fut in self._pending_futures.values():
            fut.cancel()
        self._pending_futures.clear()
        if self._vk:
            await self._vk.close()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)