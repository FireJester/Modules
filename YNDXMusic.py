__version__ = (1, 3, 3)
# meta developer: FireJester.t.me

import os
import io
import re
import time
import json
import random
import string
import logging
import tempfile
import shutil
import asyncio
import subprocess
import sys
import traceback

from aiogram.types import (
    InlineQuery,
    InlineQueryResultCachedAudio,
    InlineQueryResultArticle,
    InputTextMessageContent,
    BufferedInputFile,
    InputMediaAudio,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChosenInlineResult,
    Update,
)

from telethon.tl.types import Message, DocumentAttributeAudio
from telethon import functions

from .. import loader, utils

logger = logging.getLogger(__name__)

INLINE_QUERY_BANNER = "https://github.com/FireJester/Media/raw/main/Banner_for_inline_query_in_YNDXMusic.jpeg"
DOWNLOADING_STUB = "https://github.com/FireJester/Media/raw/main/Downloading_in_YNDXMusic.mp3"

YM_CLIENT_ID = "23cabbbdc6cd418abb4b39c32c41195d"
YM_TOKEN_PATTERN = re.compile(r"access_token=([^&]+)")
YM_ALBUM_TRACK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/album/\d+/track/(\d+)"
)
YM_DIRECT_TRACK_RE = re.compile(
    r"https?://music\.yandex\.(?:ru|com|by|kz|uz)/track/(\d+)"
)

REQUEST_OK = 200
MAX_FILE_SIZE = 50 * 1024 * 1024
CACHE_TTL = 600

LOG_ENTRIES = []
MAX_LOG = 300


def _log(tag: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}] [{tag}] {msg}"
    LOG_ENTRIES.append(entry)
    if len(LOG_ENTRIES) > MAX_LOG:
        LOG_ENTRIES.pop(0)
    logger.info(entry)


def _ensure_all_deps():
    for mod, pip in {
        "aiohttp": "aiohttp",
        "mutagen": "mutagen",
        "yandex_music": "yandex-music",
        "PIL": "Pillow",
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
from PIL import Image

try:
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, ID3NoHeaderError
except ImportError:
    ID3 = None


def escape_html(t):
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sanitize_fn(n):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"


def parse_ym_track_id(text):
    if not text:
        return None
    for pat in [YM_ALBUM_TRACK_RE, YM_DIRECT_TRACK_RE]:
        m = pat.search(text)
        if m:
            return m.group(1)
    return None


def extract_ym_token(text):
    if not text:
        return None
    m = YM_TOKEN_PATTERN.search(text)
    return m.group(1) if m else None


def _build_ym_auth_url():
    return (
        f"https://oauth.yandex.ru/authorize?"
        f"response_type=token&client_id={YM_CLIENT_ID}"
    )


def _is_ym_link(text):
    if not text:
        return False
    return bool(YM_ALBUM_TRACK_RE.search(text) or YM_DIRECT_TRACK_RE.search(text))


def normalize_cover(raw_data, max_size=None, force_jpeg=False):
    if not raw_data or len(raw_data) < 100:
        return None
    try:
        img = Image.open(io.BytesIO(raw_data))
        w, h = img.size
        needs_resize = max_size is not None and (w > max_size or h > max_size)
        if force_jpeg:
            img = img.convert("RGB")
            if needs_resize:
                ratio = min(max_size / w, max_size / h)
                img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=95)
            result = buf.getvalue()
            return result if len(result) >= 100 else None
        is_png = raw_data[:8] == b'\x89PNG\r\n\x1a\n'
        if is_png and not needs_resize:
            return raw_data
        img = img.convert("RGB")
        if needs_resize:
            ratio = min(max_size / w, max_size / h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        result = buf.getvalue()
        return result if len(result) >= 100 else None
    except Exception:
        return None


async def _download_cover_ym(cover_uri, size="1000x1000"):
    if not cover_uri:
        return None
    url = f"https://{cover_uri.replace('%%', size)}"
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != REQUEST_OK:
                    return None
                data = await resp.read()
                return data if data and len(data) > 500 else None
    except Exception:
        return None


async def _convert_to_mp3(inp, out):
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-y", "-i", inp, "-vn", "-acodec", "libmp3lame", "-ab", "320k", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=60)
        return proc.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0
    except Exception:
        return False


async def _embed_cover(mp3_path, cover_path, out_path):
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-i", mp3_path, "-i", cover_path,
            "-map", "0:a", "-map", "1:0",
            "-c:a", "copy", "-c:v", "copy",
            "-id3v2_version", "3",
            "-metadata:s:v", "title=Cover",
            "-metadata:s:v", "comment=Cover (front)",
            "-disposition:v", "attached_pic", out_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=30)
        return proc.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0
    except Exception:
        return False


def _write_id3_tags(filepath, title, artist, album_title=None, cover_data=None):
    if not ID3:
        return
    try:
        try:
            tags = ID3(filepath)
        except ID3NoHeaderError:
            tags = ID3()
        tags.add(TIT2(encoding=3, text=[title]))
        tags.add(TPE1(encoding=3, text=[artist]))
        if album_title:
            tags.add(TALB(encoding=3, text=[album_title]))
        if cover_data and len(cover_data) > 100:
            is_png = cover_data[:8] == b'\x89PNG\r\n\x1a\n'
            mime = "image/png" if is_png else "image/jpeg"
            tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=cover_data))
        tags.save(filepath)
    except Exception:
        pass


async def _upload_to_x0(data: bytes, filename: str, content_type: str = "audio/mpeg") -> str:
    try:
        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type=content_type)
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://x0.at",
                data=form,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                text = (await r.text()).strip()
                if text.startswith("http"):
                    return text
    except Exception:
        pass
    return ""


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
        if not token:
            self.reset()
            return False
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
            self.reset()
            return False

    def reset(self):
        self._token = None
        self._client = None
        self._ok = False
        self._uid = None
        self._login = None

    async def fetch_track(self, track_id):
        if not self._client:
            return None
        try:
            tracks = await self._client.tracks(track_id, with_positions=False)
            return tracks[0] if tracks else None
        except Exception:
            return None

    async def download_track_file(self, track, filepath):
        try:
            await track.download_async(filepath)
            return os.path.exists(filepath) and os.path.getsize(filepath) > 0
        except Exception:
            return False

    async def search_track(self, query, count=5):
        if not self._client:
            return []
        try:
            result = await self._client.search(query, type_="track")
            if not result or not result.tracks or not result.tracks.results:
                return []
            return result.tracks.results[:count]
        except Exception:
            try:
                self._client = None
                self._ok = False
                await self.auth(self._token)
                result = await self._client.search(query, type_="track")
                if not result or not result.tracks or not result.tracks.results:
                    return []
                return result.tracks.results[:count]
            except Exception:
                return []

    @staticmethod
    def track_artist(track):
        if track.artists:
            return ", ".join(a.name for a in track.artists if a.name) or "Unknown"
        return "Unknown"

    @staticmethod
    def track_title(track):
        return track.title or "Unknown"

    @staticmethod
    def cover_url(cover_uri, size="200x200"):
        if not cover_uri:
            return None
        return f"https://{cover_uri.replace('%%', size)}"


@loader.tds
class YNDXMusic(loader.Module):
    """Yandex Music audio downloader and search"""

    strings = {
        "name": "YNDXMusic",
        "auth_instruction": (
            "<b>YNDXMusic - Authorization</b>\n\n"
            "<blockquote>"
            "1. Open the link below\n"
            "2. Sign in and grant permissions\n"
            "3. Copy the full URL from the address bar\n"
            "4. Paste it: <code>{prefix}ymauth URL</code>"
            "</blockquote>\n\n"
            '<a href="{ym_url}">Authorize via Yandex</a>'
        ),
        "token_ok": (
            "<b>Yandex Music authorized!</b>\n\n"
            "<blockquote>UID: <code>{uid}</code>\n"
            "Login: <code>{login}</code></blockquote>"
        ),
        "token_fail": "<b>Token is invalid!</b>",
        "token_bad_format": "<b>Wrong format!</b> Provide the full URL or token.",
        "no_token": "<b>Not authorized.</b> Use <code>{prefix}ymauth</code>",
        "no_playing": "<b>Nothing is playing right now</b>",
        "fetching": "<b>Fetching current track...</b>",
        "uploading": "<b>Uploading...</b>",
        "error": "<b>Error:</b> {msg}",
        "banner_text": "<b>{title}</b>\n<blockquote>{artist}\n{device}</blockquote>",
        "track_text": "<b>{title}</b>\n<blockquote>{artist}\n{device}</blockquote>",
        "not_authorized_inline": "Not authorized",
        "not_authorized_inline_desc": "Use .ymauth to authorize",
    }

    strings_ru = {
        "name": "YNDXMusic",
        "auth_instruction": (
            "<b>YNDXMusic - \u0410\u0432\u0442\u043e\u0440\u0438\u0437\u0430\u0446\u0438\u044f</b>\n\n"
            "<blockquote>"
            "1. \u041e\u0442\u043a\u0440\u043e\u0439\u0442\u0435 \u0441\u0441\u044b\u043b\u043a\u0443 \u043d\u0438\u0436\u0435\n"
            "2. \u0412\u043e\u0439\u0434\u0438\u0442\u0435 \u0432 \u0430\u043a\u043a\u0430\u0443\u043d\u0442 \u0438 \u0434\u0430\u0439\u0442\u0435 \u0440\u0430\u0437\u0440\u0435\u0448\u0435\u043d\u0438\u044f\n"
            "3. \u0421\u043a\u043e\u043f\u0438\u0440\u0443\u0439\u0442\u0435 \u043f\u043e\u043b\u043d\u044b\u0439 URL \u0438\u0437 \u0430\u0434\u0440\u0435\u0441\u043d\u043e\u0439 \u0441\u0442\u0440\u043e\u043a\u0438\n"
            "4. \u0412\u0441\u0442\u0430\u0432\u044c\u0442\u0435 \u0435\u0433\u043e: <code>{prefix}ymauth URL</code>"
            "</blockquote>\n\n"
            '<a href="{ym_url}">\u0410\u0432\u0442\u043e\u0440\u0438\u0437\u0430\u0446\u0438\u044f \u0447\u0435\u0440\u0435\u0437 \u042f\u043d\u0434\u0435\u043a\u0441</a>'
        ),
        "token_ok": (
            "<b>Yandex Music \u0430\u0432\u0442\u043e\u0440\u0438\u0437\u043e\u0432\u0430\u043d!</b>\n\n"
            "<blockquote>UID: <code>{uid}</code>\n"
            "\u041b\u043e\u0433\u0438\u043d: <code>{login}</code></blockquote>"
        ),
        "token_fail": "<b>\u0422\u043e\u043a\u0435\u043d \u043d\u0435\u0434\u0435\u0439\u0441\u0442\u0432\u0438\u0442\u0435\u043b\u0435\u043d!</b>",
        "token_bad_format": "<b>\u041d\u0435\u0432\u0435\u0440\u043d\u044b\u0439 \u0444\u043e\u0440\u043c\u0430\u0442!</b> \u0423\u043a\u0430\u0436\u0438\u0442\u0435 \u043f\u043e\u043b\u043d\u044b\u0439 URL \u0438\u043b\u0438 \u0442\u043e\u043a\u0435\u043d.",
        "no_token": "<b>\u041d\u0435 \u0430\u0432\u0442\u043e\u0440\u0438\u0437\u043e\u0432\u0430\u043d.</b> \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 <code>{prefix}ymauth</code>",
        "no_playing": "<b>\u0421\u0435\u0439\u0447\u0430\u0441 \u043d\u0438\u0447\u0435\u0433\u043e \u043d\u0435 \u0438\u0433\u0440\u0430\u0435\u0442</b>",
        "fetching": "<b>\u041f\u043e\u043b\u0443\u0447\u0435\u043d\u0438\u0435 \u0442\u0435\u043a\u0443\u0449\u0435\u0433\u043e \u0442\u0440\u0435\u043a\u0430...</b>",
        "uploading": "<b>\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430...</b>",
        "error": "<b>\u041e\u0448\u0438\u0431\u043a\u0430:</b> {msg}",
        "banner_text": "<b>{title}</b>\n<blockquote>{artist}\n{device}</blockquote>",
        "track_text": "<b>{title}</b>\n<blockquote>{artist}\n{device}</blockquote>",
        "not_authorized_inline": "\u041d\u0435 \u0430\u0432\u0442\u043e\u0440\u0438\u0437\u043e\u0432\u0430\u043d",
        "not_authorized_inline_desc": "\u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 .ymauth \u0434\u043b\u044f \u0430\u0432\u0442\u043e\u0440\u0438\u0437\u0430\u0446\u0438\u0438",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "YM_TOKEN", "",
                "Yandex Music access token",
                validator=loader.validators.Hidden(),
            ),
            loader.ConfigValue(
                "SEARCH_LIMIT", 5,
                "Search results limit (1-10)",
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
        )
        self.inline_bot = None
        self.inline_bot_username = None
        self._tmp = None
        self._ym = None
        self._upload_lock = None
        self._patched = False
        self._real_cache = {}
        self._stub_cache = {}
        self._search_cache = {}
        self._link_cache = {}
        self._now_track_id = None
        self._now_mp3_url = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._upload_lock = asyncio.Lock()
        me = await client.get_me()
        self._me_id = me.id
        self._tmp = os.path.join(tempfile.gettempdir(), f"YNDXMusic_{me.id}")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)
        self._ym = YMApiClient()
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
            try:
                bi = await self.inline_bot.get_me()
                self.inline_bot_username = bi.username
            except Exception:
                pass
        await self._ensure_ym()
        await self._unpatch_feed_update()
        self._patch_feed_update()

    def _patch_feed_update(self):
        if self._patched:
            _log("PATCH", "Already patched, skipping")
            return
        try:
            dp = self.inline._dp
            if hasattr(dp.feed_update, "_is_patched_yndx"):
                _log("PATCH", "feed_update already has our patch")
                self._patched = True
                return
            original_feed = dp.feed_update
            dp._yndx_original_feed_update = original_feed
            module_self = self

            async def patched_feed(bot_inst, update: Update, **kw):
                if (
                    hasattr(update, "chosen_inline_result")
                    and update.chosen_inline_result is not None
                ):
                    chosen = update.chosen_inline_result
                    _log(
                        "CHOSEN_RAW",
                        f"result_id={chosen.result_id!r} "
                        f"imid={chosen.inline_message_id!r} "
                        f"user={chosen.from_user.id}"
                    )
                    asyncio.ensure_future(
                        module_self._on_chosen_inline_result(chosen)
                    )
                return await original_feed(bot_inst, update, **kw)

            patched_feed._is_patched_yndx = True
            dp.feed_update = patched_feed
            self._patched = True
            _log("PATCH", "feed_update patched OK")
        except Exception as e:
            _log("PATCH", f"Patch failed: {e}\n{traceback.format_exc()}")

    async def _unpatch_feed_update(self):
        if not self._patched:
            return
        try:
            dp = self.inline._dp
            if hasattr(dp, "_yndx_original_feed_update"):
                dp.feed_update = dp._yndx_original_feed_update
                del dp._yndx_original_feed_update
            self._patched = False
            _log("PATCH", "feed_update unpatched OK")
        except Exception as e:
            _log("PATCH", f"Unpatch failed: {e}")

    async def _on_chosen_inline_result(self, chosen: ChosenInlineResult):
        rid = chosen.result_id
        imid = chosen.inline_message_id
        user_id = chosen.from_user.id
        _log("CHOSEN", f"result_id={rid!r} inline_message_id={imid!r} user={user_id}")
        if not rid.startswith("ym_"):
            _log("CHOSEN", "Not our result_id, skipping")
            return
        if not imid:
            _log("CHOSEN", "inline_message_id is None - cannot edit!")
            return
        track_id = rid[3:]
        _log("CHOSEN", f"track_id={track_id!r}")
        if track_id in self._real_cache:
            _log("CHOSEN", "Found in real_cache, replacing immediately")
            await self._do_replace(imid, self._real_cache[track_id])
            return
        asyncio.ensure_future(
            self._bg_download_and_replace(track_id, user_id, imid)
        )

    async def _bg_download_and_replace(self, track_id, user_id, imid):
        _log("BG", f"Start download track_id={track_id}")
        try:
            result = await self._ym_download_and_upload(track_id, user_id)
            if "error" in result or "file_id" not in result:
                _log("BG", f"Download failed: {result.get('error')}")
                return
            data = (
                result["file_id"],
                result["title"],
                result["artist"],
                result["duration"],
            )
            self._real_cache[track_id] = data
            _log("BG", f"Done: file_id={data[0]!r} title={data[1]!r}")
            await self._do_replace(imid, data)
        except Exception as e:
            _log("BG", f"Exception: {e}\n{traceback.format_exc()}")

    async def _do_replace(self, imid, data):
        file_id, title, artist, duration = data
        _log(
            "REPLACE",
            f"edit_message_media imid={imid!r} "
            f"file_id={file_id!r} title={title!r} artist={artist!r} dur={duration}"
        )
        try:
            await self.inline_bot.edit_message_media(
                inline_message_id=imid,
                media=InputMediaAudio(
                    media=file_id,
                    title=title,
                    performer=artist,
                    duration=duration,
                ),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[]),
            )
            _log("REPLACE", "SUCCESS attempt 1 (empty markup)")
            return
        except Exception as e:
            _log("REPLACE", f"Attempt 1 failed: {e}")
        try:
            await self.inline_bot.edit_message_media(
                inline_message_id=imid,
                media=InputMediaAudio(
                    media=file_id,
                    title=title,
                    performer=artist,
                    duration=duration,
                ),
            )
            _log("REPLACE", "SUCCESS attempt 2 (no markup)")
            return
        except Exception as e:
            _log("REPLACE", f"Attempt 2 failed: {e}")
        try:
            await self.inline_bot.edit_message_media(
                inline_message_id=imid,
                media=InputMediaAudio(media=file_id),
            )
            _log("REPLACE", "SUCCESS attempt 3 (bare file_id)")
        except Exception as e:
            _log("REPLACE", f"Attempt 3 failed: {e}\n{traceback.format_exc()}")

    async def _get_stub_file_id(self, track_id, title, artist, cover_uri):
        if track_id in self._stub_cache:
            return self._stub_cache[track_id]
        _log("STUB", f"Creating stub for {track_id} ({artist} - {title})")
        stub_bytes = b""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    DOWNLOADING_STUB, timeout=aiohttp.ClientTimeout(total=20)
                ) as r:
                    if r.status == REQUEST_OK:
                        stub_bytes = await r.read()
        except Exception as e:
            _log("STUB", f"Stub audio download failed: {e}")
        if not stub_bytes:
            _log("STUB", "No stub bytes, returning None")
            return None
        thumb_data = None
        if cover_uri:
            raw = await _download_cover_ym(cover_uri, "300x300")
            if raw:
                thumb_data = normalize_cover(raw, max_size=320)
        try:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Downloading...",
                    callback_data=f"yndx_dl_{track_id[:32]}"
                )
            ]])
            sent = await self.inline_bot.send_audio(
                chat_id=self._me_id,
                audio=BufferedInputFile(stub_bytes, filename="Downloading.mp3"),
                title=title,
                performer=artist,
                thumbnail=(
                    BufferedInputFile(thumb_data, filename="cover.jpg")
                    if thumb_data else None
                ),
                reply_markup=kb,
            )
            if sent and sent.audio:
                fid = sent.audio.file_id
                self._stub_cache[track_id] = fid
                _log("STUB", f"Stub created: file_id={fid!r}")
                try:
                    await self.inline_bot.delete_message(
                        chat_id=self._me_id,
                        message_id=sent.message_id,
                    )
                except Exception:
                    pass
                return fid
        except Exception as e:
            _log("STUB", f"send_audio failed: {e}\n{traceback.format_exc()}")
        return None

    async def _ensure_ym(self):
        token = self.config["YM_TOKEN"]
        if not token:
            self._ym.reset()
            return False
        if self._ym.ok and self._ym._token == token:
            return True
        return await self._ym.auth(token)

    def _get_limit(self):
        try:
            return max(1, min(10, int(self.config["SEARCH_LIMIT"])))
        except Exception:
            return 5

    async def _get_ynison(self):
        token = self._ym._token
        device_id = "".join(random.choices(string.ascii_lowercase, k=16))
        ws_proto = {
            "Ynison-Device-Id": device_id,
            "Ynison-Device-Info": json.dumps({"app_name": "Chrome", "type": 1}),
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    "wss://ynison.music.yandex.ru/redirector.YnisonRedirectService/GetRedirectToYnison",
                    headers={
                        "Sec-WebSocket-Protocol": f"Bearer, v2, {json.dumps(ws_proto)}",
                        "Origin": "http://music.yandex.ru",
                        "Authorization": f"OAuth {token}",
                    },
                ) as ws:
                    resp = await ws.receive()
                    data = json.loads(resp.data)
            ws_proto["Ynison-Redirect-Ticket"] = data["redirect_ticket"]
            payload = {
                "update_full_state": {
                    "player_state": {
                        "player_queue": {
                            "current_playable_index": -1,
                            "entity_id": "",
                            "entity_type": "VARIOUS",
                            "playable_list": [],
                            "options": {"repeat_mode": "NONE"},
                            "entity_context": "BASED_ON_ENTITY_BY_DEFAULT",
                            "version": {
                                "device_id": device_id,
                                "version": 9021243204784341000,
                                "timestamp_ms": 0,
                            },
                            "from_optional": "",
                        },
                        "status": {
                            "duration_ms": 0,
                            "paused": True,
                            "playback_speed": 1,
                            "progress_ms": 0,
                            "version": {
                                "device_id": device_id,
                                "version": 8321822175199937000,
                                "timestamp_ms": 0,
                            },
                        },
                    },
                    "device": {
                        "capabilities": {
                            "can_be_player": True,
                            "can_be_remote_controller": False,
                            "volume_granularity": 16,
                        },
                        "info": {
                            "device_id": device_id,
                            "type": "WEB",
                            "title": "Chrome Browser",
                            "app_name": "Chrome",
                        },
                        "volume_info": {"volume": 0},
                        "is_shadow": True,
                    },
                    "is_currently_active": False,
                },
                "rid": "ac281c26-a047-4419-ad00-e4fbfda1cba3",
                "player_action_timestamp_ms": 0,
                "activity_interception_type": "DO_NOT_INTERCEPT_BY_DEFAULT",
            }
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    f"wss://{data['host']}/ynison_state.YnisonStateService/PutYnisonState",
                    headers={
                        "Sec-WebSocket-Protocol": f"Bearer, v2, {json.dumps(ws_proto)}",
                        "Origin": "http://music.yandex.ru",
                        "Authorization": f"OAuth {token}",
                    },
                ) as ws:
                    await ws.send_str(json.dumps(payload))
                    resp = await ws.receive()
                    return json.loads(resp.data)
        except Exception:
            return {}

    async def _get_now_playing_track(self):
        if not await self._ensure_ym():
            return None
        try:
            ynison = await self._get_ynison()
            if not ynison:
                return None
            player_state = ynison.get("player_state", {})
            queue = player_state.get("player_queue", {})
            playable_list = queue.get("playable_list", [])
            idx = queue.get("current_playable_index", -1)
            if not playable_list or idx < 0 or idx >= len(playable_list):
                return None
            raw_track = playable_list[idx]
            if raw_track.get("playable_type") == "LOCAL_TRACK":
                return None
            playable_id = raw_track.get("playable_id")
            if not playable_id:
                return None
            track_obj = (await self._ym._client.tracks(playable_id))[0]
            status = player_state.get("status", {})
            device_title = None
            volume = None
            try:
                active_id = ynison.get("active_device_id_optional", "")
                for dev in ynison.get("devices", []):
                    if dev.get("info", {}).get("device_id") == active_id:
                        device_title = dev.get("info", {}).get("title")
                        vol_raw = dev.get("volume_info", {}).get("volume")
                        if vol_raw is not None:
                            volume = round(vol_raw * 100)
                        break
            except Exception:
                pass
            return {
                "track": track_obj,
                "progress_ms": int(status.get("progress_ms", 0)),
                "duration_ms": int(status.get("duration_ms", 0)),
                "paused": status.get("paused", True),
                "playable_id": playable_id,
                "device_title": device_title,
                "volume": volume,
            }
        except Exception:
            return None

    def _device_str(self, now):
        device_title = now.get("device_title")
        volume = now.get("volume")
        if not device_title:
            return ""
        if volume is not None:
            return f"{device_title} | {volume}%"
        return device_title

    async def _upload_audio_to_tg(self, file_bytes, filename, title, artist, dur_s, thumb_data, user_id):
        async with self._upload_lock:
            audio_inp = BufferedInputFile(file_bytes, filename=filename)
            thumb_inp = None
            if thumb_data:
                is_png = thumb_data[:8] == b'\x89PNG\r\n\x1a\n'
                thumb_ext = "cover.png" if is_png else "cover.jpg"
                thumb_inp = BufferedInputFile(thumb_data, filename=thumb_ext)
            try:
                sent = await self.inline_bot.send_audio(
                    chat_id=user_id,
                    audio=audio_inp,
                    title=title,
                    performer=artist,
                    duration=dur_s,
                    thumbnail=thumb_inp,
                )
            except Exception as e:
                _log("UPLOAD", f"send_audio failed: {e}")
                return None
            if sent and sent.audio:
                file_id = sent.audio.file_id
                msg_id = sent.message_id
                await asyncio.sleep(0.5)
                for attempt in range(5):
                    try:
                        await self.inline_bot.delete_message(chat_id=user_id, message_id=msg_id)
                        break
                    except Exception:
                        await asyncio.sleep(1.0 * (attempt + 1))
                return file_id
            return None

    async def _prepare_track_file(self, track, ddir, with_cover=False):
        artist = YMApiClient.track_artist(track)
        title = YMApiClient.track_title(track)
        album_title = track.albums[0].title if track.albums else ""
        dur_s = (track.duration_ms or 0) // 1000
        raw_path = os.path.join(ddir, "raw_track")
        dl_ok = await self._ym.download_track_file(track, raw_path)
        cover_data = None
        thumb_data = None
        if with_cover and track.cover_uri:
            raw_cover = await _download_cover_ym(track.cover_uri, "1000x1000")
            if raw_cover:
                cover_data = normalize_cover(raw_cover)
                thumb_data = normalize_cover(raw_cover, max_size=320)
        if not dl_ok or os.path.getsize(raw_path) == 0:
            return None, "Download failed"
        if os.path.getsize(raw_path) > MAX_FILE_SIZE:
            return None, "File > 50 MB"
        clean_name = sanitize_fn(f"{artist} - {title}")
        final_mp3 = os.path.join(ddir, f"{clean_name}.mp3")
        try:
            with open(raw_path, "rb") as rf:
                header = rf.read(4)
            is_mp3 = header[:3] == b"ID3" or header[:2] in (b"\xff\xfb", b"\xff\xf3")
        except Exception:
            is_mp3 = True
        if is_mp3:
            try:
                os.rename(raw_path, final_mp3)
            except Exception:
                final_mp3 = raw_path
        else:
            ok = await _convert_to_mp3(raw_path, final_mp3)
            if ok:
                try:
                    os.remove(raw_path)
                except Exception:
                    pass
            else:
                final_mp3 = raw_path
        if os.path.getsize(final_mp3) > MAX_FILE_SIZE:
            return None, "File > 50 MB"
        if with_cover and cover_data and final_mp3.endswith(".mp3"):
            cover_path = os.path.join(ddir, "cover.png")
            with open(cover_path, "wb") as cf:
                cf.write(cover_data)
            covered_mp3 = os.path.join(ddir, f"{clean_name}_cover.mp3")
            embed_ok = await _embed_cover(final_mp3, cover_path, covered_mp3)
            if embed_ok:
                try:
                    os.remove(final_mp3)
                except Exception:
                    pass
                final_mp3 = covered_mp3
            else:
                _write_id3_tags(final_mp3, title, artist, album_title if album_title else None, cover_data)
        elif final_mp3.endswith(".mp3"):
            _write_id3_tags(final_mp3, title, artist, album_title if album_title else None, None)
        return {
            "path": final_mp3,
            "title": title,
            "artist": artist,
            "album_title": album_title,
            "dur_s": dur_s,
            "cover_data": cover_data,
            "thumb_data": thumb_data,
        }, None

    async def _ym_download_and_upload(self, track_id_str, user_id):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            if not await self._ensure_ym():
                return {"error": "Not authorized"}
            track = await self._ym.fetch_track(track_id_str)
            if not track:
                return {"error": "Track not found"}
            info, err = await self._prepare_track_file(track, ddir, with_cover=True)
            if err:
                return {"error": err}
            with open(info["path"], "rb") as f:
                audio_bytes = f.read()
            file_id = await self._upload_audio_to_tg(
                audio_bytes,
                os.path.basename(info["path"]),
                info["title"],
                info["artist"],
                info["dur_s"],
                info["thumb_data"],
                user_id,
            )
            if file_id:
                return {
                    "file_id": file_id,
                    "title": info["title"],
                    "artist": info["artist"],
                    "duration": info["dur_s"],
                }
            return {"error": "Telegram upload failed"}
        except Exception as e:
            return {"error": str(e)[:80]}
        finally:
            if os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    @loader.command(
        ru_doc="\u0410\u0432\u0442\u043e\u0440\u0438\u0437\u0430\u0446\u0438\u044f Yandex Music",
        en_doc="Yandex Music authorization",
    )
    async def ymauth(self, message: Message):
        """Yandex Music authorization"""
        prefix = self.get_prefix()
        args = utils.get_args_raw(message).strip()
        if not args:
            await utils.answer(
                message,
                self.strings["auth_instruction"].format(
                    prefix=prefix,
                    ym_url=_build_ym_auth_url(),
                ),
            )
            return
        token = extract_ym_token(args)
        if not token:
            if not args.startswith("http") and len(args) > 10:
                token = args
            else:
                await utils.answer(message, self.strings["token_bad_format"])
                return
        try:
            await message.delete()
        except Exception:
            pass
        ok = await self._ym.auth(token)
        if ok:
            self.config["YM_TOKEN"] = token
            await self._client.send_message(
                message.chat_id,
                self.strings["token_ok"].format(
                    uid=self._ym._uid or "?",
                    login=self._ym._login or "?",
                ),
                parse_mode="html",
            )
        else:
            await self._client.send_message(
                message.chat_id,
                self.strings["token_fail"],
                parse_mode="html",
            )

    @loader.command(
        ru_doc="\u0411\u0430\u043d\u043d\u0435\u0440 \u0442\u0435\u043a\u0443\u0449\u0435\u0433\u043e \u0442\u0440\u0435\u043a\u0430",
        en_doc="Banner of current track",
    )
    async def ymb(self, message: Message):
        """Banner of current track"""
        prefix = self.get_prefix()
        if not await self._ensure_ym():
            await utils.answer(message, self.strings["no_token"].format(prefix=prefix))
            return
        msg = await utils.answer(message, self.strings["fetching"])
        now = await self._get_now_playing_track()
        if not now:
            await utils.answer(msg, self.strings["no_playing"])
            return
        track = now["track"]
        artist = YMApiClient.track_artist(track)
        title = YMApiClient.track_title(track)
        device = self._device_str(now)
        playable_id = now["playable_id"]
        await utils.answer(msg, self.strings["uploading"])
        if self._now_track_id != playable_id:
            self._now_track_id = playable_id
            self._now_mp3_url = None
        if not track.cover_uri:
            await utils.answer(msg, self.strings["error"].format(msg="No cover"))
            return
        cover_data = await _download_cover_ym(track.cover_uri, "1000x1000")
        if not cover_data:
            await utils.answer(msg, self.strings["error"].format(msg="No cover"))
            return
        filename = sanitize_fn(f"{artist} - {title}") + ".jpg"
        cover_url = await _upload_to_x0(cover_data, filename, "image/jpeg")
        if not cover_url:
            await utils.answer(msg, self.strings["error"].format(msg="Upload failed"))
            return
        try:
            await self._client(functions.messages.GetWebPageRequest(url=cover_url, hash=0))
        except Exception:
            pass
        await asyncio.sleep(1)
        text = self.strings["banner_text"].format(
            title=escape_html(title),
            artist=escape_html(artist),
            device=escape_html(device),
        )
        try:
            from telethon.tl.types import InputMediaWebPage
            await msg.edit(
                text,
                file=InputMediaWebPage(cover_url, optional=True),
                parse_mode="html",
                link_preview=True,
                invert_media=True,
            )
        except Exception:
            await utils.answer(msg, text)

    @loader.command(
        ru_doc="\u041f\u0440\u0435\u0432\u044c\u044e \u0442\u0435\u043a\u0443\u0449\u0435\u0433\u043e \u0442\u0440\u0435\u043a\u0430. \u0424\u043b\u0430\u0433 -f \u2014 \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u0444\u0430\u0439\u043b",
        en_doc="Preview of current track. Flag -f to send file",
    )
    async def ymt(self, message: Message):
        """Preview of current track. Use -f to send audio file."""
        prefix = self.get_prefix()
        args = utils.get_args_raw(message).strip()
        send_file_only = "-f" in args.split()
        if not await self._ensure_ym():
            await utils.answer(message, self.strings["no_token"].format(prefix=prefix))
            return
        msg = await utils.answer(message, self.strings["fetching"])
        now = await self._get_now_playing_track()
        if not now:
            await utils.answer(msg, self.strings["no_playing"])
            return
        track = now["track"]
        artist = YMApiClient.track_artist(track)
        title = YMApiClient.track_title(track)
        device = self._device_str(now)
        playable_id = now["playable_id"]
        await utils.answer(msg, self.strings["uploading"])
        if self._now_track_id != playable_id:
            self._now_track_id = playable_id
            self._now_mp3_url = None
        if send_file_only:
            ddir = tempfile.mkdtemp(dir=self._tmp)
            try:
                info, err = await self._prepare_track_file(track, ddir, with_cover=True)
                if err:
                    await utils.answer(msg, self.strings["error"].format(msg=err))
                    return
                with open(info["path"], "rb") as f:
                    mp3_bytes = f.read()
                try:
                    await msg.delete()
                except Exception:
                    pass
                caption = self.strings["track_text"].format(
                    title=escape_html(title),
                    artist=escape_html(artist),
                    device=escape_html(device),
                )
                sent = False
                try:
                    audio_buf = io.BytesIO(mp3_bytes)
                    audio_buf.name = os.path.basename(info["path"])
                    thumb_buf = None
                    if info.get("thumb_data"):
                        thumb_buf = io.BytesIO(info["thumb_data"])
                        is_png = info["thumb_data"][:8] == b'\x89PNG\r\n\x1a\n'
                        thumb_buf.name = "cover.png" if is_png else "cover.jpg"
                    await self._client.send_file(
                        message.chat_id,
                        file=audio_buf,
                        caption=caption,
                        parse_mode="html",
                        attributes=[
                            DocumentAttributeAudio(
                                duration=info["dur_s"],
                                title=title,
                                performer=artist,
                            )
                        ],
                        thumb=thumb_buf,
                        voice=False,
                    )
                    sent = True
                except Exception as e:
                    _log("YMT_F", f"Telethon send_file failed: {e}")
                if not sent and self.inline_bot:
                    try:
                        thumb_inp = None
                        if info.get("thumb_data"):
                            is_png = info["thumb_data"][:8] == b'\x89PNG\r\n\x1a\n'
                            thumb_ext = "cover.png" if is_png else "cover.jpg"
                            thumb_inp = BufferedInputFile(info["thumb_data"], filename=thumb_ext)
                        await self.inline_bot.send_audio(
                            chat_id=message.chat_id,
                            audio=BufferedInputFile(
                                mp3_bytes,
                                filename=os.path.basename(info["path"]),
                            ),
                            title=title,
                            performer=artist,
                            duration=info["dur_s"],
                            thumbnail=thumb_inp,
                            caption=caption,
                            parse_mode="HTML",
                        )
                        sent = True
                    except Exception as e:
                        _log("YMT_F", f"inline_bot.send_audio failed: {e}")
                if not sent:
                    await self._client.send_message(
                        message.chat_id,
                        self.strings["error"].format(msg="Could not send audio file"),
                        parse_mode="html",
                    )
            finally:
                if os.path.exists(ddir):
                    shutil.rmtree(ddir, ignore_errors=True)
            return
        if self._now_mp3_url:
            mp3_url = self._now_mp3_url
        else:
            ddir = tempfile.mkdtemp(dir=self._tmp)
            try:
                info, err = await self._prepare_track_file(track, ddir, with_cover=False)
                if err:
                    await utils.answer(msg, self.strings["error"].format(msg=err))
                    return
                with open(info["path"], "rb") as f:
                    mp3_bytes = f.read()
                filename = sanitize_fn(f"{artist} - {title}") + ".mp3"
                mp3_url = await _upload_to_x0(mp3_bytes, filename, "audio/mpeg")
                if not mp3_url:
                    await utils.answer(msg, self.strings["error"].format(msg="Upload to x0.at failed"))
                    return
                self._now_mp3_url = mp3_url
            finally:
                if os.path.exists(ddir):
                    shutil.rmtree(ddir, ignore_errors=True)
        try:
            await self._client(functions.messages.GetWebPageRequest(url=mp3_url, hash=0))
        except Exception:
            pass
        await asyncio.sleep(1)
        text = self.strings["track_text"].format(
            title=escape_html(title),
            artist=escape_html(artist),
            device=escape_html(device),
        )
        try:
            from telethon.tl.types import InputMediaWebPage
            await msg.edit(
                text,
                file=InputMediaWebPage(mp3_url, optional=True),
                parse_mode="html",
                link_preview=True,
                invert_media=True,
            )
        except Exception:
            await utils.answer(msg, text)

    @loader.inline_handler(
        ru_doc="Yandex Music - \u043f\u043e\u0438\u0441\u043a \u0438\u043b\u0438 \u0441\u0441\u044b\u043b\u043a\u0430",
        en_doc="Yandex Music - search or link",
    )
    async def ym_inline_handler(self, query: InlineQuery):
        """Yandex Music - search or link"""
        raw = query.query.strip()
        text = raw[2:].strip() if raw.lower().startswith("ym") else raw.strip()
        _log("INLINE", f"query={text!r} from={query.from_user.id}")
        if not text:
            await self._inline_hint(query)
            return
        if not await self._ensure_ym():
            try:
                await self.inline_bot.answer_inline_query(
                    inline_query_id=query.id,
                    results=[InlineQueryResultArticle(
                        id="noauth",
                        title=self.strings["not_authorized_inline"],
                        description=self.strings["not_authorized_inline_desc"],
                        input_message_content=InputTextMessageContent(
                            message_text=self.strings["not_authorized_inline"],
                        ),
                    )],
                    cache_time=0,
                    is_personal=True,
                )
            except Exception:
                pass
            return
        if _is_ym_link(text):
            await self._handle_link_inline(query, text)
        else:
            await self._handle_search_inline(query, text)

    async def _handle_link_inline(self, query: InlineQuery, text: str):
        track_id = parse_ym_track_id(text)
        if not track_id:
            await self._inline_hint(query)
            return
        if track_id in self._real_cache:
            fid, title, artist, duration = self._real_cache[track_id]
            _log("INLINE_LINK", f"Cache hit for {track_id}")
            try:
                await self.inline_bot.answer_inline_query(
                    inline_query_id=query.id,
                    results=[InlineQueryResultCachedAudio(
                        id=f"ym_{track_id}",
                        audio_file_id=fid,
                    )],
                    cache_time=0,
                    is_personal=True,
                )
            except Exception:
                pass
            return
        track = await self._ym.fetch_track(track_id)
        if not track:
            await self._inline_msg(query, "Error", "Track not found")
            return
        title = YMApiClient.track_title(track)
        artist = YMApiClient.track_artist(track)
        stub_fid = await self._get_stub_file_id(track_id, title, artist, track.cover_uri)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Downloading...",
                callback_data=f"yndx_dl_{track_id[:32]}"
            )
        ]])
        if stub_fid:
            try:
                await self.inline_bot.answer_inline_query(
                    inline_query_id=query.id,
                    results=[InlineQueryResultCachedAudio(
                        id=f"ym_{track_id}",
                        audio_file_id=stub_fid,
                        reply_markup=kb,
                    )],
                    cache_time=0,
                    is_personal=True,
                )
            except Exception as e:
                _log("INLINE_LINK", f"answer_inline_query failed: {e}")
        else:
            await self._inline_msg(query, "Downloading...", f"{artist} - {title}")

    async def _handle_search_inline(self, query: InlineQuery, text: str):
        limit = self._get_limit()
        cache_key = f"search_{text.lower().replace(' ', '_')[:60]}"
        if cache_key not in self._search_cache:
            tracks = await self._ym.search_track(text, count=limit)
            if not tracks:
                await self._inline_msg(query, "Not found", f"No results for: {text}")
                return
            self._search_cache[cache_key] = tracks
        tracks = self._search_cache[cache_key]
        stub_results = await asyncio.gather(*[
            asyncio.ensure_future(self._get_stub_file_id(
                str(t.track_id),
                YMApiClient.track_title(t),
                YMApiClient.track_artist(t),
                t.cover_uri,
            ))
            for t in tracks
        ], return_exceptions=True)
        results = []
        for i, track in enumerate(tracks):
            tid = str(track.track_id)
            title = YMApiClient.track_title(track)
            artist = YMApiClient.track_artist(track)
            stub_fid = stub_results[i] if not isinstance(stub_results[i], Exception) else None
            _log("INLINE_SEARCH", f"Track {i}: tid={tid} stub_fid={stub_fid!r}")
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Downloading...",
                    callback_data=f"yndx_dl_{tid[:32]}"
                )
            ]])
            if tid in self._real_cache:
                fid = self._real_cache[tid][0]
                results.append(InlineQueryResultCachedAudio(
                    id=f"ym_{tid}",
                    audio_file_id=fid,
                ))
            elif stub_fid:
                results.append(InlineQueryResultCachedAudio(
                    id=f"ym_{tid}",
                    audio_file_id=stub_fid,
                    reply_markup=kb,
                ))
            else:
                kw = dict(
                    id=f"ym_{tid}",
                    title=f"{artist} - {title}",
                    description="Tap to download",
                    input_message_content=InputTextMessageContent(
                        message_text=(
                            f"<b>YNDXMusic:</b> Downloading "
                            f"<b>{escape_html(artist)} - {escape_html(title)}</b>..."
                        ),
                        parse_mode="HTML",
                    ),
                    reply_markup=kb,
                )
                if track.cover_uri:
                    kw["thumbnail_url"] = f"https://{track.cover_uri.replace('%%', '200x200')}"
                    kw["thumbnail_width"] = 200
                    kw["thumbnail_height"] = 200
                results.append(InlineQueryResultArticle(**kw))
        if not results:
            await self._inline_hint(query)
            return
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=results,
                cache_time=0,
                is_personal=True,
            )
            _log("INLINE_SEARCH", f"Answered {len(results)} results OK")
        except Exception as e:
            _log("INLINE_SEARCH", f"answer_inline_query FAILED: {e}\n{traceback.format_exc()}")

    async def _inline_hint(self, query: InlineQuery):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[InlineQueryResultArticle(
                    id="hint",
                    title="YNDXMusic",
                    description="Paste a Yandex Music link or type a song name",
                    input_message_content=InputTextMessageContent(
                        message_text="<b>YNDXMusic:</b> Paste a Yandex Music link or type a song name",
                        parse_mode="HTML",
                    ),
                    thumbnail_url=INLINE_QUERY_BANNER,
                    thumbnail_width=640,
                    thumbnail_height=360,
                )],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _inline_msg(self, query: InlineQuery, title: str, desc: str):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[InlineQueryResultArticle(
                    id="msg",
                    title=title,
                    description=desc,
                    input_message_content=InputTextMessageContent(
                        message_text=f"<b>YNDXMusic:</b> {escape_html(desc)}",
                        parse_mode="HTML",
                    ),
                )],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def on_unload(self):
        await self._unpatch_feed_update()
        self._real_cache.clear()
        self._stub_cache.clear()
        self._search_cache.clear()
        self._link_cache.clear()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)