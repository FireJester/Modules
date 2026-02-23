# -*- coding: utf-8 -*-

__version__ = (1, 0, 1)
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

from aiogram.types import (
    InlineQuery,
    InlineQueryResultCachedAudio,
    InlineQueryResultArticle,
    InputTextMessageContent,
    BufferedInputFile,
)

from .. import loader, utils

logger = logging.getLogger(__name__)


def _ensure_deps():
    for mod, pip in {
        "yandex_music": "yandex-music",
        "aiohttp": "aiohttp",
        "mutagen": "mutagen",
    }.items():
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pip, "-q"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


_ensure_deps()

import aiohttp
from yandex_music import ClientAsync

try:
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, ID3NoHeaderError
except ImportError:
    ID3 = None

REQUEST_OK = 200
MAX_FILE_SIZE = 50 * 1024 * 1024


def _escape_html(t):
    return (t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _sanitize_fn(n):
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", n).strip(". ")[:180] or "track"


def _fmt_dur(ms):
    s = (ms or 0) // 1000
    return f"{s // 60}:{s % 60:02d}" if s > 0 else "0:00"


def _track_artist(track):
    if track.artists:
        return ", ".join(a.name for a in track.artists if a.name) or "Unknown"
    return "Unknown"


def _track_title(track):
    return track.title or "Unknown"


class _YMSearch:
    def __init__(self):
        self._token = None
        self._client = None
        self._ok = False

    @property
    def ok(self):
        return self._ok and self._client is not None

    async def auth(self, token):
        if not token:
            self.reset()
            return False
        self._token = token
        try:
            self._client = ClientAsync(token)
            await self._client.init()
            self._ok = True
            return True
        except Exception:
            self.reset()
            return False

    def reset(self):
        self._token = None
        self._client = None
        self._ok = False

    async def search_track(self, query, count=5):
        if not self._client:
            return []
        try:
            result = await self._client.search(query, type_="track")
            if not result or not result.tracks or not result.tracks.results:
                return []
            return result.tracks.results[:count]
        except Exception:
            self._client = None
            self._ok = False
            try:
                await self.auth(self._token)
                result = await self._client.search(query, type_="track")
                if not result or not result.tracks or not result.tracks.results:
                    return []
                return result.tracks.results[:count]
            except Exception:
                return []

    async def download_track_bytes(self, track):
        try:
            info = await self._client.tracks_download_info(
                track.track_id, get_direct_links=True
            )
            if not info:
                return None
            best = max(info, key=lambda x: x.bitrate_in_kbps or 0)
            data = await best.download_bytes_async()
            if not data or len(data) < 1000:
                return None
            return data
        except Exception:
            return None

    async def get_cover_bytes(self, track, size="600x600"):
        if not track.cover_uri:
            return None
        url = f"https://{track.cover_uri.replace('%%', size)}"
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != REQUEST_OK:
                        return None
                    data = await resp.read()
                    return data if len(data) > 500 else None
        except Exception:
            return None

    @staticmethod
    def get_cover_url(track, size="200x200"):
        if not track.cover_uri:
            return None
        return f"https://{track.cover_uri.replace('%%', size)}"


class _TagHelper:
    @staticmethod
    def write(filepath, title, artist, album=None, cover_data=None):
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


class _Converter:
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
        except Exception:
            pass
        try:
            shutil.copy2(inp, out)
            return True
        except Exception:
            return False

    @staticmethod
    async def embed_cover(mp3_path, cover_path, out_path):
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


@loader.tds
class MusicSearch(loader.Module):
    """Yandex Music inline search and download."""

    strings = {
        "name": "MusicSearch",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            "YM_TOKEN", "", "Yandex Music access token",
            "SEARCH_LIMIT", 5, "Number of search results (1-10)",
        )
        self.inline_bot = None
        self.inline_bot_username = None
        self._ym = None
        self._tmp = None
        # {cache_key: {track_id: asyncio.Future}}
        self._search_futures = {}
        # {cache_key: {track_id: {"file_id":..., "title":..., ...} or {"error":...}}}
        self._search_results = {}
        # {cache_key: [track_info_dict, ...]}
        self._search_tracks_info = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._tmp = os.path.join(tempfile.gettempdir(), "musicsearch")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)
        self._ym = _YMSearch()
        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
            try:
                bi = await self.inline_bot.get_me()
                self.inline_bot_username = bi.username
            except Exception:
                pass

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
            v = int(self.config["SEARCH_LIMIT"])
            return max(1, min(10, v))
        except Exception:
            return 5

    async def _dl_single_track(self, track, user_id):
        ddir = tempfile.mkdtemp(dir=self._tmp)
        try:
            artist = _track_artist(track)
            title = _track_title(track)
            album_title = ""
            if track.albums:
                album_title = track.albums[0].title or ""
            dur_ms = track.duration_ms or 0
            dur_s = dur_ms // 1000

            audio_data = await self._ym.download_track_bytes(track)
            if not audio_data:
                return {"error": "Download failed"}
            if len(audio_data) > MAX_FILE_SIZE:
                return {"error": "File > 50 MB"}

            cover_data = await self._ym.get_cover_bytes(track, size="600x600")

            clean_name = _sanitize_fn(f"{artist} - {title}")
            raw_path = os.path.join(ddir, f"{clean_name}_raw")
            with open(raw_path, "wb") as f:
                f.write(audio_data)

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

            final_mp3 = os.path.join(ddir, f"{clean_name}.mp3")
            if is_mp3:
                os.rename(raw_path, final_mp3)
            else:
                ok = await _Converter.to_mp3(raw_path, final_mp3)
                if ok and os.path.exists(final_mp3) and os.path.getsize(final_mp3) > 0:
                    try:
                        os.remove(raw_path)
                    except Exception:
                        pass
                else:
                    final_mp3 = raw_path

            if cover_data:
                cover_path = os.path.join(ddir, "cover.jpg")
                with open(cover_path, "wb") as cf:
                    cf.write(cover_data)

                if final_mp3.endswith(".mp3"):
                    covered = os.path.join(ddir, f"{clean_name}_cover.mp3")
                    emb_ok = await _Converter.embed_cover(final_mp3, cover_path, covered)
                    if emb_ok:
                        try:
                            os.remove(final_mp3)
                        except Exception:
                            pass
                        final_mp3 = covered
                    else:
                        _TagHelper.write(final_mp3, title, artist, album_title, cover_data)
                else:
                    _TagHelper.write(final_mp3, title, artist, album_title, cover_data)
            elif final_mp3.endswith(".mp3"):
                _TagHelper.write(final_mp3, title, artist, album_title)

            if not os.path.exists(final_mp3) or os.path.getsize(final_mp3) == 0:
                return {"error": "Empty file"}
            if os.path.getsize(final_mp3) > MAX_FILE_SIZE:
                return {"error": "File > 50 MB"}

            with open(final_mp3, "rb") as f:
                file_bytes = f.read()

            audio_inp = BufferedInputFile(file_bytes, filename=os.path.basename(final_mp3))
            thumb_inp = (
                BufferedInputFile(cover_data, filename="cover.jpg")
                if cover_data
                else None
            )

            sent = await self.inline_bot.send_audio(
                chat_id=user_id,
                audio=audio_inp,
                title=title,
                performer=artist,
                duration=dur_s,
                thumbnail=thumb_inp,
            )

            if sent and sent.audio:
                try:
                    await self.inline_bot.delete_message(
                        chat_id=user_id, message_id=sent.message_id,
                    )
                except Exception:
                    pass
                return {
                    "file_id": sent.audio.file_id,
                    "title": title,
                    "artist": artist,
                    "duration": dur_s,
                }

            return {"error": "Upload failed"}

        except Exception as e:
            return {"error": str(e)[:80]}
        finally:
            if os.path.exists(ddir):
                shutil.rmtree(ddir, ignore_errors=True)

    def _start_downloads(self, cache_key, tracks, user_id):
        if cache_key in self._search_futures:
            return

        tracks_info = []
        for t in tracks:
            tracks_info.append({
                "track_id": str(t.track_id),
                "artist": _track_artist(t),
                "title": _track_title(t),
                "duration_ms": t.duration_ms or 0,
                "cover_url": _YMSearch.get_cover_url(t, size="200x200"),
            })
        self._search_tracks_info[cache_key] = tracks_info

        futures = {}
        for t in tracks:
            tid = str(t.track_id)
            futures[tid] = asyncio.ensure_future(
                self._dl_single_track(t, user_id)
            )
        self._search_futures[cache_key] = futures
        self._search_results[cache_key] = {}

    def _collect_results(self, cache_key):
        futures = self._search_futures.get(cache_key, {})
        results = self._search_results.get(cache_key, {})

        for tid, fut in futures.items():
            if tid not in results and fut.done():
                try:
                    results[tid] = fut.result()
                except Exception:
                    results[tid] = {"error": "Internal error"}

        self._search_results[cache_key] = results
        return results

    def _all_done(self, cache_key):
        futures = self._search_futures.get(cache_key, {})
        return all(f.done() for f in futures.values())

    def _build_inline_results(self, cache_key):
        tracks_info = self._search_tracks_info.get(cache_key, [])
        results_map = self._collect_results(cache_key)
        inline_results = []

        for i, info in enumerate(tracks_info):
            tid = info["track_id"]
            artist = info["artist"]
            title = info["title"]
            cover_url = info["cover_url"]
            dur_str = _fmt_dur(info["duration_ms"])

            res = results_map.get(tid)

            if res and "file_id" in res:
                inline_results.append(
                    InlineQueryResultCachedAudio(
                        id=f"audio_{tid}_{int(time.time())}",
                        audio_file_id=res["file_id"],
                    )
                )
            elif res and "error" in res:
                inline_results.append(
                    InlineQueryResultArticle(
                        id=f"err_{tid}_{int(time.time())}",
                        title=f"{artist} - {title}",
                        description=f"Error: {res['error']}",
                        input_message_content=InputTextMessageContent(
                            message_text=(
                                f"<b>MusicSearch:</b> Error downloading "
                                f"<b>{_escape_html(artist)} - {_escape_html(title)}</b>: "
                                f"{_escape_html(res['error'])}"
                            ),
                            parse_mode="HTML",
                        ),
                        thumbnail_url=cover_url,
                        thumbnail_width=200,
                        thumbnail_height=200,
                    )
                )
            else:
                inline_results.append(
                    InlineQueryResultArticle(
                        id=f"dl_{tid}_{int(time.time())}",
                        title=f"{artist} - {title} [{dur_str}]",
                        description="Downloading... Repeat query in ~10 sec",
                        input_message_content=InputTextMessageContent(
                            message_text=(
                                f"<b>MusicSearch:</b> Downloading "
                                f"<b>{_escape_html(artist)} - {_escape_html(title)}</b>..."
                            ),
                            parse_mode="HTML",
                        ),
                        thumbnail_url=cover_url,
                        thumbnail_width=200,
                        thumbnail_height=200,
                    )
                )

        return inline_results

    @loader.inline_handler(ru_doc="Поиск музыки в Яндекс Музыке")
    async def musicsearch_inline_handler(self, query: InlineQuery):
        raw = query.query.strip()

        prefix = "musicsearch"
        if raw.lower().startswith(prefix):
            text = raw[len(prefix):].strip()
        else:
            text = raw.strip()

        if not text:
            await self._hint(query)
            return

        if not await self._ensure_ym():
            await self._msg(query, "Not authorized", "Set YM_TOKEN in config")
            return

        limit = self._get_limit()
        cache_key = f"ymsearch_{text.lower().replace(' ', '_')[:60]}"

        if cache_key in self._search_futures:
            self._collect_results(cache_key)
            inline_results = self._build_inline_results(cache_key)

            if self._all_done(cache_key):
                has_any_file = any(
                    "file_id" in self._search_results.get(cache_key, {}).get(tid, {})
                    for tid in self._search_futures.get(cache_key, {})
                )
                if has_any_file:
                    pass

            if inline_results:
                try:
                    await self.inline_bot.answer_inline_query(
                        inline_query_id=query.id,
                        results=inline_results,
                        cache_time=0,
                        is_personal=True,
                    )
                except Exception:
                    pass
            else:
                await self._hint(query)
            return

        tracks = await self._ym.search_track(text, count=limit)
        if not tracks:
            await self._msg(query, "Not found", f"No results for: {text}")
            return

        self._start_downloads(cache_key, tracks, query.from_user.id)

        inline_results = self._build_inline_results(cache_key)

        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=inline_results,
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _hint(self, query):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"hint_{int(time.time())}",
                        title="MusicSearch",
                        description="Type a song name to search Yandex Music",
                        input_message_content=InputTextMessageContent(
                            message_text=(
                                "<b>MusicSearch:</b> Type a song name to search"
                            ),
                            parse_mode="HTML",
                        ),
                    )
                ],
                cache_time=0,
                is_personal=True,
            )
        except Exception:
            pass

    async def _msg(self, query, title, desc):
        try:
            await self.inline_bot.answer_inline_query(
                inline_query_id=query.id,
                results=[
                    InlineQueryResultArticle(
                        id=f"msg_{int(time.time())}",
                        title=title,
                        description=desc,
                        input_message_content=InputTextMessageContent(
                            message_text=f"<b>MusicSearch:</b> {_escape_html(desc)}",
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
        for futs in self._search_futures.values():
            for fut in futs.values():
                fut.cancel()
        self._search_futures.clear()
        self._search_results.clear()
        self._search_tracks_info.clear()
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)