__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

import os
import asyncio
import logging
import time
import tempfile

from .. import loader, utils

logger = logging.getLogger(__name__)


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_bytes(b):
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    if b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.1f} MB"
    return f"{b / (1024 * 1024 * 1024):.2f} GB"


def _format_time(seconds):
    m, s = divmod(int(seconds), 60)
    ms = int((seconds - int(seconds)) * 100)
    if m > 0:
        return f"{m}:{s:02d}.{ms:02d}"
    return f"{s}.{ms:02d}s"


def _detect_type(name):
    if not name:
        return "unknown"
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    types = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "heif": "image/heif",
        "heic": "image/heic",
        "mp4": "video/mp4",
        "mov": "video/quicktime",
        "mp3": "audio/mpeg",
    }
    return types.get(ext, f"file/{ext}" if ext else "unknown")


ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "heif", "heic", "mp4", "mov", "mp3"}


@loader.tds
class Uploader(loader.Module):
    """Upload files to x0.at"""

    strings = {
        "name": "Uploader",
    }

    strings_en = {
        "help": (
            "<b>Uploader</b>\n\n"
            "<blockquote>"
            "<code>{prefix}upl</code> - reply to photo/video/file to upload to x0.at"
            "</blockquote>"
        ),
        "no_reply": "<b>Reply to a photo, video or file</b>",
        "unsupported": "<b>Unsupported format</b>\n\n<blockquote>Supported: png, jpeg, jpg, heif, heic, mp4, mov, mp3</blockquote>",
        "downloading": "<b>Downloading</b>\n\n<blockquote><code>{time}</code></blockquote>",
        "uploading": "<b>Uploading to x0.at</b>\n\n<blockquote><code>{time}</code></blockquote>",
        "done": (
            "<b>Uploaded</b>\n\n"
            "<b>Link:</b>\n"
            "<blockquote><a href=\"{url}\">{url}</a></blockquote>\n\n"
            "<b>File:</b>\n"
            "<blockquote>"
            "Name: <code>{name}</code>\n"
            "Type: <code>{type}</code>\n"
            "Size: <code>{size}</code>"
            "</blockquote>\n\n"
            "<b>Time:</b>\n"
            "<blockquote>"
            "Download: <code>{dl_time}</code>\n"
            "Upload: <code>{ul_time}</code>\n"
            "Total: <code>{total_time}</code>"
            "</blockquote>"
        ),
        "upload_fail": "<b>Upload failed</b>\n\n<blockquote><code>{error}</code></blockquote>",
        "download_fail": "<b>Download failed</b>\n\n<blockquote><code>{error}</code></blockquote>",
    }

    strings_ru = {
        "help": (
            "<b>Uploader</b>\n\n"
            "<blockquote>"
            "<code>{prefix}upl</code> - реплай на фото/видео/файл для загрузки на x0.at"
            "</blockquote>"
        ),
        "no_reply": "<b>Ответьте на фото, видео или файл</b>",
        "unsupported": "<b>Неподдерживаемый формат</b>\n\n<blockquote>Поддерживаются: png, jpeg, jpg, heif, heic, mp4, mov, mp3</blockquote>",
        "downloading": "<b>Скачивание</b>\n\n<blockquote><code>{time}</code></blockquote>",
        "uploading": "<b>Загрузка на x0.at</b>\n\n<blockquote><code>{time}</code></blockquote>",
        "done": (
            "<b>Загружено</b>\n\n"
            "<b>Ссылка:</b>\n"
            "<blockquote><a href=\"{url}\">{url}</a></blockquote>\n\n"
            "<b>Файл:</b>\n"
            "<blockquote>"
            "Имя: <code>{name}</code>\n"
            "Тип: <code>{type}</code>\n"
            "Размер: <code>{size}</code>"
            "</blockquote>\n\n"
            "<b>Время:</b>\n"
            "<blockquote>"
            "Скачивание: <code>{dl_time}</code>\n"
            "Загрузка: <code>{ul_time}</code>\n"
            "Всего: <code>{total_time}</code>"
            "</blockquote>"
        ),
        "upload_fail": "<b>Ошибка загрузки</b>\n\n<blockquote><code>{error}</code></blockquote>",
        "download_fail": "<b>Ошибка скачивания</b>\n\n<blockquote><code>{error}</code></blockquote>",
    }

    def _s(self, key, **kwargs):
        prefix = self.get_prefix()
        text = self.strings.get(key, "")
        try:
            return text.format(prefix=prefix, **kwargs)
        except (KeyError, IndexError):
            return text

    async def _safe_edit(self, msg, text):
        try:
            if isinstance(msg, list):
                msg = msg[0]
            await msg.edit(text, link_preview=False)
        except Exception:
            pass

    async def _timer_loop(self, msg, key, start_time, stop_event):
        while not stop_event.is_set():
            elapsed = time.time() - start_time
            await self._safe_edit(msg, self._s(key, time=_format_time(elapsed)))
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.7)
                break
            except asyncio.TimeoutError:
                pass

    def _get_filename(self, media):
        attrs = getattr(media, "attributes", []) or []
        for attr in attrs:
            name = getattr(attr, "file_name", None)
            if name:
                return name
        if hasattr(media, "mime_type"):
            mime = media.mime_type or ""
            ext_map = {
                "image/png": "file.png",
                "image/jpeg": "file.jpg",
                "image/heif": "file.heif",
                "image/heic": "file.heic",
                "video/mp4": "file.mp4",
                "video/quicktime": "file.mov",
                "audio/mpeg": "file.mp3",
            }
            if mime in ext_map:
                return ext_map[mime]
        return "file"

    def _get_extension(self, filename):
        if "." in filename:
            return filename.rsplit(".", 1)[-1].lower()
        return ""

    @loader.command(
        ru_doc="Реплай на фото/видео/файл - загрузить на x0.at",
        en_doc="Reply to photo/video/file - upload to x0.at",
    )
    async def upl(self, message):
        """Reply to photo/video/file - upload to x0.at"""
        reply = await message.get_reply_message()
        if not reply or not reply.media:
            await utils.answer(message, self._s("no_reply"))
            return

        media = reply.media
        doc = getattr(media, "document", None) or media
        filename = self._get_filename(doc)
        ext = self._get_extension(filename)

        if hasattr(media, "photo") or type(media).__name__ == "MessageMediaPhoto":
            ext = "jpg"
            filename = "photo.jpg"

        if ext not in ALLOWED_EXTENSIONS:
            await utils.answer(message, self._s("unsupported"))
            return

        file_type = _detect_type(filename)
        m = await utils.answer(message, self._s("downloading", time="0.00s"))

        stop_event = asyncio.Event()
        dl_start = time.time()
        timer_task = asyncio.ensure_future(
            self._timer_loop(m, "downloading", dl_start, stop_event)
        )

        tmp_fd = None
        tmp_path = None
        try:
            tmp_fd = tempfile.NamedTemporaryFile(
                suffix=f".{ext}", delete=False
            )
            tmp_path = tmp_fd.name
            tmp_fd.close()

            await reply.download_media(file=tmp_path)
            dl_elapsed = time.time() - dl_start
            stop_event.set()
            await timer_task

            file_size = os.path.getsize(tmp_path)

            stop_event = asyncio.Event()
            ul_start = time.time()
            timer_task = asyncio.ensure_future(
                self._timer_loop(m, "uploading", ul_start, stop_event)
            )

            try:
                proc = await asyncio.create_subprocess_exec(
                    "curl", "-sL", "--max-time", "120",
                    "-F", f"file=@{tmp_path}",
                    "https://x0.at",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, err = await asyncio.wait_for(proc.communicate(), timeout=130)
                ul_elapsed = time.time() - ul_start
                stop_event.set()
                await timer_task

                if proc.returncode != 0 or not out:
                    error = (err or b"").decode().strip() or f"exit code {proc.returncode}"
                    await self._safe_edit(
                        m, self._s("upload_fail", error=_escape(error[:300]))
                    )
                    return

                url = out.decode().strip()
                if not url.startswith("http"):
                    await self._safe_edit(
                        m, self._s("upload_fail", error=_escape(url[:300]))
                    )
                    return

                total_elapsed = dl_elapsed + ul_elapsed

                await self._safe_edit(
                    m,
                    self._s(
                        "done",
                        url=_escape(url),
                        name=_escape(filename),
                        type=_escape(file_type),
                        size=_format_bytes(file_size),
                        dl_time=_format_time(dl_elapsed),
                        ul_time=_format_time(ul_elapsed),
                        total_time=_format_time(total_elapsed),
                    ),
                )

            except asyncio.TimeoutError:
                stop_event.set()
                await timer_task
                await self._safe_edit(
                    m, self._s("upload_fail", error="timeout")
                )
            except FileNotFoundError:
                stop_event.set()
                await timer_task
                await self._safe_edit(
                    m, self._s("upload_fail", error="curl not found")
                )

        except Exception as e:
            stop_event.set()
            try:
                await timer_task
            except Exception:
                pass
            await self._safe_edit(
                m, self._s("download_fail", error=_escape(str(e)[:300]))
            )
        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass