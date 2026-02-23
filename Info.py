# -*- coding: utf-8 -*-

__version__ = (2, 0, 0)
# meta developer: FireJester.t.me 

from telethon.tl.types import User, Channel, Message
from .. import loader, utils
import os
import asyncio
import tempfile

@loader.tds
class Info(loader.Module):
    
    strings = {
        "name": "Info",
        "prem_user_full": (
            "<blockquote><b>┌ [</b><emoji document_id=5188516803638236397>🔝</emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><emoji document_id=5188171393778359433>🤟</emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><emoji document_id=5188654053613150361>📀</emoji><b>] User ID:</b> <code>{user_id}</code>\n"
            "<b>└ [</b><emoji document_id=5188420042320020352>🪙</emoji><b>] DC:</b> {dc}</blockquote>"
        ),
        "prem_user_no_dc": (
            "<blockquote><b>┌ [</b><emoji document_id=5188516803638236397>🔝</emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><emoji document_id=5188171393778359433>🤟</emoji><b>] Username:</b> {username}\n"
            "<b>└ [</b><emoji document_id=5188654053613150361>📀</emoji><b>] User ID:</b> <code>{user_id}</code></blockquote>"
        ),
        "prem_chat_full": (
            "<blockquote><b>┌ [</b><emoji document_id=5188516803638236397>🔝</emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><emoji document_id=5188171393778359433>🤟</emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><emoji document_id=5188654053613150361>📀</emoji><b>] Chat ID:</b> <code>{chat_id}</code>\n"
            "├ <b>[</b><emoji document_id=5190758450149233016>🌡</emoji><b>] Type:</b> {type}\n"
            "<b>└ [</b><emoji document_id=5188420042320020352>🪙</emoji><b>] DC:</b> {dc}</blockquote>"
        ),
        "prem_chat_no_dc": (
            "<blockquote><b>┌ [</b><emoji document_id=5188516803638236397>🔝</emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><emoji document_id=5188171393778359433>🤟</emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><emoji document_id=5188654053613150361>📀</emoji><b>] Chat ID:</b> <code>{chat_id}</code>\n"
            "<b>└ [</b><emoji document_id=5190758450149233016>🌡</emoji><b>] Type:</b> {type}</blockquote>"
        ),
        "noprem_user_full": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ User ID:</b> <code>{user_id}</code> <b>]</b>\n"
            "<b>└[ DC:</b> {dc} <b>]</b></blockquote>"
        ),
        "noprem_user_no_dc": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "<b>└[ User ID:</b> <code>{user_id}</code> <b>]</b></blockquote>"
        ),
        "noprem_chat_full": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ Chat ID:</b> <code>{chat_id}</code> <b>]</b>\n"
            "├<b>[ Type:</b> {type} <b>]</b>\n"
            "<b>└[ DC:</b> {dc} <b>]</b></blockquote>"
        ),
        "noprem_chat_no_dc": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ Chat ID:</b> <code>{chat_id}</code> <b>]</b>\n"
            "<b>└[ Type:</b> {type} <b>]</b></blockquote>"
        ),
    }

    strings_en = {
        "error_reply": "<emoji document_id=5188512006159766094>😵</emoji><b> Error: </b>No reply or invalid username",
        "no_photo_msg": "<emoji document_id=5188512006159766094>😵</emoji><b> Error: </b>User hid avatar or blocked you",
        "no_chat_photo": "<emoji document_id=5188512006159766094>😵</emoji><b> Error: </b>Chat has no avatar",
        "not_a_chat": "<emoji document_id=5188512006159766094>😵</emoji><b> Error: </b>This command only works in groups and channels",
        "error_reply_noprem": "<b>Error:</b> No reply or invalid username",
        "no_photo_msg_noprem": "<b>Error:</b> User hid avatar or blocked you",
        "no_chat_photo_noprem": "<b>Error:</b> Chat has no avatar",
        "not_a_chat_noprem": "<b>Error:</b> This command only works in groups and channels",
        "type_channel": "Channel",
        "type_supergroup": "Supergroup",
        "type_group": "Group",
    }

    strings_ru = {
        "error_reply": "<emoji document_id=5188512006159766094>😵</emoji><b> Error: </b>Нет реплая или некорректный юзернейм",
        "no_photo_msg": "<emoji document_id=5188512006159766094>😵</emoji><b> Error: </b>Пользователь скрыл аватарку или заблокировал тебя",
        "no_chat_photo": "<emoji document_id=5188512006159766094>😵</emoji><b> Error: </b>У чата нет аватарки",
        "not_a_chat": "<emoji document_id=5188512006159766094>😵</emoji><b> Error: </b>Эта команда работает только в группах и каналах",
        "error_reply_noprem": "<b>Error:</b> Нет реплая или некорректный юзернейм",
        "no_photo_msg_noprem": "<b>Error:</b> Пользователь скрыл аватарку или заблокировал тебя",
        "no_chat_photo_noprem": "<b>Error:</b> У чата нет аватарки",
        "not_a_chat_noprem": "<b>Error:</b> Эта команда работает только в группах и каналах",
        "type_channel": "Канал",
        "type_supergroup": "Супергруппа",
        "type_group": "Группа",
    }

    def __init__(self):
        self._premium_status = None

    async def _check_premium(self, client):
        if self._premium_status is None:
            me = await client.get_me()
            self._premium_status = getattr(me, "premium", False)
        return self._premium_status

    def _get_error_string(self, key, is_premium):
        if is_premium:
            return self.strings(key)
        return self.strings(f"{key}_noprem")

    def _get_username(self, entity):
        if hasattr(entity, "username") and entity.username:
            return entity.username
        if hasattr(entity, "usernames") and entity.usernames:
            for u in entity.usernames:
                if getattr(u, "active", False):
                    return u.username
            return entity.usernames[0].username
        return None

    def _get_dc(self, entity):
        photo = getattr(entity, "photo", None)
        if photo:
            return getattr(photo, "dc_id", None)
        return None

    def _has_video_avatar(self, entity):
        photo = getattr(entity, "photo", None)
        if photo:
            return getattr(photo, "has_video", False)
        return False

    def _get_topic_id(self, message: Message):
        reply_to = getattr(message, 'reply_to', None)
        if reply_to:
            return getattr(reply_to, 'reply_to_top_id', None) or getattr(reply_to, 'reply_to_msg_id', None)
        return None

    async def _get_avatar(self, client, entity):
        try:
            is_video = self._has_video_avatar(entity)
            if is_video:
                path = tempfile.mktemp(suffix=".mp4")
                result = await client.download_profile_photo(
                    entity,
                    file=path,
                    download_big=True
                )
                if result:
                    return path, True
                return None, False
            else:
                result = await client.download_profile_photo(
                    entity,
                    download_big=True
                )
                if result:
                    return result, False
                return None, False
        except Exception:
            return None, False

    async def _add_silent_audio_async(self, video_path):
        output = tempfile.mktemp(suffix=".mp4")
        try:
            process = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                "-i", video_path,
                "-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono",
                "-c:v", "copy",
                "-c:a", "aac",
                "-shortest",
                output,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await process.wait()
            if process.returncode == 0:
                await asyncio.to_thread(os.remove, video_path)
                return output
            else:
                if os.path.exists(output):
                    await asyncio.to_thread(os.remove, output)
                return video_path
        except Exception:
            if os.path.exists(output):
                try:
                    await asyncio.to_thread(os.remove, output)
                except Exception:
                    pass
            return video_path

    async def _get_target_user(self, message, username=None):
        if username:
            try:
                return await message.client.get_entity(username)
            except Exception:
                return None
        elif message.is_reply:
            reply = await message.get_reply_message()
            if reply and reply.sender_id:
                try:
                    return await message.client.get_entity(reply.sender_id)
                except Exception:
                    return None
        return None

    def _build_user_info_text(self, user, is_premium):
        first = user.first_name or ""
        last = user.last_name or ""
        name = f"{first} {last}".strip()
        display_name = utils.escape_html(name)
        username = self._get_username(user)
        if username and username.strip():
            username_text = f"<u>@{utils.escape_html(username)}</u>"
        else:
            username_text = f'<a href="tg://user?id={user.id}">{display_name}</a>'
        dc = self._get_dc(user)
        prefix = "prem" if is_premium else "noprem"
        suffix = "_full" if dc else "_no_dc"
        key = f"{prefix}_user{suffix}"
        return self.strings(key).format(
            name=display_name,
            username=username_text,
            user_id=user.id,
            dc=dc
        )

    def _build_chat_info_text(self, chat, is_premium):
        name = utils.escape_html(chat.title or "")
        username = self._get_username(chat)
        if username and username.strip():
            username_text = f"<u>@{utils.escape_html(username)}</u>"
        else:
            username_text = "—"
        if isinstance(chat, Channel):
            if chat.megagroup:
                chat_type = self.strings("type_supergroup")
            else:
                chat_type = self.strings("type_channel")
        else:
            chat_type = self.strings("type_group")
        dc = self._get_dc(chat)
        prefix = "prem" if is_premium else "noprem"
        suffix = "_full" if dc else "_no_dc"
        key = f"{prefix}_chat{suffix}"
        return self.strings(key).format(
            name=name,
            username=username_text,
            chat_id=chat.id,
            type=chat_type,
            dc=dc
        )

    async def _cleanup_file(self, path):
        if isinstance(path, str) and os.path.exists(path):
            try:
                await asyncio.to_thread(os.remove, path)
            except Exception:
                pass

    async def _send_result(self, message: Message, text, file=None, reply_to_msg_id=None):
        topic_id = self._get_topic_id(message)
        reply_to = reply_to_msg_id or topic_id
        await message.delete()
        await message.client.send_message(
            message.chat_id,
            text,
            file=file,
            reply_to=reply_to,
            parse_mode="HTML"
        )

    async def _send_error(self, message: Message, text):
        topic_id = self._get_topic_id(message)
        await message.delete()
        await message.client.send_message(
            message.chat_id,
            text,
            reply_to=topic_id,
            parse_mode="HTML"
        )

    @loader.command(
        ru_doc="Получить информацию о пользователе (добавь + для аватарки). Использование: реплай или @username",
        en_doc="Get user information (add + for avatar). Usage: reply or @username"
    )
    async def who(self, message: Message):
        args = utils.get_args_raw(message) or ""
        with_photo = "+" in args
        clean_args = args.replace("+", "").strip()
        is_premium = await self._check_premium(message.client)
        username = None
        if clean_args and not clean_args.lstrip("-").isdigit():
            username = clean_args
        user = await self._get_target_user(message, username)
        if not user:
            await self._send_error(message, self._get_error_string("error_reply", is_premium))
            return
        text = self._build_user_info_text(user, is_premium)
        reply_to_msg_id = None
        if message.is_reply:
            reply = await message.get_reply_message()
            if reply:
                reply_to_msg_id = reply.id
        if with_photo:
            avatar, is_video = await self._get_avatar(message.client, user)
            if not avatar:
                await self._send_error(message, self._get_error_string("no_photo_msg", is_premium))
                return
            try:
                if is_video:
                    avatar = await self._add_silent_audio_async(avatar)
                await self._send_result(message, text, file=avatar, reply_to_msg_id=reply_to_msg_id)
            finally:
                await self._cleanup_file(avatar)
        else:
            await self._send_result(message, text, reply_to_msg_id=reply_to_msg_id)

    @loader.command(
        ru_doc="Получить информацию о группе/канале (+ для аватарки)",
        en_doc="Get group/channel information (+ for avatar)"
    )
    async def where(self, message: Message):
        args = utils.get_args_raw(message) or ""
        with_photo = "+" in args
        is_premium = await self._check_premium(message.client)
        chat = await message.get_chat()
        if isinstance(chat, User):
            await self._send_error(message, self._get_error_string("not_a_chat", is_premium))
            return
        text = self._build_chat_info_text(chat, is_premium)
        if with_photo:
            avatar, is_video = await self._get_avatar(message.client, chat)
            if not avatar:
                await self._send_error(message, self._get_error_string("no_chat_photo", is_premium))
                return
            try:
                if is_video:
                    avatar = await self._add_silent_audio_async(avatar)
                await self._send_result(message, text, file=avatar)
            finally:
                await self._cleanup_file(avatar)
        else:
            await self._send_result(message, text)