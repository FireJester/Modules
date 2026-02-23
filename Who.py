version = (1, 0)

# module: Who
# meta developer: GXDEE.t.me

from .. import loader, utils
from telethon.tl.types import User, VideoSize, InputPhotoFileLocation
import os
import tempfile
import subprocess

@loader.tds
class Who(loader.Module):
    strings = {
        "name": "Who",
        "error_reply": "<emoji document_id=5208777366964311643>😵</emoji><b> Error: </b>нет реплая или некорректные данные",
        "no_photo_msg": "<emoji document_id=5208777366964311643>😵</emoji><b> Error: </b>пользователь скрыл аватарку, либо заблокировал тебя",
        "field_name": "<emoji document_id=5213351489954677363>🔝</emoji><b> Name:</b> ",
        "field_username": "<emoji document_id=5210696650409935856>🤟</emoji><b> Username:</b> ",
        "field_userid": "<emoji document_id=5211051925809695623>📀</emoji><b> User ID:</b> ",
        "field_datacenter": "<emoji document_id=5314596071123486121>🪙</emoji><b> DC:</b> ",
    }
    def _get_username(self, user: User):
        if hasattr(user, "username") and user.username:
            return user.username
        if hasattr(user, "usernames") and user.usernames:
            for u in user.usernames:
                if getattr(u, "active", False):
                    return u.username
            return user.usernames[0].username
        return NoneType
    async def _get_avatar(self, client, user):
        try:
            photos = await client.get_profile_photos(user, limit=1)
            if not photos:
                return None, False            
            photo = photos[0]
            has_video = getattr(user.photo, "has_video", False) if user.photo else False            
            if has_video and hasattr(photo, "video_sizes") and photo.video_sizes:
                for vs in photo.video_sizes:
                    if isinstance(vs, VideoSize):
                        location = InputPhotoFileLocation(
                            id=photo.id,
                            access_hash=photo.access_hash,
                            file_reference=photo.file_reference,
                            thumb_size=vs.type
                        )
                        path = tempfile.mktemp(suffix='.mp4')
                        await client.download_file(location, file=path)
                        return path, True            
            media = await client.download_media(photo)
            return media, False
        except Exception:
            return None, False
    def _add_silent_audio(self, video_path):
#добавляет аудио, потому что при отправке видео без звука оно отправляется как GIF и сохраняется в GIF пользователя 
        output = tempfile.mktemp(suffix='.mp4')
        try:
            subprocess.run(
                [
                    'ffmpeg', '-y',
                    '-i', video_path,
                    '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=mono',
                    '-c:v', 'copy',
                    '-c:a', 'aac',
                    '-shortest',
                    output
                ],
                capture_output=True,
                check=True
            )
            os.remove(video_path)
            return output
        except Exception:
            if os.path.exists(output):
                os.remove(output)
            return video_path
    async def _get_target_user(self, message):
        args = utils.get_args_raw(message)        
        if args:
            args = args.strip()
            if args.lstrip('-').isdigit():
                try:
                    return await message.client.get_entity(int(args))
                except Exception:
                    return None
            try:
                return await message.client.get_entity(args)
            except Exception:
                return None
        if message.is_reply:
            reply = await message.get_reply_message()
            if reply and reply.sender_id:
                try:
                    return await message.client.get_entity(reply.sender_id)
                except Exception:
                    return None
                return None
    def _build_info_text(self, user):
        first = user.first_name or ""
        last = user.last_name or ""
        name = (first + " " + last).strip()
        display_name = utils.escape_html(name)        
        username = self._get_username(user)
        if username:
            username_text = f"@{utils.escape_html(username)}"
        else:
            username_text = f'<a href="tg://user?id={user.id}">{display_name}</a>'        
        dc = user.photo.dc_id if user.photo else "?"        
        return (
            f"{self.strings('field_name')}{display_name}\n"
            f"{self.strings('field_username')}{username_text}\n"
            f"{self.strings('field_userid')}<code>{user.id}</code>\n"
            f"{self.strings('field_datacenter')} {dc}"
        )

    @loader.command(ru_doc="Получить информацию о пользователе")
    async def who(self, message):
        user = await self._get_target_user(message)
        if not user:
            await utils.answer(message, self.strings("error_reply"))
            return        
        text = self._build_info_text(user)
        await utils.answer(message, text, parse_mode="HTML")

    @loader.command(ru_doc="Получить информацию о пользователе + аватарка")
    async def whop(self, message):
        user = await self._get_target_user(message)
        if not user:
            await utils.answer(message, self.strings("error_reply"))
            return
        avatar, is_video = await self._get_avatar(message.client, user)
        if not avatar:
            await utils.answer(message, self.strings("no_photo_msg"))
            return
        text = self._build_info_text(user)
        try:
            if is_video:
                avatar = self._add_silent_audio(avatar)            
            await message.edit(
                file=avatar,
                text=text,
                parse_mode="HTML"
            )
        finally:
            if isinstance(avatar, str) and os.path.exists(avatar):
                os.remove(avatar)