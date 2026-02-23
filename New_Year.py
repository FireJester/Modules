__version__ = (1, 0, 0)

# module: New_Year
# meta developer: GXDEE.t.me

from .. import loader, utils
from datetime import datetime, timezone, timedelta

@loader.tds
class New_Year(loader.Module):
    strings = {
        "name": "New_Year",
        "help_text": "<emoji document_id=5215241189665571769>☃️</emoji><b> Команды модуля New Year:</b>\n<blockquote expandable><b>.new year</b> - показать время до Нового года\n<b>.new set [часовой пояс]</b> - установить часовой пояс (от -12 до +12)\n<b>.new add [прямая ссылка/реплай на медиа]</b> - добавить медиафайл для отправки с .new year\n<b>.new remove</b> - удалить медиафайл\n\n<b>Примеры:</b>\n<code>.new set 3</code> - установить UTC+3 (Москва)\n<code>.new set -5</code> - установить UTC-5 (Нью-Йорк)\n<code>.new add https://example.com/New_Year.gif</code> - добавить гифку</blockquote>",
        "new_year_template": "<emoji document_id=5212986052662297552>🎩</emoji><b> До Нового {year} года осталось:</b>\n\n<emoji document_id=5217611071015125647>🎆</emoji><b> Дней: </b><code>{days}</code>\n<emoji document_id=5217496236474531914>🕯</emoji><b> Часов: </b><code>{hours}</code>\n<emoji document_id=5215645221534075191>🫐</emoji><b> Минут: </b><code>{minutes}</code>\n<emoji document_id=5213026914981153242>🎄</emoji><b> Секунд: </b><code>{seconds}</code>\n\n<blockquote><emoji document_id=5213038163500499521>🍪</emoji><b> Часовой пояс: </b>UTC{timezone_str}\n<emoji document_id=5213024307936005301>☕️</emoji><b> Текущее время: </b>{current_time}</blockquote>",
        "timezone_set": "<emoji document_id=5213276280782356417>👌</emoji><b> Часовой пояс установлен: </b>UTC{timezone_str}",
        "media_added": "<emoji document_id=5213276280782356417>👌</emoji><b> Медиафайл добавлен!</b>\n<blockquote>Теперь он будет отправляться с командой .new year</blockquote>",
        "invalid_timezone": "<emoji document_id=5213225329585325406>😵</emoji><b> Неверный часовой пояс!</b>\n<blockquote>Используйте число от -12 до +12</blockquote>",
        "invalid_media": "<emoji document_id=5213225329585325406>😵</emoji><b> Укажите ссылку на медиафайл или ответьте этой командой на него !</b>\n<blockquote>Пример: .new add https://example.com/image.gif</blockquote>",
        "media_deleted_error": "<emoji document_id=5213225329585325406>😵</emoji><b> Медиафайл больше не доступен!</b>\n<blockquote>Добавьте новый медиафайл командой .new add</blockquote>\n<blockquote>Это сообщение появляется только при ошибке, для корректной работы модуля, значение вашего медиафайла в cfg только что было сброшено</blockquote>",
        "no_media_in_reply": "<emoji document_id=5213225329585325406>😵</emoji><b> В реплае нет медиа!</b>\n<blockquote>Ответьте на фото, видео или GIF</blockquote>",
        "media_load_error": "<emoji document_id=5213225329585325406>😵</emoji><b> Не удалось загрузить медиа!</b>\n<blockquote>Проверьте ссылку или попробуйте другой файл</blockquote>",
        "media_removed": "<emoji document_id=5213478908749449426>❌</emoji><b> Медиафайл удален!</b>",
        "saved_caption": "<emoji document_id=5217839043584230575>🪟</emoji><b> Не удалять - медиа для модуля New Year</b>",
    }
    def __init__(self):
        self.config = loader.ModuleConfig(
            "TIMEZONE_OFFSET", 3, "смещение часового пояса от UTC",
            "MEDIA_URL", "", "ссылка на файл",
            "SAVED_MSG_ID", 0, "ID сохраненного сообщения в избранном"
        )
    def get_timezone_str(self, offset):
        if offset >= 0:
            return f"+{offset}"
        return str(offset)
    def get_time_until_new_year(self):
        offset = self.config.get("TIMEZONE_OFFSET", 3)
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(tz)
        current_year = now.year
        new_year = datetime(current_year + 1, 1, 1, 0, 0, 0, tzinfo=tz)
        next_year = current_year + 1
        time_diff = new_year - now
        return {
            "days": time_diff.days,
            "hours": time_diff.seconds // 3600,
            "minutes": (time_diff.seconds % 3600) // 60,
            "seconds": time_diff.seconds % 60,
            "current_time": now.strftime("%d.%m.%Y %H:%M:%S"),
            "year": next_year,
            "timezone_str": self.get_timezone_str(offset)
        }
    async def get_saved_media(self):
        try:
            msg_id = self.config.get("SAVED_MSG_ID", 0)
            if msg_id:
                saved_msg = await self.client.get_messages("me", ids=msg_id)
                if saved_msg and saved_msg.media:
                    return saved_msg.media
        except:
            pass
        return None

    @loader.command(ru_doc="Инструкция к модулю New Year")
    async def new(self, message):
        args = utils.get_args_raw(message)        
        if not args:
            await utils.answer(message, self.strings["help_text"])
            return
        args_list = args.split()
        cmd = args_list[0].lower()

        if cmd == "year":
            time_data = self.get_time_until_new_year()
            msg = self.strings["new_year_template"].format(
                year=time_data["year"],
                days=time_data["days"],
                hours=time_data["hours"],                
                minutes=time_data["minutes"],
                seconds=time_data["seconds"],
                timezone_str=time_data["timezone_str"],
                current_time=time_data["current_time"]
            )
            media_url = self.config.get("MEDIA_URL", "")            
            if media_url:
                if media_url.startswith("saved:"):
                    saved_media = await self.get_saved_media()
                    if saved_media:
                        try:
                            await utils.answer(message, msg, file=saved_media)
                        except:
                            await utils.answer(message, self.strings["media_deleted_error"])
                    else:
                        await utils.answer(message, self.strings["media_deleted_error"])
                        self.config["MEDIA_URL"] = ""
                        self.config["SAVED_MSG_ID"] = 0
                else:
                    try:
                        await utils.answer(message, msg, file=media_url)
                    except:
                        await utils.answer(message, msg)
            else:
                await utils.answer(message, msg)

        elif cmd == "set":
            if len(args_list) < 2:
                await utils.answer(message, self.strings["invalid_timezone"])
                return            
            try:
                timezone_str = args_list[1].replace('+', '')
                timezone_offset = int(timezone_str)
                if not -12 <= timezone_offset <= 12:
                    await utils.answer(message, self.strings["invalid_timezone"])
                    return
                self.config["TIMEZONE_OFFSET"] = timezone_offset
                msg = self.strings["timezone_set"].format(
                    timezone_str=self.get_timezone_str(timezone_offset)
                )
                await utils.answer(message, msg)
            except (ValueError, IndexError):
                await utils.answer(message, self.strings["invalid_timezone"])

        elif cmd == "add":
            reply = await message.get_reply_message()            
            if reply:
                if not reply.media:
                    await utils.answer(message, self.strings["no_media_in_reply"])
                    return                
                is_valid_media = False
                if reply.photo or reply.video or reply.gif:
                    is_valid_media = True
                elif reply.document:
                    if reply.document.mime_type and any(x in reply.document.mime_type.lower() 
                                                        for x in ['image', 'video', 'gif']):
                        is_valid_media = True                
                if not is_valid_media:
                    await utils.answer(message, self.strings["no_media_in_reply"])
                    return                
                try:
                    old_msg_id = self.config.get("SAVED_MSG_ID", 0)
                    if old_msg_id:
                        try:
                            old_msg = await self.client.get_messages("me", ids=old_msg_id)
                            if old_msg:
                                await old_msg.delete()
                        except:
                            pass                    
                    saved_msg = await self.client.send_file(
                        "me",
                        reply.media,
                        caption=self.strings["saved_caption"],
                        silent=True
                    )
                    self.config["SAVED_MSG_ID"] = saved_msg.id
                    self.config["MEDIA_URL"] = f"saved:{saved_msg.id}"
                    await utils.answer(message, self.strings["media_added"], file=reply.media)
                    return
                except Exception as e:
                    await utils.answer(message, f"<b>Unknown error:</b> <code>{str(e)}</code>")
                    return            
            if len(args_list) < 2:
                await utils.answer(message, self.strings["invalid_media"])
                return            
            media_url = args[4:].strip()
            old_msg_id = self.config.get("SAVED_MSG_ID", 0)
            if old_msg_id:
                try:
                    old_msg = await self.client.get_messages("me", ids=old_msg_id)
                    if old_msg:
                        await old_msg.delete()
                except:
                    pass
            self.config["SAVED_MSG_ID"] = 0            
            try:
                await utils.answer(message, self.strings["media_added"], file=media_url)
                self.config["MEDIA_URL"] = media_url
            except Exception:
                await utils.answer(message, self.strings["media_load_error"])

        elif cmd == "remove":
            msg_id = self.config.get("SAVED_MSG_ID", 0)
            if msg_id:
                try:
                    saved_msg = await self.client.get_messages("me", ids=msg_id)
                    if saved_msg:
                        await saved_msg.delete()
                except:
                    pass
            self.config["MEDIA_URL"] = ""
            self.config["SAVED_MSG_ID"] = 0
            await utils.answer(message, self.strings["media_removed"])

        else:
            await utils.answer(message, self.strings["help_text"])