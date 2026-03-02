__version__ = ("-beta",1,0,1)
# meta developer: FireJester.t.me
# update soon.. 

from .. import loader, utils
import time
from datetime import datetime, timedelta
import json
import pytz


@loader.tds
class AFK(loader.Module):

    strings = {
        "name": "AFK",
        
        "afk_status_template": """<emoji document_id=5208725127277087011>👌</emoji><b> АФК активен</b>
<emoji document_id=5208717791472943718>🗓️</emoji><b> Включен: </b>{start_str}
<emoji document_id=5208717791472943718>🗓️</emoji><b> Вернусь: </b>{end_str}
{remaining_str}
<emoji document_id=5208894671111095514>✉️</emoji><b> Ответов отправлено: </b>{responded_count}""",

        "afk_status_daily_template": """<emoji document_id=5208786480884910414>📀</emoji><b> Ежедневный АФК запланирован</b>
<emoji document_id=5208963115709927477>👉</emoji><b> Включение: </b>{start_str}
<emoji document_id=5208583848622854140>🙈</emoji><b> Отключение: </b>{end_str}

{activity_status}""",

        "afk_daily_active": "<emoji document_id=5208725127277087011>👌</emoji><b> Сейчас активен</b>",
        "afk_daily_inactive": "<emoji document_id=5208583848622854140>🙈</emoji><b> Сейчас неактивен</b>",
        
        "afk_on": "<emoji document_id=5208944814854283484>⬆️</emoji><b> AFK активен бессрочно</b>",
        "afk_off": "<emoji document_id=5208557765286468425>⛔️</emoji><b> AFK отключен</b>",
        
        "invalid_time": "<emoji document_id=5208777366964311643>😵</emoji><b> Введите корректные данные</b>",
        "not_a_group": "<emoji document_id=5208777366964311643>😵</emoji><b> Эта команда работает только в группах</b>",
        "afk_group_not_in_list": "<emoji document_id=5208777366964311643>😵</emoji><b> Группа не была в списке</b>",
        
        "afk_group_added": "<emoji document_id=5208456352518674554>👥</emoji><b> Группа добавлена в список автоответа</b>",
        "afk_group_removed": "<emoji document_id=5208825427648352217>❌</emoji><b> Группа удалена из списка автоответа</b>",
        
        "afk_status_inactive": "<emoji document_id=5208777366964311643>😵</emoji><b> AFK не активен</b>",
        
        "afk_unlimited": "<b>Бессрочно</b>",
        "afk_remaining_hours": "<emoji document_id=5208943526364088201>⌛️</emoji><b> Осталось: </b>{hours_left:.1f} часов",
        "afk_remaining_unlimited": "<emoji document_id=5208725127277087011>👌</emoji><b> Осталось: </b><code>бессрочно</code>",
        
        "afk_reply": "<emoji document_id=5208456004626320633>😴</emoji><b> К сожалению я сейчас AFK</b>\n<emoji document_id=5208583848622854140>🙈</emoji> <b> Последний раз был в сети: </b>{last_seen}",
        "afk_one_time": "<emoji document_id=5208786480884910414>📀</emoji><b> AFK установлен</b>\n<emoji document_id=5208963115709927477>👉</emoji><b> Включение: </b>{start_time}\n<emoji document_id=5208583848622854140>🙈</emoji><b> Отключение: </b>{end_time}",
        "afk_scheduled_daily": "<emoji document_id=5208786480884910414>📀</emoji><b> Ежедневный AFK установлен\n<emoji document_id=5208963115709927477>👉</emoji><b> Включение: </b>{start_str}\n<emoji document_id=5208583848622854140>🙈</emoji><b> Отключение: </b>{end_str}",
        "afk_set_time": "<emoji document_id=5208786480884910414>📀</emoji><b> AFK установлен на: </b>{duration}\n<emoji document_id=5208503000158470431>⏱</emoji><b> Вернусь: </b>{end_time}",
        "afk_groups_list_title": "<emoji document_id=5208456352518674554>👥</emoji><b> Список групп с AFK автоответом:</b>",
        "afk_groups_list_item": "\n\n<emoji document_id=5208569524906920074>🤝</emoji><b> {name} </b>\n<emoji document_id=5208786480884910414>📀</emoji><b> ID: </b><code>{chat_id}</code>",
        "afk_groups_list_empty": "<emoji document_id=5208606843877750802>✋</emoji><b> Список групп пуст</b>\n<blockquote>Используй команду <b>.afk on</b> в группе, которую хочешь добавить в список.</blockquote>",
        "afk_help_text": "<emoji document_id=5208456004626320633>😴</emoji><b> Список AFK команд:</b>\n<blockquote expandable>————————————————————\n<b>.afk [часы]</b> - включает на заданное количество часов\n(<code>.afk 2</code>)\n————————————————————\n<b>.afk [время1] [время2] - </b>установит AFK на промежуток с [время 1] до [время 2], один раз, если заданное время уже прошло  на момент создания AFK этой командой, то создание переносится на следующий день\n(<code>.afk 14:00 18:00</code>)\n————————————————————\n<b>.afk set [время1] [время2]</b> - установит AFK на промежуток с [время 1] до [время 2], ежедневное включение и выключение\n(<code>.afk set 14:00 18:00</code>)\n————————————————————\n<b>.afk unlim</b> - включает бессрочный AFK\n————————————————————\n<b>.afk stat</b> - показывает статус текущего AFK\n————————————————————\n<b>.afk list</b> - показывает список групп с установленными автоответами\n————————————————————\n<b>.afk on</b> - включить автоответ в текущей группе\n(использовать в группе)\n————————————————————\n<b>.afk off</b> - выключить автоответ в текущей группе\n(использовать в группе)\n————————————————————\n<b>.afk reset</b> - выключает AFK и сбрасывает расписание\n————————————————————</blockquote>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            "AFK_END_TIME", 0, "End time",
            "AFK_START_TIME", 0, "Start time",
            "AFK_DAILY_START", "", "Daily start",
            "AFK_DAILY_END", "", "Daily end",
            "AFK_SCHEDULED_START", "", "Scheduled start time string",
            "AFK_GROUPS", "{}", "Groups dict",
            "AFK_PM_COUNT", 0, "PM count",
            "AFK_GROUP_COUNT", 0, "Group count"
        )
        self.moscow_tz = pytz.timezone("Europe/Moscow")

    @property
    def afk_active(self):
        t, s, e = time.time(), self.config.get("AFK_START_TIME", 0), self.config.get("AFK_END_TIME", 0)
        return True if e == -1 else (s <= t < e if e > 0 else False)

    def get_groups(self):
        try:
            return json.loads(self.config.get("AFK_GROUPS", "{}"))
        except:
            return {}

    def save_groups(self, groups):
        self.config["AFK_GROUPS"] = json.dumps(groups)

    def reset(self):
        self.config["AFK_END_TIME"] = self.config["AFK_START_TIME"] = 0
        self.config["AFK_SCHEDULED_START"] = ""
        self.config["AFK_PM_COUNT"] = self.config["AFK_GROUP_COUNT"] = 0

    def norm_time(self, t):
        return t.replace('.', ':')

    def is_time(self, t):
        try:
            h, m = map(int, self.norm_time(t).split(':'))
            return 0 <= h <= 23 and 0 <= m <= 59
        except:
            return False

    def to_timestamp(self, t):
        try:
            time_obj = datetime.strptime(self.norm_time(t), "%H:%M").time()
            now_moscow = datetime.now(self.moscow_tz)
            dt_moscow = now_moscow.replace(
                hour=time_obj.hour, 
                minute=time_obj.minute, 
                second=0, 
                microsecond=0
            )
            if dt_moscow <= now_moscow:
                dt_moscow += timedelta(days=1)
            return dt_moscow.timestamp()
        except:
            return None

    def parse_dur(self, d):
        try:
            d = self.norm_time(d)
            if ':' in d:
                parts = d.split(':')
                h, m = float(parts[0]), float(parts[1])
                if not (0 <= m <= 59):
                    return None
                dur = h + m / 60
            else:
                dur = float(d)
            if dur <= 0:
                return None
            return dur
        except:
            return None

    def format_time(self, timestamp):
        dt = datetime.fromtimestamp(timestamp, self.moscow_tz)
        return dt.strftime("%d.%m.%Y %H:%M")

    async def is_mention(self, msg):
        try:
            me = await self.client.get_me()
            if hasattr(msg, 'mentioned') and msg.mentioned:
                return True
            if hasattr(msg, 'entities') and msg.entities:
                for e in msg.entities:
                    if e.__class__.__name__ in ['MessageEntityMention', 'MessageEntityMentionName']:
                        if e.__class__.__name__ == 'MessageEntityMention' or (hasattr(e, 'user_id') and e.user_id == me.id):
                            return True
            if hasattr(msg, 'text') and msg.text and me.username and f"@{me.username}" in msg.text:
                return True
        except:
            pass
        return False

    async def check_expired(self):
        t, e = time.time(), self.config.get("AFK_END_TIME", 0)
        if e in [-1, 0] or t < e:
            return False
        self.reset()
        return True

    async def reply(self, cid, rid, grp=False):
        last_seen_str = self.format_time(self.config.get("AFK_START_TIME", 0))
        txt = self.strings["afk_reply"].format(last_seen=last_seen_str)
        try:
            await self.client.send_message(cid, txt, reply_to=rid)
        except:
            try:
                await self.client.send_message(cid, txt)
            except:
                pass
        
        if grp:
            self.config["AFK_GROUP_COUNT"] = self.config.get("AFK_GROUP_COUNT", 0) + 1
        else:
            self.config["AFK_PM_COUNT"] = self.config.get("AFK_PM_COUNT", 0) + 1




# загрузчик команды afk



    @loader.command(ru_doc="Инструкция к модулю AFK")
    async def AFK(self, message):
        args = utils.get_args_raw(message)

        if not args:
            await utils.answer(message, self.strings["afk_help_text"])
            return

        args = args.replace(',', '.').split()

        # Бессрочный АФК
        if args[0].lower() == "unlim":
            self.reset()
            self.config["AFK_DAILY_START"] = self.config["AFK_DAILY_END"] = ""
            self.config["AFK_START_TIME"] = time.time()
            self.config["AFK_END_TIME"] = -1
            await utils.answer(message, self.strings["afk_on"])
            return

        # Статус (ИЗМЕНЕНО - используем единые шаблоны)
        if args[0].lower() in ["stat", "status"]:
            await self.check_expired()
            ds, de = self.config.get("AFK_DAILY_START", ""), self.config.get("AFK_DAILY_END", "")

            if ds and de:
                # Используем единый шаблон для ежедневного статуса
                activity_status = self.strings["afk_daily_active"] if self.afk_active else self.strings["afk_daily_inactive"]
                msg = self.strings["afk_status_daily_template"].format(
                    start_str=ds,
                    end_str=de,
                    activity_status=activity_status
                )
                await utils.answer(message, msg)
                return

            if self.afk_active:
                st, et = self.config.get("AFK_START_TIME", 0), self.config.get("AFK_END_TIME", 0)
                s_str = self.format_time(st)
                pm = self.config.get("AFK_PM_COUNT", 0)
                grp = self.config.get("AFK_GROUP_COUNT", 0)

                if et == -1:
                    e_str = self.strings["afk_unlimited"]
                    r_str = self.strings["afk_remaining_unlimited"]
                else:
                    e_str = self.format_time(et)
                    r_str = self.strings["afk_remaining_hours"].format(hours_left=(et - time.time()) / 3600)

                # Используем единый шаблон
                msg = self.strings["afk_status_template"].format(
                    start_str=s_str,
                    end_str=e_str,
                    remaining_str=r_str,
                    responded_count=pm + grp
                )
                await utils.answer(message, msg)
            else:
                await utils.answer(message, self.strings["afk_status_inactive"])
            return

        # Список групп
        if args[0].lower() == "list":
            groups = self.get_groups()
            if not groups:
                await utils.answer(message, self.strings["afk_groups_list_empty"])
                return

            msg = self.strings["afk_groups_list_title"]
            for chat_id, name in groups.items():
                msg += self.strings["afk_groups_list_item"].format(name=name, chat_id=chat_id)
            await utils.answer(message, msg)
            return

        # Отключение со сбросом
        if args[0].lower() == "reset":
            self.reset()
            self.config["AFK_DAILY_START"] = self.config["AFK_DAILY_END"] = ""
            await utils.answer(message, self.strings["afk_off"])
            return

        # Группы
        if args[0].lower() == "on":
            if message.is_private:
                await utils.answer(message, self.strings["not_a_group"])
                return
            try:
                chat = await message.get_chat()
                chat_name = chat.title if hasattr(chat, 'title') else str(message.chat_id)
            except:
                chat_name = str(message.chat_id)

            grps = self.get_groups()
            grps[str(message.chat_id)] = chat_name
            self.save_groups(grps)
            await utils.answer(message, self.strings["afk_group_added"])
            return

        if args[0].lower() == "off":
            if message.is_private:
                await utils.answer(message, self.strings["not_a_group"])
                return
            grps = self.get_groups()
            chat_id_str = str(message.chat_id)
            if chat_id_str in grps:
                del grps[chat_id_str]
                self.save_groups(grps)
                await utils.answer(message, self.strings["afk_group_removed"])
            else:
                await utils.answer(message, self.strings["afk_group_not_in_list"])
            return

        # Ежедневный
        if args[0].lower() == "set":
            if len(args) < 3 or not self.is_time(args[1]) or not self.is_time(args[2]):
                await utils.answer(message, self.strings["invalid_time"])
                return
            
            self.reset()
            s, e = self.norm_time(args[1]), self.norm_time(args[2])
            self.config["AFK_DAILY_START"] = s
            self.config["AFK_DAILY_END"] = e
            self.config["AFK_SCHEDULED_START"] = "" 
            
            await utils.answer(message, self.strings["afk_scheduled_daily"].format(start_str=s, end_str=e))
            await self.watcher(message, silent=True)
            return

        # Два аргумента — время включения/выключения
        if len(args) == 2:
            if not self.is_time(args[0]) or not self.is_time(args[1]):
                await utils.answer(message, self.strings["invalid_time"])
                return
            
            self.reset()
            self.config["AFK_DAILY_START"] = self.config["AFK_DAILY_END"] = ""

            s_str, e_str = self.norm_time(args[0]), self.norm_time(args[1])
            st, et = self.to_timestamp(args[0]), self.to_timestamp(args[1])
            
            if not st or not et:
                await utils.answer(message, self.strings["invalid_time"])
                return
            if et <= st:
                et += 86400
            
            self.config["AFK_START_TIME"] = st
            self.config["AFK_END_TIME"] = et
            self.config["AFK_SCHEDULED_START"] = s_str
            
            msg = self.strings["afk_one_time"].format(
                start_time=self.format_time(st),
                end_time=self.format_time(et)
            )
            await utils.answer(message, msg)
            return

        # Один аргумент — продолжительность
        if len(args) == 1:
            dur = self.parse_dur(args[0])
            if not dur:
                await utils.answer(message, self.strings["invalid_time"])
                return
            
            self.reset()
            self.config["AFK_DAILY_START"] = self.config["AFK_DAILY_END"] = ""

            st = time.time()
            et = st + dur * 3600
            self.config["AFK_START_TIME"] = st
            self.config["AFK_END_TIME"] = et
            self.config["AFK_SCHEDULED_START"] = ""
            
            h, m = int(dur), int((dur - int(dur)) * 60)
            msg = self.strings["afk_set_time"].format(
                duration=f"{h} ч {m} мин" if m > 0 else f"{h} ч",
                end_time=self.format_time(et)
            )
            await utils.answer(message, msg)
            return

        await utils.answer(message, self.strings["invalid_time"])

    @loader.watcher()
    async def watcher(self, message, silent=False):
        await self.check_expired()

        # Ежедневный режим
        ds, de = self.config.get("AFK_DAILY_START", ""), self.config.get("AFK_DAILY_END", "")
        if ds and de:
            try:
                ct = datetime.now(self.moscow_tz).time()
                st, et = datetime.strptime(ds, "%H:%M").time(), datetime.strptime(de, "%H:%M").time()
                is_in = (st <= ct <= et) if st <= et else (ct >= st or ct <= et)

                if is_in and not self.afk_active:
                    self.reset()
                    self.config["AFK_START_TIME"] = time.time()
                    self.config["AFK_END_TIME"] = -1
                
                elif not is_in and self.afk_active and self.config.get("AFK_END_TIME") == -1:
                    self.reset()
            except Exception:
                if not silent:
                    pass

        if silent or not self.afk_active or not hasattr(message, 'message') or message.out:
            return

        # ЛС
        if message.is_private:
            await self.reply(message.chat_id, message.id)
            return

        # Группы
        groups = self.get_groups()
        if str(message.chat_id) not in groups:
            return

        mention = await self.is_mention(message)
        if not mention and hasattr(message, 'reply_to_msg_id') and message.reply_to_msg_id:
            try:
                rmsg = await message.get_reply_message()
                if rmsg and rmsg.out:
                    mention = True
            except:
                pass

        if mention:
            await self.reply(message.chat_id, message.id, True)

#TODO переписать логику для тегов, сейчас серьёзные проблемы с этим потому что фильтр стоит на любой тэг, не обязательно юзера, вообще на любой срабатывает, следаловательно, надо получить тэг юзера и сохранить его в конфиге, либо автоматически либо в ручную по команде с новым аргументом исключить автоответ в чатах с ботами и для ботов в группе, проверка на юзер акк. Проблема с костылями в конфиге, скорее всего самым выгодным решением будет словарь в конфиге со словами на которые автоответчик будет тригерится, юзер сам будет выбирать количество таких слов, а фильтр просто сравнивать с листом этих слов из конфига + конструктор текстов переделать на более простой, сделать нормальную очистку конфига с листом групп для автоотета, добавить команду для добавления медиа к автоответчику по логике с использованием избранного как датабазы