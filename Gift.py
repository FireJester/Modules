__version__ = ("-beta",1,0)

# module: Gift
# meta developer: GXDEE.t.me

import io
import logging
from telethon.tl.functions.payments import GetStarGiftsRequest 
from telethon.errors import RPCError 
from telethon.tl.types import Message 
from .. import loader, utils 
logger = logging.getLogger(__name__)
@loader.tds
class Gift(loader.Module):
    strings = {"name": "Gift"}
    gift_sent_message = "<emoji document_id=5249506765170572814>🎁</emoji><b> Успех</b>"
    usage_text = "<emoji document_id=5248950420876853871>🥰</emoji><b> Команды модуля Gift:</b>\n<blockquote expandable><b>.gift [число]</b> - отправить подарок в виде стикера, где число - это индекс подарка\n<b>.gift list</b> - показать список доступных подарков</blockquote>"
    checking_gifts_message = "<emoji document_id=5248950420876853871>🥰</emoji><b> Поиск информации о подарке...</b>"
    gifts_list_intro_message = "<emoji document_id=5249129727171527157>🐸</emoji><b> Список доступных подарков:</b>" 
    gifts_chunk_header_message = "<b>Часть {current_part}/{total_parts}</b>\n\n" 
    invalid_index_message = "<emoji document_id=5249325564795323425>😵</emoji><b> Неверный индекс подарка! Используй число от 0 до {max_index}.</b>"
    download_error_message = "<emoji document_id=5249325564795323425>😵</emoji><b> Ошибка при загрузке стикера: <code>{}</code></b>"
    send_error_message = "<emoji document_id=5249325564795323425>😵</emoji><b> Ошибка при отправке стикера: <code>{}</code></b>"
    error_general_message = "<emoji document_id=5249325564795323425>😵</emoji><b> Произошла ошибка:</b> <code>{}</code>"
    no_gifts_available_message = "<emoji document_id=5249325564795323425>😵</emoji><b>Нет доступных подарков.</b>"
    async def client_ready(self, client, db):
        self.client = client 
        self.db = db          
    @loader.command(ru_doc="Инструкция к модулю Gift")
    async def gift(self, message: Message):
        args = utils.get_args_raw(message)
        if not args:
            await utils.answer(message, self.usage_text)
            return
        status_message = await utils.answer(message, self.checking_gifts_message)
        args_list = args.split()
        if args_list[0].lower() == "list":
            try:
                all_gifts_response = await self.client(GetStarGiftsRequest(0))
                if not all_gifts_response.gifts:
                    await status_message.edit(self.no_gifts_available_message)
                    return
                await status_message.edit(self.gifts_list_intro_message)
                gift_list_formatted = []
                for i, gift in enumerate(all_gifts_response.gifts):
                    gift_list_formatted.append(f"Index: <code>{i}</code>, ID: <code>{gift.id}</code>, Price: {gift.stars}⭐️")
                chunk_size = 30
                total_gifts = len(gift_list_formatted)
                total_chunks = (total_gifts + chunk_size - 1) // chunk_size 
                for i in range(0, total_gifts, chunk_size):
                    current_part = (i // chunk_size) + 1 
                    chunk = gift_list_formatted[i : i + chunk_size] 
                    chunk_text = "\n".join(chunk) 
                    inner_quote_content = self.gifts_chunk_header_message.format(
                        current_part=current_part, total_parts=total_chunks
                    ) + chunk_text
                    final_message_text = f"<blockquote expandable>{inner_quote_content}</blockquote>"
                    await self.client.send_message(
                        message.chat_id,
                        final_message_text,
                        parse_mode="html" 
                    )
            except RPCError as e:
                logger.error(f"RPC Error listing gifts: {e}")
                await status_message.edit(self.error_general_message.format(utils.escape_html(str(e)))) 
            except Exception as e:
                logger.error(f"General Error listing gifts: {e}")
                await status_message.edit(self.error_general_message.format(utils.escape_html(str(e))))

        else: 
            try:
                gift_index = int(args.strip()) 
                target_chat_id = message.chat_id 
                try:
                    all_gifts_response = await self.client(GetStarGiftsRequest(0))                    
                    gifts_list = all_gifts_response.gifts
                    if not gifts_list:
                        await status_message.edit(self.no_gifts_available_message)
                        return
                    if not (0 <= gift_index < len(gifts_list)):
                        await status_message.edit(self.invalid_index_message.format(max_index=len(gifts_list) - 1))
                        return                    
                    selected_gift = gifts_list[gift_index] 
                    try:
                        sticker_bytes = await self.client.download_media(selected_gift.sticker, bytes)
                    except Exception as e:
                        logger.error(f"Error downloading sticker at index {gift_index}: {e}")
                        await status_message.edit(self.download_error_message.format(utils.escape_html(str(e))))
                        return
                    file_obj = io.BytesIO(sticker_bytes)
                    file_obj.name = f"gift_{selected_gift.id}_by_index_{gift_index}.tgs" 
                    await self.client.send_file(
                        entity=int(target_chat_id), 
                        file=file_obj,               
                        caption=f"@GXDEE, INDX: <code>{gift_index}</code>, ID: <code>{selected_gift.id}</code>", 
                        force_document=True,         
                        parse_mode="html"             
                    )
                    await status_message.edit(self.gift_sent_message)
                except RPCError as e: 
                    logger.error(f"RPC Error sending direct gift at index {gift_index}: {e}")
                    await status_message.edit(self.send_error_message.format(utils.escape_html(str(e))))
                except Exception as e: 
                    logger.error(f"General Error sending direct gift at index {gift_index}: {e}")
                    await status_message.edit(self.send_error_message.format(utils.escape_html(str(e))))
            except ValueError: 
                await status_message.edit(self.usage_text) 
                return
#группы с топиками, стринги
#сейчас все работает, но проблема в том что делается это с сессии юб, а постоянные api запросы это не очень хорошо, на деле нужно добавить логику что этот модуль будет работать через бота, бот запускаться будет на процессе самого юб, ну и добавить новый аргумент, например .gift bot set/remove/on/off, таким образом модуль не будет делать никаких запросов с сессии аккаунта что безопасно. Для чекера придется сделать 10 ботов и запрашивать лист каждые 10 сек, таким образом получаем актуальную отладку 1 раз в сек для этого надо прописать 10 конфигов для токенов от 1 до 10 + сделать логику по которой отладка начинается 1 раз 10 сек последовательно для каждого бота, создать статусное сообщение которое будет отображать что чат добавлен в лист чатов куда прийдет уведомление о выходе новых подарков, на счет логики хз, по идее подарок должен скидываться с сессии тотго бота который нашел подарки, потому что новые подарки не на все место приходят  одинаково, но на счет логики чекера все четко, просто сравнивать айдишки подарков - при наличии новых сообщить о выходе подарков - такой метод будет требовать обновления каждый выход новых подарков для добавления новых подарков в существующие (придумать альтернативу) + вместо строчки поиск информации о подарке использовать [подключение к прокси боту]