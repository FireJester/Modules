__version__ = (1, 4, 0)
# meta developer: FireJester.t.me

import logging
import asyncio
import re
import time
import io
import random
import aiohttp

from datetime import datetime, timezone, timedelta

from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession
from telethon.tl.types import (
    Message,
    User,
    Channel,
    Chat,
    ChannelForbidden,
    ChatForbidden,
    DialogFilter,
    TextWithEntities,
    InputPeerNotifySettings,
    InputPhoto,
    InputPeerSelf,
)
from telethon.tl.functions.contacts import (
    GetContactsRequest,
    DeleteContactsRequest,
    BlockRequest,
)
from telethon.tl.functions.messages import (
    DeleteHistoryRequest,
    GetDialogFiltersRequest,
    UpdateDialogFilterRequest,
    ToggleDialogPinRequest,
    ReorderPinnedDialogsRequest,
    DeleteChatUserRequest,
)
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.tl.functions.photos import (
    GetUserPhotosRequest,
    DeletePhotosRequest,
    UploadProfilePhotoRequest,
)
from telethon.tl.functions.chatlists import (
    CheckChatlistInviteRequest,
    JoinChatlistInviteRequest,
)
from telethon.errors import (
    AuthKeyUnregisteredError,
    UserDeactivatedBanError,
    UserCreatorError,
    ChatIdInvalidError,
    ChannelPrivateError,
    FloodWaitError,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

STRING_SESSION_PATTERN = re.compile(r"(?<!\w)1[A-Za-z0-9_-]{200,}={0,2}(?!\w)")
MAX_SESSIONS = 10
TELEGRAM_ID = 777000
SPAMBOT_USERNAME = "SpamBot"
MAX_PHOTO_ITERATIONS = 20
FLOOD_EXTRA_MIN = 360
FLOOD_EXTRA_MAX = 720


def get_full_name(entity):
    if isinstance(entity, (Channel, Chat)):
        return entity.title or "Unknown"
    first = getattr(entity, "first_name", "") or ""
    last = getattr(entity, "last_name", "") or ""
    return f"{first} {last}".strip() or "Unknown"


def get_owner_username(user_entity):
    username = getattr(user_entity, "username", None)
    if username:
        return username
    usernames_list = getattr(user_entity, "usernames", None)
    if usernames_list:
        for u in usernames_list:
            if getattr(u, "active", False):
                return u.username
    return None


class AccountFloodError(Exception):
    def __init__(self, seconds, method="unknown"):
        self.seconds = seconds
        self.method = method
        super().__init__(f"Account flood wait {seconds}s in {method}")


@loader.tds
class Manager(loader.Module):
    """Multi-account session manager with cleanup capabilities"""

    strings = {
        "name": "Manager",
        "line": "--------------------",
        "help": (
            "<b>Manager - Multi Account Manager</b>\n\n"
            "<code>.manage add [session]</code> - add session\n"
            "<code>.manage add long [session]</code> - add persistent session\n"
            "<code>.manage list</code> - list connected sessions\n"
            "<code>.manage remove [number]</code> - remove session by number\n"
            "<code>.manage folder [1/2/3] [link]</code> - set folder link\n"
            "<code>.manage ava [url]</code> - set avatar image url\n"
            "<code>.manage set [offset]</code> - set timezone (from -12 to 12)\n"
            "<code>.manage start</code> - start cleanup process\n"
        ),
        "session_added": (
            "<b>Session added</b>\n"
            "{line}\n"
            "Name: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "Phone: <code>{phone}</code>\n"
            "Persistent: {persistent}\n"
            "Slot: {slot}/{max}\n"
            "{line}"
        ),
        "session_not_authorized": "<b>Error:</b> Session not authorized or invalid",
        "session_error": "<b>Error:</b> {error}",
        "session_exists": "<b>Error:</b> This account is already added",
        "session_max": "<b>Error:</b> Maximum {max} sessions reached",
        "provide_session": "<b>Error:</b> Provide StringSession via argument or reply",
        "no_sessions": "<b>No sessions added</b>",
        "session_list": (
            "<b>Connected sessions ({count}/{max}):</b>\n"
            "{line}\n{sessions}\n{line}"
        ),
        "session_removed": "<b>Session #{num} removed</b>",
        "session_remove_invalid": "<b>Error:</b> Invalid session number",
        "processing": "<b>Processing... Please wait</b>",
        "processing_flood": "<b>Processing... FloodWait: resuming at {resume_time}</b>",
        "already_processing": "<b>Error:</b> Already processing, please wait",
        "success": "<b>Cleanup completed successfully</b>",
        "error_no_sessions": "<b>Error:</b> No sessions to process",
        "error_no_api": (
            "<b>Error:</b> Set api_id and api_hash in module config first"
        ),
        "folder_set": "<b>Folder link {num} saved:</b>\n<code>{link}</code>",
        "folder_cleared": "<b>Folder link {num} cleared</b>",
        "folder_provide": "<b>Error:</b> Provide folder number (1-3) and link",
        "folder_invalid_num": "<b>Error:</b> Folder number must be 1, 2 or 3",
        "ava_set": "<b>Avatar URL saved:</b>\n<code>{url}</code>",
        "ava_cleared": "<b>Avatar URL cleared</b>",
        "ava_provide": "<b>Error:</b> Provide image URL",
        "timezone_set": "<b>Timezone set:</b> UTC{timezone_str}",
        "timezone_invalid": "<b>Error:</b> Invalid timezone. Use a number from -12 to 12",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "api_id",
                0,
                "Telegram API ID (from my.telegram.org)",
                validator=loader.validators.Integer(minimum=0),
            ),
            loader.ConfigValue(
                "api_hash",
                "",
                "Telegram API Hash (from my.telegram.org)",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "sessions_data",
                [],
                "Stored sessions data (internal)",
                validator=loader.validators.Hidden(loader.validators.Series()),
            ),
            loader.ConfigValue(
                "folder_link_1",
                "",
                "Chatlist folder invite link #1",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "folder_link_2",
                "",
                "Chatlist folder invite link #2",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "folder_link_3",
                "",
                "Chatlist folder invite link #3",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "avatar_url",
                "",
                "Avatar image URL",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "timezone_offset",
                3,
                "Timezone offset from UTC (from -12 to 12)",
                validator=loader.validators.Integer(minimum=-12, maximum=12),
            ),
        )
        self._clients = []
        self._sessions_runtime = []
        self._status_msg = None
        self._processing_lock = asyncio.Lock()
        self._flood_until = {}
        self._flood_log = []

    def _get_timezone_str(self, offset):
        if offset >= 0:
            return f"+{offset}"
        return str(offset)

    def _get_now_formatted(self):
        offset = self.config["timezone_offset"]
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(tz)
        return now.strftime("%d.%m.%Y %H:%M")

    def _get_resume_time_str(self, wait_seconds):
        offset = self.config["timezone_offset"]
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(tz)
        resume = now + timedelta(seconds=wait_seconds)
        return resume.strftime("%H:%M:%S")

    def _get_time_str_from_timestamp(self, ts):
        offset = self.config["timezone_offset"]
        tz = timezone(timedelta(hours=offset))
        dt = datetime.fromtimestamp(ts, tz=tz)
        return dt.strftime("%H:%M:%S")

    def _get_datetime_str_from_timestamp(self, ts):
        offset = self.config["timezone_offset"]
        tz = timezone(timedelta(hours=offset))
        dt = datetime.fromtimestamp(ts, tz=tz)
        return dt.strftime("%d.%m.%Y %H:%M:%S")

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()
        await self._restore_sessions()

    async def on_unload(self):
        for c in self._clients:
            try:
                await c.disconnect()
            except Exception:
                pass
        self._clients.clear()
        self._sessions_runtime.clear()

    def _get_api_credentials(self):
        api_id = self.config["api_id"]
        api_hash = self.config["api_hash"]
        if not api_id or not api_hash:
            return None, None
        return int(api_id), str(api_hash)

    async def _ensure_connected(self, client):
        if not client.is_connected():
            await client.connect()

    def _mark_flood(self, client, seconds, method="unknown", account_name="unknown"):
        extra = random.randint(FLOOD_EXTRA_MIN, FLOOD_EXTRA_MAX)
        total = seconds + extra
        resume_at = time.time() + total
        self._flood_until[id(client)] = resume_at

        flood_entry = {
            "account": account_name,
            "method": method,
            "flood_seconds": seconds,
            "extra_wait": extra,
            "total_wait": total,
            "timestamp": self._get_datetime_str_from_timestamp(time.time()),
            "resume_at": self._get_datetime_str_from_timestamp(resume_at),
        }
        self._flood_log.append(flood_entry)

        logger.warning(
            f"[MANAGER] Client {account_name} flood {seconds}s + {extra}s extra "
            f"in {method}, resume at {self._get_time_str_from_timestamp(resume_at)}"
        )

    def _is_flooded(self, client):
        deadline = self._flood_until.get(id(client))
        if deadline is None:
            return False
        if time.time() >= deadline:
            del self._flood_until[id(client)]
            return False
        return True

    def _get_flood_remaining(self, client):
        deadline = self._flood_until.get(id(client))
        if deadline is None:
            return 0
        remaining = deadline - time.time()
        return max(0, remaining)

    async def _update_flood_status(self):
        if not self._status_msg:
            return
        flooded = {
            cid: deadline
            for cid, deadline in self._flood_until.items()
            if time.time() < deadline
        }
        if not flooded:
            try:
                await utils.answer(self._status_msg, self.strings["processing"])
            except Exception:
                pass
            return
        min_deadline = min(flooded.values())
        resume_time = self._get_time_str_from_timestamp(min_deadline)
        try:
            await utils.answer(
                self._status_msg,
                self.strings["processing_flood"].format(resume_time=resume_time),
            )
        except Exception:
            pass

    async def _safe_request(self, client, request, context="", max_retries=3, account_name="unknown"):
        for attempt in range(max_retries):
            try:
                await self._ensure_connected(client)
                return await client(request)
            except FloodWaitError as e:
                self._mark_flood(client, e.seconds, method=context, account_name=account_name)
                raise AccountFloodError(e.seconds, method=context)
            except (ConnectionError, OSError) as e:
                logger.warning(
                    f"[MANAGER] Connection error in {context} "
                    f"(attempt {attempt + 1}/{max_retries}): {e}"
                )
                await asyncio.sleep(2)
                try:
                    await self._ensure_connected(client)
                except Exception:
                    pass
            except Exception:
                raise
        raise Exception(f"Max retries exceeded for {context}")

    async def _connect_session(self, session_str):
        api_id, api_hash = self._get_api_credentials()
        if not api_id:
            return None, "API credentials not configured"
        client = None
        try:
            client = TelegramClient(
                StringSession(session_str),
                api_id=api_id,
                api_hash=api_hash,
            )
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                return None, "Not authorized"
            me = await client.get_me()
            return client, me
        except AuthKeyUnregisteredError:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            return None, "Session revoked"
        except UserDeactivatedBanError:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            return None, "Account banned"
        except Exception as e:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            return None, str(e)

    async def _restore_sessions(self):
        api_id, api_hash = self._get_api_credentials()
        if not api_id:
            logger.warning(
                "[MANAGER] No API credentials configured, skipping session restore"
            )
            return
        stored = self.config["sessions_data"]
        if not stored:
            return
        for entry in stored:
            try:
                session_str = entry.get("session", "")
                persistent = entry.get("persistent", False)
                if not session_str:
                    continue
                client, result = await self._connect_session(session_str)
                if client is None:
                    logger.warning(
                        f"[MANAGER] Failed to restore session: {result}"
                    )
                    continue
                me = result
                phone = getattr(me, "phone", "Unknown") or "Hidden"
                self._sessions_runtime.append(
                    {
                        "session": session_str,
                        "user_id": me.id,
                        "name": get_full_name(me),
                        "phone": phone,
                        "persistent": persistent,
                    }
                )
                self._clients.append(client)
            except Exception as e:
                logger.warning(f"[MANAGER] Failed to restore session: {e}")

    def _save_sessions(self):
        data = []
        for s in self._sessions_runtime:
            data.append(
                {
                    "session": s["session"],
                    "persistent": s.get("persistent", False),
                    "user_id": s["user_id"],
                    "name": s["name"],
                    "phone": s["phone"],
                }
            )
        self.config["sessions_data"] = data

    def _find_string_session(self, text):
        if not text:
            return None
        match = STRING_SESSION_PATTERN.search(text)
        return match.group(0) if match else None

    def _extract_folder_hash(self, link):
        if not link:
            return None
        if "addlist/" in link:
            part = link.split("addlist/")[-1].split("?")[0].strip()
            return part if part else None
        if "slug=" in link:
            part = link.split("slug=")[-1].split("&")[0].strip()
            return part if part else None
        return None

    def _get_peer_id(self, peer):
        if hasattr(peer, "channel_id"):
            return peer.channel_id
        if hasattr(peer, "chat_id"):
            return peer.chat_id
        if hasattr(peer, "user_id"):
            return peer.user_id
        return None

    def _get_folder_links(self):
        links = []
        for key in ("folder_link_1", "folder_link_2", "folder_link_3"):
            val = self.config[key]
            if val and val.strip():
                links.append(val.strip())
        return links

    async def _get_existing_folder_ids(self, client, account_name="unknown"):
        try:
            result = await self._safe_request(
                client, GetDialogFiltersRequest(), context="GetDialogFilters",
                account_name=account_name,
            )
            filters = getattr(result, "filters", result)
            used_ids = set()
            for f in filters:
                if hasattr(f, "id"):
                    used_ids.add(f.id)
            return used_ids, filters
        except Exception:
            return set(), []

    def _get_free_folder_id(self, used_ids):
        for i in range(2, 256):
            if i not in used_ids:
                return i
        return None

    async def _get_chats_in_folders(self, filters):
        chat_ids = set()
        for f in filters:
            if not hasattr(f, "include_peers"):
                continue
            for peer in f.include_peers:
                pid = self._get_peer_id(peer)
                if pid:
                    chat_ids.add(pid)
            if hasattr(f, "pinned_peers") and f.pinned_peers:
                for peer in f.pinned_peers:
                    pid = self._get_peer_id(peer)
                    if pid:
                        chat_ids.add(pid)
        return chat_ids

    def _find_folder_by_title(self, filters, title):
        for f in filters:
            if not isinstance(f, DialogFilter):
                continue
            if not hasattr(f, "title"):
                continue
            f_title = f.title
            if hasattr(f_title, "text"):
                f_title = f_title.text
            if isinstance(f_title, str) and f_title.lower() == title.lower():
                return f
        return None

    def _clone_filter(self, folder, **overrides):
        fields = {
            "id": folder.id,
            "title": folder.title,
            "pinned_peers": (
                list(folder.pinned_peers) if folder.pinned_peers else []
            ),
            "include_peers": (
                list(folder.include_peers) if folder.include_peers else []
            ),
            "exclude_peers": (
                list(folder.exclude_peers) if folder.exclude_peers else []
            ),
        }
        for flag in (
            "contacts",
            "non_contacts",
            "groups",
            "broadcasts",
            "bots",
            "exclude_muted",
            "exclude_read",
            "exclude_archived",
        ):
            val = getattr(folder, flag, None)
            if val is not None:
                fields[flag] = val
        for field in ("emoticon", "color"):
            val = getattr(folder, field, None)
            if val is not None:
                fields[field] = val
        fields.update(overrides)
        return DialogFilter(**fields)

    async def _update_existing_folder(self, client, folder, new_entities, account_name="unknown"):
        try:
            existing_ids = set()
            for peer in folder.include_peers:
                pid = self._get_peer_id(peer)
                if pid:
                    existing_ids.add(pid)
            new_peers = list(folder.include_peers)
            added = []
            for entity in new_entities:
                eid = entity.id
                if eid not in existing_ids:
                    try:
                        await self._ensure_connected(client)
                        inp = await client.get_input_entity(entity)
                        new_peers.append(inp)
                        existing_ids.add(eid)
                        added.append(entity)
                    except Exception:
                        pass
            if not added:
                return True, []
            updated = self._clone_filter(folder, include_peers=new_peers)
            await self._safe_request(
                client,
                UpdateDialogFilterRequest(id=folder.id, filter=updated),
                context="UpdateDialogFilter",
                account_name=account_name,
            )
            return True, added
        except Exception as e:
            logger.error(f"[MANAGER] Update folder error: {e}")
            return False, []

    async def _create_folder(self, client, folder_id, title, peers, account_name="unknown"):
        try:
            input_peers = []
            for peer in peers:
                try:
                    await self._ensure_connected(client)
                    inp = await client.get_input_entity(peer)
                    input_peers.append(inp)
                except Exception:
                    pass
            dialog_filter = DialogFilter(
                id=folder_id,
                title=TextWithEntities(text=title, entities=[]),
                pinned_peers=[],
                include_peers=input_peers,
                exclude_peers=[],
            )
            await self._safe_request(
                client,
                UpdateDialogFilterRequest(id=folder_id, filter=dialog_filter),
                context="CreateFolder",
                account_name=account_name,
            )
            return True
        except Exception as e:
            logger.error(f"[MANAGER] Create folder '{title}' error: {e}")
            return False

    async def _remove_saved_from_all_folders(self, client, me_id, account_name="unknown"):
        try:
            _, filters = await self._get_existing_folder_ids(client, account_name=account_name)

            await self._ensure_connected(client)
            try:
                me_input = await client.get_input_entity(InputPeerSelf())
            except Exception:
                me_input = await client.get_input_entity(me_id)

            me_peer_id = me_id

            for f in filters:
                if not isinstance(f, DialogFilter):
                    continue
                if not hasattr(f, "include_peers"):
                    continue
                has_saved = False
                new_include = []
                for peer in f.include_peers:
                    pid = self._get_peer_id(peer)
                    if pid == me_peer_id:
                        has_saved = True
                    else:
                        new_include.append(peer)
                new_pinned = []
                if hasattr(f, "pinned_peers") and f.pinned_peers:
                    for peer in f.pinned_peers:
                        pid = self._get_peer_id(peer)
                        if pid == me_peer_id:
                            has_saved = True
                        else:
                            new_pinned.append(peer)
                if has_saved:
                    updated = self._clone_filter(
                        f,
                        pinned_peers=new_pinned,
                        include_peers=new_include,
                    )
                    try:
                        await self._safe_request(
                            client,
                            UpdateDialogFilterRequest(id=f.id, filter=updated),
                            context=f"RemoveSavedFromFolder_{f.id}",
                            account_name=account_name,
                        )
                        logger.info(
                            f"[MANAGER] Removed Saved Messages from folder {f.id}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"[MANAGER] Remove saved from folder {f.id}: {e}"
                        )
            return True
        except Exception as e:
            logger.error(f"[MANAGER] Remove saved from folders error: {e}")
            return False

    async def _clear_saved_messages(self, client, me, account_name="unknown"):
        errors = []
        try:
            await self._ensure_connected(client)
            await client(DeleteHistoryRequest(
                peer=InputPeerSelf(),
                max_id=0,
                just_clear=True,
                revoke=False,
            ))
            logger.info(f"[MANAGER] Cleared Saved Messages for {account_name}")
        except Exception as e:
            errors.append(f"Clear saved messages history: {e}")

        try:
            now_str = self._get_now_formatted()
            await self._ensure_connected(client)
            await client.send_message("me", f"successfully cleared ({now_str})")
            logger.info(f"[MANAGER] Sent clear confirmation to Saved Messages for {account_name}")
        except Exception as e:
            errors.append(f"Send clear confirmation: {e}")

        return errors

    async def _match_owner(self, client, me, account_name="unknown"):
        errors = []
        try:
            owner_entity = self._me
            owner_username = get_owner_username(owner_entity)

            if not owner_username:
                errors.append("Owner has no username, cannot match")
                return False, errors

            await self._ensure_connected(client)
            try:
                resolved = await client.get_entity(owner_username)
            except Exception as e:
                errors.append(f"Failed to resolve owner @{owner_username}: {e}")
                return False, errors

            try:
                await self._ensure_connected(client)
                await client.send_message(resolved, "successfully matched owner")
                logger.info(
                    f"[MANAGER] {account_name} sent match message to owner @{owner_username}"
                )
            except Exception as e:
                errors.append(f"Failed to send match message to owner: {e}")
                return False, errors

            await asyncio.sleep(3)

            try:
                await self._client(DeleteHistoryRequest(
                    peer=me.id,
                    max_id=0,
                    just_clear=True,
                    revoke=False,
                ))
                logger.info(
                    f"[MANAGER] Owner cleared chat with {account_name} (ID: {me.id})"
                )
            except Exception as e:
                errors.append(f"Owner failed to clear chat with account: {e}")

            return True, errors

        except Exception as e:
            errors.append(f"Match owner error: {e}")
            return False, errors

    async def _get_spambot_id(self, client):
        try:
            await self._ensure_connected(client)
            entity = await client.get_entity(SPAMBOT_USERNAME)
            return entity.id
        except Exception:
            return None

    async def _get_admin_chats(self, client):
        channels = []
        groups = []
        try:
            await self._ensure_connected(client)
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                    continue
                if isinstance(entity, Channel):
                    if not entity.admin_rights and not entity.creator:
                        continue
                    if getattr(entity, "broadcast", False):
                        channels.append(entity)
                    elif getattr(entity, "megagroup", False):
                        groups.append(entity)
                elif isinstance(entity, Chat):
                    if entity.admin_rights or getattr(entity, "creator", False):
                        groups.append(entity)
        except Exception as e:
            logger.error(f"[MANAGER] Get admin chats error: {e}")
        return channels, groups

    async def _is_peer_muted(self, client, peer):
        try:
            await self._ensure_connected(client)
            full = await client(functions.account.GetNotifySettingsRequest(peer=peer))
            mute_until = getattr(full, "mute_until", None)
            if mute_until and mute_until > int(time.time()):
                return True
            silent = getattr(full, "silent", None)
            if silent:
                return True
            return False
        except Exception:
            return False

    async def _mute_peer(self, client, peer, context="", account_name="unknown"):
        await self._safe_request(
            client,
            UpdateNotifySettingsRequest(
                peer=peer,
                settings=InputPeerNotifySettings(
                    show_previews=False,
                    silent=True,
                    mute_until=2**31 - 1,
                ),
            ),
            context=context,
            account_name=account_name,
        )

    async def _mute_and_archive(self, client, peer, dialog=None, account_name="unknown"):
        try:
            is_archived = False
            if dialog is not None:
                is_archived = getattr(dialog, "archived", False)

            if is_archived:
                return True

            already_muted = await self._is_peer_muted(client, peer)

            if not already_muted:
                await self._ensure_connected(client)
                await self._mute_peer(client, peer, context="MuteAndArchive", account_name=account_name)
                await asyncio.sleep(0.5)

            await self._ensure_connected(client)
            await client.edit_folder(peer, 1)
            return True
        except AccountFloodError:
            raise
        except FloodWaitError as e:
            self._mark_flood(client, e.seconds, method="mute_and_archive", account_name=account_name)
            raise AccountFloodError(e.seconds, method="mute_and_archive")
        except (ConnectionError, OSError) as e:
            logger.warning(f"[MANAGER] Connection lost in mute_and_archive: {e}")
            await asyncio.sleep(2)
            try:
                await self._ensure_connected(client)
                await client.edit_folder(peer, 1)
                return True
            except FloodWaitError as e2:
                self._mark_flood(client, e2.seconds, method="mute_and_archive_retry", account_name=account_name)
                raise AccountFloodError(e2.seconds, method="mute_and_archive_retry")
            except Exception:
                return False
        except Exception as e:
            logger.error(f"[MANAGER] Mute/archive error: {e}")
            return False

    async def _archive_all_except(self, client, excluded_ids, account_name="unknown"):
        archived = []
        errors = []
        try:
            await self._ensure_connected(client)
            dialogs = await client.get_dialogs()
            for dialog in dialogs:
                entity = dialog.entity
                eid = getattr(entity, "id", None)
                if eid and eid in excluded_ids:
                    continue
                if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                    continue

                is_archived = getattr(dialog, "archived", False)
                if is_archived:
                    continue

                try:
                    success = await self._mute_and_archive(
                        client, entity, dialog=dialog, account_name=account_name
                    )
                    name = (
                        get_full_name(entity)
                        if isinstance(entity, (User, Channel, Chat))
                        else str(eid)
                    )
                    if success:
                        archived.append(f"{name} (ID: {eid})")
                    else:
                        errors.append(f"Archive {eid}: returned False")
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"Archive {eid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Archive iteration: {e}")
        return archived, errors

    async def _ensure_archive_muted(self, client, excluded_ids, account_name="unknown"):
        muted_count = 0
        errors = []
        try:
            await self._ensure_connected(client)
            dialogs = await client.get_dialogs(folder=1)
            for dialog in dialogs:
                entity = dialog.entity
                eid = getattr(entity, "id", None)
                if eid and eid in excluded_ids:
                    continue
                if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                    continue
                try:
                    already_muted = await self._is_peer_muted(client, entity)
                    if not already_muted:
                        await self._mute_peer(
                            client, entity,
                            context=f"EnsureArchiveMute_{eid}",
                            account_name=account_name,
                        )
                        muted_count += 1
                        await asyncio.sleep(0.5)
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"Ensure mute {eid}: {e}")
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Ensure archive muted iteration: {e}")
        return muted_count, errors

    async def _pin_and_order_chats(self, client, me_id, owner_id, spambot_id, account_name="unknown"):
        errors = []
        pin_order = [TELEGRAM_ID]
        if spambot_id:
            pin_order.append(spambot_id)
        pin_order.append(me_id)
        if owner_id and owner_id not in pin_order:
            pin_order.append(owner_id)

        for uid in pin_order:
            try:
                await self._ensure_connected(client)
                inp = await client.get_input_entity(uid)

                already_pinned = False
                try:
                    dialogs = await client.get_dialogs(limit=20)
                    for d in dialogs:
                        if getattr(d.entity, "id", None) == uid and d.pinned:
                            already_pinned = True
                            break
                except Exception:
                    pass

                if not already_pinned:
                    await self._safe_request(
                        client,
                        ToggleDialogPinRequest(peer=inp, pinned=True),
                        context=f"PinDialog_{uid}",
                        account_name=account_name,
                    )
            except AccountFloodError:
                raise
            except Exception as e:
                errors.append(f"Pin {uid}: {e}")
            await asyncio.sleep(0.5)

        try:
            order_peers = []
            for uid in pin_order:
                try:
                    await self._ensure_connected(client)
                    inp = await client.get_input_entity(uid)
                    order_peers.append(inp)
                except Exception:
                    pass
            if order_peers:
                await self._safe_request(
                    client,
                    ReorderPinnedDialogsRequest(
                        folder_id=0, order=order_peers, force=True
                    ),
                    context="ReorderPinnedDialogs",
                    account_name=account_name,
                )
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Reorder pins: {e}")
        return errors

    async def _get_all_stories(self, client, func, account_name="unknown", **kwargs):
        stories = []
        offset_id = 0
        while True:
            try:
                request = func(
                    peer=types.InputPeerSelf(),
                    offset_id=offset_id,
                    limit=100,
                    **kwargs,
                )
                result = await self._safe_request(
                    client, request, context="GetStories",
                    account_name=account_name,
                )
            except Exception:
                break
            if not result.stories:
                break
            stories.extend(result.stories)
            offset_id = result.stories[-1].id
            if len(result.stories) < 100:
                break
        return stories

    async def _get_album_stories(self, client, album_id, account_name="unknown"):
        stories = []
        offset = 0
        while True:
            try:
                result = await self._safe_request(
                    client,
                    functions.stories.GetAlbumStoriesRequest(
                        peer=types.InputPeerSelf(),
                        album_id=album_id,
                        offset=offset,
                        limit=100,
                    ),
                    context=f"GetAlbumStories_{album_id}",
                    account_name=account_name,
                )
            except Exception:
                break
            if not result.stories:
                break
            stories.extend(result.stories)
            offset += len(result.stories)
            if len(result.stories) < 100:
                break
        return stories

    async def _get_albums(self, client, account_name="unknown"):
        try:
            result = await self._safe_request(
                client,
                functions.stories.GetAlbumsRequest(
                    peer=types.InputPeerSelf(), hash=0
                ),
                context="GetAlbums",
                account_name=account_name,
            )
            return getattr(result, "albums", [])
        except Exception:
            return []

    async def _delete_all_stories_and_albums(self, client, account_name="unknown"):
        deleted_albums = []
        deleted_stories_count = 0
        errors = []

        try:
            albums = await self._get_albums(client, account_name=account_name)
            for album in albums:
                try:
                    album_stories = await self._get_album_stories(
                        client, album.album_id, account_name=account_name
                    )
                    for s in album_stories:
                        try:
                            await self._safe_request(
                                client,
                                functions.stories.DeleteStoriesRequest(
                                    peer=types.InputPeerSelf(), id=[s.id]
                                ),
                                context=f"DeleteStory_{s.id}",
                                account_name=account_name,
                            )
                            deleted_stories_count += 1
                            await asyncio.sleep(0.3)
                        except AccountFloodError:
                            raise
                        except Exception as e:
                            errors.append(
                                f"Delete story {s.id} from album "
                                f"'{album.title}': {e}"
                            )
                    try:
                        await self._safe_request(
                            client,
                            functions.stories.DeleteAlbumRequest(
                                peer=types.InputPeerSelf(),
                                album_id=album.album_id,
                            ),
                            context=f"DeleteAlbum_{album.title}",
                            account_name=account_name,
                        )
                        deleted_albums.append(
                            f"{album.title} ({len(album_stories)} stories)"
                        )
                    except AccountFloodError:
                        raise
                    except Exception as e:
                        errors.append(f"Delete album '{album.title}': {e}")
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(
                        f"Process album '{getattr(album, 'title', '?')}': {e}"
                    )
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Get albums: {e}")

        try:
            active = await self._get_all_stories(
                client, functions.stories.GetPinnedStoriesRequest,
                account_name=account_name,
            )
            for s in active:
                try:
                    await self._safe_request(
                        client,
                        functions.stories.DeleteStoriesRequest(
                            peer=types.InputPeerSelf(), id=[s.id]
                        ),
                        context=f"DeleteActiveStory_{s.id}",
                        account_name=account_name,
                    )
                    deleted_stories_count += 1
                    await asyncio.sleep(0.3)
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"Delete active story {s.id}: {e}")
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Get active stories: {e}")

        try:
            archive = await self._get_all_stories(
                client, functions.stories.GetStoriesArchiveRequest,
                account_name=account_name,
            )
            for s in archive:
                try:
                    await self._safe_request(
                        client,
                        functions.stories.DeleteStoriesRequest(
                            peer=types.InputPeerSelf(), id=[s.id]
                        ),
                        context=f"DeleteArchiveStory_{s.id}",
                        account_name=account_name,
                    )
                    deleted_stories_count += 1
                    await asyncio.sleep(0.3)
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"Delete archive story {s.id}: {e}")
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Get archive stories: {e}")

        return deleted_albums, deleted_stories_count, errors

    async def _delete_all_profile_photos(self, client, me, account_name="unknown"):
        deleted_count = 0
        errors = []
        try:
            iteration = 0
            while iteration < MAX_PHOTO_ITERATIONS:
                iteration += 1
                result = await self._safe_request(
                    client,
                    GetUserPhotosRequest(
                        user_id=me, offset=0, max_id=0, limit=100
                    ),
                    context="GetUserPhotos",
                    account_name=account_name,
                )
                if not result.photos:
                    break
                input_photos = [
                    InputPhoto(
                        id=p.id,
                        access_hash=p.access_hash,
                        file_reference=p.file_reference,
                    )
                    for p in result.photos
                ]
                batch_deleted = 0
                try:
                    await self._safe_request(
                        client,
                        DeletePhotosRequest(id=input_photos),
                        context="DeletePhotos",
                        account_name=account_name,
                    )
                    batch_deleted = len(input_photos)
                    deleted_count += batch_deleted
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"Batch delete photos: {e}")
                    for inp in input_photos:
                        try:
                            await self._safe_request(
                                client,
                                DeletePhotosRequest(id=[inp]),
                                context=f"DeletePhoto_{inp.id}",
                                account_name=account_name,
                            )
                            deleted_count += 1
                            batch_deleted += 1
                        except AccountFloodError:
                            raise
                        except Exception as e2:
                            errors.append(f"Delete photo {inp.id}: {e2}")
                        await asyncio.sleep(0.2)
                if batch_deleted == 0:
                    break
                if len(result.photos) < 100:
                    break
                await asyncio.sleep(0.3)
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Get photos: {e}")
        return deleted_count, errors

    async def _set_avatar(self, client, url, account_name="unknown"):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return False, f"HTTP error {resp.status}"
                    photo_bytes = await resp.read()
            if not photo_bytes:
                return False, "Empty response"
            await self._ensure_connected(client)
            uploaded = await client.upload_file(
                io.BytesIO(photo_bytes), file_name="avatar.jpg"
            )
            await self._safe_request(
                client,
                UploadProfilePhotoRequest(file=uploaded),
                context="UploadProfilePhoto",
                account_name=account_name,
            )
            return True, None
        except AccountFloodError:
            raise
        except Exception as e:
            logger.error(f"[MANAGER] Set avatar error: {e}")
            return False, str(e)

    async def _join_folder(self, client, folder_link, account_name="unknown"):
        folder_hash = self._extract_folder_hash(folder_link)
        if not folder_hash:
            return False, "Invalid folder link format", []
        joined_peers = []
        try:
            check = await self._safe_request(
                client,
                CheckChatlistInviteRequest(slug=folder_hash),
                context="CheckChatlistInvite",
                account_name=account_name,
            )
            peers = list(check.peers)
            await self._safe_request(
                client,
                JoinChatlistInviteRequest(slug=folder_hash, peers=peers),
                context="JoinChatlistInvite",
                account_name=account_name,
            )
            for p in peers:
                pid = self._get_peer_id(p)
                if pid:
                    joined_peers.append(pid)
            return True, None, joined_peers
        except AccountFloodError:
            raise
        except Exception as e:
            if "already" in str(e).lower():
                try:
                    await self._safe_request(
                        client,
                        JoinChatlistInviteRequest(slug=folder_hash, peers=[]),
                        context="JoinChatlistInvite_refresh",
                        account_name=account_name,
                    )
                    return True, "Already joined, refreshed", []
                except AccountFloodError:
                    raise
                except Exception as e2:
                    return False, str(e2), []
            return False, str(e), []

    async def _mute_folder_chats(self, client, peer_ids, account_name="unknown"):
        muted = []
        errors = []
        for pid in peer_ids:
            try:
                await self._ensure_connected(client)
                entity = await client.get_entity(pid)

                already_muted = await self._is_peer_muted(client, entity)
                if not already_muted:
                    await self._mute_peer(
                        client, entity, context=f"MuteFolderChat_{pid}",
                        account_name=account_name,
                    )
                    await asyncio.sleep(0.3)

                await self._ensure_connected(client)
                await client.edit_folder(entity, 1)
                name = (
                    get_full_name(entity)
                    if isinstance(entity, (User, Channel, Chat))
                    else str(pid)
                )
                muted.append(f"{name} (ID: {pid})")
                await asyncio.sleep(0.5)
            except AccountFloodError:
                raise
            except FloodWaitError as e:
                self._mark_flood(client, e.seconds, method="mute_folder_chats", account_name=account_name)
                raise AccountFloodError(e.seconds, method="mute_folder_chats")
            except Exception as e:
                errors.append(f"Mute folder chat {pid}: {e}")
        return muted, errors

    async def _process_account(self, client, me, account_name="unknown"):
        summary = []
        detailed = []
        owner_id = self._me.id
        spambot_id = await self._get_spambot_id(client)

        excluded_ids = {me.id, owner_id, TELEGRAM_ID}
        if spambot_id:
            excluded_ids.add(spambot_id)

        acc_name = get_full_name(me)
        detailed.append(f"=== Account: {acc_name} (ID: {me.id}) ===\n")

        # Step 0: Match owner
        match_ok, match_err = await self._match_owner(client, me, account_name=account_name)
        summary.append(f"Match owner: {'Done' if match_ok else 'Error'}")
        detailed.append("\n--- Match owner ---")
        if match_ok:
            detailed.append(f"  Matched owner successfully (ID: {owner_id})")
        if match_err:
            for e in match_err:
                detailed.append(f"  {e}")

        # Step 1: Delete stories & albums
        del_albums, del_stories, st_err = await self._delete_all_stories_and_albums(
            client, account_name=account_name
        )
        summary.append(f"Albums deleted: {len(del_albums)}")
        summary.append(f"Stories deleted: {del_stories}")
        detailed.append(f"\n--- Deleted albums ({len(del_albums)}) ---")
        for a in del_albums:
            detailed.append(f"  {a}")
        detailed.append(f"\n--- Total stories deleted: {del_stories} ---")
        if st_err:
            detailed.append("\n--- Stories/Albums Errors ---")
            for e in st_err:
                detailed.append(f"  {e}")

        # Step 2: Delete profile photos
        ph_del, ph_err = await self._delete_all_profile_photos(client, me, account_name=account_name)
        summary.append(f"Profile photos deleted: {ph_del}")
        detailed.append(f"\n--- Profile photos deleted: {ph_del} ---")
        if ph_err:
            detailed.append("\n--- Photos Errors ---")
            for e in ph_err:
                detailed.append(f"  {e}")

        # Step 3: Join folders (1, 2, 3)
        folder_links = self._get_folder_links()
        all_folder_peers = []
        if folder_links:
            for fi, folder_link in enumerate(folder_links, 1):
                ok, err, folder_peers = await self._join_folder(
                    client, folder_link, account_name=account_name
                )
                all_folder_peers.extend(folder_peers)
                summary.append(f"Join folder #{fi}: {'Done' if ok else 'Error'}")
                detailed.append(f"\n--- Join folder #{fi} ---")
                if ok:
                    detailed.append(f"  {err or 'Joined successfully'}: {folder_link}")
                    detailed.append(f"  Chats joined: {len(folder_peers)}")
                else:
                    detailed.append(f"  Error: {err}")
                if fi < len(folder_links):
                    await asyncio.sleep(10)
        else:
            summary.append("Join folders: Skipped (no links)")
            detailed.append("\n--- Join folders: Skipped ---")

        # Step 4: Create/update admin folders (channels, groups) — BEFORE mute+archive
        channels, groups = await self._get_admin_chats(client)
        used_ids, existing_filters = await self._get_existing_folder_ids(
            client, account_name=account_name
        )

        for label, entities in (("channels", channels), ("groups", groups)):
            existing = self._find_folder_by_title(existing_filters, label)
            if existing:
                if entities:
                    ok, added = await self._update_existing_folder(
                        client, existing, entities, account_name=account_name
                    )
                    summary.append(
                        f"Folder '{label}' update: {'Done' if ok else 'Error'}"
                    )
                    detailed.append(
                        f"\n--- Folder '{label}' (existing, ID: {existing.id}) ---"
                    )
                    for ent in added:
                        detailed.append(
                            f"  Added: {getattr(ent, 'title', '?')} (ID: {ent.id})"
                        )
                    if not added:
                        detailed.append("  No new entries to add")
                else:
                    summary.append(f"Folder '{label}' update: Skipped (empty)")
                    detailed.append(f"\n--- Folder '{label}': exists, nothing to add ---")
            else:
                fid = self._get_free_folder_id(used_ids)
                if fid and entities:
                    ok = await self._create_folder(
                        client, fid, label, entities, account_name=account_name
                    )
                    used_ids.add(fid)
                    summary.append(
                        f"Folder '{label}' create: {'Done' if ok else 'Error'}"
                    )
                    detailed.append(f"\n--- Folder '{label}' (ID: {fid}) ---")
                    if ok:
                        for ent in entities:
                            detailed.append(
                                f"  Added: {getattr(ent, 'title', '?')} "
                                f"(ID: {ent.id})"
                            )
                    else:
                        detailed.append("  FAILED")
                elif not entities:
                    summary.append(f"Folder '{label}' create: Skipped (empty)")
                    detailed.append(f"\n--- Folder '{label}': Skipped (no entries) ---")
                else:
                    summary.append(f"Folder '{label}': no free folder ID")
                    detailed.append(f"\n--- Folder '{label}': no free folder ID ---")

        # Step 5: Mute + archive folder chats — AFTER creating folders
        if all_folder_peers:
            m_ok, m_err = await self._mute_folder_chats(
                client, all_folder_peers, account_name=account_name
            )
            summary.append(f"Folder chats muted+archived: {len(m_ok)}")
            detailed.append(
                f"\n--- Folder chats muted & archived ({len(m_ok)}) ---"
            )
            for m in m_ok:
                detailed.append(f"  {m}")
            if m_err:
                detailed.append("\n--- Folder mute Errors ---")
                for e in m_err:
                    detailed.append(f"  {e}")

        # Step 6: Remove Saved Messages from all folders
        await asyncio.sleep(0.5)
        sv_ok = await self._remove_saved_from_all_folders(
            client, me.id, account_name=account_name
        )
        summary.append(f"Remove 'Saved' from folders: {'Done' if sv_ok else 'Error'}")
        detailed.append(
            f"\n--- Remove 'Saved' from folders: {'Done' if sv_ok else 'Error'} ---"
        )

        # Step 7: Clear Saved Messages + send confirmation
        sv_clear_err = await self._clear_saved_messages(
            client, me, account_name=account_name
        )
        summary.append(f"Clear Saved Messages: {'Done' if not sv_clear_err else 'Partial'}")
        detailed.append("\n--- Clear Saved Messages ---")
        if sv_clear_err:
            for e in sv_clear_err:
                detailed.append(f"  {e}")
        else:
            detailed.append("  Cleared and confirmation sent")

        # Step 8: Delete PMs + block users
        blocked = []
        deleted_pm = []
        s8_err = []
        try:
            await self._ensure_connected(client)
            dialogs = await client.get_dialogs()
            for d in dialogs:
                ent = d.entity
                if not isinstance(ent, User):
                    continue
                uid = ent.id
                if uid in excluded_ids:
                    continue
                try:
                    await self._safe_request(
                        client,
                        DeleteHistoryRequest(peer=ent, max_id=0, revoke=True),
                        context=f"DeleteHistory_{uid}",
                        account_name=account_name,
                    )
                    deleted_pm.append(f"{get_full_name(ent)} (ID: {uid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s8_err.append(f"Delete history {uid}: {e}")
                try:
                    await self._safe_request(
                        client, BlockRequest(id=ent), context=f"Block_{uid}",
                        account_name=account_name,
                    )
                    blocked.append(f"{get_full_name(ent)} (ID: {uid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s8_err.append(f"Block {uid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            s8_err.append(f"Dialog iteration: {e}")

        summary.append(f"Private chats deleted: {len(deleted_pm)}")
        summary.append(f"Users blocked: {len(blocked)}")
        detailed.append(f"\n--- Deleted PMs ({len(deleted_pm)}) ---")
        for d in deleted_pm:
            detailed.append(f"  {d}")
        detailed.append(f"\n--- Blocked ({len(blocked)}) ---")
        for b in blocked:
            detailed.append(f"  {b}")
        if s8_err:
            detailed.append("\n--- Step 8 Errors ---")
            for e in s8_err:
                detailed.append(f"  {e}")

        # Step 9: Leave chats not in folders
        left = []
        s9_err = []
        try:
            _, all_filters = await self._get_existing_folder_ids(
                client, account_name=account_name
            )
            in_folders = await self._get_chats_in_folders(all_filters)
            await self._ensure_connected(client)
            dialogs = await client.get_dialogs()
            for d in dialogs:
                ent = d.entity
                if isinstance(ent, (ChannelForbidden, ChatForbidden, User)):
                    continue
                eid = ent.id
                if eid in in_folders:
                    continue
                try:
                    if isinstance(ent, Channel):
                        try:
                            await self._safe_request(
                                client,
                                LeaveChannelRequest(channel=ent),
                                context=f"LeaveChannel_{eid}",
                                account_name=account_name,
                            )
                            left.append(f"{ent.title} (ID: {eid})")
                        except UserCreatorError:
                            s9_err.append(f"Creator of {ent.title} ({eid})")
                        except ChannelPrivateError:
                            left.append(f"{ent.title} (ID: {eid}, private/left)")
                    elif isinstance(ent, Chat):
                        try:
                            await self._ensure_connected(client)
                            me_inp = await client.get_input_entity(me.id)
                            await self._safe_request(
                                client,
                                DeleteChatUserRequest(
                                    chat_id=ent.id,
                                    user_id=me_inp,
                                    revoke_history=True,
                                ),
                                context=f"LeaveChat_{eid}",
                                account_name=account_name,
                            )
                            left.append(f"{ent.title} (ID: {eid}, chat)")
                        except UserCreatorError:
                            s9_err.append(f"Creator of {ent.title} ({eid})")
                        except ChatIdInvalidError:
                            s9_err.append(f"Invalid chat ID: {ent.title} ({eid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s9_err.append(f"Leave {eid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            s9_err.append(f"Leave iteration: {e}")

        summary.append(f"Left chats/channels: {len(left)}")
        detailed.append(f"\n--- Left chats/channels ({len(left)}) ---")
        for item in left:
            detailed.append(f"  {item}")
        if s9_err:
            detailed.append("\n--- Step 9 Errors ---")
            for e in s9_err:
                detailed.append(f"  {e}")

        # Step 10: Delete contacts
        del_contacts = []
        s10_err = []
        try:
            cr = await self._safe_request(
                client, GetContactsRequest(hash=0), context="GetContacts",
                account_name=account_name,
            )
            if hasattr(cr, "users") and cr.users:
                try:
                    await self._safe_request(
                        client,
                        DeleteContactsRequest(id=cr.users),
                        context="DeleteContacts",
                        account_name=account_name,
                    )
                    for cu in cr.users:
                        del_contacts.append(f"{get_full_name(cu)} (ID: {cu.id})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s10_err.append(f"Batch delete contacts: {e}")
                    for cu in cr.users:
                        try:
                            await self._safe_request(
                                client,
                                DeleteContactsRequest(id=[cu]),
                                context=f"DeleteContact_{cu.id}",
                                account_name=account_name,
                            )
                            del_contacts.append(
                                f"{get_full_name(cu)} (ID: {cu.id})"
                            )
                        except AccountFloodError:
                            raise
                        except Exception as e2:
                            s10_err.append(f"Delete contact {cu.id}: {e2}")
                        await asyncio.sleep(0.2)
        except AccountFloodError:
            raise
        except Exception as e:
            s10_err.append(f"Get contacts: {e}")

        summary.append(f"Contacts deleted: {len(del_contacts)}")
        detailed.append(f"\n--- Deleted contacts ({len(del_contacts)}) ---")
        for dc in del_contacts:
            detailed.append(f"  {dc}")
        if s10_err:
            detailed.append("\n--- Step 10 Errors ---")
            for e in s10_err:
                detailed.append(f"  {e}")

        # Step 11: Archive all except excluded (with checks)
        arch, arch_err = await self._archive_all_except(
            client, excluded_ids, account_name=account_name
        )
        summary.append(f"Chats archived: {len(arch)}")
        detailed.append(f"\n--- Archived chats ({len(arch)}) ---")
        for a in arch:
            detailed.append(f"  {a}")
        if arch_err:
            detailed.append("\n--- Archive Errors ---")
            for e in arch_err:
                detailed.append(f"  {e}")

        # Step 12: Pin & order chats
        pin_err = await self._pin_and_order_chats(
            client, me.id, owner_id, spambot_id, account_name=account_name
        )
        pin_ok = not pin_err
        summary.append(f"Pin & order: {'Done' if pin_ok else 'Partial/Error'}")
        detailed.append("\n--- Pin & order chats ---")
        detailed.append("  Order: Telegram -> SpamBot -> Saved -> Owner")
        if pin_err:
            for e in pin_err:
                detailed.append(f"  Error: {e}")
        else:
            detailed.append("  All pinned successfully")

        # Step 13: Set avatar
        avatar_url = self.config["avatar_url"]
        if avatar_url:
            av_ok, av_err = await self._set_avatar(client, avatar_url, account_name=account_name)
            summary.append(f"Set avatar: {'Done' if av_ok else 'Error'}")
            detailed.append("\n--- Set avatar ---")
            if av_ok:
                detailed.append("  Avatar set successfully")
            else:
                detailed.append(f"  Error: {av_err}")
        else:
            summary.append("Set avatar: Skipped (no URL)")
            detailed.append("\n--- Set avatar: Skipped ---")

        # Step 14: Ensure all archived chats are muted
        muted_fix, mute_fix_err = await self._ensure_archive_muted(
            client, excluded_ids, account_name=account_name
        )
        summary.append(f"Archive mute check: {muted_fix} fixed")
        detailed.append(f"\n--- Archive mute check ---")
        detailed.append(f"  Fixed unmuted archived chats: {muted_fix}")
        if mute_fix_err:
            detailed.append("\n--- Archive mute check Errors ---")
            for e in mute_fix_err:
                detailed.append(f"  {e}")

        return summary, detailed

    @loader.command(
        ru_doc="Управление модулем",
        en_doc="Manage module",
    )
    async def manage(self, message: Message):
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []
        if not args_list:
            return await utils.answer(message, self.strings["help"])

        cmd = args_list[0].lower()
        handlers = {
            "add": self._cmd_add,
            "list": self._cmd_list,
            "remove": self._cmd_remove,
            "folder": self._cmd_folder,
            "ava": self._cmd_ava,
            "set": self._cmd_set,
            "start": self._cmd_start,
        }
        handler = handlers.get(cmd)
        if handler:
            if cmd in ("add", "remove", "folder", "ava", "set"):
                await handler(message, args_list)
            else:
                await handler(message)
        else:
            await utils.answer(message, self.strings["help"])

    async def _cmd_add(self, message: Message, args):
        api_id, api_hash = self._get_api_credentials()
        if not api_id:
            return await utils.answer(message, self.strings["error_no_api"])
        if len(self._sessions_runtime) >= MAX_SESSIONS:
            return await utils.answer(
                message, self.strings["session_max"].format(max=MAX_SESSIONS)
            )

        persistent = False
        session_str = None

        if len(args) > 1 and args[1].lower() == "long":
            persistent = True
            if len(args) > 2:
                session_str = self._find_string_session(" ".join(args[2:]))
        elif len(args) > 1:
            session_str = self._find_string_session(" ".join(args[1:]))

        if not session_str:
            reply = await message.get_reply_message()
            if reply and reply.text:
                session_str = self._find_string_session(reply.text)

        if not session_str:
            return await utils.answer(message, self.strings["provide_session"])

        for s in self._sessions_runtime:
            if s["session"] == session_str:
                return await utils.answer(message, self.strings["session_exists"])

        client, result = await self._connect_session(session_str)
        if client is None:
            return await utils.answer(
                message,
                self.strings["session_not_authorized"]
                + f"\n{self.strings['line']}\nReason: {result}",
            )

        me = result
        phone = getattr(me, "phone", "Unknown") or "Hidden"

        for s in self._sessions_runtime:
            if s["user_id"] == me.id:
                await client.disconnect()
                return await utils.answer(message, self.strings["session_exists"])

        self._sessions_runtime.append(
            {
                "session": session_str,
                "user_id": me.id,
                "name": get_full_name(me),
                "phone": phone,
                "persistent": persistent,
            }
        )
        self._clients.append(client)
        self._save_sessions()

        try:
            await message.delete()
        except Exception:
            pass

        topic_id = None
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            topic_id = getattr(reply_to, "reply_to_top_id", None) or getattr(
                reply_to, "reply_to_msg_id", None
            )

        await self._client.send_message(
            message.chat_id,
            self.strings["session_added"].format(
                line=self.strings["line"],
                name=get_full_name(me),
                user_id=me.id,
                phone=phone,
                persistent="Yes" if persistent else "No",
                slot=len(self._sessions_runtime),
                max=MAX_SESSIONS,
            ),
            reply_to=topic_id,
            parse_mode="html",
        )

    async def _cmd_list(self, message: Message):
        if not self._sessions_runtime:
            return await utils.answer(message, self.strings["no_sessions"])
        lines = []
        for i, s in enumerate(self._sessions_runtime, 1):
            tag = " [long]" if s.get("persistent") else ""
            lines.append(
                f"{i}. {s['name']} | <code>{s['user_id']}</code> | "
                f"<code>{s['phone']}</code>{tag}"
            )
        await utils.answer(
            message,
            self.strings["session_list"].format(
                count=len(self._sessions_runtime),
                max=MAX_SESSIONS,
                line=self.strings["line"],
                sessions="\n".join(lines),
            ),
        )

    async def _cmd_remove(self, message: Message, args):
        if not self._sessions_runtime:
            return await utils.answer(message, self.strings["no_sessions"])
        if len(args) < 2:
            return await utils.answer(
                message, self.strings["session_remove_invalid"]
            )
        try:
            num = int(args[1])
            if num < 1 or num > len(self._sessions_runtime):
                raise ValueError
        except ValueError:
            return await utils.answer(
                message, self.strings["session_remove_invalid"]
            )
        idx = num - 1
        try:
            await self._clients[idx].disconnect()
        except Exception:
            pass
        self._sessions_runtime.pop(idx)
        self._clients.pop(idx)
        self._save_sessions()
        await utils.answer(
            message, self.strings["session_removed"].format(num=num)
        )

    async def _cmd_folder(self, message: Message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["folder_provide"])

        try:
            folder_num = int(args[1])
            if folder_num not in (1, 2, 3):
                return await utils.answer(message, self.strings["folder_invalid_num"])
        except ValueError:
            return await utils.answer(message, self.strings["folder_invalid_num"])

        key = f"folder_link_{folder_num}"

        if len(args) < 3:
            if self.config[key]:
                self.config[key] = ""
                return await utils.answer(
                    message, self.strings["folder_cleared"].format(num=folder_num)
                )
            return await utils.answer(message, self.strings["folder_provide"])

        self.config[key] = args[2]
        await utils.answer(
            message, self.strings["folder_set"].format(num=folder_num, link=args[2])
        )

    async def _cmd_ava(self, message: Message, args):
        if len(args) < 2:
            if self.config["avatar_url"]:
                self.config["avatar_url"] = ""
                return await utils.answer(message, self.strings["ava_cleared"])
            return await utils.answer(message, self.strings["ava_provide"])
        self.config["avatar_url"] = args[1]
        await utils.answer(
            message, self.strings["ava_set"].format(url=args[1])
        )

    async def _cmd_set(self, message: Message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["timezone_invalid"])
        try:
            offset_str = args[1].replace("+", "")
            offset = int(offset_str)
            if not -12 <= offset <= 12:
                return await utils.answer(
                    message, self.strings["timezone_invalid"]
                )
            self.config["timezone_offset"] = offset
            await utils.answer(
                message,
                self.strings["timezone_set"].format(
                    timezone_str=self._get_timezone_str(offset)
                ),
            )
        except (ValueError, IndexError):
            await utils.answer(message, self.strings["timezone_invalid"])

    async def _cmd_start(self, message: Message):
        api_id, _ = self._get_api_credentials()
        if not api_id:
            return await utils.answer(message, self.strings["error_no_api"])
        if not self._sessions_runtime or not self._clients:
            return await utils.answer(message, self.strings["error_no_sessions"])
        if self._processing_lock.locked():
            return await utils.answer(
                message, self.strings["already_processing"]
            )

        async with self._processing_lock:
            status = await utils.answer(message, self.strings["processing"])
            self._status_msg = status[0] if isinstance(status, list) else status
            self._flood_until.clear()
            self._flood_log.clear()

            all_results = {}
            pending = {}

            for i, (sdata, client) in enumerate(
                zip(self._sessions_runtime, self._clients)
            ):
                pending[i] = {
                    "sdata": sdata,
                    "client": client,
                    "summary": [],
                    "detailed": [],
                    "done": False,
                    "error": None,
                }

            while True:
                ran_any = False
                all_flooded_now = True

                for i, info in pending.items():
                    if info["done"]:
                        continue

                    client = info["client"]

                    if self._is_flooded(client):
                        continue

                    all_flooded_now = False
                    ran_any = True
                    account_name = info["sdata"]["name"]

                    try:
                        await self._ensure_connected(client)
                        me = await client.get_me()
                        name = get_full_name(me)
                        s, d = await self._process_account(
                            client, me, account_name=name
                        )
                        info["summary"] = [f"\n=== [{i + 1}] {name} (ID: {me.id}) ==="] + s
                        info["detailed"] = [f"\n{'=' * 50}"] + d
                        info["done"] = True
                    except AccountFloodError as fe:
                        logger.info(
                            f"[MANAGER] Account [{i + 1}] {account_name} "
                            f"got flood in {fe.method}, switching to next"
                        )
                        await self._update_flood_status()
                    except Exception as e:
                        info["summary"] = [
                            f"\n=== [{i + 1}] {info['sdata']['name']} ===",
                            f"FATAL ERROR: {e}",
                        ]
                        info["detailed"] = [
                            f"\n=== [{i + 1}] {info['sdata']['name']} ===",
                            f"FATAL ERROR: {e}",
                        ]
                        info["done"] = True
                        info["error"] = str(e)
                        logger.error(
                            f"[MANAGER] Process account [{i + 1}] error: {e}"
                        )

                if all(info["done"] for info in pending.values()):
                    break

                not_done = [
                    i for i, info in pending.items() if not info["done"]
                ]

                if not not_done:
                    break

                if not ran_any or all_flooded_now:
                    flood_times = []
                    for i in not_done:
                        remaining = self._get_flood_remaining(pending[i]["client"])
                        if remaining > 0:
                            flood_times.append(remaining)

                    if flood_times:
                        wait_time = min(flood_times)
                        resume_ts = time.time() + wait_time
                        resume_time_str = self._get_time_str_from_timestamp(resume_ts)
                        logger.info(
                            f"[MANAGER] All accounts flooded, waiting {wait_time:.0f}s "
                            f"until {resume_time_str}"
                        )
                        if self._status_msg:
                            try:
                                await utils.answer(
                                    self._status_msg,
                                    self.strings["processing_flood"].format(
                                        resume_time=resume_time_str
                                    ),
                                )
                            except Exception:
                                pass
                        await asyncio.sleep(wait_time)
                        if self._status_msg:
                            try:
                                await utils.answer(
                                    self._status_msg,
                                    self.strings["processing"],
                                )
                            except Exception:
                                pass
                    else:
                        await asyncio.sleep(1)

            all_summary = []
            all_detailed = []
            for i in sorted(pending.keys()):
                all_summary.extend(pending[i]["summary"])
                all_detailed.extend(pending[i]["detailed"])

            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            cnt = len(self._sessions_runtime)
            sep = "=" * 50

            summary_text = (
                f"MANAGER - CLEANUP SUMMARY\nDate: {ts}\n"
                f"Accounts processed: {cnt}\n{sep}\n"
                + "\n".join(all_summary)
            )
            detailed_text = (
                f"MANAGER - DETAILED LOG\nDate: {ts}\n"
                f"Accounts processed: {cnt}\n{sep}\n"
                + "\n".join(all_detailed)
            )

            flood_log_text = f"MANAGER - FLOOD LOG\nDate: {ts}\n{sep}\n"
            if self._flood_log:
                for entry in self._flood_log:
                    flood_log_text += (
                        f"\nAccount: {entry['account']}\n"
                        f"Method: {entry['method']}\n"
                        f"Flood seconds: {entry['flood_seconds']}\n"
                        f"Extra wait: {entry['extra_wait']}\n"
                        f"Total wait: {entry['total_wait']}\n"
                        f"Timestamp: {entry['timestamp']}\n"
                        f"Resume at: {entry['resume_at']}\n"
                        f"{'-' * 30}\n"
                    )
            else:
                flood_log_text += "\nNo flood events recorded.\n"

            topic_id = None
            reply_to = getattr(message, "reply_to", None)
            if reply_to:
                topic_id = getattr(
                    reply_to, "reply_to_top_id", None
                ) or getattr(reply_to, "reply_to_msg_id", None)

            try:
                await utils.answer(self._status_msg, self.strings["success"])
            except Exception:
                pass
            self._status_msg = None

            sf = io.BytesIO(summary_text.encode("utf-8"))
            sf.name = "summary.txt"
            await self._client.send_file(
                message.chat_id,
                sf,
                caption="<b>Cleanup Summary</b>",
                reply_to=topic_id,
                parse_mode="html",
            )

            df = io.BytesIO(detailed_text.encode("utf-8"))
            df.name = "detailed.txt"
            await self._client.send_file(
                message.chat_id,
                df,
                caption="<b>Detailed Log</b>",
                reply_to=topic_id,
                parse_mode="html",
            )

            ff = io.BytesIO(flood_log_text.encode("utf-8"))
            ff.name = "flood_log.txt"
            await self._client.send_file(
                message.chat_id,
                ff,
                caption="<b>Flood Log</b>",
                reply_to=topic_id,
                parse_mode="html",
            )

            to_remove = []
            for i in range(len(self._sessions_runtime) - 1, -1, -1):
                if not self._sessions_runtime[i].get("persistent", False):
                    try:
                        await self._clients[i].disconnect()
                    except Exception:
                        pass
                    to_remove.append(i)
            for i in to_remove:
                self._sessions_runtime.pop(i)
                self._clients.pop(i)
            self._save_sessions()
            self._flood_until.clear()