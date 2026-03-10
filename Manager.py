__version__ = (1, 5, 0)
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
    StartBotRequest,
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
    BotMethodInvalidError,
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
        "error_no_api": "<b>Error:</b> Set api_id and api_hash in module config first",
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
                "api_id", 0, "Telegram API ID (from my.telegram.org)",
                validator=loader.validators.Integer(minimum=0),
            ),
            loader.ConfigValue(
                "api_hash", "", "Telegram API Hash (from my.telegram.org)",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "sessions_data", [], "Stored sessions data (internal)",
                validator=loader.validators.Hidden(loader.validators.Series()),
            ),
            loader.ConfigValue(
                "folder_link_1", "", "Chatlist folder invite link #1",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "folder_link_2", "", "Chatlist folder invite link #2",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "folder_link_3", "", "Chatlist folder invite link #3",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "avatar_url", "", "Avatar image URL",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "timezone_offset", 3, "Timezone offset from UTC (from -12 to 12)",
                validator=loader.validators.Integer(minimum=-12, maximum=12),
            ),
        )
        self._clients = []
        self._sessions_runtime = []
        self._status_msg = None
        self._processing_lock = asyncio.Lock()
        self._flood_until = {}
        self._flood_log = []
        self._current_message = None

    def _tz(self):
        return timezone(timedelta(hours=self.config["timezone_offset"]))

    def _now_str(self):
        return datetime.now(self._tz()).strftime("%d.%m.%Y %H:%M")

    def _time_str(self, ts):
        return datetime.fromtimestamp(ts, tz=self._tz()).strftime("%H:%M:%S")

    def _datetime_str(self, ts):
        return datetime.fromtimestamp(ts, tz=self._tz()).strftime("%d.%m.%Y %H:%M:%S")

    def _tz_label(self, offset):
        return f"+{offset}" if offset >= 0 else str(offset)

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

    def _get_api(self):
        a, h = self.config["api_id"], self.config["api_hash"]
        return (int(a), str(h)) if a and h else (None, None)

    async def _ec(self, client):
        if not client.is_connected():
            await client.connect()

    def _mark_flood(self, client, seconds, method="unknown", account_name="unknown"):
        extra = random.randint(FLOOD_EXTRA_MIN, FLOOD_EXTRA_MAX)
        total = seconds + extra
        resume_at = time.time() + total
        self._flood_until[id(client)] = resume_at
        entry = {
            "account": account_name, "method": method,
            "flood_seconds": seconds, "extra_wait": extra, "total_wait": total,
            "timestamp": self._datetime_str(time.time()),
            "resume_at": self._datetime_str(resume_at),
        }
        self._flood_log.append(entry)
        logger.warning(
            f"[MANAGER] {account_name} flood {seconds}s+{extra}s in {method}, "
            f"resume {self._time_str(resume_at)}"
        )

    def _is_flooded(self, client):
        d = self._flood_until.get(id(client))
        if d is None:
            return False
        if time.time() >= d:
            del self._flood_until[id(client)]
            return False
        return True

    def _flood_remaining(self, client):
        d = self._flood_until.get(id(client))
        return max(0, d - time.time()) if d else 0

    async def _send_flood_report(self, account_name, method, seconds, chat_id, topic_id):
        txt = (
            f"FLOOD WAIT REPORT\n"
            f"Date: {self._now_str()}\n"
            f"Account: {account_name}\n"
            f"Method: {method}\n"
            f"Flood seconds: {seconds}\n"
            f"{'=' * 40}\n\n"
            f"All flood events:\n"
        )
        for e in self._flood_log:
            txt += (
                f"\nAccount: {e['account']}\n"
                f"Method: {e['method']}\n"
                f"Flood: {e['flood_seconds']}s + {e['extra_wait']}s extra\n"
                f"Time: {e['timestamp']}\n"
                f"Resume: {e['resume_at']}\n"
                f"{'-' * 30}\n"
            )
        f = io.BytesIO(txt.encode("utf-8"))
        f.name = "flood_report.txt"
        try:
            await self._client.send_file(
                chat_id, f, caption=f"<b>⚠️ FloodWait: {account_name} in {method} ({seconds}s)</b>",
                reply_to=topic_id, parse_mode="html",
            )
        except Exception:
            pass

    async def _update_flood_status(self):
        if not self._status_msg:
            return
        flooded = {c: d for c, d in self._flood_until.items() if time.time() < d}
        if not flooded:
            try:
                await utils.answer(self._status_msg, self.strings["processing"])
            except Exception:
                pass
            return
        rt = self._time_str(min(flooded.values()))
        try:
            await utils.answer(
                self._status_msg,
                self.strings["processing_flood"].format(resume_time=rt),
            )
        except Exception:
            pass

    async def _sr(self, client, request, ctx="", account_name="unknown", retries=3):
        for attempt in range(retries):
            try:
                await self._ec(client)
                return await client(request)
            except FloodWaitError as e:
                self._mark_flood(client, e.seconds, method=ctx, account_name=account_name)
                if self._current_message:
                    tid = None
                    rt = getattr(self._current_message, "reply_to", None)
                    if rt:
                        tid = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)
                    await self._send_flood_report(
                        account_name, ctx, e.seconds,
                        self._current_message.chat_id, tid,
                    )
                raise AccountFloodError(e.seconds, method=ctx)
            except (ConnectionError, OSError) as e:
                logger.warning(f"[MANAGER] Conn error {ctx} ({attempt+1}/{retries}): {e}")
                await asyncio.sleep(2)
                try:
                    await self._ec(client)
                except Exception:
                    pass
            except Exception:
                raise
        raise Exception(f"Max retries for {ctx}")

    async def _connect_session(self, ss):
        aid, ah = self._get_api()
        if not aid:
            return None, "No API"
        c = None
        try:
            c = TelegramClient(StringSession(ss), api_id=aid, api_hash=ah)
            await c.connect()
            if not await c.is_user_authorized():
                await c.disconnect()
                return None, "Not authorized"
            return c, await c.get_me()
        except AuthKeyUnregisteredError:
            if c:
                try: await c.disconnect()
                except Exception: pass
            return None, "Session revoked"
        except UserDeactivatedBanError:
            if c:
                try: await c.disconnect()
                except Exception: pass
            return None, "Account banned"
        except Exception as e:
            if c:
                try: await c.disconnect()
                except Exception: pass
            return None, str(e)

    async def _restore_sessions(self):
        aid, _ = self._get_api()
        if not aid:
            return
        for entry in (self.config["sessions_data"] or []):
            try:
                ss = entry.get("session", "")
                if not ss:
                    continue
                c, r = await self._connect_session(ss)
                if c is None:
                    logger.warning(f"[MANAGER] Restore fail: {r}")
                    continue
                self._sessions_runtime.append({
                    "session": ss, "user_id": r.id,
                    "name": get_full_name(r),
                    "phone": getattr(r, "phone", "Unknown") or "Hidden",
                    "persistent": entry.get("persistent", False),
                })
                self._clients.append(c)
            except Exception as e:
                logger.warning(f"[MANAGER] Restore error: {e}")

    def _save_sessions(self):
        self.config["sessions_data"] = [
            {"session": s["session"], "persistent": s.get("persistent", False),
             "user_id": s["user_id"], "name": s["name"], "phone": s["phone"]}
            for s in self._sessions_runtime
        ]

    def _find_ss(self, text):
        if not text:
            return None
        m = STRING_SESSION_PATTERN.search(text)
        return m.group(0) if m else None

    def _folder_hash(self, link):
        if not link:
            return None
        for sep in ("addlist/", "slug="):
            if sep in link:
                p = link.split(sep)[-1].split("?" if sep == "addlist/" else "&")[0].strip()
                return p or None
        return None

    def _peer_id(self, peer):
        for attr in ("channel_id", "chat_id", "user_id"):
            if hasattr(peer, attr):
                return getattr(peer, attr)
        return None

    def _folder_links(self):
        return [v.strip() for k in ("folder_link_1", "folder_link_2", "folder_link_3")
                if (v := self.config[k]) and v.strip()]

    async def _get_filters(self, client, an="unknown"):
        try:
            r = await self._sr(client, GetDialogFiltersRequest(), "GetDialogFilters", an)
            fs = getattr(r, "filters", r)
            return {f.id for f in fs if hasattr(f, "id")}, fs
        except Exception:
            return set(), []

    def _free_fid(self, used):
        for i in range(2, 256):
            if i not in used:
                return i
        return None

    def _chats_in_folders(self, filters):
        ids = set()
        for f in filters:
            for attr in ("include_peers", "pinned_peers"):
                for p in (getattr(f, attr, None) or []):
                    pid = self._peer_id(p)
                    if pid:
                        ids.add(pid)
        return ids

    def _find_folder(self, filters, title):
        for f in filters:
            if not isinstance(f, DialogFilter) or not hasattr(f, "title"):
                continue
            t = f.title
            if hasattr(t, "text"):
                t = t.text
            if isinstance(t, str) and t.lower() == title.lower():
                return f
        return None

    def _clone_filter(self, f, **kw):
        fields = {
            "id": f.id, "title": f.title,
            "pinned_peers": list(f.pinned_peers or []),
            "include_peers": list(f.include_peers or []),
            "exclude_peers": list(f.exclude_peers or []),
        }
        for flag in ("contacts", "non_contacts", "groups", "broadcasts", "bots",
                      "exclude_muted", "exclude_read", "exclude_archived"):
            v = getattr(f, flag, None)
            if v is not None:
                fields[flag] = v
        for fld in ("emoticon", "color"):
            v = getattr(f, fld, None)
            if v is not None:
                fields[fld] = v
        fields.update(kw)
        return DialogFilter(**fields)

    async def _mute_peer(self, client, peer, ctx="", an="unknown"):
        await self._sr(client, UpdateNotifySettingsRequest(
            peer=peer,
            settings=InputPeerNotifySettings(
                show_previews=False, silent=True, mute_until=2**31 - 1,
            ),
        ), ctx, an)

    async def _mute_and_archive(self, client, entity, an="unknown"):
        try:
            await self._ec(client)
            await self._mute_peer(client, entity, "MuteBeforeArchive", an)
            await asyncio.sleep(0.5)
            await self._ec(client)
            await client.edit_folder(entity, 1)
            return True
        except AccountFloodError:
            raise
        except FloodWaitError as e:
            self._mark_flood(client, e.seconds, "mute_and_archive", an)
            raise AccountFloodError(e.seconds, "mute_and_archive")
        except Exception as e:
            logger.error(f"[MANAGER] Mute/archive: {e}")
            return False

    async def _start_spambot(self, client, an="unknown"):
        errors = []
        try:
            await self._ec(client)
            bot = await client.get_entity(SPAMBOT_USERNAME)
            bot_inp = await client.get_input_entity(bot)
            try:
                await self._sr(client, StartBotRequest(
                    bot=bot_inp, peer=bot_inp,
                    random_id=random.randint(1, 2**63 - 1),
                    start_param="start",
                ), "StartSpamBot", an)
                return True, errors
            except (BotMethodInvalidError, Exception):
                pass
            try:
                await self._ec(client)
                await client.send_message(bot, "/start")
                return True, errors
            except Exception as e:
                errors.append(f"Send /start: {e}")
                return False, errors
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"SpamBot error: {e}")
            return False, errors

    async def _match_owner(self, client, me, an="unknown"):
        errors = []
        try:
            ou = get_owner_username(self._me)
            if not ou:
                errors.append("Owner has no username")
                return False, errors
            await self._ec(client)
            try:
                resolved = await client.get_entity(ou)
            except Exception as e:
                errors.append(f"Resolve owner @{ou}: {e}")
                return False, errors
            try:
                await self._ec(client)
                await client.send_message(resolved, "successfully matched owner")
            except Exception as e:
                errors.append(f"Send match msg: {e}")
                return False, errors
            await asyncio.sleep(3)
            try:
                await self._client(DeleteHistoryRequest(
                    peer=me.id, max_id=0, just_clear=True, revoke=False,
                ))
            except Exception as e:
                errors.append(f"Owner clear chat: {e}")
            return True, errors
        except Exception as e:
            errors.append(f"Match owner: {e}")
            return False, errors

    async def _get_spambot_id(self, client):
        try:
            await self._ec(client)
            return (await client.get_entity(SPAMBOT_USERNAME)).id
        except Exception:
            return None

    async def _get_admin_chats(self, client):
        channels, groups = [], []
        try:
            await self._ec(client)
            async for d in client.iter_dialogs():
                e = d.entity
                if isinstance(e, (ChannelForbidden, ChatForbidden)):
                    continue
                if isinstance(e, Channel):
                    if not e.admin_rights and not e.creator:
                        continue
                    if getattr(e, "broadcast", False):
                        channels.append(e)
                    elif getattr(e, "megagroup", False):
                        groups.append(e)
                elif isinstance(e, Chat):
                    if e.admin_rights or getattr(e, "creator", False):
                        groups.append(e)
        except Exception as e:
            logger.error(f"[MANAGER] Admin chats: {e}")
        return channels, groups

    async def _remove_saved_from_all_folders(self, client, me_id, an="unknown"):
        removed = []
        try:
            _, filters = await self._get_filters(client, an)
            for f in filters:
                if not isinstance(f, DialogFilter) or not hasattr(f, "include_peers"):
                    continue
                has = False
                ni = []
                for p in f.include_peers:
                    if self._peer_id(p) == me_id:
                        has = True
                    else:
                        ni.append(p)
                np_ = []
                for p in (f.pinned_peers or []):
                    if self._peer_id(p) == me_id:
                        has = True
                    else:
                        np_.append(p)
                if has:
                    t = f.title
                    if hasattr(t, "text"):
                        t = t.text
                    u = self._clone_filter(f, pinned_peers=np_, include_peers=ni)
                    try:
                        await self._sr(client, UpdateDialogFilterRequest(id=f.id, filter=u),
                                       f"RemoveSaved_{f.id}", an)
                        removed.append(f"'{t}' (ID:{f.id})")
                    except Exception as e:
                        logger.warning(f"[MANAGER] Remove saved {f.id}: {e}")
                    await asyncio.sleep(0.5)
            return True, removed
        except Exception as e:
            logger.error(f"[MANAGER] Remove saved: {e}")
            return False, removed

    async def _clear_saved_messages(self, client, an="unknown"):
        errors = []
        try:
            await self._ec(client)
            await client(DeleteHistoryRequest(
                peer=InputPeerSelf(), max_id=0, just_clear=True, revoke=False,
            ))
        except Exception as e:
            errors.append(f"Clear saved: {e}")
        try:
            await self._ec(client)
            await client.send_message("me", f"successfully cleared ({self._now_str()})")
        except Exception as e:
            errors.append(f"Send confirmation: {e}")
        return errors

    async def _join_folder(self, client, link, an="unknown"):
        fh = self._folder_hash(link)
        if not fh:
            return False, "Invalid link", []
        peers = []
        try:
            check = await self._sr(client, CheckChatlistInviteRequest(slug=fh), "CheckChatlist", an)
            pl = list(check.peers)
            await self._sr(client, JoinChatlistInviteRequest(slug=fh, peers=pl), "JoinChatlist", an)
            for p in pl:
                pid = self._peer_id(p)
                if pid:
                    peers.append(pid)
            return True, None, peers
        except AccountFloodError:
            raise
        except Exception as e:
            if "already" in str(e).lower():
                try:
                    await self._sr(client, JoinChatlistInviteRequest(slug=fh, peers=[]),
                                   "JoinChatlist_refresh", an)
                    return True, "Already joined", []
                except AccountFloodError:
                    raise
                except Exception as e2:
                    return False, str(e2), []
            return False, str(e), []

    async def _mute_folder_chats(self, client, pids, an="unknown"):
        muted, errors = [], []
        for pid in pids:
            try:
                await self._ec(client)
                ent = await client.get_entity(pid)
                await self._mute_peer(client, ent, f"MuteFolder_{pid}", an)
                await asyncio.sleep(0.3)
                await self._ec(client)
                await client.edit_folder(ent, 1)
                name = get_full_name(ent) if isinstance(ent, (User, Channel, Chat)) else str(pid)
                muted.append(f"{name} (ID:{pid})")
                await asyncio.sleep(0.5)
            except AccountFloodError:
                raise
            except FloodWaitError as e:
                self._mark_flood(client, e.seconds, "mute_folder", an)
                raise AccountFloodError(e.seconds, "mute_folder")
            except Exception as e:
                errors.append(f"Mute folder {pid}: {e}")
        return muted, errors

    async def _update_folder(self, client, folder, entities, an="unknown"):
        try:
            existing = {self._peer_id(p) for p in folder.include_peers}
            new_peers = list(folder.include_peers)
            added = []
            for ent in entities:
                if ent.id not in existing:
                    try:
                        await self._ec(client)
                        new_peers.append(await client.get_input_entity(ent))
                        existing.add(ent.id)
                        added.append(ent)
                    except Exception:
                        pass
            if not added:
                return True, []
            u = self._clone_filter(folder, include_peers=new_peers)
            await self._sr(client, UpdateDialogFilterRequest(id=folder.id, filter=u),
                           "UpdateFolder", an)
            return True, added
        except Exception as e:
            logger.error(f"[MANAGER] Update folder: {e}")
            return False, []

    async def _create_folder(self, client, fid, title, peers, an="unknown"):
        try:
            ips = []
            for p in peers:
                try:
                    await self._ec(client)
                    ips.append(await client.get_input_entity(p))
                except Exception:
                    pass
            df = DialogFilter(
                id=fid, title=TextWithEntities(text=title, entities=[]),
                pinned_peers=[], include_peers=ips, exclude_peers=[],
            )
            await self._sr(client, UpdateDialogFilterRequest(id=fid, filter=df), "CreateFolder", an)
            return True
        except Exception as e:
            logger.error(f"[MANAGER] Create folder '{title}': {e}")
            return False

    async def _mute_all_archived(self, client, excluded_ids, an="unknown"):
        muted, errors = 0, []
        try:
            await self._ec(client)
            dialogs = await client.get_dialogs(folder=1, limit=None)
            for d in dialogs:
                ent = d.entity
                eid = getattr(ent, "id", None)
                if eid and eid in excluded_ids:
                    continue
                if isinstance(ent, (ChannelForbidden, ChatForbidden)):
                    continue
                try:
                    await self._mute_peer(client, ent, f"MuteArch_{eid}", an)
                    muted += 1
                    await asyncio.sleep(0.3)
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"Mute arch {eid}: {e}")
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Mute archived iter: {e}")
        return muted, errors

    async def _archive_non_excluded(self, client, excluded_ids, an="unknown"):
        archived, errors = [], []
        try:
            await self._ec(client)
            dialogs = await client.get_dialogs()
            for d in dialogs:
                if d.archived:
                    continue
                ent = d.entity
                eid = getattr(ent, "id", None)
                if eid and eid in excluded_ids:
                    continue
                if isinstance(ent, (ChannelForbidden, ChatForbidden)):
                    continue
                try:
                    ok = await self._mute_and_archive(client, ent, an)
                    name = get_full_name(ent) if isinstance(ent, (User, Channel, Chat)) else str(eid)
                    if ok:
                        archived.append(f"{name} (ID:{eid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"Archive {eid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Archive iter: {e}")
        return archived, errors

    async def _pin_and_order(self, client, me_id, owner_id, spambot_id, an="unknown"):
        errors = []
        order = [TELEGRAM_ID]
        if spambot_id:
            order.append(spambot_id)
        order.append(me_id)
        if owner_id and owner_id not in order:
            order.append(owner_id)

        pinned = set()
        try:
            for d in await client.get_dialogs(limit=30):
                if d.pinned:
                    eid = getattr(d.entity, "id", None)
                    if eid:
                        pinned.add(eid)
        except Exception:
            pass

        for uid in order:
            if uid in pinned:
                continue
            try:
                await self._ec(client)
                inp = await client.get_input_entity(uid)
                await self._sr(client, ToggleDialogPinRequest(peer=inp, pinned=True),
                               f"Pin_{uid}", an)
            except AccountFloodError:
                raise
            except Exception as e:
                errors.append(f"Pin {uid}: {e}")
            await asyncio.sleep(0.5)

        try:
            ops = []
            for uid in order:
                try:
                    await self._ec(client)
                    ops.append(await client.get_input_entity(uid))
                except Exception:
                    pass
            if ops:
                await self._sr(client, ReorderPinnedDialogsRequest(
                    folder_id=0, order=ops, force=True,
                ), "ReorderPins", an)
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Reorder: {e}")
        return errors

    async def _get_all_stories(self, client, func, an="unknown", **kw):
        stories, oid = [], 0
        while True:
            try:
                r = await self._sr(client, func(
                    peer=types.InputPeerSelf(), offset_id=oid, limit=100, **kw,
                ), "GetStories", an)
            except Exception:
                break
            if not r.stories:
                break
            stories.extend(r.stories)
            oid = r.stories[-1].id
            if len(r.stories) < 100:
                break
        return stories

    async def _get_album_stories(self, client, aid, an="unknown"):
        stories, off = [], 0
        while True:
            try:
                r = await self._sr(client, functions.stories.GetAlbumStoriesRequest(
                    peer=types.InputPeerSelf(), album_id=aid, offset=off, limit=100,
                ), f"AlbumStories_{aid}", an)
            except Exception:
                break
            if not r.stories:
                break
            stories.extend(r.stories)
            off += len(r.stories)
            if len(r.stories) < 100:
                break
        return stories

    async def _del_stories_albums(self, client, an="unknown"):
        da, ds, errs = [], 0, []
        try:
            r = await self._sr(client, functions.stories.GetAlbumsRequest(
                peer=types.InputPeerSelf(), hash=0,
            ), "GetAlbums", an)
            for alb in getattr(r, "albums", []):
                try:
                    ss = await self._get_album_stories(client, alb.album_id, an)
                    for s in ss:
                        try:
                            await self._sr(client, functions.stories.DeleteStoriesRequest(
                                peer=types.InputPeerSelf(), id=[s.id],
                            ), f"DelStory_{s.id}", an)
                            ds += 1
                            await asyncio.sleep(0.3)
                        except AccountFloodError:
                            raise
                        except Exception as e:
                            errs.append(f"Del story {s.id}: {e}")
                    try:
                        await self._sr(client, functions.stories.DeleteAlbumRequest(
                            peer=types.InputPeerSelf(), album_id=alb.album_id,
                        ), f"DelAlbum_{alb.title}", an)
                        da.append(f"{alb.title} ({len(ss)})")
                    except AccountFloodError:
                        raise
                    except Exception as e:
                        errs.append(f"Del album '{alb.title}': {e}")
                except AccountFloodError:
                    raise
                except Exception as e:
                    errs.append(f"Album '{getattr(alb, 'title', '?')}': {e}")
        except AccountFloodError:
            raise
        except Exception as e:
            errs.append(f"Albums: {e}")

        for getter in (functions.stories.GetPinnedStoriesRequest,
                        functions.stories.GetStoriesArchiveRequest):
            try:
                for s in await self._get_all_stories(client, getter, an):
                    try:
                        await self._sr(client, functions.stories.DeleteStoriesRequest(
                            peer=types.InputPeerSelf(), id=[s.id],
                        ), f"DelStory_{s.id}", an)
                        ds += 1
                        await asyncio.sleep(0.3)
                    except AccountFloodError:
                        raise
                    except Exception as e:
                        errs.append(f"Del story {s.id}: {e}")
            except AccountFloodError:
                raise
            except Exception as e:
                errs.append(f"Stories: {e}")
        return da, ds, errs

    async def _del_photos(self, client, me, an="unknown"):
        dc, errs = 0, []
        try:
            for _ in range(MAX_PHOTO_ITERATIONS):
                r = await self._sr(client, GetUserPhotosRequest(
                    user_id=me, offset=0, max_id=0, limit=100,
                ), "GetPhotos", an)
                if not r.photos:
                    break
                ips = [InputPhoto(id=p.id, access_hash=p.access_hash,
                                   file_reference=p.file_reference) for p in r.photos]
                try:
                    await self._sr(client, DeletePhotosRequest(id=ips), "DelPhotos", an)
                    dc += len(ips)
                except AccountFloodError:
                    raise
                except Exception:
                    for ip in ips:
                        try:
                            await self._sr(client, DeletePhotosRequest(id=[ip]),
                                           f"DelPhoto_{ip.id}", an)
                            dc += 1
                        except AccountFloodError:
                            raise
                        except Exception as e2:
                            errs.append(f"Photo {ip.id}: {e2}")
                        await asyncio.sleep(0.2)
                if len(r.photos) < 100:
                    break
                await asyncio.sleep(0.3)
        except AccountFloodError:
            raise
        except Exception as e:
            errs.append(f"Photos: {e}")
        return dc, errs

    async def _set_avatar(self, client, url, an="unknown"):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url) as r:
                    if r.status != 200:
                        return False, f"HTTP {r.status}"
                    data = await r.read()
            if not data:
                return False, "Empty"
            await self._ec(client)
            up = await client.upload_file(io.BytesIO(data), file_name="avatar.jpg")
            await self._sr(client, UploadProfilePhotoRequest(file=up), "SetAvatar", an)
            return True, None
        except AccountFloodError:
            raise
        except Exception as e:
            return False, str(e)

    async def _process_account(self, client, me, an="unknown"):
        sm, dt = [], []
        oid = self._me.id
        sbid = await self._get_spambot_id(client)
        excl = {me.id, oid, TELEGRAM_ID}
        if sbid:
            excl.add(sbid)

        dt.append(f"=== Account: {an} (ID: {me.id}) ===\n")

        # Step 0: Match owner
        ok, err = await self._match_owner(client, me, an)
        sm.append(f"Match owner: {'Done' if ok else 'Error'}")
        dt.append("\n--- Match owner ---")
        if ok:
            dt.append(f"  OK (owner ID: {oid})")
        for e in err:
            dt.append(f"  {e}")

        # Step 0.5: Start SpamBot
        ok, err = await self._start_spambot(client, an)
        sm.append(f"Start SpamBot: {'Done' if ok else 'Error'}")
        dt.append("\n--- Start SpamBot ---")
        if ok:
            dt.append("  Started")
        for e in err:
            dt.append(f"  {e}")
        await asyncio.sleep(2)
        new_sb = await self._get_spambot_id(client)
        if new_sb:
            sbid = new_sb
            excl.add(sbid)

        # Step 1: Mute all archived chats first
        mc, merr = await self._mute_all_archived(client, excl, an)
        sm.append(f"Mute archived: {mc}")
        dt.append(f"\n--- Mute archived: {mc} ---")
        for e in merr:
            dt.append(f"  {e}")

        # Step 2: Delete stories & albums
        da, ds, serr = await self._del_stories_albums(client, an)
        sm.append(f"Albums: {len(da)}, Stories: {ds}")
        dt.append(f"\n--- Albums ({len(da)}) ---")
        for a in da:
            dt.append(f"  {a}")
        dt.append(f"\n--- Stories deleted: {ds} ---")
        for e in serr:
            dt.append(f"  {e}")

        # Step 3: Delete photos
        pc, perr = await self._del_photos(client, me, an)
        sm.append(f"Photos: {pc}")
        dt.append(f"\n--- Photos: {pc} ---")
        for e in perr:
            dt.append(f"  {e}")

        # Step 4: Join folders
        flinks = self._folder_links()
        all_fp = []
        if flinks:
            for i, fl in enumerate(flinks, 1):
                ok, err, fps = await self._join_folder(client, fl, an)
                all_fp.extend(fps)
                sm.append(f"Folder #{i}: {'Done' if ok else 'Error'}")
                dt.append(f"\n--- Folder #{i} ---")
                dt.append(f"  {err or 'Joined'}: {fl} ({len(fps)} chats)")
                if i < len(flinks):
                    await asyncio.sleep(10)
        else:
            sm.append("Folders: Skip")

        # Step 5: Create/update admin folders
        chs, grs = await self._get_admin_chats(client)
        uids, efs = await self._get_filters(client, an)
        for label, ents in (("channels", chs), ("groups", grs)):
            ef = self._find_folder(efs, label)
            if ef:
                if ents:
                    ok, added = await self._update_folder(client, ef, ents, an)
                    sm.append(f"'{label}' update: {'Done' if ok else 'Err'}")
                    dt.append(f"\n--- '{label}' (ID:{ef.id}) ---")
                    for a in added:
                        dt.append(f"  +{getattr(a, 'title', '?')} ({a.id})")
                    if not added:
                        dt.append("  No new")
            else:
                fid = self._free_fid(uids)
                if fid and ents:
                    ok = await self._create_folder(client, fid, label, ents, an)
                    uids.add(fid)
                    sm.append(f"'{label}' create: {'Done' if ok else 'Err'}")
                    dt.append(f"\n--- '{label}' (ID:{fid}) ---")
                    for a in ents:
                        dt.append(f"  +{getattr(a, 'title', '?')} ({a.id})")

        # Step 6: Mute + archive folder chats
        if all_fp:
            m, merr = await self._mute_folder_chats(client, all_fp, an)
            sm.append(f"Folder mute+arch: {len(m)}")
            dt.append(f"\n--- Folder chats ({len(m)}) ---")
            for x in m:
                dt.append(f"  {x}")
            for e in merr:
                dt.append(f"  ERR: {e}")

        # Step 7: Remove Saved from ALL folders + clear
        await asyncio.sleep(0.5)
        ok, removed = await self._remove_saved_from_all_folders(client, me.id, an)
        sm.append(f"Remove Saved from folders: {len(removed)}")
        dt.append(f"\n--- Remove Saved from folders ---")
        for r in removed:
            dt.append(f"  {r}")
        if not removed:
            dt.append("  Not found in any")

        # Step 8: Clear Saved Messages
        cerr = await self._clear_saved_messages(client, an)
        sm.append(f"Clear Saved: {'Done' if not cerr else 'Partial'}")
        dt.append("\n--- Clear Saved ---")
        for e in cerr:
            dt.append(f"  {e}")
        if not cerr:
            dt.append("  Cleared + confirmation sent")

        # Step 9: Delete PMs + block
        blocked, deleted, s9e = [], [], []
        try:
            await self._ec(client)
            for d in await client.get_dialogs():
                ent = d.entity
                if not isinstance(ent, User):
                    continue
                uid = ent.id
                if uid in excl or getattr(ent, "is_self", False):
                    continue
                try:
                    await self._sr(client, DeleteHistoryRequest(
                        peer=ent, max_id=0, revoke=True,
                    ), f"DelHist_{uid}", an)
                    deleted.append(f"{get_full_name(ent)} ({uid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s9e.append(f"DelHist {uid}: {e}")
                try:
                    await self._sr(client, BlockRequest(id=ent), f"Block_{uid}", an)
                    blocked.append(f"{get_full_name(ent)} ({uid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s9e.append(f"Block {uid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            s9e.append(f"PM iter: {e}")
        sm.append(f"PMs: {len(deleted)}, Blocked: {len(blocked)}")
        dt.append(f"\n--- PMs ({len(deleted)}) ---")
        for x in deleted:
            dt.append(f"  {x}")
        dt.append(f"\n--- Blocked ({len(blocked)}) ---")
        for x in blocked:
            dt.append(f"  {x}")
        for e in s9e:
            dt.append(f"  ERR: {e}")

        # Step 10: Leave chats not in folders
        left, s10e = [], []
        try:
            _, afs = await self._get_filters(client, an)
            inf = self._chats_in_folders(afs)
            await self._ec(client)
            for d in await client.get_dialogs():
                ent = d.entity
                if isinstance(ent, (ChannelForbidden, ChatForbidden, User)):
                    continue
                eid = ent.id
                if eid in inf:
                    continue
                try:
                    if isinstance(ent, Channel):
                        try:
                            await self._sr(client, LeaveChannelRequest(channel=ent),
                                           f"Leave_{eid}", an)
                            left.append(f"{ent.title} ({eid})")
                        except UserCreatorError:
                            s10e.append(f"Creator {ent.title}")
                        except ChannelPrivateError:
                            left.append(f"{ent.title} ({eid}, private)")
                    elif isinstance(ent, Chat):
                        try:
                            await self._ec(client)
                            mi = await client.get_input_entity(me.id)
                            await self._sr(client, DeleteChatUserRequest(
                                chat_id=ent.id, user_id=mi, revoke_history=True,
                            ), f"LeaveChat_{eid}", an)
                            left.append(f"{ent.title} ({eid})")
                        except UserCreatorError:
                            s10e.append(f"Creator {ent.title}")
                        except ChatIdInvalidError:
                            s10e.append(f"Invalid {ent.title}")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s10e.append(f"Leave {eid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            s10e.append(f"Leave iter: {e}")
        sm.append(f"Left: {len(left)}")
        dt.append(f"\n--- Left ({len(left)}) ---")
        for x in left:
            dt.append(f"  {x}")
        for e in s10e:
            dt.append(f"  ERR: {e}")

        # Step 11: Delete contacts
        dc, s11e = [], []
        try:
            cr = await self._sr(client, GetContactsRequest(hash=0), "GetContacts", an)
            if hasattr(cr, "users") and cr.users:
                try:
                    await self._sr(client, DeleteContactsRequest(id=cr.users), "DelContacts", an)
                    dc = [f"{get_full_name(u)} ({u.id})" for u in cr.users]
                except AccountFloodError:
                    raise
                except Exception:
                    for u in cr.users:
                        try:
                            await self._sr(client, DeleteContactsRequest(id=[u]),
                                           f"DelContact_{u.id}", an)
                            dc.append(f"{get_full_name(u)} ({u.id})")
                        except AccountFloodError:
                            raise
                        except Exception as e2:
                            s11e.append(f"Contact {u.id}: {e2}")
                        await asyncio.sleep(0.2)
        except AccountFloodError:
            raise
        except Exception as e:
            s11e.append(f"Contacts: {e}")
        sm.append(f"Contacts: {len(dc)}")
        dt.append(f"\n--- Contacts ({len(dc)}) ---")
        for x in dc:
            dt.append(f"  {x}")
        for e in s11e:
            dt.append(f"  ERR: {e}")

        # Step 12: Archive all non-excluded
        arch, aerr = await self._archive_non_excluded(client, excl, an)
        sm.append(f"Archived: {len(arch)}")
        dt.append(f"\n--- Archived ({len(arch)}) ---")
        for x in arch:
            dt.append(f"  {x}")
        for e in aerr:
            dt.append(f"  ERR: {e}")

        # Step 13: Pin & order
        perr = await self._pin_and_order(client, me.id, oid, sbid, an)
        sm.append(f"Pin: {'Done' if not perr else 'Partial'}")
        dt.append("\n--- Pin & order ---")
        dt.append("  Telegram -> SpamBot -> Saved -> Owner")
        for e in perr:
            dt.append(f"  ERR: {e}")

        # Step 14: Set avatar
        aurl = self.config["avatar_url"]
        if aurl:
            ok, err = await self._set_avatar(client, aurl, an)
            sm.append(f"Avatar: {'Done' if ok else 'Error'}")
            dt.append(f"\n--- Avatar: {'Done' if ok else err} ---")
        else:
            sm.append("Avatar: Skip")

        # Step 15: Final — remove Saved from ALL folders again + clear
        await asyncio.sleep(0.5)
        ok2, rem2 = await self._remove_saved_from_all_folders(client, me.id, an)
        sm.append(f"Final remove Saved: {len(rem2)}")
        dt.append(f"\n--- Final remove Saved ---")
        for r in rem2:
            dt.append(f"  {r}")

        cerr2 = await self._clear_saved_messages(client, an)
        sm.append(f"Final clear Saved: {'Done' if not cerr2 else 'Partial'}")
        dt.append("\n--- Final clear Saved ---")
        for e in cerr2:
            dt.append(f"  {e}")
        if not cerr2:
            dt.append("  Cleared + confirmation sent")

        return sm, dt

    @loader.command(ru_doc="Управление", en_doc="Manage")
    async def manage(self, message: Message):
        args = utils.get_args_raw(message)
        al = args.split() if args else []
        if not al:
            return await utils.answer(message, self.strings["help"])
        cmd = al[0].lower()
        h = {
            "add": self._cmd_add, "list": self._cmd_list,
            "remove": self._cmd_remove, "folder": self._cmd_folder,
            "ava": self._cmd_ava, "set": self._cmd_set, "start": self._cmd_start,
        }
        handler = h.get(cmd)
        if not handler:
            return await utils.answer(message, self.strings["help"])
        if cmd in ("add", "remove", "folder", "ava", "set"):
            await handler(message, al)
        else:
            await handler(message)

    async def _cmd_add(self, message, args):
        aid, _ = self._get_api()
        if not aid:
            return await utils.answer(message, self.strings["error_no_api"])
        if len(self._sessions_runtime) >= MAX_SESSIONS:
            return await utils.answer(message, self.strings["session_max"].format(max=MAX_SESSIONS))
        persistent, ss = False, None
        if len(args) > 1 and args[1].lower() == "long":
            persistent = True
            if len(args) > 2:
                ss = self._find_ss(" ".join(args[2:]))
        elif len(args) > 1:
            ss = self._find_ss(" ".join(args[1:]))
        if not ss:
            reply = await message.get_reply_message()
            if reply and reply.text:
                ss = self._find_ss(reply.text)
        if not ss:
            return await utils.answer(message, self.strings["provide_session"])
        if any(s["session"] == ss for s in self._sessions_runtime):
            return await utils.answer(message, self.strings["session_exists"])
        c, r = await self._connect_session(ss)
        if c is None:
            return await utils.answer(message,
                f"{self.strings['session_not_authorized']}\n{self.strings['line']}\n{r}")
        me = r
        if any(s["user_id"] == me.id for s in self._sessions_runtime):
            await c.disconnect()
            return await utils.answer(message, self.strings["session_exists"])
        ph = getattr(me, "phone", "Unknown") or "Hidden"
        self._sessions_runtime.append({
            "session": ss, "user_id": me.id, "name": get_full_name(me),
            "phone": ph, "persistent": persistent,
        })
        self._clients.append(c)
        self._save_sessions()
        try:
            await message.delete()
        except Exception:
            pass
        tid = None
        rt = getattr(message, "reply_to", None)
        if rt:
            tid = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)
        await self._client.send_message(message.chat_id, self.strings["session_added"].format(
            line=self.strings["line"], name=get_full_name(me), user_id=me.id,
            phone=ph, persistent="Yes" if persistent else "No",
            slot=len(self._sessions_runtime), max=MAX_SESSIONS,
        ), reply_to=tid, parse_mode="html")

    async def _cmd_list(self, message):
        if not self._sessions_runtime:
            return await utils.answer(message, self.strings["no_sessions"])
        lines = [
            f"{i}. {s['name']} | <code>{s['user_id']}</code> | <code>{s['phone']}</code>"
            f"{' [long]' if s.get('persistent') else ''}"
            for i, s in enumerate(self._sessions_runtime, 1)
        ]
        await utils.answer(message, self.strings["session_list"].format(
            count=len(self._sessions_runtime), max=MAX_SESSIONS,
            line=self.strings["line"], sessions="\n".join(lines),
        ))

    async def _cmd_remove(self, message, args):
        if not self._sessions_runtime:
            return await utils.answer(message, self.strings["no_sessions"])
        if len(args) < 2:
            return await utils.answer(message, self.strings["session_remove_invalid"])
        try:
            n = int(args[1])
            assert 1 <= n <= len(self._sessions_runtime)
        except (ValueError, AssertionError):
            return await utils.answer(message, self.strings["session_remove_invalid"])
        idx = n - 1
        try:
            await self._clients[idx].disconnect()
        except Exception:
            pass
        self._sessions_runtime.pop(idx)
        self._clients.pop(idx)
        self._save_sessions()
        await utils.answer(message, self.strings["session_removed"].format(num=n))

    async def _cmd_folder(self, message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["folder_provide"])
        try:
            fn = int(args[1])
            assert fn in (1, 2, 3)
        except (ValueError, AssertionError):
            return await utils.answer(message, self.strings["folder_invalid_num"])
        key = f"folder_link_{fn}"
        if len(args) < 3:
            if self.config[key]:
                self.config[key] = ""
                return await utils.answer(message, self.strings["folder_cleared"].format(num=fn))
            return await utils.answer(message, self.strings["folder_provide"])
        self.config[key] = args[2]
        await utils.answer(message, self.strings["folder_set"].format(num=fn, link=args[2]))

    async def _cmd_ava(self, message, args):
        if len(args) < 2:
            if self.config["avatar_url"]:
                self.config["avatar_url"] = ""
                return await utils.answer(message, self.strings["ava_cleared"])
            return await utils.answer(message, self.strings["ava_provide"])
        self.config["avatar_url"] = args[1]
        await utils.answer(message, self.strings["ava_set"].format(url=args[1]))

    async def _cmd_set(self, message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["timezone_invalid"])
        try:
            o = int(args[1].replace("+", ""))
            assert -12 <= o <= 12
            self.config["timezone_offset"] = o
            await utils.answer(message, self.strings["timezone_set"].format(
                timezone_str=self._tz_label(o)))
        except (ValueError, AssertionError):
            await utils.answer(message, self.strings["timezone_invalid"])

    async def _cmd_start(self, message):
        aid, _ = self._get_api()
        if not aid:
            return await utils.answer(message, self.strings["error_no_api"])
        if not self._sessions_runtime or not self._clients:
            return await utils.answer(message, self.strings["error_no_sessions"])
        if self._processing_lock.locked():
            return await utils.answer(message, self.strings["already_processing"])

        async with self._processing_lock:
            self._current_message = message
            status = await utils.answer(message, self.strings["processing"])
            self._status_msg = status[0] if isinstance(status, list) else status
            self._flood_until.clear()
            self._flood_log.clear()

            pending = {}
            for i, (sd, cl) in enumerate(zip(self._sessions_runtime, self._clients)):
                pending[i] = {
                    "sd": sd, "cl": cl, "sm": [], "dt": [],
                    "done": False, "error": None,
                }

            while True:
                ran, all_fl = False, True
                for i, info in pending.items():
                    if info["done"]:
                        continue
                    if self._is_flooded(info["cl"]):
                        continue
                    all_fl = False
                    ran = True
                    an = info["sd"]["name"]
                    try:
                        await self._ec(info["cl"])
                        me = await info["cl"].get_me()
                        name = get_full_name(me)
                        s, d = await self._process_account(info["cl"], me, name)
                        info["sm"] = [f"\n=== [{i+1}] {name} (ID:{me.id}) ==="] + s
                        info["dt"] = [f"\n{'='*50}"] + d
                        info["done"] = True
                    except AccountFloodError as fe:
                        logger.info(f"[MANAGER] [{i+1}] {an} flood in {fe.method}")
                        await self._update_flood_status()
                    except Exception as e:
                        info["sm"] = [f"\n=== [{i+1}] {an} ===", f"FATAL: {e}"]
                        info["dt"] = [f"\n=== [{i+1}] {an} ===", f"FATAL: {e}"]
                        info["done"] = True
                        info["error"] = str(e)

                if all(x["done"] for x in pending.values()):
                    break
                nd = [i for i, x in pending.items() if not x["done"]]
                if not nd:
                    break
                if not ran or all_fl:
                    fts = [self._flood_remaining(pending[i]["cl"]) for i in nd]
                    fts = [t for t in fts if t > 0]
                    if fts:
                        wt = min(fts)
                        rts = self._time_str(time.time() + wt)
                        if self._status_msg:
                            try:
                                await utils.answer(self._status_msg,
                                    self.strings["processing_flood"].format(resume_time=rts))
                            except Exception:
                                pass
                        await asyncio.sleep(wt)
                        if self._status_msg:
                            try:
                                await utils.answer(self._status_msg, self.strings["processing"])
                            except Exception:
                                pass
                    else:
                        await asyncio.sleep(1)

            a_sm, a_dt = [], []
            for i in sorted(pending):
                a_sm.extend(pending[i]["sm"])
                a_dt.extend(pending[i]["dt"])

            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            cnt = len(self._sessions_runtime)
            sep = "=" * 50

            tid = None
            rt = getattr(message, "reply_to", None)
            if rt:
                tid = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)

            try:
                await utils.answer(self._status_msg, self.strings["success"])
            except Exception:
                pass
            self._status_msg = None

            for name, lines in (("summary", a_sm), ("detailed", a_dt)):
                txt = f"MANAGER - {name.upper()}\nDate: {ts}\nAccounts: {cnt}\n{sep}\n" + "\n".join(lines)
                f = io.BytesIO(txt.encode("utf-8"))
                f.name = f"{name}.txt"
                await self._client.send_file(
                    message.chat_id, f, caption=f"<b>{name.title()}</b>",
                    reply_to=tid, parse_mode="html",
                )

            if self._flood_log:
                ft = f"MANAGER - FLOOD LOG\nDate: {ts}\n{sep}\n"
                for e in self._flood_log:
                    ft += (f"\nAccount: {e['account']}\nMethod: {e['method']}\n"
                           f"Flood: {e['flood_seconds']}s+{e['extra_wait']}s\n"
                           f"Time: {e['timestamp']}\nResume: {e['resume_at']}\n{'-'*30}\n")
                ff = io.BytesIO(ft.encode("utf-8"))
                ff.name = "flood_log.txt"
                await self._client.send_file(
                    message.chat_id, ff, caption="<b>Flood Log</b>",
                    reply_to=tid, parse_mode="html",
                )

            to_rm = []
            for i in range(len(self._sessions_runtime) - 1, -1, -1):
                if not self._sessions_runtime[i].get("persistent", False):
                    try:
                        await self._clients[i].disconnect()
                    except Exception:
                        pass
                    to_rm.append(i)
            for i in to_rm:
                self._sessions_runtime.pop(i)
                self._clients.pop(i)
            self._save_sessions()
            self._flood_until.clear()
            self._current_message = None