__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

import logging
import asyncio
import re
import time
import io

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
)
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.tl.functions.messages import DeleteChatUserRequest
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

STRING_SESSION_PATTERN = re.compile(r'1[A-Za-z0-9_-]{200,}={0,2}')
MAX_SESSIONS = 10
TELEGRAM_ID = 777000
SPAMBOT_USERNAME = "SpamBot"
FLOOD_EXTRA_WAIT = 10


def get_full_name(entity):
    if isinstance(entity, Channel):
        return entity.title or "Unknown"
    first = getattr(entity, "first_name", "") or ""
    last = getattr(entity, "last_name", "") or ""
    return f"{first} {last}".strip() or "Unknown"


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
            "<code>.manage folder [link]</code> - set folder link\n"
            "<code>.manage ava [url]</code> - set avatar image url\n"
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
        "session_list": "<b>Connected sessions ({count}/{max}):</b>\n{line}\n{sessions}\n{line}",
        "session_removed": "<b>Session #{num} removed</b>",
        "session_remove_invalid": "<b>Error:</b> Invalid session number",
        "processing": "<b>Processing... Please wait</b>",
        "processing_flood": "<b>Processing... FloodWait {minutes}m {seconds}s</b>",
        "success": "<b>Cleanup completed successfully</b>",
        "error_no_sessions": "<b>Error:</b> No sessions to process",
        "folder_set": "<b>Folder link saved:</b>\n<code>{link}</code>",
        "folder_cleared": "<b>Folder link cleared</b>",
        "folder_provide": "<b>Error:</b> Provide folder link",
        "ava_set": "<b>Avatar URL saved:</b>\n<code>{url}</code>",
        "ava_cleared": "<b>Avatar URL cleared</b>",
        "ava_provide": "<b>Error:</b> Provide image URL",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "sessions_data",
                [],
                "Stored sessions data (internal)",
                validator=loader.validators.Hidden(loader.validators.Series()),
            ),
            loader.ConfigValue(
                "folder_link",
                "",
                "Chatlist folder invite link",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "avatar_url",
                "",
                "Avatar image URL",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
        )
        self._clients = []
        self._sessions_runtime = []
        self._status_msg = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()
        await self._restore_sessions()

    async def on_unload(self):
        for c in self._clients:
            try:
                await c.disconnect()
            except:
                pass
        self._clients.clear()
        self._sessions_runtime.clear()

    async def _restore_sessions(self):
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
                    continue
                me = result
                phone = getattr(me, "phone", "Unknown") or "Hidden"
                self._sessions_runtime.append({
                    "session": session_str,
                    "user_id": me.id,
                    "name": get_full_name(me),
                    "phone": phone,
                    "persistent": persistent,
                })
                self._clients.append(client)
            except:
                pass

    def _save_sessions(self):
        data = []
        for s in self._sessions_runtime:
            data.append({
                "session": s["session"],
                "persistent": s.get("persistent", False),
                "user_id": s["user_id"],
                "name": s["name"],
                "phone": s["phone"],
            })
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
            return link.split("addlist/")[-1].split("?")[0]
        elif "slug=" in link:
            return link.split("slug=")[-1].split("&")[0]
        return link

    async def _handle_flood(self, e, context=""):
        wait_time = e.seconds + FLOOD_EXTRA_WAIT
        minutes = wait_time // 60
        seconds = wait_time % 60
        logger.warning(f"[MANAGER] FloodWait {context}: waiting {minutes}m {seconds}s")
        if self._status_msg:
            try:
                await utils.answer(
                    self._status_msg,
                    self.strings["processing_flood"].format(minutes=minutes, seconds=seconds)
                )
            except:
                pass
        await asyncio.sleep(wait_time)
        if self._status_msg:
            try:
                await utils.answer(self._status_msg, self.strings["processing"])
            except:
                pass

    async def _safe_request(self, coro_func, *args, context="", max_retries=3, **kwargs):
        for attempt in range(max_retries):
            try:
                return await coro_func(*args, **kwargs)
            except FloodWaitError as e:
                await self._handle_flood(e, context=context)
            except Exception:
                raise
        raise Exception(f"Max retries exceeded for {context}")

    async def _connect_session(self, session_str):
        try:
            client = TelegramClient(
                StringSession(session_str),
                api_id=2040,
                api_hash="b18441a1ff607e10a989891a5462e627"
            )
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                return None, "Not authorized"
            me = await client.get_me()
            return client, me
        except AuthKeyUnregisteredError:
            return None, "Session revoked"
        except UserDeactivatedBanError:
            return None, "Account banned"
        except Exception as e:
            return None, str(e)

    async def _get_spambot_id(self, client):
        try:
            entity = await client.get_entity(SPAMBOT_USERNAME)
            return entity.id
        except:
            return None

    async def _get_admin_chats(self, client):
        channels = []
        groups = []
        try:
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

    async def _get_existing_folder_ids(self, client):
        try:
            result = await client(GetDialogFiltersRequest())
            filters = getattr(result, "filters", result)
            used_ids = set()
            for f in filters:
                if hasattr(f, "id"):
                    used_ids.add(f.id)
            return used_ids, filters
        except:
            return set(), []

    def _get_free_folder_id(self, used_ids):
        for i in range(2, 256):
            if i not in used_ids:
                return i
        return None

    async def _get_chats_in_folders(self, filters):
        chat_ids_in_folders = set()
        for f in filters:
            if not hasattr(f, "include_peers"):
                continue
            for peer in f.include_peers:
                peer_id = self._get_peer_id(peer)
                if peer_id:
                    chat_ids_in_folders.add(peer_id)
            if hasattr(f, "pinned_peers"):
                for peer in f.pinned_peers:
                    peer_id = self._get_peer_id(peer)
                    if peer_id:
                        chat_ids_in_folders.add(peer_id)
        return chat_ids_in_folders

    def _get_peer_id(self, peer):
        if hasattr(peer, "channel_id"):
            return peer.channel_id
        elif hasattr(peer, "chat_id"):
            return peer.chat_id
        elif hasattr(peer, "user_id"):
            return peer.user_id
        return None

    def _find_folder_by_title(self, filters, title):
        for f in filters:
            if not hasattr(f, "title"):
                continue
            f_title = f.title
            if hasattr(f_title, "text"):
                f_title = f_title.text
            if isinstance(f_title, str) and f_title.lower() == title.lower():
                return f
        return None

    async def _update_existing_folder(self, client, folder, new_entities):
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
                        inp = await client.get_input_entity(entity)
                        new_peers.append(inp)
                        existing_ids.add(eid)
                        added.append(entity)
                    except:
                        pass
            if not added:
                return True, []
            updated_filter = DialogFilter(
                id=folder.id,
                title=folder.title,
                pinned_peers=list(folder.pinned_peers) if hasattr(folder, "pinned_peers") else [],
                include_peers=new_peers,
                exclude_peers=list(folder.exclude_peers) if hasattr(folder, "exclude_peers") else [],
            )
            if hasattr(folder, "emoticon") and folder.emoticon:
                updated_filter.emoticon = folder.emoticon
            if hasattr(folder, "color") and folder.color is not None:
                updated_filter.color = folder.color
            await self._safe_request(
                client, UpdateDialogFilterRequest(id=folder.id, filter=updated_filter),
                context="UpdateDialogFilter"
            )
            return True, added
        except Exception as e:
            logger.error(f"[MANAGER] Update folder error: {e}")
            return False, []

    async def _create_folder(self, client, folder_id, title, peers):
        try:
            input_peers = []
            for peer in peers:
                try:
                    inp = await client.get_input_entity(peer)
                    input_peers.append(inp)
                except:
                    pass
            title_entity = TextWithEntities(text=title, entities=[])
            dialog_filter = DialogFilter(
                id=folder_id,
                title=title_entity,
                pinned_peers=[],
                include_peers=input_peers,
                exclude_peers=[],
            )
            await self._safe_request(
                client, UpdateDialogFilterRequest(id=folder_id, filter=dialog_filter),
                context="CreateFolder"
            )
            return True
        except Exception as e:
            logger.error(f"[MANAGER] Create folder '{title}' error: {e}")
            return False

    async def _remove_saved_from_all_folders(self, client, me_id):
        try:
            _, filters = await self._get_existing_folder_ids(client)
            for f in filters:
                if not hasattr(f, "include_peers"):
                    continue
                has_saved = False
                new_include = []
                for peer in f.include_peers:
                    pid = self._get_peer_id(peer)
                    if pid == me_id:
                        has_saved = True
                    else:
                        new_include.append(peer)
                new_pinned = []
                if hasattr(f, "pinned_peers"):
                    for peer in f.pinned_peers:
                        pid = self._get_peer_id(peer)
                        if pid == me_id:
                            has_saved = True
                        else:
                            new_pinned.append(peer)
                if has_saved:
                    updated_filter = DialogFilter(
                        id=f.id,
                        title=f.title,
                        pinned_peers=new_pinned,
                        include_peers=new_include,
                        exclude_peers=list(f.exclude_peers) if hasattr(f, "exclude_peers") else [],
                    )
                    if hasattr(f, "emoticon") and f.emoticon:
                        updated_filter.emoticon = f.emoticon
                    if hasattr(f, "color") and f.color is not None:
                        updated_filter.color = f.color
                    try:
                        await self._safe_request(
                            client, UpdateDialogFilterRequest(id=f.id, filter=updated_filter),
                            context="RemoveSavedFromFolder"
                        )
                    except:
                        pass
            return True
        except Exception as e:
            logger.error(f"[MANAGER] Remove saved from folders error: {e}")
            return False

    async def _mute_peer(self, client, peer, context=""):
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
            context=context
        )

    async def _mute_and_archive(self, client, peer):
        try:
            await self._mute_peer(client, peer, context="MuteAndArchive")
            await asyncio.sleep(0.3)
            await client.edit_folder(peer, 1)
            return True
        except FloodWaitError as e:
            await self._handle_flood(e, context="MuteAndArchive_editFolder")
            try:
                await client.edit_folder(peer, 1)
                return True
            except:
                return False
        except Exception as e:
            logger.error(f"[MANAGER] Mute/archive error: {e}")
            return False

    async def _archive_all_except(self, client, excluded_ids):
        archived = []
        errors = []
        try:
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                eid = None
                if isinstance(entity, User):
                    eid = entity.id
                elif isinstance(entity, (Channel, Chat)):
                    eid = entity.id
                if eid and eid in excluded_ids:
                    continue
                if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                    continue
                try:
                    success = await self._mute_and_archive(client, entity)
                    if success:
                        name = get_full_name(entity) if isinstance(entity, (User, Channel)) else getattr(entity, "title", str(eid))
                        archived.append(f"{name} (ID: {eid})")
                    else:
                        errors.append(f"Archive {eid}: mute_and_archive returned False")
                except Exception as e:
                    errors.append(f"Archive {eid}: {e}")
                await asyncio.sleep(0.3)
        except Exception as e:
            errors.append(f"Archive iteration: {e}")
        return archived, errors

    async def _pin_and_order_chats(self, client, me_id, owner_id, spambot_id):
        errors = []
        pin_order = []
        pin_order.append(TELEGRAM_ID)
        if spambot_id:
            pin_order.append(spambot_id)
        pin_order.append(me_id)
        if owner_id and owner_id not in pin_order:
            pin_order.append(owner_id)
        for uid in pin_order:
            try:
                inp = await client.get_input_entity(uid)
                await self._safe_request(
                    client, ToggleDialogPinRequest(peer=inp, pinned=True),
                    context=f"PinDialog_{uid}"
                )
            except Exception as e:
                errors.append(f"Pin {uid}: {e}")
            await asyncio.sleep(0.3)
        try:
            order_peers = []
            for uid in pin_order:
                try:
                    inp = await client.get_input_entity(uid)
                    order_peers.append(inp)
                except:
                    pass
            if order_peers:
                await self._safe_request(
                    client,
                    ReorderPinnedDialogsRequest(folder_id=0, order=order_peers, force=True),
                    context="ReorderPinnedDialogs"
                )
        except Exception as e:
            errors.append(f"Reorder pins: {e}")
        return errors

    async def _get_all_stories(self, client, func, **kwargs):
        stories = []
        offset_id = 0
        while True:
            result = await client(func(
                peer=types.InputPeerSelf(),
                offset_id=offset_id,
                limit=100,
                **kwargs
            ))
            if not result.stories:
                break
            stories.extend(result.stories)
            offset_id = result.stories[-1].id
            if len(result.stories) < 100:
                break
        return stories

    async def _get_album_stories(self, client, album_id):
        stories = []
        offset = 0
        while True:
            result = await client(functions.stories.GetAlbumStoriesRequest(
                peer=types.InputPeerSelf(),
                album_id=album_id,
                offset=offset,
                limit=100
            ))
            if not result.stories:
                break
            stories.extend(result.stories)
            offset += len(result.stories)
            if len(result.stories) < 100:
                break
        return stories

    async def _get_albums(self, client):
        try:
            result = await client(functions.stories.GetAlbumsRequest(
                peer=types.InputPeerSelf(),
                hash=0
            ))
            if hasattr(result, "albums"):
                return result.albums
            return []
        except:
            return []

    async def _delete_all_stories_and_albums(self, client):
        deleted_albums = []
        deleted_stories_count = 0
        errors = []
        try:
            albums = await self._get_albums(client)
            for album in albums:
                try:
                    album_stories = await self._get_album_stories(client, album.album_id)
                    album_story_ids = [s.id for s in album_stories]
                    for sid in album_story_ids:
                        try:
                            await self._safe_request(
                                client,
                                functions.stories.DeleteStoriesRequest(
                                    peer=types.InputPeerSelf(), id=[sid]
                                ),
                                context=f"DeleteStory_{sid}"
                            )
                            deleted_stories_count += 1
                            await asyncio.sleep(0.3)
                        except Exception as e:
                            errors.append(f"Delete story {sid} from album '{album.title}': {e}")
                    try:
                        await self._safe_request(
                            client,
                            functions.stories.DeleteAlbumRequest(
                                peer=types.InputPeerSelf(), album_id=album.album_id
                            ),
                            context=f"DeleteAlbum_{album.title}"
                        )
                        deleted_albums.append(f"{album.title} ({len(album_story_ids)} stories)")
                    except Exception as e:
                        errors.append(f"Delete album '{album.title}': {e}")
                except Exception as e:
                    errors.append(f"Process album '{album.title}': {e}")
        except Exception as e:
            errors.append(f"Get albums: {e}")
        try:
            active_stories = await self._get_all_stories(client, functions.stories.GetPinnedStoriesRequest)
            for s in active_stories:
                try:
                    await self._safe_request(
                        client,
                        functions.stories.DeleteStoriesRequest(
                            peer=types.InputPeerSelf(), id=[s.id]
                        ),
                        context=f"DeleteActiveStory_{s.id}"
                    )
                    deleted_stories_count += 1
                    await asyncio.sleep(0.3)
                except Exception as e:
                    errors.append(f"Delete active story {s.id}: {e}")
        except Exception as e:
            errors.append(f"Get active stories: {e}")
        try:
            archive_stories = await self._get_all_stories(client, functions.stories.GetStoriesArchiveRequest)
            for s in archive_stories:
                try:
                    await self._safe_request(
                        client,
                        functions.stories.DeleteStoriesRequest(
                            peer=types.InputPeerSelf(), id=[s.id]
                        ),
                        context=f"DeleteArchiveStory_{s.id}"
                    )
                    deleted_stories_count += 1
                    await asyncio.sleep(0.3)
                except Exception as e:
                    errors.append(f"Delete archive story {s.id}: {e}")
        except Exception as e:
            errors.append(f"Get archive stories: {e}")
        return deleted_albums, deleted_stories_count, errors

    async def _delete_all_profile_photos(self, client, me):
        deleted_count = 0
        errors = []
        try:
            while True:
                result = await client(GetUserPhotosRequest(
                    user_id=types.InputPeerSelf(),
                    offset=0,
                    max_id=0,
                    limit=100
                ))
                if not result.photos:
                    break
                input_photos = []
                for photo in result.photos:
                    input_photos.append(InputPhoto(
                        id=photo.id,
                        access_hash=photo.access_hash,
                        file_reference=photo.file_reference
                    ))
                try:
                    await self._safe_request(
                        client, DeletePhotosRequest(id=input_photos),
                        context="DeletePhotos"
                    )
                    deleted_count += len(input_photos)
                except Exception as e:
                    errors.append(f"Batch delete photos: {e}")
                    for inp_photo in input_photos:
                        try:
                            await self._safe_request(
                                client, DeletePhotosRequest(id=[inp_photo]),
                                context=f"DeletePhoto_{inp_photo.id}"
                            )
                            deleted_count += 1
                        except Exception as e2:
                            errors.append(f"Delete photo {inp_photo.id}: {e2}")
                        await asyncio.sleep(0.2)
                if len(result.photos) < 100:
                    break
                await asyncio.sleep(0.3)
        except Exception as e:
            errors.append(f"Get photos: {e}")
        return deleted_count, errors

    async def _set_avatar(self, client, url):
        try:
            photo_bytes = await self._client.download_file(url)
            if not photo_bytes:
                file_msg = await self._client.send_file("me", url, silent=True)
                photo_bytes = await self._client.download_media(file_msg, bytes)
                await file_msg.delete()
            uploaded = await client.upload_file(io.BytesIO(photo_bytes), file_name="avatar.jpg")
            await self._safe_request(
                client,
                UploadProfilePhotoRequest(file=uploaded),
                context="UploadProfilePhoto"
            )
            return True, None
        except Exception as e:
            logger.error(f"[MANAGER] Set avatar error: {e}")
            return False, str(e)

    async def _join_folder(self, client, folder_link):
        folder_hash = self._extract_folder_hash(folder_link)
        if not folder_hash:
            return False, "Invalid folder link", []
        joined_peers = []
        try:
            try:
                check = await self._safe_request(
                    client, CheckChatlistInviteRequest(slug=folder_hash),
                    context="CheckChatlistInvite"
                )
                peers = [p for p in check.peers]
                await self._safe_request(
                    client, JoinChatlistInviteRequest(slug=folder_hash, peers=peers),
                    context="JoinChatlistInvite"
                )
                for p in peers:
                    pid = self._get_peer_id(p)
                    if pid:
                        joined_peers.append(pid)
                return True, None, joined_peers
            except FloodWaitError as e:
                await self._handle_flood(e, context="JoinFolder")
                check = await client(CheckChatlistInviteRequest(slug=folder_hash))
                peers = [p for p in check.peers]
                await client(JoinChatlistInviteRequest(slug=folder_hash, peers=peers))
                for p in peers:
                    pid = self._get_peer_id(p)
                    if pid:
                        joined_peers.append(pid)
                return True, None, joined_peers
            except Exception as e:
                if "already" in str(e).lower():
                    try:
                        await client(JoinChatlistInviteRequest(slug=folder_hash, peers=[]))
                        return True, "Already joined, refreshed", []
                    except Exception as e2:
                        return False, str(e2), []
                return False, str(e), []
        except Exception as e:
            return False, str(e), []

    async def _mute_folder_chats(self, client, peer_ids):
        muted = []
        errors = []
        for pid in peer_ids:
            try:
                entity = await client.get_entity(pid)
                await self._mute_peer(client, entity, context=f"MuteFolderChat_{pid}")
                await asyncio.sleep(0.3)
                await client.edit_folder(entity, 1)
                name = get_full_name(entity) if isinstance(entity, (User, Channel)) else getattr(entity, "title", str(pid))
                muted.append(f"{name} (ID: {pid})")
                await asyncio.sleep(0.3)
            except FloodWaitError as e:
                await self._handle_flood(e, context=f"MuteFolderChat_{pid}")
                try:
                    entity = await client.get_entity(pid)
                    await self._mute_peer(client, entity, context=f"MuteFolderChat_retry_{pid}")
                    await asyncio.sleep(0.3)
                    await client.edit_folder(entity, 1)
                    name = get_full_name(entity) if isinstance(entity, (User, Channel)) else getattr(entity, "title", str(pid))
                    muted.append(f"{name} (ID: {pid})")
                except Exception as e2:
                    errors.append(f"Mute folder chat {pid}: {e2}")
            except Exception as e:
                errors.append(f"Mute folder chat {pid}: {e}")
        return muted, errors

    async def _process_account(self, client, me):
        log_summary = []
        log_detailed = []
        owner_id = self._me.id
        spambot_id = await self._get_spambot_id(client)

        excluded_ids = {me.id, owner_id, TELEGRAM_ID}
        if spambot_id:
            excluded_ids.add(spambot_id)

        acc_name = get_full_name(me)
        log_detailed.append(f"=== Account: {acc_name} (ID: {me.id}) ===\n")

        deleted_albums, deleted_stories_count, stories_errors = await self._delete_all_stories_and_albums(client)
        log_summary.append(f"Albums deleted: {len(deleted_albums)}")
        log_summary.append(f"Stories deleted: {deleted_stories_count}")
        log_detailed.append(f"\n--- Deleted albums ({len(deleted_albums)}) ---")
        for a in deleted_albums:
            log_detailed.append(f"  {a}")
        log_detailed.append(f"\n--- Total stories deleted: {deleted_stories_count} ---")
        if stories_errors:
            log_detailed.append(f"\n--- Stories/Albums Errors ---")
            for e in stories_errors:
                log_detailed.append(f"  {e}")

        photos_deleted, photos_errors = await self._delete_all_profile_photos(client, me)
        log_summary.append(f"Profile photos deleted: {photos_deleted}")
        log_detailed.append(f"\n--- Profile photos deleted: {photos_deleted} ---")
        if photos_errors:
            log_detailed.append(f"\n--- Photos Errors ---")
            for e in photos_errors:
                log_detailed.append(f"  {e}")

        folder_link = self.config["folder_link"]
        folder_joined_peers = []
        if folder_link:
            success, err, folder_joined_peers = await self._join_folder(client, folder_link)
            log_summary.append(f"Join folder: {'Done' if success else 'Error'}")
            log_detailed.append(f"\n--- Join folder ---")
            if success:
                if err:
                    log_detailed.append(f"  {err}")
                else:
                    log_detailed.append(f"  Joined successfully: {folder_link}")
                    log_detailed.append(f"  Chats joined: {len(folder_joined_peers)}")
            else:
                log_detailed.append(f"  Error: {err}")
        else:
            log_summary.append("Join folder: Skipped (no link)")
            log_detailed.append("\n--- Join folder: Skipped (no link configured) ---")

        if folder_joined_peers:
            muted_folder, mute_folder_errors = await self._mute_folder_chats(client, folder_joined_peers)
            log_summary.append(f"Folder chats muted+archived: {len(muted_folder)}")
            log_detailed.append(f"\n--- Folder chats muted & archived ({len(muted_folder)}) ---")
            for m in muted_folder:
                log_detailed.append(f"  {m}")
            if mute_folder_errors:
                log_detailed.append(f"\n--- Folder mute Errors ---")
                for e in mute_folder_errors:
                    log_detailed.append(f"  {e}")

        channels, groups = await self._get_admin_chats(client)
        used_ids, existing_filters = await self._get_existing_folder_ids(client)

        existing_channels_folder = self._find_folder_by_title(existing_filters, "channels")
        if existing_channels_folder:
            if channels:
                success, added = await self._update_existing_folder(client, existing_channels_folder, channels)
                log_summary.append(f"Folder 'channels' update: {'Done' if success else 'Error'}")
                log_detailed.append(f"\n--- Folder 'channels' (existing, ID: {existing_channels_folder.id}) ---")
                if added:
                    for ch in added:
                        log_detailed.append(f"  Added: {ch.title} (ID: {ch.id})")
                else:
                    log_detailed.append("  No new channels to add")
            else:
                log_summary.append("Folder 'channels' update: Skipped (no admin channels)")
                log_detailed.append("\n--- Folder 'channels': exists, no new admin channels ---")
        else:
            channels_folder_id = self._get_free_folder_id(used_ids)
            if channels_folder_id and channels:
                success = await self._create_folder(client, channels_folder_id, "channels", channels)
                used_ids.add(channels_folder_id)
                log_summary.append(f"Folder 'channels' create: {'Done' if success else 'Error'}")
                if success:
                    log_detailed.append(f"\n--- Folder 'channels' (ID: {channels_folder_id}) ---")
                    for ch in channels:
                        log_detailed.append(f"  Added: {ch.title} (ID: {ch.id})")
                else:
                    log_detailed.append("\n--- Folder 'channels': FAILED ---")
            elif not channels:
                log_summary.append("Folder 'channels' create: Skipped (no admin channels)")
                log_detailed.append("\n--- Folder 'channels': Skipped (no admin channels) ---")
            else:
                log_summary.append("Folder 'channels' create: Error (no free folder ID)")
                log_detailed.append("\n--- Folder 'channels': Error (no free folder ID) ---")

        existing_groups_folder = self._find_folder_by_title(existing_filters, "groups")
        if existing_groups_folder:
            if groups:
                success, added = await self._update_existing_folder(client, existing_groups_folder, groups)
                log_summary.append(f"Folder 'groups' update: {'Done' if success else 'Error'}")
                log_detailed.append(f"\n--- Folder 'groups' (existing, ID: {existing_groups_folder.id}) ---")
                if added:
                    for gr in added:
                        title = gr.title if hasattr(gr, "title") else "Chat"
                        log_detailed.append(f"  Added: {title} (ID: {gr.id})")
                else:
                    log_detailed.append("  No new groups to add")
            else:
                log_summary.append("Folder 'groups' update: Skipped (no admin groups)")
                log_detailed.append("\n--- Folder 'groups': exists, no new admin groups ---")
        else:
            groups_folder_id = self._get_free_folder_id(used_ids)
            if groups_folder_id and groups:
                success = await self._create_folder(client, groups_folder_id, "groups", groups)
                used_ids.add(groups_folder_id)
                log_summary.append(f"Folder 'groups' create: {'Done' if success else 'Error'}")
                if success:
                    log_detailed.append(f"\n--- Folder 'groups' (ID: {groups_folder_id}) ---")
                    for gr in groups:
                        title = gr.title if hasattr(gr, "title") else "Chat"
                        log_detailed.append(f"  Added: {title} (ID: {gr.id})")
                else:
                    log_detailed.append("\n--- Folder 'groups': FAILED ---")
            elif not groups:
                log_summary.append("Folder 'groups' create: Skipped (no admin groups)")
                log_detailed.append("\n--- Folder 'groups': Skipped (no admin groups) ---")
            else:
                log_summary.append("Folder 'groups' create: Error (no free folder ID)")
                log_detailed.append("\n--- Folder 'groups': Error (no free folder ID) ---")

        await asyncio.sleep(0.5)
        saved_remove_ok = await self._remove_saved_from_all_folders(client, me.id)
        log_summary.append(f"Remove 'Saved' from folders: {'Done' if saved_remove_ok else 'Error'}")
        log_detailed.append(f"\n--- Remove 'Saved Messages' from all folders: {'Done' if saved_remove_ok else 'Error'} ---")

        blocked_users = []
        deleted_chats = []
        step3_errors = []
        try:
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if not isinstance(entity, User):
                    continue
                uid = entity.id
                if uid in excluded_ids:
                    continue
                try:
                    await self._safe_request(
                        client,
                        DeleteHistoryRequest(peer=entity, max_id=0, revoke=True),
                        context=f"DeleteHistory_{uid}"
                    )
                    deleted_chats.append(f"{get_full_name(entity)} (ID: {uid})")
                except Exception as e:
                    step3_errors.append(f"Delete history {uid}: {e}")
                try:
                    await self._safe_request(
                        client, BlockRequest(id=entity),
                        context=f"Block_{uid}"
                    )
                    blocked_users.append(f"{get_full_name(entity)} (ID: {uid})")
                except Exception as e:
                    step3_errors.append(f"Block {uid}: {e}")
                await asyncio.sleep(0.3)
        except Exception as e:
            step3_errors.append(f"Dialog iteration: {e}")

        log_summary.append(f"Private chats deleted: {len(deleted_chats)}")
        log_summary.append(f"Users blocked: {len(blocked_users)}")
        log_detailed.append(f"\n--- Deleted private chats ({len(deleted_chats)}) ---")
        for d in deleted_chats:
            log_detailed.append(f"  {d}")
        log_detailed.append(f"\n--- Blocked users ({len(blocked_users)}) ---")
        for b in blocked_users:
            log_detailed.append(f"  {b}")
        if step3_errors:
            log_detailed.append(f"\n--- Step 3 Errors ---")
            for e in step3_errors:
                log_detailed.append(f"  {e}")

        left_chats = []
        step4_errors = []
        try:
            _, all_filters = await self._get_existing_folder_ids(client)
            chats_in_folders = await self._get_chats_in_folders(all_filters)
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                    continue
                if isinstance(entity, User):
                    continue
                entity_id = entity.id
                if entity_id in chats_in_folders:
                    continue
                try:
                    if isinstance(entity, Channel):
                        try:
                            await self._safe_request(
                                client, LeaveChannelRequest(channel=entity),
                                context=f"LeaveChannel_{entity_id}"
                            )
                            left_chats.append(f"{entity.title} (ID: {entity_id}, type: channel/megagroup)")
                        except UserCreatorError:
                            step4_errors.append(f"Creator of {entity.title} ({entity_id}), can't leave")
                        except ChannelPrivateError:
                            left_chats.append(f"{entity.title} (ID: {entity_id}, already private/left)")
                    elif isinstance(entity, Chat):
                        try:
                            me_input = await client.get_input_entity(me.id)
                            await self._safe_request(
                                client,
                                DeleteChatUserRequest(chat_id=entity.id, user_id=me_input, revoke_history=True),
                                context=f"LeaveChatGroup_{entity_id}"
                            )
                            left_chats.append(f"{entity.title} (ID: {entity_id}, type: chat)")
                        except UserCreatorError:
                            step4_errors.append(f"Creator of {entity.title} ({entity_id}), can't leave")
                        except ChatIdInvalidError:
                            step4_errors.append(f"Invalid chat ID: {entity.title} ({entity_id})")
                except Exception as e:
                    step4_errors.append(f"Leave {entity_id}: {e}")
                await asyncio.sleep(0.3)
        except Exception as e:
            step4_errors.append(f"Leave iteration: {e}")

        log_summary.append(f"Left chats/channels: {len(left_chats)}")
        log_detailed.append(f"\n--- Left chats/channels ({len(left_chats)}) ---")
        for l in left_chats:
            log_detailed.append(f"  {l}")
        if step4_errors:
            log_detailed.append(f"\n--- Step 4 Errors ---")
            for e in step4_errors:
                log_detailed.append(f"  {e}")

        deleted_contacts = []
        step5_errors = []
        try:
            contacts_result = await client(GetContactsRequest(hash=0))
            if hasattr(contacts_result, "users") and contacts_result.users:
                contact_users = contacts_result.users
                if contact_users:
                    try:
                        await self._safe_request(
                            client, DeleteContactsRequest(id=contact_users),
                            context="DeleteContacts"
                        )
                        for cu in contact_users:
                            deleted_contacts.append(f"{get_full_name(cu)} (ID: {cu.id})")
                    except Exception as e:
                        step5_errors.append(f"Batch delete contacts: {e}")
                        for cu in contact_users:
                            try:
                                await self._safe_request(
                                    client, DeleteContactsRequest(id=[cu]),
                                    context=f"DeleteContact_{cu.id}"
                                )
                                deleted_contacts.append(f"{get_full_name(cu)} (ID: {cu.id})")
                            except Exception as e2:
                                step5_errors.append(f"Delete contact {cu.id}: {e2}")
                            await asyncio.sleep(0.2)
        except Exception as e:
            step5_errors.append(f"Get contacts: {e}")

        log_summary.append(f"Contacts deleted: {len(deleted_contacts)}")
        log_detailed.append(f"\n--- Deleted contacts ({len(deleted_contacts)}) ---")
        for dc in deleted_contacts:
            log_detailed.append(f"  {dc}")
        if step5_errors:
            log_detailed.append(f"\n--- Step 5 Errors ---")
            for e in step5_errors:
                log_detailed.append(f"  {e}")

        archived, archive_errors = await self._archive_all_except(client, excluded_ids)
        log_summary.append(f"Chats archived: {len(archived)}")
        log_detailed.append(f"\n--- Archived chats ({len(archived)}) ---")
        for a in archived:
            log_detailed.append(f"  {a}")
        if archive_errors:
            log_detailed.append(f"\n--- Archive Errors ---")
            for e in archive_errors:
                log_detailed.append(f"  {e}")

        pin_errors = await self._pin_and_order_chats(client, me.id, owner_id, spambot_id)
        pin_ok = not pin_errors
        log_summary.append(f"Pin & order chats: {'Done' if pin_ok else 'Partial/Error'}")
        log_detailed.append(f"\n--- Pin & order chats ---")
        log_detailed.append(f"  Order: Telegram -> SpamBot -> Saved -> Owner")
        if pin_errors:
            for e in pin_errors:
                log_detailed.append(f"  Error: {e}")
        else:
            log_detailed.append(f"  All pinned successfully")

        avatar_url = self.config["avatar_url"]
        if avatar_url:
            ava_ok, ava_err = await self._set_avatar(client, avatar_url)
            log_summary.append(f"Set avatar: {'Done' if ava_ok else 'Error'}")
            log_detailed.append(f"\n--- Set avatar ---")
            if ava_ok:
                log_detailed.append(f"  Avatar set successfully")
            else:
                log_detailed.append(f"  Error: {ava_err}")
        else:
            log_summary.append("Set avatar: Skipped (no URL)")
            log_detailed.append("\n--- Set avatar: Skipped (no URL configured) ---")

        return log_summary, log_detailed

    @loader.command(ru_doc="- Управление модулем", en_doc="- Manage Module")

    async def manage(self, message: Message):
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []
        if not args_list:
            await utils.answer(message, self.strings["help"])
            return
        cmd = args_list[0].lower()
        if cmd == "add":
            await self._cmd_add(message, args_list)
        elif cmd == "list":
            await self._cmd_list(message)
        elif cmd == "remove":
            await self._cmd_remove(message, args_list)
        elif cmd == "folder":
            await self._cmd_folder(message, args_list)
        elif cmd == "ava":
            await self._cmd_ava(message, args_list)
        elif cmd == "start":
            await self._cmd_start(message)
        else:
            await utils.answer(message, self.strings["help"])

    async def _cmd_add(self, message: Message, args):
        if len(self._sessions_runtime) >= MAX_SESSIONS:
            return await utils.answer(
                message,
                self.strings["session_max"].format(max=MAX_SESSIONS)
            )
        persistent = False
        session_str = None
        if len(args) > 1 and args[1].lower() == "long":
            persistent = True
            if len(args) > 2:
                text = " ".join(args[2:])
                session_str = self._find_string_session(text)
        elif len(args) > 1:
            text = " ".join(args[1:])
            session_str = self._find_string_session(text)
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
                + f"\n{self.strings['line']}\nReason: {result}"
            )
        me = result
        phone = getattr(me, "phone", "Unknown") or "Hidden"
        for s in self._sessions_runtime:
            if s["user_id"] == me.id:
                await client.disconnect()
                return await utils.answer(message, self.strings["session_exists"])
        self._sessions_runtime.append({
            "session": session_str,
            "user_id": me.id,
            "name": get_full_name(me),
            "phone": phone,
            "persistent": persistent,
        })
        self._clients.append(client)
        self._save_sessions()
        await message.delete()
        topic_id = None
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            topic_id = (
                getattr(reply_to, "reply_to_top_id", None)
                or getattr(reply_to, "reply_to_msg_id", None)
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
            p_mark = " [long]" if s.get("persistent") else ""
            lines.append(
                f"{i}. {s['name']} | <code>{s['user_id']}</code> | <code>{s['phone']}</code>{p_mark}"
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
            return await utils.answer(message, self.strings["session_remove_invalid"])
        try:
            num = int(args[1])
            if num < 1 or num > len(self._sessions_runtime):
                raise ValueError
        except ValueError:
            return await utils.answer(message, self.strings["session_remove_invalid"])
        idx = num - 1
        try:
            await self._clients[idx].disconnect()
        except:
            pass
        self._sessions_runtime.pop(idx)
        self._clients.pop(idx)
        self._save_sessions()
        await utils.answer(
            message,
            self.strings["session_removed"].format(num=num),
        )

    async def _cmd_folder(self, message: Message, args):
        if len(args) < 2:
            if self.config["folder_link"]:
                self.config["folder_link"] = ""
                return await utils.answer(message, self.strings["folder_cleared"])
            return await utils.answer(message, self.strings["folder_provide"])
        link = args[1]
        self.config["folder_link"] = link
        await utils.answer(message, self.strings["folder_set"].format(link=link))

    async def _cmd_ava(self, message: Message, args):
        if len(args) < 2:
            if self.config["avatar_url"]:
                self.config["avatar_url"] = ""
                return await utils.answer(message, self.strings["ava_cleared"])
            return await utils.answer(message, self.strings["ava_provide"])
        url = args[1]
        self.config["avatar_url"] = url
        await utils.answer(message, self.strings["ava_set"].format(url=url))

    async def _cmd_start(self, message: Message):
        if not self._sessions_runtime or not self._clients:
            return await utils.answer(message, self.strings["error_no_sessions"])
        self._status_msg = await utils.answer(message, self.strings["processing"])
        all_summary = []
        all_detailed = []
        for i, (session_data, client) in enumerate(
            zip(self._sessions_runtime, self._clients), 1
        ):
            try:
                if not client.is_connected():
                    await client.connect()
                me = await client.get_me()
                acc_name = get_full_name(me)
                all_summary.append(f"\n=== [{i}] {acc_name} (ID: {me.id}) ===")
                all_detailed.append(f"\n{'='*50}")
                summary, detailed = await self._process_account(client, me)
                all_summary.extend(summary)
                all_detailed.extend(detailed)
            except Exception as e:
                all_summary.append(f"\n=== [{i}] {session_data['name']} ===")
                all_summary.append(f"FATAL ERROR: {e}")
                all_detailed.append(f"\n=== [{i}] {session_data['name']} ===")
                all_detailed.append(f"FATAL ERROR: {e}")
                logger.error(f"[MANAGER] Process account [{i}] error: {e}")
        summary_text = "MANAGER - CLEANUP SUMMARY\n"
        summary_text += f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        summary_text += f"Accounts processed: {len(self._sessions_runtime)}\n"
        summary_text += "=" * 50 + "\n"
        summary_text += "\n".join(all_summary)
        detailed_text = "MANAGER - DETAILED LOG\n"
        detailed_text += f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        detailed_text += f"Accounts processed: {len(self._sessions_runtime)}\n"
        detailed_text += "=" * 50 + "\n"
        detailed_text += "\n".join(all_detailed)
        topic_id = None
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            topic_id = (
                getattr(reply_to, "reply_to_top_id", None)
                or getattr(reply_to, "reply_to_msg_id", None)
            )
        try:
            await utils.answer(self._status_msg, self.strings["success"])
        except:
            pass
        self._status_msg = None
        summary_file = io.BytesIO(summary_text.encode("utf-8"))
        summary_file.name = "summary.txt"
        await self._client.send_file(
            message.chat_id,
            summary_file,
            caption="<b>Cleanup Summary</b>",
            reply_to=topic_id,
            parse_mode="html",
        )
        detailed_file = io.BytesIO(detailed_text.encode("utf-8"))
        detailed_file.name = "detailed.txt"
        await self._client.send_file(
            message.chat_id,
            detailed_file,
            caption="<b>Detailed Log</b>",
            reply_to=topic_id,
            parse_mode="html",
        )
        to_remove = []
        for i in range(len(self._sessions_runtime) - 1, -1, -1):
            if not self._sessions_runtime[i].get("persistent", False):
                try:
                    await self._clients[i].disconnect()
                except:
                    pass
                to_remove.append(i)
        for i in to_remove:
            self._sessions_runtime.pop(i)
            self._clients.pop(i)
        self._save_sessions()