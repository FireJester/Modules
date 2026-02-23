# -*- coding: utf-8 -*-

__version__ = (1, 0, 0)
# meta developer: FireJester.t.me 

import struct
import asyncio
from telethon.tl import tlobject
from telethon.tl.types import InputDocument
from telethon import functions

from .. import loader, utils


class SavedMusic(tlobject.TLObject):
    def __init__(self, count, documents):
        self.count = count
        self.documents = documents

    @classmethod
    def from_reader(cls, reader):
        return cls(count=reader.read_int(), documents=reader.tgread_vector())


class GetSavedMusic(tlobject.TLRequest):
    def __init__(self, id, offset, limit, hash):
        self.id = id
        self.offset = offset
        self.limit = limit
        self.hash = hash

    def _bytes(self):
        return b''.join((b'\xe3\x7f\x8dx', self.id._bytes(),
                         struct.pack('<i', self.offset), struct.pack('<i', self.limit),
                         struct.pack('<q', self.hash)))

    def read_result(self, reader):
        reader.read_int()
        return SavedMusic.from_reader(reader)


@loader.tds
class DeleteMusic(loader.Module):
    """Delete music in your profile"""
    strings = {"name": "DeleteMusic"}

    @loader.command()
    async def clearmusiccmd(self, message):
        await utils.answer(message, "Processing...")
        try:
            me = await self.client.get_input_entity("me")
            music = await self.client(GetSavedMusic(me, 0, 100, 0))
            if not music or not music.documents:
                return await utils.answer(message, "Error: no tracks in your profile")
            c = 0
            while music and music.documents:
                for doc in music.documents:
                    try:
                        await self.client(functions.account.SaveMusicRequest(
                            id=InputDocument(
                                id=doc.id,
                                access_hash=doc.access_hash,
                                file_reference=doc.file_reference
                            ),
                            unsave=True
                        ))
                        c += 1
                        await asyncio.sleep(2)
                    except:
                        pass
                music = await self.client(GetSavedMusic(me, 0, 100, 0))
            await utils.answer(message, f"Deleted: {c} tracks")
        except Exception as e:
            await utils.answer(message, f"Error: {e}")