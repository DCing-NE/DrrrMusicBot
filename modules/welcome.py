# -*- coding: utf-8 -*-
"""欢迎模块。

实际播放时调用 music_bot.enqueue_welcome()，由音乐模块即时搜索音源、
若配置了 ne_id，则直接走 GD 音乐台 netease 源播放，不再搜索 gequbao。
"""

from __future__ import annotations

import asyncio
import random

from modules.base import Module, on_event
from models import Message, MessageType

#欢迎曲示例
class Welcome(Module):
    CHRISTOPHER_USERS = {"Christopher", "christopher"}
    CHRISTOPHER_MSG = "Welcome! Christopher!"
    CHRISTOPHER_SONG = {"name": "冬天的秘密", "artist": "周传雄", "ne_id": "189545", "duration_ms":272000}

    GENERAL_MSG = "Welcome！Please enjoy your time here!"

    @on_event(MessageType.JOIN)
    async def greet(self, msg: Message) -> None:
        if msg.user is None:
            return

        await asyncio.sleep(3.0)

        if not getattr(self.bot, "room_connected", False):
            return
        if msg.user.id not in self.bot.room.users:
            return

        user_name = msg.user.name

        if user_name in self.CHRISTOPHER_USERS:
            self.bot.send(self.CHRISTOPHER_MSG)
            await self._play_welcome_song([self.CHRISTOPHER_SONG])
            self._notify_welcome_done(user_name)
            return

        self.bot.whisper(self.GENERAL_MSG)
        self._notify_welcome_done(user_name)

    async def _play_welcome_song(self, songs: list[dict[str, str]]) -> None:
        music_bot = self.bot.registry._modules.get("music_bot")
        if music_bot is None or not hasattr(music_bot, "enqueue_welcome"):
            self.bot.logger.warning("[Welcome] music_bot 不可用，无法播放欢迎曲")
            return
        song = random.choice(songs)
        await music_bot.enqueue_welcome(
            song["name"], song.get("artist"), song.get("ne_id"), song.get("duration_ms", 0)
        )

    def _notify_welcome_done(self, user_name: str) -> None:
        message_box = self.bot.registry._modules.get("message_box")
        if message_box is not None:
            message_box.on_welcome_done(user_name)

        help_module = self.bot.registry._modules.get("help")
        if help_module is not None:
            help_module.on_welcome_done(user_name)
