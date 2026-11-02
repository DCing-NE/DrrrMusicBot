# -*- coding: utf-8 -*-
"""留言模块。

命令（房间内 / 私信 bot）：
  /留言 <用户名> <留言内容>

机制：
  - 留言存入留言箱（持久化到运行目录下的 message_box.json）
  - 目标用户进房后，等 welcome 流程结束 3 秒再发送
  - Bot 启动时若目标用户已在房间，延迟 3 秒后发送
  - 公开留言在房间内用 /me 广播；私密留言走私信
  - 发送后从留言箱删除；目标已在房间时提示发送者
"""

from __future__ import annotations

import asyncio
import json
import os

from modules.base import Module, command, on_event
from models import Message, MessageType

MESSAGE_BOX_FILE = "message_box.json"
WELCOME_DONE_DELAY = 3.0
STARTUP_CHECK_DELAY = 3.0


class MessageBox(Module):
    def __init__(self, bot):
        super().__init__(bot)
        self._message_box: dict[str, list[dict[str, object]]] = {}
        self._pending_targets: set[str] = set()
        self._schedule_lock = asyncio.Lock()
        self._startup_task: asyncio.Task | None = None
        self._load()

        self._startup_task = asyncio.create_task(self._check_existing_users())

        self.bot.logger.info(
            "[MessageBox] 模块已初始化，当前留言数: %d", self._count_messages())

    def _count_messages(self) -> int:
        return sum(len(v) for v in self._message_box.values())

    def _save(self) -> None:
        try:
            with open(MESSAGE_BOX_FILE, "w", encoding="utf-8") as f:
                json.dump(self._message_box, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.bot.logger.error("[MessageBox] 保存留言箱失败: %s", e)

    def _load(self) -> None:
        if not os.path.exists(MESSAGE_BOX_FILE):
            self._message_box = {}
            return
        try:
            with open(MESSAGE_BOX_FILE, "r", encoding="utf-8") as f:
                self._message_box = json.load(f)
        except Exception as e:
            self.bot.logger.error("[MessageBox] 加载留言箱失败: %s", e)
            self._message_box = {}

    def _add_message(
        self, target: str, sender: str, content: str, is_private: bool
    ) -> None:
        if target not in self._message_box:
            self._message_box[target] = []
        self._message_box[target].append({
            "sender": sender,
            "content": content,
            "is_private": is_private,
        })
        self._save()

    def _get_messages_for_target(self, target: str) -> list[dict[str, object]]:
        return self._message_box.get(target, [])

    def _clear_messages_for_target(self, target: str) -> None:
        if target in self._message_box:
            del self._message_box[target]
            self._save()

    def _find_user_id(self, name: str) -> str | None:
        return self.bot.find_user(name=name)

    def _is_content_valid(self, target: str, sender: str, content: str) -> bool:
        full = f"/me@{target} {sender}向您留言：{content}"
        return len(full) <= 140

    def _send_public_message(self, target: str, sender: str, content: str) -> None:
        self.bot.me(f"@{target} {sender}向您留言：{content}")

    def _send_private_message(self, target_id: str, sender: str, content: str) -> None:
        self.bot.dm(target_id, f"{sender}向您留言：{content}")

    async def _schedule_delivery(self, target: str, delay: float) -> None:
        async with self._schedule_lock:
            if target in self._pending_targets:
                return
            if not self._get_messages_for_target(target):
                return
            self._pending_targets.add(target)
        asyncio.create_task(self._deliver_messages(target, delay))

    def _handle_single_leave_message(
        self, sender: str, target: str, content: str, is_private: bool
    ) -> None:
        if not target or not content:
            if is_private:
                sender_id = self.bot.find_user(name=sender)
                if sender_id:
                    self.bot.dm(sender_id, "格式错误，请使用：/留言 <用户名> <留言内容>")
            else:
                self.bot.me("格式错误，请使用：/留言 <用户名> <留言内容>")
            return

        if not self._is_content_valid(target, sender, content):
            if is_private:
                sender_id = self.bot.find_user(name=sender)
                if sender_id:
                    self.bot.dm(sender_id, "抱歉，您的留言内容过长，请缩减字数或分条发送")
            else:
                self.bot.me("抱歉，您的留言内容过长，请缩减字数或分条发送")
            return

        if self._find_user_id(target):
            if is_private:
                sender_id = self.bot.find_user(name=sender)
                if sender_id:
                    self.bot.dm(sender_id, "该用户已在房间内")
            else:
                self.bot.me(f"@{sender} 该用户已在房间内")
            return

        self._add_message(target, sender, content, is_private)

        if is_private:
            sender_id = self.bot.find_user(name=sender)
            if sender_id:
                self.bot.dm(sender_id, "收到！您的留言现已被存入留言箱。")
        else:
            self.bot.me("收到！您的留言现已被存入留言箱。")

    async def _check_existing_users(self) -> None:
        while not getattr(self.bot, "room_connected", False):
            await asyncio.sleep(1)

        await asyncio.sleep(STARTUP_CHECK_DELAY)

        if not getattr(self.bot, "room_connected", False):
            return

        own_id = getattr(self.bot.own_user, "id", None) if self.bot.own_user else None

        for user in list(self.bot.room.users.values()):
            if own_id and user.id == own_id:
                continue
            await self._schedule_delivery(user.name, delay=0)

    def on_welcome_done(self, user_name: str) -> None:
        asyncio.create_task(
            self._schedule_delivery((user_name or "").strip(), WELCOME_DONE_DELAY))

    async def _deliver_messages(self, target: str, delay: float) -> None:
        if delay > 0:
            await asyncio.sleep(delay)

        target_id = self._find_user_id(target)
        if not target_id:
            self.bot.logger.info(
                "[MessageBox] 目标用户 %s 在等待期间离开了房间，留言暂不发送", target)
            self._pending_targets.discard(target)
            return

        messages = list(self._get_messages_for_target(target))
        if not messages:
            self._pending_targets.discard(target)
            return

        for entry in messages:
            sender = entry["sender"]
            content = entry["content"]
            is_private = entry["is_private"]

            if is_private:
                self._send_private_message(target_id, sender, content)
                self.bot.logger.info(
                    "[MessageBox] 私密留言已送达: %s -> %s", sender, target)
            else:
                self._send_public_message(target, sender, content)
                self.bot.logger.info(
                    "[MessageBox] 公开留言已送达: %s -> %s", sender, target)

        self._clear_messages_for_target(target)
        self._pending_targets.discard(target)

    @on_event(MessageType.JOIN)
    async def on_user_join(self, msg: Message) -> None:
        if msg.user is None:
            return
        if "welcome" in self.bot.registry.loaded_modules():
            return

        await self._schedule_delivery(msg.user.name, WELCOME_DONE_DELAY)

    @command(r"^/留言\s+.+")
    async def cmd_leave_message(self, msg: Message) -> None:
        if msg.sender is None or self._is_own_message(msg):
            return
        if msg.type == MessageType.DM:
            return

        parts = msg.content.strip().split(maxsplit=2)
        if len(parts) < 3:
            self.bot.me("格式错误，请使用：/留言 <用户名> <留言内容>")
            return

        target = parts[1].strip()
        content = parts[2].strip()
        self._handle_single_leave_message(
            msg.sender.name, target, content, is_private=False)

    @on_event(MessageType.DM)
    async def on_private_message(self, msg: Message) -> None:
        if msg.sender is None or self._is_own_message(msg):
            return

        content = msg.content.strip()
        if not content.startswith("/留言"):
            return

        parts = content.split(maxsplit=2)
        if len(parts) < 3:
            sender_id = self.bot.find_user(name=msg.sender.name)
            if sender_id:
                self.bot.dm(sender_id, "格式错误，请使用：/留言 <用户名> <留言内容>")
            return

        target = parts[1].strip()
        message_text = parts[2].strip()
        self._handle_single_leave_message(
            msg.sender.name, target, message_text, is_private=True)

    def _is_own_message(self, msg: Message) -> bool:
        own = getattr(self.bot, "own_user", None)
        sender = getattr(msg, "sender", None)
        return own is not None and sender is not None and sender.id == own.id

    def unload(self) -> None:
        if self._startup_task and not self._startup_task.done():
            self._startup_task.cancel()
        self._save()
        self.bot.logger.info("[MessageBox] 模块已卸载，留言箱已保存")
        super().unload()