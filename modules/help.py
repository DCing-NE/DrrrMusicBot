# -*- coding: utf-8 -*-
"""帮助模块
/help 发送功能介绍；新用户进房时由 welcome 通知私信提醒。
私信发送 /help → 私信回复；房间发送 /help → 房间回复。
"""

from __future__ import annotations
from modules.base import Module, command
from models import Message, MessageType
from models.outgoing import OutgoingLegacy

LOUDNESS_WHISPER = 3
MAX_SEGMENT_CHARS = 140
PREFIX_RESERVE = 7
REMINDER_TEXT = "欢迎！如有需要，可使用命令【/help】查看本bot的功能介绍"

class Help(Module):
    def __init__(self, bot):
        super().__init__(bot)
        self._help_lines = [
            "",
            "留言命令：",
            "/留言 [name] [text]",
            "注意事项：",
            "1.注意空格的使用",
            "2.每条留言的字数在120以内最稳妥，更多可能需要分条发送",
            "3.如果你想保持留言内容私密，可将命令私信发给bot",
            "",
            "注意事项：",
            "1.命令和参数之间要有一个空格",
            "2.仅支持来自网易云/QQ的链接",
            "3.bot点歌指令请勿与音乐房自带指令混用,以免造成队列混乱",
            "4.如果没有声音,请关闭设置中的‘仅允许来自知名网站的音乐’",
            "",
            "歌单命令：",
            "/歌单列表",
            "/新增歌单 <歌单名> <url> 保存网易云 / QQ音乐歌单",
            "/删除歌单 <歌单名>",
            "/同步歌单 <歌单名> <url> 用自己的新歌单覆盖同名歌单",
            "/播放歌单 <歌单名>",
            "",
            "音乐播放功能介绍：",
            "/点歌 <链接>或<歌名-歌手>或<歌名>",
            "/队列-查看当前播放队列",
            "/随机播放-按随机顺序重置目前队列",
            "/跳至<数字/歌名> -下一首跳至队列中指定曲目播放",
            "/清空-清空播放队列",
        ]

    # ---------- 命令 ----------
    @command(r"^/help$")
    async def cmd_help(self, msg: Message) -> None:
        if msg.sender is None:
            return
        if msg.type in (MessageType.DM, MessageType.DM_URL):
            receiver = msg.sender.id
            if receiver:
                self._send_segments(self._help_lines, dm_receiver=receiver)
        else:
            self._send_segments(self._help_lines)

    # ---------- welcome 完成钩子 ----------
    def on_welcome_done(self, user_name: str) -> None:
        """welcome 模块在欢迎流程结束后调用；给新用户私信提醒。"""
        receiver = self._find_user_id((user_name or "").strip())
        if not receiver:
            return
        try:
            self.bot._enqueue(OutgoingLegacy(data={
                "message": REMINDER_TEXT,
                "to": receiver,
                "loudness": LOUDNESS_WHISPER,
            }))
        except Exception:
            self.bot.logger.exception("[Help] 新用户私信发送失败")

    def _find_user_id(self, user_name: str) -> str:
        for user in self.bot.room.users.values():
            if user.name == user_name:
                return user.id
        return ""

    # ---------- 分段发送 ----------
    def _send_segments(self, lines: list[str], dm_receiver: str = "") -> None:
        """分段发送帮助内容。
        dm_receiver 非空时走私信，否则走房间 whisper。
        """
        limit = MAX_SEGMENT_CHARS - PREFIX_RESERVE
        segments: list[str] = []
        current = ""

        for line in lines:
            line = line[:MAX_SEGMENT_CHARS]
            if line == "":
                if current:
                    segments.append(current)
                    current = ""
                continue
            candidate = f"{current}\n{line}" if current else line
            if len(candidate) <= limit:
                current = candidate
            else:
                if current:
                    segments.append(current)
                current = line
        if current:
            segments.append(current)

        total = len(segments)
        for idx, seg in enumerate(segments):
            text = f"({total - idx}/{total}) \n{seg}"
            if dm_receiver:
                self._safe_dm(dm_receiver, text)
            else:
                self.bot.whisper(text)

    def _safe_dm(self, receiver: str, text: str) -> None:
        try:
            self.bot._enqueue(OutgoingLegacy(data={
                "message": text,
                "to": receiver,
                "loudness": LOUDNESS_WHISPER,
            }))
        except Exception as e:
            self.bot.logger.error("[Help] 私信发送失败: %s", e)