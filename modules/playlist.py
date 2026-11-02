# -*- coding: utf-8 -*-
"""歌单模块。

命令：
    /新增歌单 <歌单名> <url>    保存网易云 / QQ音乐歌单（或单曲）
    /同步歌单 <歌单名> <url>    用自己的新歌单覆盖同名歌单（仅创建者）
    /歌单列表                   展示已存储歌单（按创建者分组）
    /播放歌单 <歌单名>          将歌单内歌曲追加进播放队列
    /删除歌单 <歌单名>          仅创建者可删除

歌单名支持空格：带 URL 的命令从尾部匹配 URL 作为边界；
只有歌单名的命令取剩余全部内容。

存储：运行目录下 playlists.json
{
    "歌单名": {
        "owner": "用户名",
        "songs": [
            {"name": "...", "artist": "...", "duration_ms": 0, "ne_id": "..."},
            {"name": "...", "artist": "...", "duration_ms": 0, "qq_mid": "..."}
        ]
    }
}

说明：
- 空歌单不允许创建 / 同步（解析结果为空会直接拒绝）。
- 歌单名全局唯一，权限按用户名判定。
- 网易云歌曲带 ne_id；QQ 音乐歌曲带 qq_mid。入队时按 ne_id 判定是否延迟搜索，
  QQ 歌曲目前统一下发 _need_search=True（由 music_bot 现场搜索音源）。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any

from modules.base import Module, command
from models import Message

PLAYLIST_FILE = "playlists.json"
MAX_MSG_CHARS = 140

URL_TAIL_RE = re.compile(r"(https?://\S+)\s*$")


class Playlist(Module):
    def __init__(self, bot):
        super().__init__(bot)
        self._playlists: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._load()
        self.bot.logger.info(
            "[Playlist] 模块已初始化，当前歌单数: %d", len(self._playlists)
        )

    def _load(self) -> None:
        if not os.path.exists(PLAYLIST_FILE):
            self._playlists = {}
            return
        try:
            with open(PLAYLIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._playlists = data if isinstance(data, dict) else {}
        except Exception as e:
            self.bot.logger.error("[Playlist] 加载歌单失败: %s", e)
            self._playlists = {}

    def _save(self) -> None:
        try:
            with open(PLAYLIST_FILE, "w", encoding="utf-8") as f:
                json.dump(self._playlists, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.bot.logger.error("[Playlist] 保存歌单失败: %s", e)

    def _is_own_message(self, msg: Message) -> bool:
        own = getattr(self.bot, "own_user", None)
        sender = getattr(msg, "sender", None)
        return own is not None and sender is not None and sender.id == own.id

    def _get_music_bot(self):
        return self.bot.registry._modules.get("music_bot")

    @staticmethod
    def _truncate(line: str, limit: int = MAX_MSG_CHARS) -> str:
        return line if len(line) <= limit else line[: limit - 3] + "..."

    def _send_lines(self, lines: list[str]) -> None:
        """把多行内容合并发送，保证每条 whisper ≤ 140 字符。"""
        current = ""
        for raw in lines:
            line = self._truncate(raw)
            if not current:
                current = line
            elif len(current) + 1 + len(line) <= MAX_MSG_CHARS:
                current += "\n" + line
            else:
                self.bot.whisper(current)
                current = line
        if current:
            self.bot.whisper(current)

    @staticmethod
    def _split_name_url(rest: str) -> tuple[str, str] | None:
        """把「<歌单名> <url>」切成 (name, url)；不合法返回 None。"""
        m = URL_TAIL_RE.search(rest)
        if not m:
            return None
        name = rest[: m.start()].strip()
        url = m.group(1).strip()
        if not name or not url:
            return None
        return name, url

    @staticmethod
    def _split_name(msg: Message) -> str | None:
        """把「<歌单名>」取出（允许含空格）。"""
        parts = msg.content.split(maxsplit=1)
        if len(parts) < 2:
            return None
        name = parts[1].strip()
        return name or None

    def _check_owner(
        self, msg: Message, name: str
    ) -> tuple[dict[str, Any] | None, str | None]:
        """歌单必须存在且归发送者所有；返回 (data, error)。"""
        data = self._playlists.get(name)
        if not data:
            return None, f"歌单「{name}」不存在"
        if data.get("owner") != msg.sender.name:
            return None, f"你没有权限操作歌单「{name}」"
        return data, None

    def _check_create_conflict(self, msg: Message, name: str) -> str | None:
        """`/新增歌单` 冲突检查；返回 None 表示可以创建。"""
        data = self._playlists.get(name)
        if data is None:
            return None
        if data.get("owner") == msg.sender.name:
            return f"歌单「{name}」已存在，如需更新请使用：/同步歌单 {name} <url>"
        return f"歌单「{name}」已存在（创建者：{data.get('owner')}）"

    async def _parse_music_url(
        self, music_bot, url: str
    ) -> list[dict[str, Any]] | None:
        """解析网易云 / QQ音乐链接为歌曲列表；解析失败返回 None。"""
        try:
            if "163cn.tv" in url or "163cn.link" in url:
                resolved = await music_bot._resolve_short_link(url)
                if resolved:
                    url = resolved

            if music_bot._is_netease_url(url):
                parsed = music_bot._parse_netease_url(url)
                if not parsed:
                    return None
                link_type, ne_id = parsed
                if link_type == "playlist":
                    return await music_bot._get_netease_playlist(ne_id)
                info = await music_bot._get_netease_song(ne_id)
                if not info:
                    return None
                info["ne_id"] = ne_id
                return [info]

            if music_bot._qq.is_qq_url(url):
                info = await music_bot._qq.resolve(url)
                if not info:
                    return None
                if "songs" in info:
                    return info["songs"]
                return [info]

            return None
        except Exception:
            self.bot.logger.exception("[Playlist] 解析音乐链接失败: %s", url)
            return None

    @staticmethod
    def _to_queue_song(stored: dict[str, Any]) -> dict[str, Any]:
        """把存储的歌曲转成 music_bot 队列项。"""
        song: dict[str, Any] = {
            "name": stored.get("name", ""),
            "artist": stored.get("artist"),
            "duration_ms": stored.get("duration_ms") or 0,
        }
        if stored.get("ne_id"):
            song["ne_id"] = stored["ne_id"]
        else:
            song["_need_search"] = True
        return song

    async def _save_playlist(
        self, msg: Message, name: str, url: str, *, is_sync: bool
    ) -> None:
        music_bot = self._get_music_bot()
        if music_bot is None:
            self.bot.whisper("音乐模块未加载，无法处理歌单")
            return
        owner = msg.sender.name

        if is_sync:
            _, err = self._check_owner(msg, name)
            if err:
                self.bot.whisper(err)
                return
        else:
            conflict = self._check_create_conflict(msg, name)
            if conflict:
                self.bot.whisper(conflict)
                return

        songs = await self._parse_music_url(music_bot, url)
        if songs is None:
            self.bot.whisper("歌单解析失败，请检查链接")
            return
        if not songs:
            self.bot.whisper("歌单为空或没有可用歌曲")
            return

        async with self._lock:
            if is_sync:
                data, err = self._check_owner(msg, name)
                if err:
                    self.bot.whisper(err)
                    return
                data["songs"] = songs
                action = "同步"
            else:
                if name in self._playlists:
                    self.bot.whisper(f"歌单「{name}」已存在")
                    return
                self._playlists[name] = {"owner": owner, "songs": songs}
                action = "保存"
            self._save()

        self.bot.whisper(f"歌单「{name}」已{action}，共 {len(songs)} 首歌曲")
        self.bot.logger.info(
            "[Playlist] %s %s歌单「%s」，%d 首",
            owner, "同步" if is_sync else "创建", name, len(songs)
        )

    @command(r"^/新增歌单(?:\s+.*)?$")
    async def cmd_create(self, msg: Message) -> None:
        if msg.sender is None or self._is_own_message(msg):
            return
        parts = msg.content.split(maxsplit=1)
        rest = parts[1].strip() if len(parts) > 1 else ""
        parsed = self._split_name_url(rest) if rest else None
        if not parsed:
            self.bot.whisper("格式错误，请使用：/新增歌单 <歌单名> <url>")
            return
        name, url = parsed
        await self._save_playlist(msg, name, url, is_sync=False)

    @command(r"^/同步歌单(?:\s+.*)?$")
    async def cmd_sync(self, msg: Message) -> None:
        if msg.sender is None or self._is_own_message(msg):
            return
        parts = msg.content.split(maxsplit=1)
        rest = parts[1].strip() if len(parts) > 1 else ""
        parsed = self._split_name_url(rest) if rest else None
        if not parsed:
            self.bot.whisper("格式错误，请使用：/同步歌单 <歌单名> <url>")
            return
        name, url = parsed
        await self._save_playlist(msg, name, url, is_sync=True)

    @command(r"^/歌单列表\s*$")
    async def cmd_list(self, msg: Message) -> None:
        if msg.sender is None or self._is_own_message(msg):
            return

        async with self._lock:
            snapshot = {
                name: (data.get("owner", "未知"), len(data.get("songs") or []))
                for name, data in self._playlists.items()
            }
        if not snapshot:
            self.bot.whisper("当前没有存储的歌单")
            return

        groups: dict[str, list[tuple[str, int]]] = {}
        for name, (owner, count) in snapshot.items():
            groups.setdefault(owner, []).append((name, count))

        lines: list[str] = []
        first_group = True
        for owner in sorted(groups):
            if not first_group:
                lines.append("")
            first_group = False
            lines.append(f"{owner}的歌单：")
            for idx, (name, count) in enumerate(
                sorted(groups[owner], key=lambda x: x[0]), 1
            ):
                lines.append(f"{idx}.{name}（{count}首）")

        self._send_lines(lines)

    @command(r"^/播放歌单(?:\s+.*)?$")
    async def cmd_play(self, msg: Message) -> None:
        if msg.sender is None or self._is_own_message(msg):
            return
        music_bot = self._get_music_bot()
        if music_bot is None:
            self.bot.whisper("音乐模块未加载，无法播放歌单")
            return
        name = self._split_name(msg)
        if not name:
            self.bot.whisper("格式错误，请使用：/播放歌单 <歌单名>")
            return

        async with self._lock:
            data = self._playlists.get(name)
            if not data:
                self.bot.whisper(f"歌单「{name}」不存在")
                return
            songs = list(data.get("songs") or [])
        if not songs:
            self.bot.whisper(f"歌单「{name}」是空的")
            return

        queue_songs = [self._to_queue_song(s) for s in songs]
        async with music_bot._queue_lock:
            music_bot._queue.extend(queue_songs)
        music_bot._resume_playback()

        self.bot.whisper(f"已将歌单「{name}」的 {len(queue_songs)} 首歌曲加入队列")
        self.bot.logger.info(
            "[Playlist] %s 播放歌单「%s」，%d 首",
            msg.sender.name, name, len(queue_songs)
        )

    @command(r"^/删除歌单(?:\s+.*)?$")
    async def cmd_delete(self, msg: Message) -> None:
        if msg.sender is None or self._is_own_message(msg):
            return
        name = self._split_name(msg)
        if not name:
            self.bot.whisper("格式错误，请使用：/删除歌单 <歌单名>")
            return

        async with self._lock:
            _, err = self._check_owner(msg, name)
            if err:
                self.bot.whisper(err)
                return
            del self._playlists[name]
            self._save()

        self.bot.whisper(f"歌单「{name}」已删除")
        self.bot.logger.info("[Playlist] %s 删除歌单「%s」", msg.sender.name, name)

    def unload(self) -> None:
        self._save()
        self.bot.logger.info("[Playlist] 模块已卸载，歌单已保存")
        super().unload()