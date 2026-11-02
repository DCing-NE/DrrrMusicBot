# -*- coding: utf-8 -*-
"""音乐机器人模块。

命令：
    /点歌 <链接>          支持网易云音乐 / QQ音乐链接（单曲/歌单）
    /点歌 <歌名>-[歌手]   通过 gequbao 搜索并播放
    /队列                  查看当前播放与待播队列
    /跳至 <N|歌名>          跳到队列第 N 首或匹配歌名
    /清空                  清空队列并停止播放
外部插播：await music_bot.interrupt_play({...})
"""

from __future__ import annotations

import asyncio
import ast
import html as html_lib
import json
import random
import re
import time
from typing import Any
from urllib.parse import quote

import aiohttp
import musicbrainzngs
from mp3_duration import get_mp3_duration

from modules.base import Module, command, on_event
from models import Message, MessageType
from music_sources.qq_music import QQMusicSource

musicbrainzngs.set_useragent(
    "drrr-MusicBot", "1.5.0",
    "https://github.com/",
)

NETEASE_API = "https://music.163.com/api"
NETEASE_API_V6 = "https://music.163.com/api/v6"
GD_API = "https://music-api.gdstudio.xyz/api.php"

GEQUBAO_HOSTS = [
    "https://www.gequbao.com",
    "https://www.gequbao.org",
    "https://www.xiagebao.com",
]
CF_BLOCK_TTL = 30 * 60
GEQUBAO_COOLDOWN = 30 * 60
GEQUBAO_SEMAPHORE = 1
GEQUBAO_MIN_INTERVAL = 0.6
GD_SEARCH_COUNT = 10

STARTUP_BUFFER = 0.5
END_BUFFER = 0.5
MUSIC_EVENT_TIMEOUT = 45
PREFETCH_BEFORE_END = 60.0
PLAY_FAIL_STREAK_LIMIT = 3

PC_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

NETEASE_HEADERS = {
    "User-Agent": PC_UA,
    "Referer": "https://music.163.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

GD_HEADERS = {
    "User-Agent": PC_UA,
    "Accept": "application/json, text/plain, */*",
}

GEQUBAO_HEADERS = {
    "User-Agent": PC_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}


class MusicBrainzClient:

    def __init__(self, logger, max_aliases: int = 3):
        self._logger = logger
        self._cache: dict[str, list[str]] = {}
        self._max_aliases = max_aliases

    async def get_artist_aliases(self, name: str) -> list[str]:
        name = name.strip()
        if not name:
            return []
        if name in self._cache:
            return self._cache[name]
        aliases = await asyncio.to_thread(self._fetch_aliases, name)
        self._cache[name] = aliases
        return aliases

    def _fetch_aliases(self, name: str) -> list[str]:
        try:
            result = musicbrainzngs.search_artists(artist=name, limit=1)
            artists = result.get("artist-list", [])
            if not artists:
                self._logger.debug("[MusicBrainz] 未找到艺术家: %s", name)
                return []

            artist = artists[0]
            canonical_name = artist.get("name", name)
            detail = musicbrainzngs.get_artist_by_id(
                artist["id"], includes=["aliases"]
            )
            alias_list = detail.get("artist", {}).get("alias-list", [])

            primary_aliases: list[str] = []
            other_aliases: list[str] = []
            seen: set[str] = set()
            for a in alias_list:
                alias_name = (a.get("alias") or "").strip()
                if not alias_name or alias_name in seen:
                    continue
                locale = (a.get("locale") or "").lower()
                script = (a.get("script") or "").lower()

                if not (
                        locale.startswith("ja")
                        or locale == "zh"
                        or locale.startswith("zh_cn")
                        or script in ("hans", "latn")
                ):
                    continue

                seen.add(alias_name)
                if a.get("primary") == "primary":
                    primary_aliases.append(alias_name)
                else:
                    other_aliases.append(alias_name)

            ordered = [canonical_name] + primary_aliases + other_aliases
            result_list: list[str] = []
            for a in ordered:
                if a and a != name and a not in result_list:
                    result_list.append(a)
                if len(result_list) >= self._max_aliases:
                    break

            self._logger.info(
                "[MusicBrainz] %s -> 取 %d 个别名: %s",
                name, len(result_list), result_list,
            )
            return result_list
        except Exception as e:
            self._logger.warning("[MusicBrainz] 查询失败: %s, error: %s", name, e)
            return []


class MusicBot(Module):
    def __init__(self, bot):
        super().__init__(bot)
        self._running = True
        self._queue: list[dict[str, Any]] = []
        self._queue_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        self._current_song: dict[str, Any] | None = None
        self._play_started: asyncio.Event | None = None
        self._skip_event = asyncio.Event()
        self._prefetch_task: asyncio.Task | None = None
        self._gequbao_sem = asyncio.Semaphore(GEQUBAO_SEMAPHORE)
        self._gqb_rate_lock = asyncio.Lock()
        self._gqb_last_request = 0.0
        self._gqb_cooldown_until = 0.0
        self._play_fail_streak = 0
        self._playback_paused = False
        self._gqb_blocked: dict[str, float] = {}

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        self._player_task = self._loop.create_task(self._player_loop())
        self._mb_client = MusicBrainzClient(self.bot.logger)
        self._qq = QQMusicSource(
            logger=self.bot.logger,
            session_getter=lambda: self.session,
        )

        self.bot.logger.info("[MusicBot] 模块已初始化")

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _player_loop(self) -> None:
        self.bot.logger.info("[MusicBot] 播放器循环已启动")
        try:
            while self._running:
                song: dict[str, Any] | None = None
                try:
                    if self._playback_paused:
                        await asyncio.sleep(0.5)
                        continue
                    song = await self._peek_song()
                    if song is None:
                        await asyncio.sleep(0.5)
                        continue
                    await self._play_song(song)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.bot.logger.exception("[MusicBot] 播放当前歌曲时出现未预期异常，已跳过")
                    async with self._queue_lock:
                        if self._queue and self._queue[0] is song:
                            self._queue.pop(0)
                    self._current_song = None
                    self._play_started = None
                    self._cancel_prefetch()
                    await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            self.bot.logger.info("[MusicBot] 播放器任务已取消")
            raise
        self.bot.logger.info("[MusicBot] 播放器循环已结束")

    async def _play_song(self, song: dict[str, Any]) -> None:
        self._skip_event.clear()

        if not await self._wait_for_room_connection():
            return

        popped = await self._pop_song(song)
        if popped is not song:
            return
        display = self._format_display_name(song["name"], song.get("artist"))

        if not song.get("play_url"):
            if not await self._resolve_play_url(song):
                # 断线导致的失败不视为歌曲失败，恢复原曲等待重连后继续
                if not self._room_connected():
                    await self._requeue_front(song)
                    return
                self.bot.whisper(f"抱歉，「{display}」播放失败")
                await self._note_play_failure(display)
                return
        if not song.get("duration_ms"):
            self.bot.whisper(f"抱歉，「{display}」播放失败")
            self.bot.logger.warning("[MusicBot] 歌曲时长缺失，已跳过: %s", display)
            await self._note_play_failure(display)
            return
        if self._skip_event.is_set():
            self._skip_event.clear()
            return

        self._current_song = song
        self.bot.logger.info("[MusicBot] 正在播放: %s", display)
        self.bot.logger.debug("[MusicBot] 播放URL: %s", song["play_url"][:80])

        self._play_started = asyncio.Event()
        try:
            self.bot.music(display, song["play_url"])
        except Exception as e:
            self.bot.logger.error("[MusicBot] music() 失败: %s", e)
            self._current_song = None
            self._play_started = None
            return

        try:
            result = await asyncio.wait_for(
                self._wait_music_or_skip(), timeout=MUSIC_EVENT_TIMEOUT
            )
        except asyncio.TimeoutError:
            result = "timeout"

        if result == "timeout":
            if not self._room_connected():
                await self._replay_after_reconnect(song, display)
                return
            self.bot.whisper(f"抱歉，「{display}」播放超时，已跳过")
            self._current_song = None
            self._play_started = None
            await self._note_play_failure(display)
            return
        if result == "skip":
            self.bot.logger.info("[MusicBot] 歌曲被中断")
            self._skip_event.clear()
            self._current_song = None
            self._play_started = None
            return

        self._play_fail_streak = 0
        self._playback_paused = False
        duration_sec = song["duration_ms"] / 1000
        prefetch_delay = duration_sec - PREFETCH_BEFORE_END
        self._prefetch_task = asyncio.create_task(
            self._prefetch_next_when_idle(prefetch_delay))

        self._play_started = None

        await asyncio.sleep(STARTUP_BUFFER)
        if self._skip_event.is_set():
            self.bot.logger.info("[MusicBot] 歌曲在启动缓冲期被中断")
            self._skip_event.clear()
            self._current_song = None
            self._cancel_prefetch()
            return

        wait_sec = max(duration_sec + END_BUFFER - STARTUP_BUFFER, 0.1)
        self.bot.logger.debug(
            "[MusicBot] 歌曲时长 %.1f 秒，等待 %.1f 秒", duration_sec, wait_sec)

        self._skip_event.clear()
        try:
            await asyncio.wait_for(self._skip_event.wait(), timeout=wait_sec)
            self.bot.logger.info("[MusicBot] 歌曲在播放期间被中断")
        except asyncio.TimeoutError:
            if not self._room_connected():
                await self._replay_after_reconnect(song, display)
                return

        self._skip_event.clear()
        self._current_song = None
        self._cancel_prefetch()

    def _cancel_prefetch(self) -> None:
        if self._prefetch_task and not self._prefetch_task.done():
            self._prefetch_task.cancel()
        self._prefetch_task = None

    async def _note_play_failure(self, display: str) -> None:
        self._play_fail_streak += 1
        if self._play_fail_streak < PLAY_FAIL_STREAK_LIMIT:
            return
        self._playback_paused = True
        self._cancel_prefetch()
        self.bot.logger.warning(
            "[MusicBot] 连续 %d 首播放失败，已暂停队列，最后失败: %s",
            self._play_fail_streak, display)
        self.bot.whisper("连续多首歌曲播放失败，已暂停播放队列；重新点歌后恢复")

    def _resume_playback(self) -> None:
        self._play_fail_streak = 0
        self._playback_paused = False

    def _room_connected(self) -> bool:
        return bool(getattr(self.bot, "room_connected", False))

    async def _wait_for_room_connection(self) -> bool:
        if self._room_connected():
            return True
        self.bot.logger.info("[MusicBot] 房间未连接，暂停播放任务，等待重连")
        while self._running and not self._room_connected():
            await asyncio.sleep(1)
        return self._running

    async def _wait_music_or_skip(self) -> str:
        music_task = asyncio.create_task(self._play_started.wait())
        skip_task = asyncio.create_task(self._skip_event.wait())
        try:
            done, pending = await asyncio.wait(
                [music_task, skip_task], return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            music_task.cancel()
            skip_task.cancel()
            raise
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        return "skip" if skip_task in done else "music"

    async def _requeue_front(self, song: dict[str, Any]) -> None:
        async with self._queue_lock:
            self._queue.insert(0, song)

    async def _replay_after_reconnect(self, song: dict[str, Any], display: str) -> None:
        self._cancel_prefetch()
        self._current_song = None
        self._play_started = None
        self.bot.logger.warning("[MusicBot] 播放期间房间断线，等待重连后重播: %s", display)

        if not await self._wait_for_room_connection():
            return
        if self._skip_event.is_set():
            self._skip_event.clear()
            return

        song.pop("play_url", None)
        if not await self._resolve_play_url(song):
            self.bot.whisper(f"抱歉，「{display}」播放失败")
            return
        await self._requeue_front(song)
        self.bot.logger.info("[MusicBot] 房间已恢复，当前歌曲已重新排队: %s", display)

    async def _resolve_play_url(self, song: dict[str, Any]) -> bool:
        """为歌曲补全 play_url 与 duration_ms，成功返回 True。"""
        if not self._room_connected():
            self.bot.logger.info("[MusicBot] 房间未连接，暂缓解析播放链接")
            return False

        play_url: str | None = None
        gqb_duration = 0

        if song.get("ne_id"):
            play_url = await self._get_gd_url(song["ne_id"])
            if not play_url:
                self.bot.logger.warning(
                    "[MusicBot] GD 取链失败, ne_id=%s, 尝试 gequbao 回退",
                    song["ne_id"])
                if not self._gqb_in_cooldown():
                    result = await self._search_gequbao(
                        song["name"], song.get("artist"))
                    if result:
                        play_url = result["play_url"]
                        gqb_duration = result.get("duration_ms") or 0

        elif song.get("_need_search"):
            if not self._gqb_in_cooldown():
                result = await self._search_gequbao(
                    song["name"], song.get("artist"))
                if result:
                    play_url = result["play_url"]
                    gqb_duration = result.get("duration_ms") or 0
            if not play_url:
                play_url = await self._search_gd_song(song)

        else:
            self.bot.logger.warning("[MusicBot] 歌曲缺少播放源: %s", song.get("name"))
            return False

        if not play_url:
            return False

        song["play_url"] = play_url
        song["duration_ms"] = (
                await self._get_duration_from_url(play_url)
                or gqb_duration
                or song.get("duration_ms")
                or 0
        )
        return True

    async def _prefetch_next_when_idle(self, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            if not self._room_connected():
                self.bot.logger.info("[MusicBot] 房间未连接，跳过本次预解析")
                return
            async with self._queue_lock:
                if not self._queue:
                    return
                next_song = self._queue[0]
            if next_song.get("play_url"):
                return
            display = self._format_display_name(next_song["name"], next_song.get("artist"))
            self.bot.logger.info("[MusicBot] 预解析下一首: %s", display)
            ok = await self._resolve_play_url(next_song)
            if not ok and self._room_connected():
                await asyncio.sleep(1)
                if not next_song.get("play_url"):
                    self.bot.logger.info("[MusicBot] 预解析重试: %s", display)
                    ok = await self._resolve_play_url(next_song)
            if ok:
                async with self._queue_lock:
                    still_head = bool(self._queue) and self._queue[0] is next_song
                if still_head:
                    self.bot.logger.info("[MusicBot] 预解析成功: %s", display)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.bot.logger.warning("[MusicBot] 预解析异常: %s", e)

    async def _peek_song(self) -> dict[str, Any] | None:
        async with self._queue_lock:
            return self._queue[0] if self._queue else None

    async def _pop_song(self, expected: dict[str, Any] | None = None) -> dict[str, Any] | None:
        async with self._queue_lock:
            if not self._queue:
                return None
            if expected is not None and self._queue[0] is not expected:
                return None
            return self._queue.pop(0)

    @staticmethod
    def _format_display_name(name: str, artist: str | None) -> str:
        if artist and artist not in ("", "未知歌手", "未知"):
            return f"{name} - {artist}"
        return name

    async def _get_duration_from_url(self, play_url: str) -> int:
        try:
            seconds = await asyncio.to_thread(get_mp3_duration, play_url)
            if seconds and seconds > 0:
                return int(seconds * 1000)
        except Exception as e:
            self.bot.logger.warning("[MusicBot] 远程获取时长失败: %s", e)
        return 0

    async def enqueue_welcome(
        self, name: str, artist: str | None = None, ne_id: str | None = None, duration_ms: int = 0,
    ) -> bool:
        name = (name or "").strip()
        artist = (artist or "").strip() or None
        if not name:
            return False
        display = self._format_display_name(name, artist)
        if not self._room_connected():
            self.bot.logger.info("[MusicBot] 房间未连接，暂不播放欢迎曲: %s", display)
            return False

        if ne_id:
            song = {
                "name": name,
                "artist": artist,
                "ne_id": str(ne_id),
                "duration_ms": duration_ms,
            }
        else:
            song = self._make_search_song({
                "name": name,
                "artist": artist,
            })
        await self.interrupt_play(song)
        return True

    async def interrupt_play(self, song: dict[str, Any]) -> None:
        song_copy = song.copy()
        song_copy["_interrupt"] = True
        if (not song_copy.get("play_url") and not song_copy.get("ne_id")
                and not song_copy.get("_need_search")):
            self.bot.logger.error("[MusicBot] 插播歌曲缺少播放源")
            return
        self._resume_playback()
        async with self._queue_lock:
            remaining = self._queue.copy()
            self._queue.clear()
            self._queue.append(song_copy)
            self._queue.extend(remaining)
        self._skip_event.set()
        self.bot.logger.info(
            "[MusicBot] 插播: %s，原队列剩余 %d 首",
            self._format_display_name(song_copy.get("name", "未知"), song_copy.get("artist")),
            len(remaining),
        )

    def _is_own_message(self, msg: Message) -> bool:
        own = getattr(self.bot, "own_user", None)
        sender = getattr(msg, "sender", None)
        return own is not None and sender is not None and sender.id == own.id

    @on_event(MessageType.MUSIC)
    async def on_music_event(self, msg: Message) -> None:
        if not self._is_own_message(msg):
            return
        play_started = self._play_started
        current_song = self._current_song
        if play_started is None or current_song is None or play_started.is_set():
            return
        if current_song.get("_interrupt"):
            self.bot.logger.debug("[MusicBot] 插播歌曲跳过 URL 匹配，直接接受 MUSIC 事件")
            play_started.set()
            return
        msg_url = getattr(msg, "play_url", None)
        if msg_url is not None and msg_url != current_song.get("play_url"):
            self.bot.logger.debug("[MusicBot] 忽略延迟/不匹配的 MUSIC 事件")
            return
        play_started.set()

    @command(r"^/点歌(?:\s+.*)?$")
    async def cmd_play(self, msg: Message) -> None:
        if self._is_own_message(msg) or msg.sender is None:
            return
        parts = msg.content.strip().split(maxsplit=1)
        text = parts[1].strip() if len(parts) > 1 else ""
        if not text:
            self.bot.whisper("格式错误，请使用：/点歌 <链接> 或 /点歌 <歌名>-[歌手]")
            return

        try:
            m = re.search(r"https?://\S+", text)
            if m:
                url = m.group(0).rstrip("。，,;；)）]】")

                if self._is_netease_url(url):
                    await self._handle_netease(url)
                elif self._qq.is_qq_url(url):
                    await self._handle_qq(url)
                else:
                    self.bot.whisper("不支持的链接格式，目前仅支持网易云音乐和QQ音乐")
                return

            name, _, artist = text.partition("-")
            name, artist = name.strip(), artist.strip() or None
            if not name:
                self.bot.whisper("请提供歌名，例如：/点歌 晴天-周杰伦")
                return
            await self._enqueue_lazy({"name": name, "artist": artist})
        except Exception:
            self.bot.logger.exception("[MusicBot] 处理点歌失败")
            self.bot.whisper("点歌失败，请稍后重试")

    @command(r"^/队列\s*$")
    async def cmd_queue(self, msg: Message) -> None:
        if self._is_own_message(msg):
            return
        lines: list[str] = []
        is_empty = False
        if self._current_song:
            display = self._format_display_name(
                self._current_song["name"], self._current_song.get("artist"))
            lines.append(f"▶ 正在播放：{display}")
        async with self._queue_lock:
            queue_len = len(self._queue)
            if queue_len == 0:
                if not lines:
                    is_empty = True
            else:
                for i, song in enumerate(self._queue[:30], 1):
                    lines.append(
                        f"{i}. {self._format_display_name(song['name'], song.get('artist'))}")
                if queue_len > 30:
                    lines.append(f"... 共 {queue_len} 首待播")
        if is_empty:
            self.bot.whisper("当前队列为空")
            return

        MAX_BODY_CHARS = 132
        segments: list[str] = []
        current = ""
        for line in lines:
            if len(line) > MAX_BODY_CHARS:
                line = line[:MAX_BODY_CHARS]
            candidate = current + "\n" + line if current else line
            if len(candidate) <= MAX_BODY_CHARS:
                current = candidate
            else:
                if current:
                    segments.append(current)
                current = line
        if current:
            segments.append(current)

        total = len(segments)
        for idx, seg in enumerate(segments):
            if total > 1:
                self.bot.whisper(f"({idx + 1}/{total})\n" + seg)
            else:
                self.bot.whisper(seg)

    @command(r"^/跳至(?:\s+.*)?$")
    async def cmd_jump(self, msg: Message) -> None:
        if self._is_own_message(msg):
            return
        parts = msg.content.strip().split(maxsplit=1)
        arg = parts[1].strip() if len(parts) > 1 else ""
        if not arg:
            self.bot.whisper("格式错误，请使用：/跳至 <序号|歌名>（如 /跳至 3 或 /跳至 晴天）")
            return

        try:
            target_idx = int(arg)
        except ValueError:
            target_idx = None

        self._resume_playback()
        async with self._queue_lock:
            queue_len = len(self._queue)
            if queue_len == 0:
                self.bot.whisper("队列为空，无法跳转")
                return

            if target_idx is not None:
                if target_idx < 1:
                    self.bot.whisper("序号必须大于 0")
                    return
                if target_idx > queue_len:
                    self.bot.whisper(f"队列中只有 {queue_len} 首，无法跳至第 {target_idx} 首")
                    return
                del self._queue[:target_idx - 1]
                target_song = self._queue[0]
                display = self._format_display_name(
                    target_song["name"], target_song.get("artist"))
                self.bot.whisper(f"已设置跳至第 {target_idx} 首 {display}")
                return

            arg_lower = arg.lower()
            matched_idx = None
            for idx, song in enumerate(self._queue, start=1):
                if arg_lower in song["name"].lower():
                    matched_idx = idx
                    break
            if matched_idx is None:
                self.bot.whisper(f"队列中没有匹配「{arg}」的歌曲")
                return

            if matched_idx > 1:
                del self._queue[:matched_idx - 1]
            target_song = self._queue[0]
            display = self._format_display_name(
                target_song["name"], target_song.get("artist"))
        self.bot.whisper(f"已设置跳至「{display}」")

    @command(r"^/清空\s*$")
    async def cmd_clear(self, msg: Message) -> None:
        if self._is_own_message(msg):
            return
        self._resume_playback()
        async with self._queue_lock:
            count = len(self._queue)
            self._queue.clear()
        self._skip_event.set()
        self.bot.whisper(f"已清空队列（{count} 首）")
        self.bot.logger.info("[MusicBot] 队列已清空 %d 首", count)

    @command(r"^/随机播放\s*$")
    async def cmd_shuffle(self, msg: Message) -> None:
        if self._is_own_message(msg):
            return
        async with self._queue_lock:
            count = len(self._queue)
            if count < 2:
                self.bot.whisper("队列中歌曲不足 2 首，无需随机")
                return
            random.shuffle(self._queue)
        self._resume_playback()
        self.bot.whisper(f"已将队列中 {count} 首歌曲随机排序")
        self.bot.logger.info("[MusicBot] 当前队列已随机排序 %d 首", count)

    @staticmethod
    def _is_netease_url(url: str) -> bool:
        return "music.163.com" in url or "163cn.tv" in url or "163cn.link" in url

    async def _resolve_short_link(self, url: str) -> str | None:
        if not self._room_connected():
            self.bot.logger.info("[MusicBot] 房间未连接，暂缓短链解析")
            return None
        headers = {"User-Agent": PC_UA, "Accept-Language": "zh-CN,zh;q=0.9"}
        try:
            async with self.session.head(
                url, allow_redirects=True, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if str(resp.url) != url:
                    return str(resp.url)
        except Exception:
            pass
        try:
            async with self.session.get(
                url, allow_redirects=True, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if str(resp.url) != url:
                    return str(resp.url)
                body = await resp.text(errors="ignore")
                m = re.search(r'property="og:url"\s+content="([^"]+)"', body)
                if m:
                    return m.group(1).replace("//n/", "/n/")
        except Exception as e:
            self.bot.logger.warning("[MusicBot] 短链解析失败: %s, error: %s", url, e)
        return None

    async def _handle_netease(self, url: str) -> None:
        if "163cn.tv" in url or "163cn.link" in url:
            resolved = await self._resolve_short_link(url)
            if not resolved:
                self.bot.whisper("网易云短链解析失败")
                return
            url = resolved
            self.bot.logger.info("[MusicBot] 网易云短链解析: %s", url)

        parsed = self._parse_netease_url(url)
        if not parsed:
            self.bot.whisper("链接格式不正确")
            return
        link_type, ne_id = parsed
        if link_type == "song":
            await self._add_song(ne_id)
        elif link_type == "playlist":
            await self._add_playlist(ne_id)

    @staticmethod
    def _parse_netease_url(url: str) -> tuple[str, str] | None:
        m = re.search(r"/(?:m/)?(song|playlist)/(\d+)", url)
        if m:
            return (m.group(1), m.group(2))
        m = re.search(r"/#?/song\?[^#]*\bid=(\d+)", url)
        if m:
            return ("song", m.group(1))
        m = re.search(r"/#?/playlist\?[^#]*\bid=(\d+)", url)
        if m:
            return ("playlist", m.group(1))
        m = re.search(r"[?&]id=(\d+)", url)
        if m:
            return ("playlist" if "playlist" in url or "album" in url else "song",
                    m.group(1))
        return None

    async def _handle_qq(self, url: str) -> None:
        info = await self._qq.resolve(url)
        if not info:
            self.bot.whisper("获取QQ音乐信息失败，请检查链接是否有效")
            return

        if "songs" in info:
            songs = info["songs"]
            if not songs:
                self.bot.whisper("歌单为空或无可用的歌曲")
                return
            self.bot.whisper(f"正在处理歌单，共 {len(songs)} 首...")
            self._resume_playback()
            for s in songs:
                s["_need_search"] = True
            async with self._queue_lock:
                self._queue.extend(songs)
            self.bot.whisper(f"已加入队列 {len(songs)} 首歌曲")
            self.bot.logger.info("[MusicBot] QQ歌单已入队 %d 首", len(songs))
            return

        info["_need_search"] = True
        self._resume_playback()
        async with self._queue_lock:
            self._queue.append(info)
        display = self._format_display_name(info["name"], info.get("artist"))
        self.bot.whisper(f"已加入队列：{display}")
        self.bot.logger.info("[MusicBot] QQ单曲已入队: %s", display)

    @staticmethod
    def _make_search_song(entry: dict[str, Any]) -> dict[str, Any]:
        """构造延迟解析的队列项。"""
        name = (entry.get("name") or "").strip()
        artist = (entry.get("artist") or "").strip() or None
        return {
            "name": name,
            "artist": artist,
            "duration_ms": entry.get("duration_ms") or 0,
            "_need_search": True,
        }

    async def _enqueue_lazy(self, entry: dict[str, Any], quiet: bool = False) -> None:
        song = self._make_search_song(entry)
        name = song["name"]
        artist = song.get("artist")
        self._resume_playback()
        async with self._queue_lock:
            self._queue.append(song)
        display = self._format_display_name(name, artist)
        if not quiet:
            self.bot.whisper(f"已加入队列：{display}")
        self.bot.logger.info("[MusicBot] 已入队(待解析): %s", display)

    def _unblocked_gqb_hosts(self) -> list[str]:
        now = time.time()
        expired = [host for host, ts in self._gqb_blocked.items()
                   if now - ts >= CF_BLOCK_TTL]
        for host in expired:
            del self._gqb_blocked[host]
            self.bot.logger.info("[MusicBot] %s 的 Cloudflare 黑名单已自动恢复", host)
        return [host for host in GEQUBAO_HOSTS if host not in self._gqb_blocked]

    def _mark_gqb_blocked(self, host: str) -> None:
        self._gqb_blocked[host] = time.time()
        self.bot.logger.warning(
            "[MusicBot] %s 被 Cloudflare 拦截，%d 分钟后自动恢复",
            host, CF_BLOCK_TTL // 60)

    def _gqb_in_cooldown(self) -> bool:
        remaining = self._gqb_cooldown_until - time.time()
        if remaining <= 0:
            return False
        self.bot.logger.debug(
            "[MusicBot] gequbao 冷却中，剩余 %.1f 分钟，改用 GD netease",
            remaining / 60)
        return True

    def _enter_gqb_cooldown(self, reason: str) -> None:
        now = time.time()
        new_until = now + GEQUBAO_COOLDOWN
        was_cooling = self._gqb_cooldown_until > now
        if new_until <= self._gqb_cooldown_until:
            return
        self._gqb_cooldown_until = new_until
        if was_cooling:
            self.bot.logger.info("[MusicBot] gequbao 冷却期已延长: %s", reason)
        else:
            self.bot.logger.warning(
                "[MusicBot] gequbao 进入 %d 分钟冷却: %s",
                GEQUBAO_COOLDOWN // 60, reason)

    async def _gqb_throttle(self) -> None:
        async with self._gqb_rate_lock:
            wait = GEQUBAO_MIN_INTERVAL - (time.time() - self._gqb_last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            self._gqb_last_request = time.time()

    @staticmethod
    def _is_gqb_site_error(status: int, text: str) -> bool:
        return (MusicBot._is_cf_challenge(status, text)
                or status in (403, 429, 520)
                or status >= 500)

    async def _gqb_get(self, path: str) -> tuple[str | None, str | None, bool]:
        if self._gqb_in_cooldown():
            return None, None, False
        for host in self._unblocked_gqb_hosts():
            if not self._room_connected():
                self.bot.logger.info("[MusicBot] 房间未连接，暂缓 gequbao 请求")
                return None, None, False
            try:
                await self._gqb_throttle()
                async with self.session.get(
                    host + path, headers=GEQUBAO_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    text = await resp.text()
                    if self._is_cf_challenge(resp.status, text):
                        self._mark_gqb_blocked(host)
                        continue
                    if resp.status == 404:
                        return None, None, True
                    if resp.status != 200:
                        self.bot.logger.warning(
                            "[MusicBot] %s HTTP %s: %s", host, resp.status, path)
                        if self._is_gqb_site_error(resp.status, text):
                            self._enter_gqb_cooldown(
                                f"{host} HTTP {resp.status}")
                        return None, None, False
                    return text, host, False
            except Exception as e:
                self.bot.logger.warning("[MusicBot] %s 请求异常: %s", host, e)
                return None, None, False
        if not self._unblocked_gqb_hosts():
            self._enter_gqb_cooldown("所有镜像站均处于 Cloudflare 黑名单")
        return None, None, False

    @staticmethod
    def _is_cf_challenge(status: int, text: str) -> bool:
        return status in (403, 503) and "Just a moment" in text

    @staticmethod
    def _build_search_candidates(name: str, artist: str | None) -> list[str]:
        core = re.sub(r"[（(][^）)]*[）)]", "", name).strip() or name
        first_artist = re.split(r"[/&,，、&]", artist)[0].strip() if artist else ""
        cands = (core, f"{core} {first_artist}".strip(), f"{name} {first_artist}".strip())
        return [c for c in dict.fromkeys(cands) if c]

    async def _expand_with_mb_aliases(
            self, name: str, artist: str, base: list[str], limit: int,
    ) -> tuple[list[str], list[str]]:
        aliases = await self._mb_client.get_artist_aliases(artist)
        if not aliases:
            self.bot.logger.debug(
                "[MusicBot] MusicBrainz 无可用别名: %s", artist)
        match_artists = [artist] + [a for a in aliases if a != artist]
        base_set = set(base)
        cands = (f"{name} {a}".strip() for a in aliases)
        extra = [c for c in dict.fromkeys(cands) if c and c not in base_set]
        return extra[:limit], match_artists

    async def _search_gequbao(self, name: str,
                              artist: str | None) -> dict[str, Any] | None:
        if self._gqb_in_cooldown():
            return None

        primary = self._build_search_candidates(name, artist)

        async with self._gequbao_sem:
            result = await self._search_gequbao_candidates(primary, name, artist)
        if result:
            return result

        extra: list[str] = []
        match_artists: list[str] = []
        if artist:
            extra, match_artists = await self._expand_with_mb_aliases(
                name, artist, primary, limit=3)

        if extra:
            self.bot.logger.info(
                "[MusicBot] gequbao 尝试 MusicBrainz 扩展关键词: %s",
                " / ".join(extra))
            async with self._gequbao_sem:
                result = await self._search_gequbao_candidates(
                    extra, name, match_artists)
            if result:
                return result

        self.bot.logger.warning(
            "[MusicBot] gequbao 各级关键词均无匹配: %s",
            " / ".join(primary + extra))
        return None

    async def _search_gequbao_candidates(
            self, candidates: list[str], name: str, artists: str | list[str] | None,
    ) -> dict[str, Any] | None:
        """用给定候选关键词依次搜 gequbao，命中返回结果，全败返回 None。"""
        for cand in candidates:
            page, host, not_found = await self._gqb_get(f"/s/{quote(cand)}")
            if page is None:
                if not_found:
                    self.bot.logger.info("[MusicBot] gequbao 无结果(404): %s", cand)
                    continue
                self.bot.logger.warning(
                    "[MusicBot] gequbao 搜索失败(站点或网络不可用): %s", cand)
                return None

            rows = self._parse_gqb_search(page)
            if not rows:
                self.bot.logger.info("[MusicBot] gequbao 结果为空: %s", cand)
                continue

            pick = self._match_gqb(rows, name, artists)
            if not pick:
                self.bot.logger.info(
                    "[MusicBot] gequbao 关键词 %s 有 %d 条结果但无匹配，降级",
                    cand, len(rows))
                continue

            self.bot.logger.info(
                "[MusicBot] gequbao 命中: %s - %s (/music/%s, 关键词: %s)",
                pick["name"], pick.get("artist"), pick["id"], cand)
            play = await self._get_gqb_play_url(pick["id"], host)
            if not play:
                return None
            play.setdefault("name", pick["name"])
            play.setdefault("artist", pick.get("artist"))
            return play
        return None

    @staticmethod
    def _parse_gqb_search(page: str) -> list[dict[str, Any]]:
        rows = []
        for m in re.finditer(
                r'<a href="/music/(\d+)" class="hover-zoom[^"]*"[^>]*>(.*?)</a>',
                page, re.DOTALL):
            song_id, block = m.group(1), m.group(2)
            name_m = re.search(
                r'<span class="text-primary[^"]*"[^>]*>\s*(.*?)\s*</span>',
                block, re.DOTALL)
            if not name_m:
                continue
            artist_m = re.search(
                r'<small class="text-jade[^"]*"[^>]*>\s*(.*?)\s*</small>',
                block, re.DOTALL)
            rows.append({
                "id": song_id,
                "name": html_lib.unescape(re.sub(r"<[^>]+>", "", name_m.group(1))).strip(),
                "artist": (html_lib.unescape(re.sub(r"<[^>]+>", "", artist_m.group(1))).strip()
                           if artist_m else None),
            })
        return rows

    @classmethod
    def _match_gqb(cls, rows: list[dict[str, Any]], name: str,
                   artists: str | list[str] | None) -> dict[str, Any] | None:
        if isinstance(artists, str):
            artists = [artists]
        target_as = [n for a in (artists or []) if a
                     and (n := cls._norm_text(a))]

        target_full = cls._norm_text(name)
        target_core = cls._norm_text(re.sub(r"[（(][^）)]*[）)]", "", name))
        paren_tokens = [cls._norm_text(t)
                        for t in re.findall(r"[（(]([^）)]*)[）)]", name)]
        best: dict[str, Any] | None = None
        best_key: tuple[int, int, int] | None = None
        for row in rows:
            rn = cls._norm_text(row["name"])
            if not rn:
                continue
            if rn == target_full or rn == target_core:
                name_score = 2
            elif target_full and (rn in target_full or target_core in rn):
                name_score = 1
            else:
                continue
            a_score = 0
            if target_as:
                ra = cls._norm_text(row.get("artist"))
                if not ra:
                    a_score = 0
                else:
                    hits = []
                    for ta in target_as:
                        if ra == ta:
                            hits.append(2)
                        elif ta in ra or ra in ta:
                            hits.append(1)
                    if not hits:
                        continue
                    a_score = max(hits)
            bonus = sum(1 for t in paren_tokens if t and t in rn)
            key = (name_score, a_score, bonus)
            if best_key is None or key > best_key:
                best_key = key
                best = row
        return best

    @staticmethod
    def _norm_text(s: str | None) -> str:
        if not s:
            return ""
        s = html_lib.unescape(s).lower()
        return re.sub(r"[\s\-–—_&、,，/\\·.（）()\[\]【】]", "", s)

    async def _get_gqb_play_url(self, song_id: str, host: str) -> dict[str, Any] | None:
        if self._gqb_in_cooldown():
            return None
        page, real_host, _ = await self._gqb_get(f"/music/{song_id}")
        if page is None:
            return None
        host = real_host or host

        app_data = self._parse_gqb_appdata(page)
        if not app_data or not app_data.get("play_id"):
            self.bot.logger.warning("[MusicBot] gequbao 详情页无 play_id: /music/%s", song_id)
            return None

        post_hosts = [host] + [h for h in self._unblocked_gqb_hosts() if h != host]
        for post_host in post_hosts:
            if post_host in self._gqb_blocked:
                continue
            if not self._room_connected():
                self.bot.logger.info("[MusicBot] 房间未连接，暂缓 gequbao 取链")
                return None
            if self._gqb_in_cooldown():
                return None
            try:
                await self._gqb_throttle()
                async with self.session.post(
                    f"{post_host}/member/common-play-url",
                    data={"id": app_data["play_id"], "purpose": "play"},
                    headers={
                        **GEQUBAO_HEADERS,
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": f"{post_host}/music/{song_id}",
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                    text = await resp.text()
                    if self._is_cf_challenge(resp.status, text):
                        self._mark_gqb_blocked(post_host)
                        continue
                    if resp.status != 200:
                        self.bot.logger.warning(
                            "[MusicBot] common-play-url HTTP %s", resp.status)
                        if self._is_gqb_site_error(resp.status, text):
                            self._enter_gqb_cooldown(
                                f"common-play-url HTTP {resp.status}")
                        return None
                    data = json.loads(text)
            except Exception as e:
                self.bot.logger.warning("[MusicBot] common-play-url 异常: %s", e)
                return None

            if data.get("code") == 2:
                self.bot.logger.warning("[MusicBot] common-play-url 要求人机验证: %s", text[:120])
                self._enter_gqb_cooldown("common-play-url 要求人机验证")
                return None
            if data.get("code") != 1:
                self.bot.logger.warning("[MusicBot] common-play-url 返回: %s", text[:120])
                return None
            url = (data.get("data") or {}).get("url")
            if not url:
                return None
            return {
                "play_url": url,
                "duration_ms": self._parse_gqb_duration(app_data.get("mp3_duration")),
            }
        return None

    @staticmethod
    def _parse_gqb_appdata(page: str) -> dict[str, Any] | None:
        """页面里是 window.appData = JSON.parse('...')，需先按 JS 字符串反转义。"""
        m = re.search(
            r"window\.appData\s*=\s*JSON\.parse\('((?:[^'\\]|\\.)*)'\)", page)
        if not m:
            return None
        try:
            raw = m.group(1).replace("\\/", "/")
            decoded = ast.literal_eval("'" + raw + "'")
            data = json.loads(decoded)
        except (SyntaxError, ValueError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def _parse_gqb_duration(s: str | None) -> int:
        if not s:
            return 0
        parts = str(s).split(":")
        try:
            if len(parts) == 2:
                return (int(parts[0]) * 60 + int(parts[1])) * 1000
            if len(parts) == 3:
                return (int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])) * 1000
        except ValueError:
            pass
        return 0

    async def _add_song(self, ne_id: str) -> None:
        self.bot.logger.info("[MusicBot] 获取单曲信息, id=%s", ne_id)
        info = await self._get_netease_song(ne_id)
        if not info:
            self.bot.whisper("获取歌曲信息失败，请检查链接是否有效")
            return
        song = {"name": info["name"], "artist": info.get("artist"),
                "duration_ms": info["duration_ms"], "ne_id": ne_id}
        self._resume_playback()
        async with self._queue_lock:
            self._queue.append(song)
        display = self._format_display_name(song["name"], song.get("artist"))
        self.bot.whisper(f"已加入队列：{display}")
        self.bot.logger.info("[MusicBot] 已入队: %s", display)

    async def _add_playlist(self, playlist_id: str) -> None:
        self.bot.logger.info("[MusicBot] 获取歌单, id=%s", playlist_id)
        songs = await self._get_netease_playlist(playlist_id)
        if songs is None:
            self.bot.whisper("获取歌单失败，请检查链接是否有效")
            return
        if not songs:
            self.bot.whisper("歌单为空或无可用的歌曲")
            return
        self.bot.whisper(f"正在处理歌单，共 {len(songs)} 首...")
        self._resume_playback()
        added = 0
        for s in songs:
            song = {"name": s["name"], "artist": s.get("artist"),
                    "duration_ms": s["duration_ms"], "ne_id": s["ne_id"]}
            async with self._queue_lock:
                self._queue.append(song)
            added += 1
        self.bot.whisper(f"已加入队列 {added} 首歌曲")
        self.bot.logger.info("[MusicBot] 歌单已入队 %d 首", added)

    async def _netease_request(self, url: str) -> dict[str, Any] | None:
        if not self._room_connected():
            self.bot.logger.info("[MusicBot] 房间未连接，暂缓网易云API请求")
            return None
        try:
            async with self.session.get(
                url, headers=NETEASE_HEADERS, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    self.bot.logger.warning("[MusicBot] 网易云API HTTP错误: %s", resp.status)
                    return None
                text = await resp.text()
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    self.bot.logger.warning("[MusicBot] 网易云API返回非JSON: %s", text[:200])
                    return None
        except asyncio.TimeoutError:
            self.bot.logger.warning("[MusicBot] 网易云API超时: %s", url[:80])
            return None
        except Exception as e:
            self.bot.logger.error("[MusicBot] 网易云API请求异常: %s", e)
            return None

    async def _get_netease_song(self, song_id: str) -> dict[str, Any] | None:
        data = await self._netease_request(f"{NETEASE_API}/song/detail?ids=[{song_id}]")
        if not data or data.get("code") != 200:
            return None
        songs = data.get("songs", [])
        return self._parse_netease_track(songs[0]) if songs else None

    async def _get_netease_playlist(self, playlist_id: str) -> list[dict[str, Any]] | None:
        data = await self._netease_request(f"{NETEASE_API_V6}/playlist/detail?id={playlist_id}")
        if not data or data.get("code") != 200:
            return None

        playlist = data.get("playlist", {})
        track_ids = playlist.get("trackIds", [])
        all_ids = [
            str(t.get("id", t))
            for t in track_ids
            if t and (t.get("id") if isinstance(t, dict) else t)
        ]
        if not all_ids:
            return None

        return await self._fetch_songs_by_ids(all_ids)

    async def _fetch_songs_by_ids(self, song_ids: list[str]) -> list[dict[str, Any]]:
        requested = list(dict.fromkeys(
            str(x).strip() for x in song_ids if str(x).strip()
        ))
        collected: dict[str, dict[str, Any]] = {}
        remaining = requested

        while remaining:
            batch = await self._get_netease_songs_batch(remaining)
            if not batch and len(remaining) > 1:
                # 整批请求失败时二分，避免单次 URL 过长或上游拒收
                mid = len(remaining) // 2
                for part in (remaining[:mid], remaining[mid:]):
                    batch.extend(await self._get_netease_songs_batch(part))
            if not batch:
                break

            for song in batch:
                sid = str(song.get("ne_id") or "")
                if sid:
                    collected[sid] = song

            new_remaining = [sid for sid in remaining if sid not in collected]
            if len(new_remaining) == len(remaining):
                break  # 本轮无新增，退出
            remaining = new_remaining

        return [collected[sid] for sid in requested if sid in collected]

    async def _get_netease_songs_batch(self, song_ids: list[str]) -> list[dict[str, Any]]:
        data = await self._netease_request(
            f"{NETEASE_API}/song/detail?ids=[{','.join(song_ids)}]")
        if not data or data.get("code") != 200:
            return []
        result = []
        for s in data.get("songs", []):
            parsed = self._parse_netease_track(s)
            if parsed:
                parsed["ne_id"] = str(s.get("id", ""))
                result.append(parsed)
        return result

    @staticmethod
    def _parse_netease_track(track: dict[str, Any]) -> dict[str, Any] | None:
        if not track:
            return None
        artists = track.get("ar") or track.get("artists") or []
        artist = artists[0].get("name", "未知歌手") if artists else "未知歌手"
        duration_ms = track.get("dt") or track.get("duration") or 0
        return {"name": track.get("name", "未知歌曲"),
                "artist": artist, "duration_ms": duration_ms}

    async def _search_gd_song(self, song: dict[str, Any]) -> str | None:
        if not self._room_connected():
            self.bot.logger.info("[MusicBot] 房间未连接，暂缓GD搜索")
            return None

        name = song["name"]
        artist = song.get("artist")
        primary = self._build_search_candidates(name, artist)

        url = await self._search_gd_candidates(primary, name, artist)
        if url:
            return url

        if artist:
            extra, match_artists = await self._expand_with_mb_aliases(
                name, artist, primary, limit=5)
            if extra:
                self.bot.logger.info(
                    "[MusicBot] GD 尝试 MusicBrainz 扩展关键词: %s",
                    " / ".join(extra))
                url = await self._search_gd_candidates(extra, name, match_artists)
                if url:
                    return url

        self.bot.logger.warning(
            "[MusicBot] GD netease 未找到可用音源: %s",
            self._format_display_name(name, artist))
        return None

    async def _search_gd_candidates(
            self, candidates: list[str], name: str,
            artists: str | list[str] | None,
    ) -> str | None:
        for cand in candidates:
            rows = await self._gd_search(cand)
            if not rows:
                continue
            pick = self._match_gqb(rows, name, artists)
            if not pick:
                continue
            gd_id = str(pick.get("url_id") or pick.get("id") or "")
            if not gd_id:
                continue
            url = await self._get_gd_url(gd_id)
            if url:
                self.bot.logger.info(
                    "[MusicBot] GD netease 命中: %s - %s (id=%s, 关键词: %s)",
                    pick["name"], pick.get("artist"), gd_id, cand)
                return url
        return None

    async def _gd_search(self, keyword: str, source: str = "netease") -> list[dict[str, Any]]:
        if not self._room_connected():
            return []
        url = (f"{GD_API}?types=search&source={quote(source)}"
               f"&name={quote(keyword)}&count={GD_SEARCH_COUNT}&pages=1")
        try:
            async with self.session.get(
                url, headers=GD_HEADERS, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    self.bot.logger.warning(
                        "[MusicBot] GD 搜索 HTTP %s, source=%s, keyword=%s",
                        resp.status, source, keyword)
                    return []
                data = await resp.json(content_type=None)
        except Exception as e:
            self.bot.logger.warning(
                "[MusicBot] GD 搜索异常, source=%s, keyword=%s: %s", source, keyword, e)
            return []
        if not isinstance(data, list):
            return []

        rows: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            song_id = str(item.get("id") or "").strip()
            if not name or not song_id:
                continue
            artist_value = item.get("artist")
            if isinstance(artist_value, list):
                artist = "/".join(str(a).strip() for a in artist_value if str(a).strip())
            else:
                artist = str(artist_value or "").strip()
            rows.append({
                "id": song_id,
                "url_id": str(item.get("url_id") or "").strip(),
                "name": html_lib.unescape(name),
                "artist": html_lib.unescape(artist) or None,
            })
        return rows

    async def _get_gd_url(self, ne_id: str) -> str | None:
        for br in (128, 192, 320):
            try:
                async with self.session.get(
                        f"{GD_API}?types=url&source=netease&id={ne_id}&br={br}",
                        headers=GD_HEADERS,
                        timeout=aiohttp.ClientTimeout(total=4),
                ) as resp:
                    if resp.status == 200:
                        url = (await resp.json()).get("url")
                        if url:
                            return url
            except Exception:
                pass
        self.bot.logger.warning(
            "[MusicBot] GD 所有码率均无可用链接, ne_id=%s", ne_id)
        return None

    def unload(self) -> None:
        self._running = False
        self._skip_event.set()
        self._cancel_prefetch()
        if self._player_task and not self._player_task.done():
            self._player_task.cancel()
        if self._session and not self._session.closed:
            try:
                asyncio.get_running_loop().create_task(self._session.close())
            except RuntimeError:
                pass
        self.bot.logger.info("[MusicBot] 模块已卸载")
        super().unload()