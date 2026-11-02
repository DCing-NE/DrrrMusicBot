"""BotClient：机器人门面。"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

from core.limits import Deduplicator, HistoryFilter, RateLimiter
from core.logger import MessageLogger, get_logger
from core.parser import Parser
from core.registry import ModuleRegistry
from net.session import HttpSession
from models import Message, MessageType, OutgoingType, Room, User
from models.outgoing import (
    OutgoingBan,
    OutgoingChangeDescription,
    OutgoingChangeTitle,
    OutgoingDmUrl,
    OutgoingDirectMessage,
    OutgoingHandoverHost,
    OutgoingKick,
    OutgoingLegacy,
    OutgoingMessage,
    OutgoingMusic,
    OutgoingUrlMessage,
)
from sio import SIoClient

ENDPOINT = "https://drrr.com"
WS_URL = "wss://drrr.com/conn/"
CHAR_LIMIT = 140


class BotClient:
    """drrr 聊天室机器人门面。"""

    def __init__(
        self,
        *,
        config,
        logger: logging.Logger | None = None,
    ) -> None:
        self.logger = logger or get_logger("bot")
        self.config = config
        self.name = config.name
        self.tc = config.tc
        self.avatar = config.avatar
        self.room_id = config.room_id
        self.throttle = max(config.throttle, 0.0)
        self.admins = list(config.admin_tc)
        self.owner_tc = str(getattr(config, "owner_tc", "") or "").strip()

        self.http = HttpSession(
            username=self.name, avatar=config.avatar, qps=config.qps, logger=self.logger
        )
        self.parser = Parser(self.logger)
        self.registry = ModuleRegistry(self)
        self.room = Room()
        self.own_user: User | None = None
        self.room_connected = False

        self.dedup = Deduplicator()
        self.history = HistoryFilter()
        self.sender = RateLimiter(self.throttle)
        self.messages_log = MessageLogger()

        self.sio: SIoClient | None = None

        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []

        self._stopping = False
        self._stopped = False

    async def start(self) -> None:
        """启动机器人：登录 → 进房 → 连接 Socket.IO → 接收循环。"""
        self.logger.info("启动中（用户：%s，房间：%s）", self.name, self.room_id)

        if not await self.http.login(self.tc):
            self.logger.error("登录失败，程序退出")
            return

        await self._run()

    async def _run(self) -> None:
        """登录成功后的主流程（进房 → 连接 → 接收）。"""
        await self.http.update_room_state(self.room)
        if self.room.room_id:
            self.room_connected = True
            self.logger.info("已连接房间【%s】", self.room.name)
        else:
            if not await self.http.join_room(self.room_id, self.room):
                self.logger.error("进入房间失败，程序退出")
                return
            self.room_connected = True

        self.history.mark_joined()
        await self._load_own_user()

        self._tasks.append(asyncio.create_task(self._send_loop()))
        self._tasks.append(asyncio.create_task(self._console_loop()))

        await self._connect_sio()

    async def stop(self) -> None:
        """停止机器人并清理资源。"""
        if self._stopping and self._stopped:
            return
        self._stopping = True
        self.logger.info("正在停止...")

        if self.room_connected:
            self.room_connected = False
            try:
                await self.http.send_post({"leave": "leave"})
                await asyncio.sleep(0.5)
            except Exception:
                pass

        if self.sio:
            self.sio.close()
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current and not task.done():
                task.cancel()
        for name in list(self.registry._modules.keys()):
            try:
                self.registry.unload(name)
            except Exception:
                self.logger.exception("卸载模块 %s 失败", name)
        await self.http.close()
        self.messages_log.close()
        self._stopped = True

    async def _connect_sio(self) -> None:
        self.sio = SIoClient(
            url=WS_URL,
            headers=self._build_ws_headers(),
            impersonate="chrome",
            logger=self.logger,
        )
        self.sio.on_event(self._on_sio_event)
        self.sio.on_disconnect(self._on_sio_disconnect)

        config_payload = {
            "webpush_config": None,
            "push_all_messages": False,
        }
        self.logger.info("连接 Socket.IO ...")
        await self.sio.run_forever(config_payload)

    def _build_ws_headers(self) -> dict[str, str]:
        headers = {"User-Agent": self.http.ua}
        try:
            cookie_str = "; ".join(
                f"{k}={v}" for k, v in self.http.session.cookies.items()
            )
        except Exception:
            cookie_str = ""
        if cookie_str:
            headers["Cookie"] = cookie_str
        return headers

    async def _on_sio_disconnect(self) -> None:
        if self._stopping:
            return
        self.room_connected = False
        self.logger.warning("Socket.IO 连接断开，等待自动重连")

    async def _on_sio_event(self, event: str, data: Any) -> None:
        try:
            if (event in ("in-room", "rewind-done", "new-talk", "rewind")
                    and not self.room_connected and not self._stopping):
                self.room_connected = True
                self.logger.info("Socket.IO 已恢复，房间连接状态已同步")
            if event == "new-talk":
                await self._handle_talk(data)
            elif event == "rewind":
                await self._handle_rewind(data)
            elif event == "leave":
                await self._handle_leave(data)
            elif event == "room-not-exist":
                self.logger.warning("房间不存在，尝试重新进入")
                await self._rejoin()
            elif event == "not-in-any-room":
                self.logger.warning("不在任何房间，尝试重新进入")
                await self._rejoin()
            else:
                self.logger.debug("忽略事件 %s", event)
        except Exception:
            self.logger.exception("处理事件 %s 失败", event)

    async def _handle_talk(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        if data.get("time"):
            self.sio.update_last_time(float(data["time"]))
        msg = self.parser.talk_to_message(data, self.room)
        if msg is None:
            return
        await self._process_message(msg)

    async def _handle_rewind(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        talks = data.get("talks") or []
        msgs = self.parser.talks_to_messages(talks, self.room)
        for msg in msgs:
            await self._process_message(msg)
        if talks and self.sio:
            last = talks[-1].get("time")
            if last:
                self.sio.update_last_time(float(last))
        self.logger.info("rewind 补消息完成，共 %d 条", len(msgs))

    async def _handle_leave(self, data: Any) -> None:
        reason = (data or {}).get("reason", "leave") if isinstance(data, dict) else "leave"
        self.logger.warning("收到 leave 事件，reason=%s", reason)
        self.room_connected = False

        if reason in ("kick", "ban"):
            self.logger.info("被踢出/封禁，停止机器人")
            self._stopping = True
            if self.sio:
                self.sio.close()
            return

        if self._stopping:
            return
        await self._rejoin()

    async def _rejoin(self) -> None:
        if self._stopping:
            return
        for _ in range(5):
            if self._stopping:
                return
            await asyncio.sleep(3)
            self.logger.info("重新进入房间 %s ...", self.room_id)
            self.room = Room()
            if await self.http.join_room(self.room_id, self.room):
                self.room_connected = True
                self.history.mark_joined()
                await self._load_own_user()
                self.logger.info("重新加入房间【%s】", self.room.name)
                return
        self.logger.error("重新进入房间多次失败")

    async def _load_own_user(self) -> None:
        """加载 own_user，失败时重试最多 3 次。"""
        for attempt in range(3):
            try:
                await self.http._throttle()
                resp = await self.http.session.get(f"{ENDPOINT}/room/?api=json")
                data = resp.json()
                uid = data.get("profile", {}).get("uid")
                if uid:
                    self.own_user = self.room.users.get(str(uid))
                    if self.own_user:
                        self.logger.debug("own_user 加载成功: %s", self.own_user.name)
                        return
                self.logger.warning("own_user 未找到（尝试 %d/3）", attempt + 1)
            except Exception as e:
                self.logger.warning("获取 own_user 失败（尝试 %d/3）: %s", attempt + 1, e)
            await asyncio.sleep(1)
        self.logger.error("无法获取 own_user，部分功能可能不可用")

    async def _process_message(self, msg: Message) -> None:
        if msg.id is not None:
            if not self.dedup.check_and_mark(msg.id):
                return

        if msg.time and msg.time < self.history.join_time:
            return

        if msg.content == "/quit":
            if (self.owner_tc and msg.sender
                    and msg.sender.tc == self.owner_tc):
                self.logger.info("收到停止运行命令，退出房间")
                self._stopping = True
                self.room_connected = False
                try:
                    await self.http.send_post({"leave": "leave"})
                except Exception:
                    pass
                if self.sio:
                    self.sio.close()
                return
            else:
                self.whisper("你的权限不足")
                return

        if msg.content and msg.content != "keep":
            user = msg.sender.name if msg.sender else "系统消息"
            self.logger.info("%s | %s", user, msg.content)
            tripcode = msg.sender.tc if msg.sender else ""
            self.messages_log.log(user, tripcode, msg.content)

        if msg.type == MessageType.JOIN:
            await self.http.update_room_state(self.room)
            await self.registry.dispatch(msg)

        elif msg.type == MessageType.LEAVE:
            if msg.sender is not None and msg.sender.id == getattr(self.own_user, "id", None):
                self.room_connected = False
                self.room = Room()
                self.own_user = None
                self.logger.info("离开房间，停止机器人")
                self._stopping = True
                if self.sio:
                    self.sio.close()
                return
            await self.http.update_room_state(self.room)
            await self.registry.dispatch(msg)

        elif msg.type == MessageType.NEW_HOST:
            if msg.sender is not None:
                self.room.host_id = msg.sender.id
            await self.registry.dispatch(msg)

        elif msg.type == MessageType.KICK:
            target = msg.target
            if isinstance(target, User):
                self.room.users.pop(target.id, None)
                if target.id == getattr(self.own_user, "id", None):
                    self.room_connected = False
                    self.room = Room()
                    self.own_user = None
                    self.logger.info("被踢出房间，停止机器人")
                    self._stopping = True
                    if self.sio:
                        self.sio.close()
                    return
            await self.registry.dispatch(msg)

        elif msg.type == MessageType.BAN:
            banned_id = msg.target.id if isinstance(msg.target, User) else msg.target
            if banned_id and str(banned_id) != getattr(self.own_user, "id", None):
                self.room.banned_ids.add(str(banned_id))
                self.room.users.pop(str(banned_id), None)
            await self.registry.dispatch(msg)

        elif msg.type == MessageType.UNBAN:
            if isinstance(msg.target, User):
                self.room.banned_ids.discard(msg.target.id)
            await self.registry.dispatch(msg)

        elif msg.type == MessageType.SYSTEM:
            await self.http.update_room_state(self.room)
            await self.registry.dispatch(msg)

        elif msg.type in (MessageType.ROOM_PROFILE, MessageType.NEW_DESCRIPTION):
            await self.http.update_room_state(self.room)
            await self.registry.dispatch(msg)

        elif msg.type == MessageType.MUSIC:
            if isinstance(msg, Message):
                self.room.music_np = msg.content
            await self.registry.dispatch(msg)

        else:
            await self.registry.dispatch(msg)

    async def _send_loop(self) -> None:
        self.logger.debug("发送循环启动")
        while True:
            item = await self._send_queue.get()
            await self.sender.acquire()
            try:
                await self._do_send(item)
            except Exception:
                self.logger.exception("发送失败: %r", item)

    async def _console_loop(self) -> None:
        """终端交互：输入即用 bot 账号发到房间（轻言细语）。

        /me xxx  → 动作消息
        /quit    → 停止 bot
        """
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        def _reader() -> None:
            while True:
                try:
                    line: str | None = input()
                except (EOFError, KeyboardInterrupt):
                    line = None
                try:
                    loop.call_soon_threadsafe(queue.put_nowait, line)
                except RuntimeError:
                    return
                if line is None:
                    return

        threading.Thread(target=_reader, daemon=True).start()

        while (line := await queue.get()) is not None:
            line = line.strip()
            if not line:
                continue
            if line == "/quit":
                await self.stop()
                return
            if line.startswith("/me "):
                self.me(line[4:])
            else:
                self.whisper(line)

    async def _do_send(self, item: Any) -> None:
        if not self.room_connected:
            self.logger.warning("尚未连接房间，丢弃出站消息")
            return

        t = item.type
        if t == OutgoingType.MESSAGE:
            data = {"message": item.msg}
        elif t == OutgoingType.DM:
            data = {"message": item.msg, "to": item.receiver}
        elif t == OutgoingType.URL:
            data = {"message": item.msg, "url": item.url}
        elif t == OutgoingType.DM_URL:
            data = {"message": item.msg, "url": item.url, "to": item.receiver}
        elif t == OutgoingType.MUSIC:
            data = {"music": "music", "name": item.name, "url": item.url}
        elif t == OutgoingType.HANDOVER_HOST:
            data = {"new_host": item.receiver}
        elif t == OutgoingType.KICK:
            data = {"kick": item.receiver}
        elif t == OutgoingType.BAN:
            data = {"ban": item.receiver}
        elif t == OutgoingType.CHANGE_TITLE:
            data = {"room_name": item.title}
        elif t == OutgoingType.CHANGE_DESCRIPTION:
            data = {"room_description": item.description}
        elif t == OutgoingType.UNBAN:
            data = {"unban": item.receiver}
        elif t == OutgoingType.LEAVE:
            data = {"leave": "leave"}
        elif t == OutgoingType.MUSIC_SKIP:
            data = {"message": "/skip"}
        elif t == OutgoingType.ROOM_LIMIT:
            data = {"room_limit": item.receiver}
        elif t == OutgoingType.DJ_MODE:
            data = {"dj_mode": item.receiver}
        elif t == OutgoingType.MUSIC_FULL_MODE:
            data = {"music_full_mode": item.receiver}
        elif t == OutgoingType.LEGACY:
            data = item.data
        else:
            self.logger.error("未知出站类型: %s", t)
            return

        ok = await self.http.send_post(data)
        if not ok:
            self.logger.warning("发送未成功: %r", data)

    def _enqueue(self, item: Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._send_queue.put_nowait(item)
        else:
            loop.call_soon_threadsafe(self._send_queue.put_nowait, item)

    def _chunk_and_enqueue(self, text: str, builder) -> None:
        chunked = [text[i : i + CHAR_LIMIT] for i in range(0, len(text), CHAR_LIMIT)]
        for chunk in chunked:
            self._enqueue(builder(chunk))

    def send(self, msg: str) -> None:
        self._chunk_and_enqueue(msg, lambda c: OutgoingMessage(msg=c))

    def me(self, msg: str) -> None:
        self.send(f"/me{msg}")

    def whisper(self, msg: str) -> None:
        self._enqueue(OutgoingLegacy(data={"message": msg, "loudness": 3}))

    def dm(self, receiver: str, msg: str) -> None:
        self._chunk_and_enqueue(
            msg, lambda c: OutgoingDirectMessage(msg=c, receiver=receiver)
        )

    def send_url(self, msg: str, url: str) -> None:
        chunked = [msg[i : i + CHAR_LIMIT] for i in range(0, len(msg), CHAR_LIMIT)]
        if len(chunked) == 1:
            self._enqueue(OutgoingUrlMessage(msg=chunked[0], url=url))
            return
        for c in chunked[:-1]:
            self._enqueue(OutgoingMessage(msg=c))
        self._enqueue(OutgoingUrlMessage(msg=chunked[-1], url=url))

    def dm_url(self, receiver: str, msg: str, url: str) -> None:
        chunked = [msg[i : i + CHAR_LIMIT] for i in range(0, len(msg), CHAR_LIMIT)]
        if len(chunked) == 1:
            self._enqueue(OutgoingDmUrl(msg=chunked[0], receiver=receiver, url=url))
            return
        for c in chunked[:-1]:
            self._enqueue(OutgoingDirectMessage(msg=c, receiver=receiver))
        self._enqueue(OutgoingDmUrl(msg=chunked[-1], receiver=receiver, url=url))

    def music(self, name: str, url: str) -> None:
        self._enqueue(OutgoingMusic(name=name, url=url))

    def chown(self, receiver: str) -> None:
        self._enqueue(OutgoingHandoverHost(receiver=receiver))

    def kick(self, receiver: str) -> None:
        self._enqueue(OutgoingKick(receiver=receiver))

    def ban(self, receiver: str) -> None:
        self._enqueue(OutgoingBan(receiver=receiver))

    def unban(self, receiver: str) -> None:
        self._enqueue(OutgoingLegacy(data={"unban": receiver}))

    def title(self, name: str) -> None:
        self._enqueue(OutgoingChangeTitle(title=name))

    def desc(self, description: str) -> None:
        self._enqueue(OutgoingChangeDescription(description=description))

    def leave_room(self) -> None:
        self._enqueue(OutgoingLegacy(data={"leave": "leave"}))

    def music_skip(self) -> None:
        self._enqueue(OutgoingLegacy(data={"message": "/skip"}))

    def room_limit(self, limit: int) -> None:
        self._enqueue(OutgoingLegacy(data={"room_limit": str(limit)}))

    def toggle_dj_mode(self, enabled: bool = True) -> None:
        self._enqueue(OutgoingLegacy(data={"dj_mode": enabled}))

    def toggle_music_full_mode(self, enabled: bool = True) -> None:
        self._enqueue(OutgoingLegacy(data={"music_full_mode": enabled}))

    def find_user(self, name: str | None = None, tc: str | None = None) -> str | None:
        for user in self.room.users.values():
            if tc and user.tc == tc:
                return user.id
            if name and user.name == name:
                return user.id
        return None

    def findUser(self, name: str | None = None, tc: str | None = None) -> str | None:
        return self.find_user(name=name, tc=tc)