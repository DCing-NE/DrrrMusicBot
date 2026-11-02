"""手动实现的轻量 Socket.IO 客户端。"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any

from curl_cffi import CurlWsFlag, WebSocket

from core.logger import get_logger

EIO_OPEN = "0"
EIO_CLOSE = "1"
EIO_PING = "2"
EIO_PONG = "3"
EIO_MESSAGE = "4"
EIO_UPGRADE = "5"
EIO_NOOP = "6"

SIO_CONNECT = "0"
SIO_DISCONNECT = "1"
SIO_EVENT = "2"
SIO_ACK = "3"
SIO_ERROR = "4"

WS_QUERY_VERSION = "4.1"

EventHandler = Callable[[str, Any], Awaitable[None] | None]
DisconnectHandler = Callable[[], Awaitable[None] | None]


class SIoClient:
    def __init__(
        self,
        *,
        url: str,
        headers: dict[str, str] | None = None,
        cookies: str | None = None,
        impersonate: str = "chrome",
        logger: logging.Logger | None = None,
    ) -> None:
        self.url = url
        self.headers = dict(headers or {})
        self.cookies = cookies
        self.impersonate = impersonate
        self.logger = logger or get_logger("sio")

        self._ws: WebSocket | None = None
        self._recv_thread: threading.Thread | None = None
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._running = threading.Event()
        self._send_lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

        self.sid: str | None = None
        self.ping_interval: float = 25000.0
        self.ping_timeout: float = 20000.0

        self._event_handlers: list[EventHandler] = []
        self._disconnect_handlers: list[DisconnectHandler] = []

        self._heartbeat_task: asyncio.Task[None] | None = None
        self._last_ping_at: float = 0.0

        self._reconnect_delay: float = 3.0
        self._manual_close = False
        self.last_time: float = 0.0

        # 防止重复通知
        self._disconnect_notified: bool = False

        # 唤醒重连等待
        self._stop_event: asyncio.Event | None = None

    def connect(self) -> None:
        if self._ws is not None:
            return
        self._disconnect_notified = False
        self.logger.info("正在连接 %s ...", self.url)
        ws = WebSocket()
        params = {"EIO": "4", "transport": "websocket", "version": WS_QUERY_VERSION}
        ws.connect(
            self.url,
            params=params,
            headers=self.headers or None,
            cookies=self.cookies,
            impersonate=self.impersonate,
            allow_redirects=False,
        )
        self._ws = ws
        self._manual_close = False
        self._running.set()
        self._recv_thread = threading.Thread(
            target=self._recv_loop, name="sio-recv", daemon=True
        )
        self._recv_thread.start()
        self.logger.info("WebSocket 已连接")

    def is_connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    def on_event(self, handler: EventHandler) -> None:
        self._event_handlers.append(handler)

    def on_disconnect(self, handler: DisconnectHandler) -> None:
        self._disconnect_handlers.append(handler)

    def send(self, payload: str) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            self.logger.warning("WebSocket 未连接，丢弃发送: %r", payload[:80])
            return
        with self._send_lock:
            try:
                ws.send_str(payload)
            except Exception as e:
                self.logger.error("发送失败: %s", e)

    def send_event(self, event: str, data: Any = None) -> None:
        payload = json.dumps([event, data], ensure_ascii=False)
        self.send(EIO_MESSAGE + SIO_EVENT + payload)

    def send_ack(self, ack_id: int) -> None:
        payload = f"{ack_id}[]"
        self.send(EIO_MESSAGE + SIO_ACK + payload)

    def send_ping(self) -> None:
        self.send(EIO_PING)

    def send_pong(self) -> None:
        self.send(EIO_PONG)

    def send_config(self, config: dict[str, Any]) -> None:
        self.send_event("config", config)

    def send_recover(self, last_time: float) -> None:
        self.send_event("recover", {"last_time": last_time})

    def _recv_loop(self) -> None:
        ws = self._ws
        if ws is None:
            return
        while self._running.is_set():
            try:
                data, flags = ws.recv()
            except Exception as e:
                if self._running.is_set():
                    self.logger.warning("接收线程退出: %s", e, exc_info=True)
                break
            if not data:
                continue
            if flags & CurlWsFlag.TEXT:
                text = data.decode("utf-8", errors="replace")
                self.logger.debug("recv frame flags=%s len=%d head=%r",
                                  flags, len(text), text[:60])
                if self._loop is None:
                    self.logger.warning("事件循环未绑定，丢弃收到的帧")
                    continue
                self._loop.call_soon_threadsafe(self._queue.put_nowait, text)
            if flags & CurlWsFlag.CLOSE:
                self.logger.info("收到 WebSocket 关闭帧")
                break
        self._running.clear()
        if self._loop is not None and not self._manual_close:
            self._loop.call_soon_threadsafe(self._notify_disconnect)

    def _notify_disconnect(self) -> None:
        if self._disconnect_notified:
            return
        self._disconnect_notified = True
        for handler in self._disconnect_handlers:
            try:
                coro = handler()
                if coro is not None and self._loop is not None:
                    self._loop.create_task(coro)
            except Exception:
                self.logger.exception("断线回调执行失败")

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        while self._running.is_set():
            try:
                frame = await asyncio.wait_for(self._queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            self._handle_frame(frame)

    async def run_forever(self, config: dict[str, Any]) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

        while not self._manual_close:
            try:
                await self._connect_once(config)
            except asyncio.CancelledError:
                self._manual_close = True
                raise
            except Exception as e:
                self.logger.error("连接失败: %s", e)

            if self._manual_close:
                return

            if self._ws is not None:
                try:
                    self._ws.close()
                except Exception:
                    pass
                self._ws = None
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None

            delay = self._reconnect_delay
            self.logger.info("将在 %.1f 秒后重连...", delay)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

            if self._manual_close:
                return

            self._reconnect_delay = min(delay * 2 + random.uniform(0, 1), 60)

    async def _connect_once(self, config: dict[str, Any]) -> None:
        self._disconnect_notified = False
        self.connect()
        frame = await asyncio.wait_for(self._queue.get(), timeout=15.0)
        self._handle_frame(frame)
        if not self.sid:
            raise RuntimeError("Socket.IO 握手未完成（未收到 open 帧）")

        self.send(EIO_MESSAGE + SIO_CONNECT)
        try:
            frame = await asyncio.wait_for(self._queue.get(), timeout=10.0)
            self._handle_frame(frame)
        except asyncio.TimeoutError:
            self.logger.warning("等待命名空间连接确认超时，继续尝试发送 config")

        if self.last_time:
            self.send_recover(self.last_time)
        else:
            self.send_config(config)

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self.run()
        finally:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()

    def _handle_frame(self, frame: str) -> None:
        if not frame:
            return
        eio_type = frame[0]
        body = frame[1:]

        if eio_type == EIO_OPEN:
            self._handle_open(body)
        elif eio_type == EIO_PING:
            self._last_ping_at = time.monotonic()
            self.send_pong()
        elif eio_type == EIO_PONG:
            self._last_ping_at = time.monotonic()
        elif eio_type == EIO_MESSAGE:
            self._handle_sio_packet(body)
        elif eio_type == EIO_CLOSE:
            self.logger.info("服务器关闭连接")
        else:
            self.logger.debug("忽略 Engine.IO 帧: %r", frame)

    def _handle_open(self, body: str) -> None:
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self.logger.error("open 帧解析失败: %r", body[:200])
            return
        self.sid = data.get("sid")
        self.ping_interval = data.get("pingInterval", 25000) / 1000.0
        self.ping_timeout = data.get("pingTimeout", 20000) / 1000.0
        self._last_ping_at = time.monotonic()
        self.logger.info(
            "Socket.IO 握手成功 sid=%s ping=%.1fs timeout=%.1fs",
            self.sid, self.ping_interval, self.ping_timeout,
        )

    def _handle_sio_packet(self, packet: str) -> None:
        if not packet:
            return
        ptype = packet[0]
        body = packet[1:]

        if ptype == SIO_EVENT:
            self._dispatch_event(body)
        elif ptype == SIO_ACK:
            self.logger.debug("收到 ack: %r", body[:200])
        elif ptype == SIO_CONNECT:
            self.logger.info("Socket.IO 命名空间连接成功")
        elif ptype == SIO_DISCONNECT:
            self.logger.info("Socket.IO 断开")
        elif ptype == SIO_ERROR:
            self.logger.warning("Socket.IO 错误: %r", body[:200])
        else:
            self.logger.debug("忽略 Socket.IO 包: %r", packet[:200])

    def _dispatch_event(self, body: str) -> None:
        ack_id: int | None = None
        i = 0
        while i < len(body) and body[i].isdigit():
            i += 1
        if i:
            try:
                ack_id = int(body[:i])
            except ValueError:
                ack_id = None
            body = body[i:]

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self.logger.error("事件包解析失败: %r", body[:200])
            return
        if not isinstance(payload, list) or not payload:
            self.logger.warning("非法事件包: %r", body[:200])
            return

        event = payload[0]
        data = payload[1] if len(payload) > 1 else None

        self.logger.debug("收到事件 %s%s", event, f"(ack={ack_id})" if ack_id else "")
        for handler in self._event_handlers:
            try:
                result = handler(event, data)
                if asyncio.iscoroutine(result):
                    if self._loop is not None:
                        self._loop.create_task(result)
            except Exception:
                self.logger.exception("事件处理器执行失败: %s", event)

        if ack_id is not None:
            self.send_ack(ack_id)

    async def _heartbeat_loop(self) -> None:
        interval = max(self.ping_interval, 5.0)
        while self._running.is_set():
            await asyncio.sleep(interval)
            if not self._running.is_set():
                break
            idle = time.monotonic() - self._last_ping_at
            if idle > self.ping_interval + self.ping_timeout:
                self.logger.warning("心跳超时(%.1fs)，主动 ping", idle)
                self.send_ping()

    def close(self, notify: bool = True) -> None:
        self._manual_close = True
        self._running.clear()
        if self._stop_event is not None:
            self._stop_event.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        if notify and self._loop is not None:
            self._loop.call_soon_threadsafe(self._notify_disconnect)

    def update_last_time(self, t: float) -> None:
        if t:
            self.last_time = max(self.last_time, t)