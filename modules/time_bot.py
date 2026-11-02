# -*- coding: utf-8 -*-
"""报时模块。
每 30 分钟用 /me 播报一次北京时间。
格式：现在是北京时间2026/08/18 14:30
"""

from __future__ import annotations
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from modules.base import Module


class TimeBot(Module):
    BOUNDARY_MINUTES = 30   # 报时间隔（分钟）
    BOUNDARY_WINDOW = 10    # 边界后多少秒内算作报时窗口

    def __init__(self, bot):
        super().__init__(bot)
        self._stop = asyncio.Event()
        self._last_key: str | None = None
        self._task = asyncio.create_task(self._loop())

    # ---------- 时间工具 ----------
    @staticmethod
    def _now() -> datetime:
        return datetime.now(ZoneInfo("Asia/Shanghai"))

    @classmethod
    def _seconds_to_boundary(cls, now: datetime) -> float:
        """距离下一个报时边界的秒数。"""
        interval = cls.BOUNDARY_MINUTES * 60
        seconds_into = (now.minute % cls.BOUNDARY_MINUTES) * 60 \
            + now.second + now.microsecond / 1_000_000
        return max(interval - seconds_into, 0.0)

    @classmethod
    def _in_window(cls, now: datetime) -> bool:
        return (now.minute % cls.BOUNDARY_MINUTES == 0
                and now.second <= cls.BOUNDARY_WINDOW)

    # ---------- 主循环 ----------
    async def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                if not await self._wait_connected():
                    return
                if not await self._wait_boundary():
                    return
                self._report()
                await asyncio.sleep(self.BOUNDARY_WINDOW + 1)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.bot.logger.exception("[TimeBot] 报时循环异常退出")

    async def _wait_connected(self) -> bool:
        """等房间连接；返回 False 表示已停止。"""
        while not getattr(self.bot, "room_connected", False):
            if self._stop.is_set():
                return False
            await asyncio.sleep(5)
        return True

    async def _wait_boundary(self) -> bool:
        """循环等到边界窗口；返回 False 表示已停止。"""
        while not self._stop.is_set():
            now = self._now()
            if self._in_window(now):
                return True
            wait = max(self._seconds_to_boundary(now), 0.1)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except asyncio.TimeoutError:
                continue
            else:
                return False
        return False

    def _report(self) -> None:
        now = self._now()
        key = now.strftime("%Y%m%d%H%M")
        if key == self._last_key:
            return
        try:
            self.bot.me(now.strftime("现在是北京时间%Y/%m/%d %H:%M"))
            self._last_key = key
        except Exception:
            self.bot.logger.exception("[TimeBot] 报时发送失败")

    # ---------- 生命周期 ----------
    def unload(self) -> None:
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
        super().unload()