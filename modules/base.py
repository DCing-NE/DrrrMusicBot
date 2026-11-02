r"""装饰器模块基类。

模块作者只需继承 ``Module`` 并用装饰器声明处理器：

.. code-block:: python

    from modules.base import Module, command, on_event
    from models import Message, MessageType

    class RoomAdmin(Module):
        @command(r"^/踢人\s+\S+", admin=True)
        def kick(self, msg: Message):
            self.bot.me(f"正在踢出 {msg.user.name}")

        @on_event(MessageType.JOIN)
        def on_join(self, msg: Message):
            self.bot.send(f"欢迎 @{msg.user.name} 进入房间")

模块卸载：覆盖 ``unload()`` 释放资源（定时器/任务等）。
"""

from __future__ import annotations

import re
import threading
from abc import ABC
from collections.abc import Callable
from typing import Any, TypeVar

from models import Message, MessageType

T = TypeVar("T", bound=Callable[..., Any])


def command(pattern: str, *, admin: bool = False) -> Callable[[T], T]:
    """注册消息命令处理器。"""

    def deco(fn: T) -> T:
        fn._drrr_cmd = re.compile(pattern)  # type: ignore[attr-defined]
        fn._drrr_is_handler = True  # type: ignore[attr-defined]
        if admin:
            fn._drrr_admin = True  # type: ignore[attr-defined]
        return fn

    return deco


def on_event(event: MessageType) -> Callable[[T], T]:
    """注册事件处理器（join/leave/new_host/music 等）。"""

    def deco(fn: T) -> T:
        fn._drrr_event = event  # type: ignore[attr-defined]
        fn._drrr_is_handler = True  # type: ignore[attr-defined]
        return fn

    return deco


def admin(fn: T) -> T:
    """标记为需要管理员权限的处理器。"""
    fn._drrr_admin = True  # type: ignore[attr-defined]
    return fn


def is_handler(fn: Any) -> bool:
    """判断函数是否通过装饰器注册为处理器。"""
    return bool(getattr(fn, "_drrr_is_handler", False))


class Module(ABC):
    """模块基类。子类通过装饰器声明处理器，由 ModuleRegistry 收集并分发。"""

    def __init__(self, bot: Any) -> None:
        self.bot = bot
        self.on = True
        self._timers: list[threading.Timer] = []

    def unload(self) -> None:
        """卸载时调用，子类可覆盖以释放资源。"""
        self.cancel_all_timers()

    def schedule(self, delay: float, fn: Callable[[], None]) -> threading.Timer:
        """启动一个一次性定时器（daemon），unload 时自动取消。"""
        timer = threading.Timer(delay, fn)
        timer.daemon = True
        timer.start()
        self._timers.append(timer)
        return timer

    def cancel_all_timers(self) -> None:
        for timer in self._timers:
            timer.cancel()
        self._timers.clear()

    def is_admin(self, msg: Message) -> bool:
        """判断发言用户是否为管理员（tripcode 匹配 bot 的 admin_tc 列表）。"""
        if msg.user is None or not msg.user.tc:
            return False
        return msg.user.tc in self.bot.admins

    def require_admin(self, msg: Message) -> bool:
        """管理员校验辅助：无权限时提示并返回 False。"""
        if self.is_admin(msg):
            return True
        name = msg.user.name if msg.user else "未知"
        self.bot.me(f"@{name} 没有权限")
        return False