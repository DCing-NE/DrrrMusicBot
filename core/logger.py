"""统一日志模块。

提供格式化日志初始化，供入口程序使用。
日志格式：`2026-08-09 12:00:00 | INFO | message`
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Final

LOG_FORMAT: Final[str] = "%(asctime)s | %(levelname)s | %(message)s"
DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
ROOT_LOGGER_NAME: Final[str] = "drrr"
LOG_DIR: Final[str] = "logs"


class _SafeFormatter(logging.Formatter):
    """对日志消息做安全替换，避免 GBK/GB2312 终端无法编码的字符
    （如零宽空格 \\u200b）导致日志崩溃。"""

    _REPLACE = {
        "\u200b": "<ZWSP>",
    }

    def formatMessage(self, record: logging.LogRecord) -> str:
        msg = super().formatMessage(record)
        for ch, rep in self._REPLACE.items():
            msg = msg.replace(ch, rep)
        return msg


def setup_logger(
    level: int = logging.INFO,
    *,
    name: str = ROOT_LOGGER_NAME,
    stream=None,
) -> logging.Logger:
    """初始化根 logger（幂等）。"""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        return logger

    out = stream or sys.stdout
    try:
        out.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass

    handler = logging.StreamHandler(out)
    handler.setFormatter(_SafeFormatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_logger(name: str = ROOT_LOGGER_NAME) -> logging.Logger:
    """获取子 logger，自动继承根 logger 的 handler 与格式。"""
    if name == ROOT_LOGGER_NAME:
        return logging.getLogger(ROOT_LOGGER_NAME)
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")


class MessageLogger:
    """聊天消息 CSV 落盘。

    按日期写入 ``logs/YYYY年MM月DD日.csv``，每行格式：
    ``HH:MM:SS,用户名,tripcode,消息``。
    消息中的换行转义为 ``\\\\n``，逗号替换为中文逗号，保证单行合法。
    """

    def __init__(self, log_dir: str = LOG_DIR) -> None:
        self.log_dir = log_dir
        self._file = None
        self._current_date = ""

    def _ensure_file(self) -> None:
        today = datetime.now().strftime("%Y年%m月%d日")
        if self._file is not None and today == self._current_date:
            return
        if self._file is not None:
            try:
                self._file.close()
            except OSError:
                pass
        os.makedirs(self.log_dir, exist_ok=True)
        path = os.path.join(self.log_dir, f"{today}.csv")
        self._file = open(path, "a", encoding="utf-8-sig")
        self._current_date = today

    def log(self, username: str, tripcode: str, message: str) -> None:
        """写入一条消息日志。"""
        try:
            self._ensure_file()
        except OSError:
            return
        now = datetime.now().strftime("%H:%M:%S ")
        content = message.replace("\n", "\\n").replace(",", "，")
        try:
            self._file.write(f"{now},{username},{tripcode},{content}\n")
            self._file.flush()
        except OSError:
            pass

    def close(self) -> None:
        if self._file is not None:
            try:
                self._file.close()
            except OSError:
                pass
            self._file = None