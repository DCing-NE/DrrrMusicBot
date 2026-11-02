"""drrr-py-bot 入口程序。"""

from __future__ import annotations

import asyncio
import logging
import sys

from bot import BotClient
from core.config import load_config
from core.logger import get_logger, setup_logger

# Windows 控制台默认 GBK，stdin 读中文会乱码
try:
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

logger = get_logger("main")


def _safe_config_repr(config) -> str:
    """日志输出前隐藏 LLM API Key。"""
    text = repr(config)
    api_key = getattr(config, "llm_api_key", "")
    if api_key:
        text = text.replace(api_key, "<redacted>")
    return text


def main() -> None:
    setup_logger(level=logging.INFO)
    logger.info("程序启动")

    config = load_config()
    logger.info("配置：%s", _safe_config_repr(config))
    logger.info("加载模块：%s", ", ".join(config.mods))

    bot = BotClient(config=config)

    async def run() -> None:
        try:
            bot.registry.load(config.mods)
            await bot.start()
        finally:
            await bot.stop()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("收到退出信号，程序结束")
    except Exception:
        logger.exception("程序异常退出")
    finally:
        logger.info("程序结束")


if __name__ == "__main__":
    main()