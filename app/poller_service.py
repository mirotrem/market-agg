import asyncio
import logging
import logging.handlers
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from . import cache, config, db, poller

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_formatter)

# 10MB per file, 5 backups kept (poller.log, poller.log.1, ... poller.log.5) - old ones are
# dropped automatically once the backup count is exceeded.
_file_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "poller.log", maxBytes=10 * 1024 * 1024, backupCount=5
)
_file_handler.setFormatter(_formatter)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(_console_handler)
root_logger.addHandler(_file_handler)

logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("poller_service")

scheduler = AsyncIOScheduler()


async def main() -> None:
    await db.init_pool()
    await db.init_db()

    scheduler.add_job(poller.refresh_all_orders, "interval", seconds=config.ORDERS_POLL_SECONDS, id="orders")
    scheduler.add_job(poller.refresh_all_history, "interval", seconds=config.HISTORY_POLL_SECONDS, id="history")
    scheduler.start()
    logger.info("poller service started (orders every %ss, history every %ss)", config.ORDERS_POLL_SECONDS, config.HISTORY_POLL_SECONDS)

    try:
        await asyncio.Event().wait()  # run forever
    finally:
        scheduler.shutdown()
        await db.close_pool()
        await cache.close()


if __name__ == "__main__":
    asyncio.run(main())
