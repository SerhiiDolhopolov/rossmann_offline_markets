import logging
import asyncio
import aiohttp
from contextlib import asynccontextmanager

from aiochclient import ChClient

from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_HTTP_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_SHOP_REPORTS_DB,
)

logger = logging.getLogger(__name__)

CLICKHOUSE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}" 


@asynccontextmanager
async def get_client(max_retries: int = 5, retry_delay: float = 5.0):
    session = aiohttp.ClientSession()
    client = ChClient(
        session,
        url=CLICKHOUSE_URL,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_SHOP_REPORTS_DB,
    )
    logger.info("Connecting to ClickHouse at %s...", CLICKHOUSE_URL)
    for attempt in range(1, max_retries + 1):
        try:
            await client.execute("SELECT 1")
            logger.info("ClickHouse connection established")
            yield client
            break
        except Exception as e:
            logger.error(
                "Failed to connect to ClickHouse. %s\nAttempt %s", 
                e,
                attempt,
                exc_info=True,
            )
            if attempt == max_retries:
                await session.close()
                raise
            await asyncio.sleep(retry_delay)
    await session.close()