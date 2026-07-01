import asyncio
import logging

import pytest
import pytest_asyncio
import websockets
from websockets import InvalidStatus


logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 502, 503, 504}

@pytest.fixture
def ws_url_v2():
    """Return the public Kraken WebSocket v2 endpoint."""
    yield "wss://ws.kraken.com/v2"


async def connect_with_retry(url: str, attempts: int = 5):
    """Open a WebSocket connection, retrying with backoff on Kraken HTTP 429 rate limits."""
    logger.debug(f"Connecting to {url}")
    last_error = None
    for attempt in range(attempts):
        try:
            return await websockets.connect(url, ping_interval=20)
        except InvalidStatus as exc:
            last_error = exc
            if exc.response.status_code not in RETRYABLE_STATUS_CODES:
                raise

            await asyncio.sleep(2**attempt)

    raise last_error


@pytest_asyncio.fixture
async def kraken_ws(ws_url_v2):
    """Yield an open Kraken WebSocket connection and close it after the test."""
    ws = await connect_with_retry(ws_url_v2)
    logger.debug("Successfully connected to Kraken WebSocket v2 endpoint")
    try:
        yield ws
    finally:
        logger.debug("Closing Kraken WebSocket connection")
        await ws.close()
