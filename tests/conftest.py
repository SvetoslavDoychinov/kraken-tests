import asyncio

import pytest
import pytest_asyncio
import websockets
from websockets import InvalidStatus


@pytest.fixture
def ws_url_v2():
    yield "wss://ws.kraken.com/v2"


async def connect_with_retry(url: str, attempts: int = 5):
    last_error = None

    for attempt in range(attempts):
        try:
            return await websockets.connect(url, ping_interval=20)
        except InvalidStatus as exc:
            last_error = exc
            if exc.response.status_code != 429:
                raise

            await asyncio.sleep(2**attempt)

    raise last_error


@pytest_asyncio.fixture
async def kraken_ws(ws_url_v2):
    ws = await connect_with_retry(ws_url_v2)

    try:
        yield ws
    finally:
        await ws.close()