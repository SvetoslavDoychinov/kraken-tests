import asyncio
import json
import re
from decimal import Decimal
from typing import Any, List, Optional


RFC3339_UTC_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T"
    r"\d{2}:\d{2}:\d{2}"
    r"(\.\d+)?Z$"
)

async def recv_json(ws, timeout: float = 10.0) -> dict[str, Any]:
    """Receive one WebSocket message as JSON within the given timeout."""
    async with asyncio.timeout(timeout):
        raw = await ws.recv()

    return json.loads(raw, parse_float=Decimal)


async def recv_until(ws, predicate, timeout: float = 15.0) -> dict[str, Any]:
    """Receive messages until one matches the predicate or the timeout expires."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError("Timed out waiting for expected WebSocket message")
        msg = await recv_json(ws, timeout=remaining)

        if predicate(msg):
            return msg


def create_generic_subscribe_msg(
    snapshot: bool,
    channel: str,
    symbol: List[str],
    req_id: int | None = None,
) -> dict[str, Any]:
    """Build a Kraken WebSocket v2 subscribe request for one or more symbols."""
    msg: dict[str, Any] = {
        "method": "subscribe",
        "params": {
            "channel": channel,
            "symbol": symbol,
            "snapshot": snapshot,
        },
    }
    if req_id is not None:
        msg["req_id"] = req_id

    return msg


def create_generic_unsubscribe_msg(
    channel: str,
    symbol: List[str],
    req_id: int | None = None,
) -> dict[str, Any]:
    """Build a Kraken WebSocket v2 unsubscribe request for one or more symbols."""
    msg: dict[str, Any] = {
        "method": "unsubscribe",
        "params": {
            "channel": channel,
            "symbol": symbol,
        },
    }
    if req_id is not None:
        msg["req_id"] = req_id

    return msg


def assert_number(value: Any, unsigned: bool) -> None:
    """Assert that a value is numeric and optionally non-negative."""
    assert isinstance(value, int | float | Decimal), (
        f"Field must be a number, got {type(value).__name__}: {value!r}"
    )
    assert not isinstance(value, bool), "Field must be a number, got bool"
    if unsigned:
        assert value >= 0, f"Field must be non-negative, got {value!r}"


def assert_generic_subscribe_unsubscribe_response(
    msg: dict[str, Any],
    channel: str,
    symbol: str,
    snapshot: bool,
    req_id: Optional[int],
    unsubscribe: bool = False,
) -> None:
    """Assert a successful Kraken subscribe or unsubscribe response."""
    assert isinstance(msg, dict), f"msg must be dict, got {type(msg).__name__}"

    required_top_level_keys = {
        "method",
        "result",
        "success",
        "time_in",
        "time_out",
    }
    missing = required_top_level_keys - msg.keys()
    assert not missing, f"missing top-level keys: {sorted(missing)}"

    expected_method = "unsubscribe" if unsubscribe else "subscribe"
    assert msg["method"] == expected_method
    assert msg["success"] is True

    assert isinstance(msg["time_in"], str)
    assert isinstance(msg["time_out"], str)
    assert RFC3339_UTC_RE.match(msg["time_in"]), (
        f"time_in is not RFC3339 UTC format: {msg['time_in']!r}"
    )
    assert RFC3339_UTC_RE.match(msg["time_out"]), (
        f"time_out is not RFC3339 UTC format: {msg['time_out']!r}"
    )

    result = msg["result"]
    assert isinstance(result, dict), (
        f"result must be dict, got {type(result).__name__}"
    )

    required_result_keys = {
        "channel",
        "symbol",
    }
    if not unsubscribe:
        required_result_keys.add("snapshot")

    missing = required_result_keys - result.keys()
    assert not missing, f"missing result keys: {sorted(missing)}"

    assert result["channel"] == channel
    assert result["symbol"] == symbol
    if not unsubscribe:
        assert result["snapshot"] == snapshot

    if req_id is not None:
        assert msg.get("req_id") == req_id
    else:
        assert "req_id" not in msg
