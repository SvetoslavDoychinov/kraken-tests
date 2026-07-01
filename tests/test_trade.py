import json
from typing import Any

import pytest

from tests.helpers import create_generic_subscribe_msg, recv_until, assert_generic_subscribe_unsubscribe_response, \
    assert_number, RFC3339_UTC_RE


def assert_trade_snapshot_message(msg: dict[str, Any], symbol: str) -> None:
    """Assert that a Kraken trade snapshot contains 50 valid trades."""
    assert isinstance(msg, dict), f"msg must be dict, got {type(msg).__name__}"

    required_top_level_keys = {
        "channel",
        "type",
        "data",
    }
    missing = required_top_level_keys - msg.keys()
    assert not missing, f"missing top-level keys: {sorted(missing)}"

    assert msg["channel"] == "trade"
    assert msg["type"] == "snapshot"

    data = msg["data"]
    assert isinstance(data, list), f"data must be list, got {type(data).__name__}"
    assert len(data) == 50, f"expected 50 trades in snapshot, got {len(data)}"

    for trade in data:
        assert isinstance(trade, dict), (
            f"trade item must be dict, got {type(trade).__name__}"
        )

        required_trade_keys = {
            "symbol",
            "side",
            "price",
            "qty",
            "ord_type",
            "trade_id",
            "timestamp",
        }
        missing = required_trade_keys - trade.keys()
        assert not missing, f"missing trade keys: {sorted(missing)}"

        assert trade["symbol"] == symbol

        assert trade["side"] in {"buy", "sell"}
        assert trade["ord_type"] in {"market", "limit"}
        assert_number(trade["trade_id"], True)
        assert isinstance(trade["timestamp"], str), (
            f"timestamp must be str, got {type(trade['timestamp']).__name__}"
        )
        assert RFC3339_UTC_RE.match(trade["timestamp"]), (
            f"timestamp is not RFC3339 UTC format: {trade['timestamp']!r}"
        )
        assert_number(trade["price"], True)
        assert_number(trade["qty"], True)


@pytest.mark.asyncio
async def test_trade_subscribe_with_snapshot_sends_valid_snapshot(kraken_ws) -> None:
    """Subscribe to the trade channel and validate the returned snapshot."""
    symbol = ["BTC/USD"]
    req_id = 13
    await kraken_ws.send(
        json.dumps(
            create_generic_subscribe_msg(
                channel="trade",
                req_id=req_id,
                snapshot=True,
                symbol=symbol,
            )
        )
    )
    subscribe_message = await recv_until(kraken_ws, lambda msg: msg.get("method") == "subscribe")
    snapshot = await recv_until(kraken_ws, lambda msg: msg.get("type") == "snapshot")

    assert_generic_subscribe_unsubscribe_response(msg=subscribe_message, channel="trade", symbol=symbol[0],
                                                  snapshot=True, req_id=req_id)
    assert_trade_snapshot_message(snapshot, symbol[0])