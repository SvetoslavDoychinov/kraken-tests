import json
from typing import Any, List

import pytest

from tests.helpers import recv_until, create_generic_subscribe_msg, assert_number, RFC3339_UTC_RE, \
    assert_generic_subscribe_unsubscribe_response


def assert_ticker_snapshot_message(
    msg: dict[str, Any],
    symbol: str,
) -> None:
    """Assert that a Kraken ticker snapshot has valid schema and top-of-book data."""
    assert isinstance(msg, dict), f"msg must be dict, got {type(msg).__name__}"
    required_top_level_keys = {
        "channel",
        "type",
        "data",
    }
    missing = required_top_level_keys - msg.keys()
    assert not missing, f"missing top-level keys: {sorted(missing)}"

    assert msg["channel"] == "ticker"
    assert msg["type"] == "snapshot"

    data = msg["data"]
    assert isinstance(data, list), f"data must be list, got {type(data).__name__}"
    assert data, "data must not be empty"

    ticker_items_for_symbol = [
        item for item in data
        if isinstance(item, dict) and item.get("symbol") == symbol
    ]

    assert ticker_items_for_symbol, f"no ticker snapshot found for symbol {symbol!r}"

    ticker = ticker_items_for_symbol[0]

    required_ticker_keys = {
        "symbol",
        "bid",
        "bid_qty",
        "ask",
        "ask_qty",
        "last",
        "volume",
        "vwap",
        "low",
        "high",
        "change",
        "change_pct",
        "timestamp",
    }
    missing = required_ticker_keys - ticker.keys()
    assert not missing, f"missing ticker keys: {sorted(missing)}"

    assert ticker["symbol"] == symbol

    assert isinstance(ticker["timestamp"], str), (
        f"timestamp must be str, got {type(ticker['timestamp']).__name__}"
    )
    assert RFC3339_UTC_RE.match(ticker["timestamp"]), (
        f"timestamp is not RFC3339 UTC format: {ticker['timestamp']!r}"
    )

    non_negative_fields = {
        "bid",
        "bid_qty",
        "ask",
        "ask_qty",
        "last",
        "volume",
        "vwap",
        "low",
        "high",
    }

    signed_fields = {
        "change",
        "change_pct",
    }

    for field in non_negative_fields:
        assert_number(ticker[field], True)

    for field in signed_fields:
        assert_number(ticker[field], False)

    assert ticker["bid"] < ticker["ask"], (
        f"ticker is crossed: bid={ticker['bid']} ask={ticker['ask']}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("req_id", [13, None])
@pytest.mark.parametrize("symbol", [["BTC/USD"], ["ETH/USD"]])
async def test_ticker_subscribe_with_snapshot_sends_valid_snapshot(
        kraken_ws,
        req_id: int,
        symbol: List[str]
) -> None:
    """Subscribe to the ticker channel and validate the returned snapshot."""
    await kraken_ws.send(
        json.dumps(
            create_generic_subscribe_msg(
                channel="ticker",
                req_id=req_id,
                snapshot=True,
                symbol=symbol,
            )
        )
    )
    subscribe_message = await recv_until(kraken_ws, lambda msg: msg.get("method") == "subscribe")
    snapshot = await recv_until(kraken_ws, lambda msg: msg.get("type") == "snapshot")

    assert_generic_subscribe_unsubscribe_response(msg=subscribe_message, channel="ticker", symbol=symbol[0],
                                                  snapshot=True, req_id=req_id)
    assert_ticker_snapshot_message(msg=snapshot, symbol=symbol[0])
