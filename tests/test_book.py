import asyncio
import json
import logging
import zlib
from typing import List, Any, Union

import pytest

from tests.helpers import recv_until, assert_generic_subscribe_unsubscribe_response, assert_number, RFC3339_UTC_RE


logger = logging.getLogger(__name__)

def create_book_subscribe_msg(
    snapshot: bool,
    channel: str,
    symbol: List[str],
    depth: int,
    req_id: int | None = None,
) -> dict[str, Any]:
    """Build a Kraken book subscribe request with a chosen depth."""
    msg: dict[str, Any] = {
        "method": "subscribe",
        "params": {
            "channel": channel,
            "symbol": symbol,
            "snapshot": snapshot,
            "depth": depth
        },
    }
    if req_id is not None:
        msg["req_id"] = req_id

    return msg


def assert_book_data(level: dict[str, Any], side: str) -> None:
    """Assert that one book bid or ask level has valid price and quantity fields."""
    logger.info(f"Asserting book {side} level.")
    assert isinstance(level, dict), (
        f"{side} level must be dict, got {type(level).__name__}"
    )

    required_level_keys = {"price", "qty"}
    missing = required_level_keys - level.keys()
    assert not missing, f"missing {side} level keys: {sorted(missing)}"

    assert_number(level["price"], True)
    assert_number(level["qty"], True)


def assert_book_snapshot_message(
    msg: dict[str, Any],
    *,
    symbols: List[str],
    depth: int,
) -> None:
    """Assert that a Kraken book snapshot has valid schema, depth, ordering, and spread."""
    logger.info("Asserting book snapshot message.")
    assert isinstance(msg, dict), f"msg must be dict, got {type(msg).__name__}"
    required_top_level_keys = {
        "channel",
        "type",
        "data",
    }
    missing = required_top_level_keys - msg.keys()
    assert not missing, f"missing top-level keys: {sorted(missing)}"

    assert msg["channel"] == "book"
    assert msg["type"] == "snapshot"

    data = msg["data"]
    assert isinstance(data, list), f"data must be list, got {type(data).__name__}"
    assert data, "data must not be empty"

    for book_item in data:
        assert isinstance(book_item, dict), (
            f"book item must be dict, got {type(book_item).__name__}"
        )

        required_book_keys = {
            "symbol",
            "bids",
            "asks",
            "checksum",
            "timestamp",
        }
        missing = required_book_keys - book_item.keys()
        assert not missing, f"missing book item keys: {sorted(missing)}"

        symbol = book_item["symbol"]
        assert symbol in symbols, (
            f"unexpected symbol in book snapshot: {symbol!r}, "
            f"expected one of {sorted(symbols)}"
        )

        assert isinstance(book_item["timestamp"], str), (
            f"timestamp must be str, got {type(book_item['timestamp']).__name__}"
        )
        assert RFC3339_UTC_RE.match(book_item["timestamp"]), (
            f"timestamp is not RFC3339 UTC format: {book_item['timestamp']!r}"
        )

        assert isinstance(book_item["checksum"], int), (
            f"checksum must be int, got {type(book_item['checksum']).__name__}"
        )
        assert book_item["checksum"] >= 0, "checksum must be non-negative"

        bids = book_item["bids"]
        asks = book_item["asks"]

        assert isinstance(bids, list), f"bids must be list, got {type(bids).__name__}"
        assert isinstance(asks, list), f"asks must be list, got {type(asks).__name__}"

        assert len(bids) == depth, f"expected {depth} bids, got {len(bids)}"
        assert len(asks) == depth, f"expected {depth} asks, got {len(asks)}"

        for bid in bids:
            assert_book_data(bid, "bid")
        for ask in asks:
            assert_book_data(ask, "ask")

        bid_prices = [bid["price"] for bid in bids]
        ask_prices = [ask["price"] for ask in asks]

        assert bid_prices == sorted(bid_prices, reverse=True), (
            f"bids must be sorted descending by price: {bid_prices}"
        )
        assert ask_prices == sorted(ask_prices), (
            f"asks must be sorted ascending by price: {ask_prices}"
        )

        best_bid = bid_prices[0]
        best_ask = ask_prices[0]

        assert best_bid < best_ask, (
            f"book is crossed for {symbol}: best_bid={best_bid} best_ask={best_ask}"
        )


def _format_checksum_number(value: Union[int, float, str]) -> str:
    """Format a price or quantity value according to Kraken checksum rules."""
    formatted = str(value).replace(".", "").lstrip("0")
    return formatted or "0"


def calculate_book_checksum(book_item: dict[str, Any]) -> int:
    """Calculate the CRC32 checksum for the top 10 ask and bid levels."""
    logger.info("Calculating book checksum")
    asks = sorted(book_item["asks"], key=lambda level: level["price"])[:10]
    bids = sorted(book_item["bids"], key=lambda level: level["price"], reverse=True)[:10]

    checksum_input = ""

    for ask in asks:
        checksum_input += _format_checksum_number(ask["price"])
        checksum_input += _format_checksum_number(ask["qty"])

    for bid in bids:
        checksum_input += _format_checksum_number(bid["price"])
        checksum_input += _format_checksum_number(bid["qty"])

    return zlib.crc32(checksum_input.encode("utf-8")) & 0xFFFFFFFF


@pytest.mark.asyncio
@pytest.mark.parametrize("depth", [10, 25])
async def test_book_subscribe_with_snapshot_sends_valid_snapshot(kraken_ws, depth: int) -> None:
    """Subscribe to the book channel and validate snapshots for each requested symbol."""
    symbol = ["BTC/USD", "ETH/USD"]
    req_id = 3

    logger.info("Sending book subscribe message.")
    await kraken_ws.send(
        json.dumps(
            create_book_subscribe_msg(
                channel="book",
                req_id=req_id,
                snapshot=True,
                symbol=symbol,
                depth=depth,
            )
        )
    )
    logger.info("Validating that book subscribe was successful.")
    for value in symbol:
        subscribe_message = await recv_until(kraken_ws, lambda msg: msg.get("method") == "subscribe", timeout=5.0)
        assert_generic_subscribe_unsubscribe_response(msg=subscribe_message, channel="book", symbol=value,
                                                      snapshot=True, req_id=req_id)
        assert subscribe_message["result"]["depth"] == depth

    expected_snapshot_symbols = set(symbol)
    seen_snapshot_symbols: set[str] = set()

    deadline = asyncio.get_running_loop().time() + 20.0

    logger.info("Validating that valid snapshots were received.")
    while seen_snapshot_symbols != expected_snapshot_symbols:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError(
                f"Timed out waiting for book snapshots. "
                f"expected={sorted(expected_snapshot_symbols)}, "
                f"seen={sorted(seen_snapshot_symbols)}"
            )
        snapshot = await recv_until(
            kraken_ws,
            lambda msg: msg.get("channel") == "book" and msg.get("type") == "snapshot",
            timeout=min(10.0, remaining),
        )

        assert_book_snapshot_message(
            snapshot,
            symbols=symbol,
            depth=depth,
        )

        for item in snapshot["data"]:
            seen_snapshot_symbols.add(item["symbol"])


@pytest.mark.asyncio
async def test_book_snapshot_checksum_is_valid(kraken_ws) -> None:
    """Validate that the book snapshot checksum matches the calculated CRC32 value."""
    symbol = ["BTC/USD"]
    depth = 10

    logger.info("Sending book subscribe message.")
    await kraken_ws.send(
        json.dumps(
            create_book_subscribe_msg(
                channel="book",
                req_id=10,
                snapshot=True,
                symbol=symbol,
                depth=depth,
            )
        )
    )
    logger.info("Validating that book subscribe was successful.")
    await recv_until(kraken_ws, lambda msg: msg.get("method") == "subscribe", timeout=5.0)
    snapshot = await recv_until(
        kraken_ws,
        lambda msg: msg.get("channel") == "book"
        and msg.get("type") == "snapshot",
        timeout=10.0,
    )

    logger.info("Validating that valid snapshots are were received.")
    assert_book_snapshot_message(snapshot, symbols=symbol, depth=depth)
    book_item = snapshot["data"][0]
    logger.info("Validating that the checksum matches the calculated CRC32 value.")
    expected_checksum = calculate_book_checksum(book_item)
    assert book_item["checksum"] == expected_checksum


@pytest.mark.asyncio
async def test_book_subscribe_invalid_depth_error(kraken_ws) -> None:
    """Verify that subscribing to the book channel with an unsupported depth fails."""
    symbol = ["BTC/USD", "ETH/USD"]
    req_id = 3
    depth = 33

    logger.info("Sending an invalid subscribe message with bad depth.")
    await kraken_ws.send(
        json.dumps(
            create_book_subscribe_msg(
                channel="book",
                req_id=req_id,
                snapshot=True,
                symbol=symbol,
                depth=depth,
            )
        )
    )
    logger.info("Validating that book subscribe wasn't successful.")
    error_message = await recv_until(kraken_ws, lambda msg: "error" in msg, timeout=5.0)

    assert error_message["error"] == "Subscription depth not supported"
    assert error_message["method"] == "subscribe"
    assert error_message["success"] is False
    assert error_message["req_id"] == req_id

    assert isinstance(error_message["time_in"], str)
    assert isinstance(error_message["time_out"], str)

    assert RFC3339_UTC_RE.match(error_message["time_in"]), (
        f"time_in is not RFC3339 UTC format: {error_message['time_in']!r}"
    )
    assert RFC3339_UTC_RE.match(error_message["time_out"]), (
        f"time_out is not RFC3339 UTC format: {error_message['time_out']!r}"
    )
