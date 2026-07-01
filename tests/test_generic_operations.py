import json
import logging
from typing import Any, List

import pytest

from tests.helpers import create_generic_subscribe_msg, recv_until, assert_generic_subscribe_unsubscribe_response, \
    create_generic_unsubscribe_msg, RFC3339_UTC_RE


logger = logging.getLogger(__name__)

def assert_heartbeat_message(msg: dict[str, Any]) -> None:
    """Assert that a message is a valid Kraken heartbeat."""
    logger.info("Validating that valid heartbeat message was received.")
    assert isinstance(msg, dict), f"msg must be dict, got {type(msg).__name__}"

    required_top_level_keys = {"channel"}
    missing = required_top_level_keys - msg.keys()
    assert not missing, f"missing top-level keys: {sorted(missing)}"

    assert msg["channel"] == "heartbeat"


def assert_status_message(msg: dict[str, Any]) -> None:
    """Assert that a message is a valid Kraken status update."""
    logger.info("Validating that valid status message was received.")
    assert isinstance(msg, dict), f"msg must be dict, got {type(msg).__name__}"

    required_top_level_keys = {
        "channel",
        "type",
        "data",
    }
    missing = required_top_level_keys - msg.keys()
    assert not missing, f"missing top-level keys: {sorted(missing)}"

    assert msg["channel"] == "status"
    assert msg["type"] == "update"

    data = msg["data"]
    assert isinstance(data, list), f"data must be list, got {type(data).__name__}"
    assert len(data) == 1, f"data must contain exactly one item, got {len(data)}"

    status = data[0]
    assert isinstance(status, dict), (
        f"status item must be dict, got {type(status).__name__}"
    )

    required_status_keys = {
        "version",
        "system",
        "api_version",
        "connection_id",
    }
    missing = required_status_keys - status.keys()
    assert not missing, f"missing status keys: {sorted(missing)}"

    assert isinstance(status["version"], str)
    assert status["version"], "version must not be empty"

    assert isinstance(status["system"], str)
    assert status["system"] in {
        "online",
        "maintenance",
        "cancel_only",
        "post_only",
    }

    assert status["api_version"] == "v2"

    assert isinstance(status["connection_id"], int)


@pytest.mark.asyncio
async def test_status_message_received_after_connection(kraken_ws) -> None:
    """Verify that Kraken sends a status message after the WebSocket connects."""
    status_message = await recv_until(
        kraken_ws,
        lambda msg: msg.get("channel") == "status",
        timeout=5.0,
    )
    assert_status_message(msg=status_message)


@pytest.mark.asyncio
@pytest.mark.parametrize("req_id", [33, None])
@pytest.mark.parametrize("symbol", [["BTC/USD", "ETH/USD"], ["ETH/USD"]])
@pytest.mark.parametrize("channel", ["ticker", "trade"])
async def test_generic_subscribe_unsubscribe(
        kraken_ws,
        req_id: int,
        symbol: List[str],
        channel: str
) -> None:
    """Verify subscribe and unsubscribe responses for generic public channels."""
    logger.info(f"Subscribing to {channel} for {symbol}")
    await kraken_ws.send(
        json.dumps(
            create_generic_subscribe_msg(
                channel=channel,
                req_id=req_id,
                snapshot=False,
                symbol=symbol,
            )
        )
    )

    for value in symbol:
        subscribe_message = await recv_until(kraken_ws, lambda msg: msg.get("method") == "subscribe", timeout=5.0)
        assert_generic_subscribe_unsubscribe_response(msg=subscribe_message, channel=channel, symbol=value,
                                                      snapshot=False, req_id=req_id)

    logger.info(f"Unsubscribing to {channel} for {symbol}")
    await kraken_ws.send(
        json.dumps(
            create_generic_unsubscribe_msg(
                channel=channel,
                req_id=req_id,
                symbol=symbol,
            )
        )
    )

    for value in symbol:
        unsubscribe_message = await recv_until(kraken_ws, lambda msg: msg.get("method") == "unsubscribe", timeout=5.0)
        assert_generic_subscribe_unsubscribe_response(msg=unsubscribe_message, channel=channel, symbol=value,
                                                      snapshot=False, req_id=req_id, unsubscribe=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["ticker", "trade"])
async def test_heartbeat_sent_after_subscribe(kraken_ws, channel: str) -> None:
    """Verify that a heartbeat message is received after subscribing to a channel."""
    logger.info(f"Subscribing to {channel}")
    await kraken_ws.send(
        json.dumps(
            create_generic_subscribe_msg(
                channel=channel,
                req_id=None,
                snapshot=False,
                symbol=["BTC/USD"],
            )
        )
    )
    subscribe_message = await recv_until(kraken_ws, lambda msg: msg.get("method") == "subscribe", timeout=5.0)
    assert_generic_subscribe_unsubscribe_response(msg=subscribe_message, channel=channel, symbol="BTC/USD",
                                                  snapshot=False, req_id=None)

    heartbeat_message = await recv_until(kraken_ws, lambda msg: msg.get("channel") == "heartbeat", timeout=15.0)

    assert_heartbeat_message(msg=heartbeat_message)


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["ticker", "trade"])
async def test_unsubscribe_error(
        kraken_ws,
        channel: str
) -> None:
    """Verify that unsubscribing from a missing subscription returns an error."""
    symbol = "BTC/USD"
    logger.info(f"Unsubscribing from channel {channel} without having subscribed before")
    await kraken_ws.send(
        json.dumps(
            create_generic_unsubscribe_msg(
                channel=channel,
                req_id=None,
                symbol=[symbol],
            )
        )
    )
    logger.info(f"Validating that an error thrown.")
    error_message = await recv_until(kraken_ws, lambda msg: "error" in msg, timeout=5.0)

    assert error_message["error"] == "Subscription Not Found"
    assert error_message["method"] == "subscribe"
    assert error_message["success"] is False
    assert error_message["symbol"] == symbol

    assert isinstance(error_message["time_in"], str)
    assert isinstance(error_message["time_out"], str)

    assert RFC3339_UTC_RE.match(error_message["time_in"]), (
        f"time_in is not RFC3339 UTC format: {error_message['time_in']!r}"
    )
    assert RFC3339_UTC_RE.match(error_message["time_out"]), (
        f"time_out is not RFC3339 UTC format: {error_message['time_out']!r}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["ticker", "trade"])
async def test_subscribe_without_snapshot_sends_no_snapshot(
        kraken_ws,
        channel: str
) -> None:
    """Verify that snapshot=False prevents initial snapshots for ticker and trade."""
    symbol = ["BTC/USD"]
    req_id = 333
    logger.info(f"Subscribing to {channel} without snapshot")
    await kraken_ws.send(
        json.dumps(
            create_generic_subscribe_msg(
                channel=channel,
                req_id=req_id,
                snapshot=False,
                symbol=symbol,
            )
        )
    )
    subscribe_message = await recv_until(kraken_ws, lambda msg: msg.get("method") == "subscribe", timeout=5.0)

    assert_generic_subscribe_unsubscribe_response(msg=subscribe_message, channel=channel, symbol=symbol[0],
                                                  snapshot=False, req_id=req_id)

    logger.info("Assert that no snapshot message is received.")
    with pytest.raises(TimeoutError):
        await recv_until(
            ws=kraken_ws,
            predicate=lambda msg: msg.get("type") == "snapshot" and msg.get("channel") == channel,
            timeout=10.0
        )
    # Not going to assert whether I receive updates or not because real market data, therefore not sure when I can get one
