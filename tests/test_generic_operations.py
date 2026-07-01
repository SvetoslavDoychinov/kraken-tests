import json
from typing import Any, List

import pytest

from tests.helpers import create_generic_subscribe_msg, recv_until, assert_generic_subscribe_unsubscribe_response, \
    create_generic_unsubscribe_msg, RFC3339_UTC_RE


def assert_heartbeat_message(msg: dict[str, Any]) -> None:
    assert isinstance(msg, dict), f"msg must be dict, got {type(msg).__name__}"

    required_top_level_keys = {"channel"}
    missing = required_top_level_keys - msg.keys()
    assert not missing, f"missing top-level keys: {sorted(missing)}"

    assert msg["channel"] == "heartbeat"


def assert_status_message(msg: dict[str, Any]) -> None:
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
    status_message = await recv_until(
        kraken_ws,
        lambda msg: msg.get("channel") == "status",
        timeout=5.0,
    )
    assert_status_message(msg=status_message)


@pytest.mark.asyncio
@pytest.mark.parametrize("req_id", [33, 77, None])
@pytest.mark.parametrize("symbol", [["BTC/USD", "ETH/USD"], ["ETH/USD"]])
@pytest.mark.parametrize("channel", ["ticker", "trade"])
async def test_generic_subscribe_unsubscribe(
        kraken_ws,
        req_id: int,
        symbol: List[str],
        channel: str
) -> None:
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
async def test_heartbeat_sent_after_subscribe(
        kraken_ws,
        channel: str
) -> None:
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
    symbol = "BTC/USD"

    await kraken_ws.send(
        json.dumps(
            create_generic_unsubscribe_msg(
                channel=channel,
                req_id=None,
                symbol=[symbol],
            )
        )
    )
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
