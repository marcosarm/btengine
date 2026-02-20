from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from btengine.data.cryptohftdata import iter_liquidations
from btengine.data.cryptohftdata.liquidations import iter_liquidations_advanced


def test_iter_liquidations_sorts_and_casts(tmp_path: Path) -> None:
    p = tmp_path / "liquidations.parquet"

    # Two rows, deliberately out of order by event_time.
    rows = [
        (2_000_000_000_000_000_000, 2_000, "BTCUSDT", "BUY", "LIMIT", "IOC", "0.2", "100.0", "101.0", "FILLED", "0.2", "0.2", 2_000),
        (1_000_000_000_000_000_000, 1_000, "BTCUSDT", "SELL", "LIMIT", "IOC", "0.1", "99.0", "99.5", "FILLED", "0.1", "0.1", 1_000),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "side": pa.array([r[3] for r in rows], type=pa.string()),
            "order_type": pa.array([r[4] for r in rows], type=pa.string()),
            "time_in_force": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "price": pa.array([r[7] for r in rows], type=pa.string()),
            "average_price": pa.array([r[8] for r in rows], type=pa.string()),
            "order_status": pa.array([r[9] for r in rows], type=pa.string()),
            "last_filled_quantity": pa.array([r[10] for r in rows], type=pa.string()),
            "filled_quantity": pa.array([r[11] for r in rows], type=pa.string()),
            "trade_time": pa.array([r[12] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_liquidations(p))
    assert [e.event_time_ms for e in out] == [1_000, 2_000]
    assert out[0].quantity == 0.1
    assert out[1].quantity == 0.2


def test_iter_liquidations_detects_disorder_in_later_row_group(tmp_path: Path) -> None:
    p = tmp_path / "liquidations_rg.parquet"

    rows = [
        (1_000_000_000_000_000_000, 1_000, "BTCUSDT", "SELL", "LIMIT", "IOC", "0.1", "99.0", "99.5", "FILLED", "0.1", "0.1", 1_000),
        (2_000_000_000_000_000_000, 2_000, "BTCUSDT", "BUY", "LIMIT", "IOC", "0.2", "100.0", "101.0", "FILLED", "0.2", "0.2", 2_000),
        (3_000_000_000_000_000_000, 1_500, "BTCUSDT", "BUY", "LIMIT", "IOC", "0.15", "99.5", "100.5", "FILLED", "0.15", "0.15", 1_500),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "side": pa.array([r[3] for r in rows], type=pa.string()),
            "order_type": pa.array([r[4] for r in rows], type=pa.string()),
            "time_in_force": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "price": pa.array([r[7] for r in rows], type=pa.string()),
            "average_price": pa.array([r[8] for r in rows], type=pa.string()),
            "order_status": pa.array([r[9] for r in rows], type=pa.string()),
            "last_filled_quantity": pa.array([r[10] for r in rows], type=pa.string()),
            "filled_quantity": pa.array([r[11] for r in rows], type=pa.string()),
            "trade_time": pa.array([r[12] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p, row_group_size=1)

    out = list(iter_liquidations(p))
    assert [e.event_time_ms for e in out] == [1_000, 1_500, 2_000]


def test_iter_liquidations_sorts_equal_event_time_by_trade_time(tmp_path: Path) -> None:
    p = tmp_path / "liquidations_tie.parquet"
    rows = [
        (3, 2_000, "BTCUSDT", "BUY", "LIMIT", "IOC", "0.3", "101.0", "101.5", "FILLED", "0.3", "0.3", 2_000),
        (2, 1_000, "BTCUSDT", "BUY", "LIMIT", "IOC", "0.2", "100.0", "101.0", "FILLED", "0.2", "0.2", 2_000),
        (1, 1_000, "BTCUSDT", "SELL", "LIMIT", "IOC", "0.1", "99.0", "99.5", "FILLED", "0.1", "0.1", 1_000),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "side": pa.array([r[3] for r in rows], type=pa.string()),
            "order_type": pa.array([r[4] for r in rows], type=pa.string()),
            "time_in_force": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "price": pa.array([r[7] for r in rows], type=pa.string()),
            "average_price": pa.array([r[8] for r in rows], type=pa.string()),
            "order_status": pa.array([r[9] for r in rows], type=pa.string()),
            "last_filled_quantity": pa.array([r[10] for r in rows], type=pa.string()),
            "filled_quantity": pa.array([r[11] for r in rows], type=pa.string()),
            "trade_time": pa.array([r[12] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_liquidations(p))
    assert [e.trade_time_ms for e in out] == [1_000, 2_000, 2_000]


def test_iter_liquidations_sort_row_limit_blocks_large_in_memory_sort(tmp_path: Path) -> None:
    p = tmp_path / "liquidations_limit.parquet"
    rows = [
        (2, 2_000, "BTCUSDT", "BUY", "LIMIT", "IOC", "0.2", "100.0", "101.0", "FILLED", "0.2", "0.2", 2_000),
        (1, 1_000, "BTCUSDT", "SELL", "LIMIT", "IOC", "0.1", "99.0", "99.5", "FILLED", "0.1", "0.1", 1_000),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "side": pa.array([r[3] for r in rows], type=pa.string()),
            "order_type": pa.array([r[4] for r in rows], type=pa.string()),
            "time_in_force": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "price": pa.array([r[7] for r in rows], type=pa.string()),
            "average_price": pa.array([r[8] for r in rows], type=pa.string()),
            "order_status": pa.array([r[9] for r in rows], type=pa.string()),
            "last_filled_quantity": pa.array([r[10] for r in rows], type=pa.string()),
            "filled_quantity": pa.array([r[11] for r in rows], type=pa.string()),
            "trade_time": pa.array([r[12] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    try:
        list(iter_liquidations_advanced(p, sort_mode="always", sort_row_limit=1))
        assert False, "expected MemoryError due to sort_row_limit"
    except MemoryError as e:
        assert "iter_liquidations_advanced" in str(e)
