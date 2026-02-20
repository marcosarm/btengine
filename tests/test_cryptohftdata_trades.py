from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from btengine.data.cryptohftdata import iter_trades
from btengine.data.cryptohftdata.trades import iter_trades_advanced


def test_iter_trades_keeps_sorted_order_and_uses_trade_time_as_event_time(tmp_path: Path) -> None:
    p = tmp_path / "trades.parquet"

    rows = [
        (1_000_000_000_000_000_000, 9_999, 1_000, "BTCUSDT", 10, "100.0", "0.1", True),
        (1_000_000_000_000_000_001, 9_998, 1_001, "BTCUSDT", 11, "101.0", "0.2", False),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "trade_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "trade_id": pa.array([r[4] for r in rows], type=pa.int64()),
            "price": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "is_buyer_maker": pa.array([r[7] for r in rows], type=pa.bool_()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_trades(p))
    assert [e.trade_id for e in out] == [10, 11]
    assert [e.trade_time_ms for e in out] == [1_000, 1_001]
    # Canonical event time comes from trade_time, not source event_time.
    assert [e.event_time_ms for e in out] == [1_000, 1_001]


def test_iter_trades_sorts_out_of_order_by_trade_time(tmp_path: Path) -> None:
    p = tmp_path / "trades.parquet"

    rows = [
        (1_000_000_000_000_000_001, 2_000, 2_000, "BTCUSDT", 20, "102.0", "0.2", False),
        (1_000_000_000_000_000_000, 1_000, 1_000, "BTCUSDT", 19, "101.0", "0.1", True),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "trade_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "trade_id": pa.array([r[4] for r in rows], type=pa.int64()),
            "price": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "is_buyer_maker": pa.array([r[7] for r in rows], type=pa.bool_()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_trades(p))
    assert [e.trade_time_ms for e in out] == [1_000, 2_000]
    assert [e.trade_id for e in out] == [19, 20]


def test_iter_trades_sorts_equal_trade_time_by_trade_id(tmp_path: Path) -> None:
    p = tmp_path / "trades_tie.parquet"
    rows = [
        (3, 0, 2_000, "BTCUSDT", 30, "103.0", "0.3", True),
        (2, 0, 1_000, "BTCUSDT", 20, "102.0", "0.2", False),
        (1, 0, 1_000, "BTCUSDT", 10, "101.0", "0.1", True),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "trade_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "trade_id": pa.array([r[4] for r in rows], type=pa.int64()),
            "price": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "is_buyer_maker": pa.array([r[7] for r in rows], type=pa.bool_()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_trades(p))
    assert [e.trade_id for e in out] == [10, 20, 30]


def test_iter_trades_detects_disorder_in_later_row_group(tmp_path: Path) -> None:
    p = tmp_path / "trades_rg.parquet"

    # First row group looks fine, disorder appears only later.
    rows = [
        (1_000_000_000_000_000_000, 0, 1_000, "BTCUSDT", 1, "100.0", "0.1", True),
        (1_000_000_000_000_000_001, 0, 2_000, "BTCUSDT", 2, "101.0", "0.1", False),
        (1_000_000_000_000_000_002, 0, 1_500, "BTCUSDT", 3, "102.0", "0.1", True),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "trade_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "trade_id": pa.array([r[4] for r in rows], type=pa.int64()),
            "price": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "is_buyer_maker": pa.array([r[7] for r in rows], type=pa.bool_()),
        }
    )
    pq.write_table(table, p, row_group_size=1)

    out = list(iter_trades(p))
    assert [e.trade_time_ms for e in out] == [1_000, 1_500, 2_000]
    assert [e.trade_id for e in out] == [1, 3, 2]


def test_iter_trades_sort_row_limit_blocks_large_in_memory_sort(tmp_path: Path) -> None:
    p = tmp_path / "trades_limit.parquet"
    rows = [
        (2, 0, 2, "BTCUSDT", 2, "101.0", "0.1", True),
        (1, 0, 1, "BTCUSDT", 1, "100.0", "0.1", False),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "trade_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "trade_id": pa.array([r[4] for r in rows], type=pa.int64()),
            "price": pa.array([r[5] for r in rows], type=pa.string()),
            "quantity": pa.array([r[6] for r in rows], type=pa.string()),
            "is_buyer_maker": pa.array([r[7] for r in rows], type=pa.bool_()),
        }
    )
    pq.write_table(table, p)

    try:
        list(iter_trades_advanced(p, sort_mode="always", sort_row_limit=1))
        assert False, "expected MemoryError due to sort_row_limit"
    except MemoryError as e:
        assert "iter_trades_advanced" in str(e)
