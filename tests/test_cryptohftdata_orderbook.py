from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from btengine.data.cryptohftdata import iter_depth_updates
from btengine.data.cryptohftdata.orderbook import iter_depth_updates_advanced


def _write_orderbook_parquet(path: Path) -> None:
    # Minimal flattened orderbook file with 2 depth messages (final_update_id 10 and 11).
    rows = [
        # msg 10
        (1_000_000_000_000_000_000, 1_000, 999, "BTCUSDT", "update", 1, 10, 9, "bid", "100.0", "1.0"),
        (1_000_000_000_000_000_000, 1_000, 999, "BTCUSDT", "update", 1, 10, 9, "ask", "101.0", "2.0"),
        # msg 11
        (1_000_000_000_000_000_100, 1_001, 1_000, "BTCUSDT", "update", 11, 11, 10, "ask", "101.0", "1.5"),
        (1_000_000_000_000_000_100, 1_001, 1_000, "BTCUSDT", "update", 11, 11, 10, "bid", "100.0", "0.0"),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "transaction_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "event_type": pa.array([r[4] for r in rows], type=pa.string()),
            "first_update_id": pa.array([r[5] for r in rows], type=pa.int64()),
            "final_update_id": pa.array([r[6] for r in rows], type=pa.int64()),
            "prev_final_update_id": pa.array([r[7] for r in rows], type=pa.int64()),
            "last_update_id": pa.array([None for _ in rows], type=pa.float64()),
            "side": pa.array([r[8] for r in rows], type=pa.string()),
            "price": pa.array([r[9] for r in rows], type=pa.string()),
            "quantity": pa.array([r[10] for r in rows], type=pa.string()),
            "order_count": pa.array([None for _ in rows], type=pa.float64()),
        }
    )

    pq.write_table(table, path)


def test_iter_depth_updates_groups_by_final_update_id(tmp_path: Path) -> None:
    p = tmp_path / "orderbook_00.parquet"
    _write_orderbook_parquet(p)

    updates = list(iter_depth_updates(p))
    assert len(updates) == 2

    u0, u1 = updates

    assert u0.final_update_id == 10
    assert u0.prev_final_update_id == 9
    assert u0.bid_updates == [(100.0, 1.0)]
    assert u0.ask_updates == [(101.0, 2.0)]

    assert u1.final_update_id == 11
    assert u1.prev_final_update_id == 10
    assert u1.bid_updates == [(100.0, 0.0)]
    assert u1.ask_updates == [(101.0, 1.5)]


def test_iter_depth_updates_sorts_interleaved_rows(tmp_path: Path) -> None:
    p = tmp_path / "orderbook_00.parquet"

    # Interleave rows from two depth messages (final_update_id 10 and 11).
    rows = [
        # msg 10 (bid first)
        (1_000_000_000_000_000_000, 1_000, 999, "BTCUSDT", "update", 1, 10, 9, "bid", "100.0", "1.0"),
        # msg 11 (ask)
        (1_000_000_000_000_000_100, 1_001, 1_000, "BTCUSDT", "update", 11, 11, 10, "ask", "101.0", "1.5"),
        # msg 10 (ask)
        (1_000_000_000_000_000_000, 1_000, 999, "BTCUSDT", "update", 1, 10, 9, "ask", "101.0", "2.0"),
        # msg 11 (bid delete)
        (1_000_000_000_000_000_100, 1_001, 1_000, "BTCUSDT", "update", 11, 11, 10, "bid", "100.0", "0.0"),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "transaction_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "event_type": pa.array([r[4] for r in rows], type=pa.string()),
            "first_update_id": pa.array([r[5] for r in rows], type=pa.int64()),
            "final_update_id": pa.array([r[6] for r in rows], type=pa.int64()),
            "prev_final_update_id": pa.array([r[7] for r in rows], type=pa.int64()),
            "last_update_id": pa.array([None for _ in rows], type=pa.float64()),
            "side": pa.array([r[8] for r in rows], type=pa.string()),
            "price": pa.array([r[9] for r in rows], type=pa.string()),
            "quantity": pa.array([r[10] for r in rows], type=pa.string()),
            "order_count": pa.array([None for _ in rows], type=pa.float64()),
        }
    )
    pq.write_table(table, p)

    updates = list(iter_depth_updates(p))
    assert len(updates) == 2

    u0, u1 = updates
    assert u0.final_update_id == 10
    assert sorted(u0.bid_updates) == [(100.0, 1.0)]
    assert sorted(u0.ask_updates) == [(101.0, 2.0)]

    assert u1.final_update_id == 11
    assert sorted(u1.bid_updates) == [(100.0, 0.0)]
    assert sorted(u1.ask_updates) == [(101.0, 1.5)]


def test_iter_depth_updates_detects_disorder_in_later_row_group(tmp_path: Path) -> None:
    p = tmp_path / "orderbook_00.parquet"

    # First row groups look monotonic; later row group regresses final_update_id.
    rows = [
        # final_id 10
        (1_000_000_000_000_000_000, 1_000, 999, "BTCUSDT", "update", 1, 10, 9, "bid", "100.0", "1.0"),
        (1_000_000_000_000_000_000, 1_000, 999, "BTCUSDT", "update", 1, 10, 9, "ask", "101.0", "1.0"),
        # final_id 11
        (1_000_000_000_000_000_100, 1_001, 1_000, "BTCUSDT", "update", 11, 11, 10, "bid", "100.5", "1.0"),
        (1_000_000_000_000_000_100, 1_001, 1_000, "BTCUSDT", "update", 11, 11, 10, "ask", "101.5", "1.0"),
        # final_id 9 appears later in file (requires sort to restore order)
        (1_000_000_000_000_000_050, 999, 998, "BTCUSDT", "update", 0, 9, 8, "bid", "99.5", "1.0"),
        (1_000_000_000_000_000_050, 999, 998, "BTCUSDT", "update", 0, 9, 8, "ask", "100.5", "1.0"),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "transaction_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "event_type": pa.array([r[4] for r in rows], type=pa.string()),
            "first_update_id": pa.array([r[5] for r in rows], type=pa.int64()),
            "final_update_id": pa.array([r[6] for r in rows], type=pa.int64()),
            "prev_final_update_id": pa.array([r[7] for r in rows], type=pa.int64()),
            "last_update_id": pa.array([None for _ in rows], type=pa.float64()),
            "side": pa.array([r[8] for r in rows], type=pa.string()),
            "price": pa.array([r[9] for r in rows], type=pa.string()),
            "quantity": pa.array([r[10] for r in rows], type=pa.string()),
            "order_count": pa.array([None for _ in rows], type=pa.float64()),
        }
    )
    pq.write_table(table, p, row_group_size=2)

    updates = list(iter_depth_updates(p))
    assert [u.final_update_id for u in updates] == [9, 10, 11]


def test_iter_depth_updates_sort_row_limit_blocks_large_in_memory_sort(tmp_path: Path) -> None:
    p = tmp_path / "orderbook_limit.parquet"
    rows = [
        (1, 1_000, 1_000, "BTCUSDT", "update", 1, 11, 10, "bid", "100.0", "1.0"),
        (2, 1_001, 1_001, "BTCUSDT", "update", 1, 10, 9, "ask", "101.0", "1.0"),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "transaction_time": pa.array([r[2] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[3] for r in rows], type=pa.string()),
            "event_type": pa.array([r[4] for r in rows], type=pa.string()),
            "first_update_id": pa.array([r[5] for r in rows], type=pa.int64()),
            "final_update_id": pa.array([r[6] for r in rows], type=pa.int64()),
            "prev_final_update_id": pa.array([r[7] for r in rows], type=pa.int64()),
            "side": pa.array([r[8] for r in rows], type=pa.string()),
            "price": pa.array([r[9] for r in rows], type=pa.string()),
            "quantity": pa.array([r[10] for r in rows], type=pa.string()),
        }
    )
    pq.write_table(table, p)

    try:
        list(iter_depth_updates_advanced(p, sort_mode="always", sort_row_limit=1))
        assert False, "expected MemoryError due to sort_row_limit"
    except MemoryError as e:
        assert "iter_depth_updates_advanced" in str(e)
