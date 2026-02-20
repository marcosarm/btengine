from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from btengine.data.cryptohftdata import iter_ticker
from btengine.data.cryptohftdata.ticker import iter_ticker_advanced


def test_iter_ticker_sorts_and_casts(tmp_path: Path) -> None:
    p = tmp_path / "ticker.parquet"

    # Two rows, deliberately out of order by event_time.
    rows = [
        # t=2000
        (2_000_000_000_000_000_000, 2_000, "BTCUSDT", "1.0", "0.1", "100.0", "101.0", "0.5", "99.0", "102.0", "98.0", "10.0", "1000.0", 0, 2_000, 1, 2, 10),
        # t=1000
        (1_000_000_000_000_000_000, 1_000, "BTCUSDT", "2.0", "0.2", "100.0", "103.0", "1.5", "99.0", "104.0", "98.0", "11.0", "1100.0", 0, 1_000, 1, 3, 11),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "price_change": pa.array([r[3] for r in rows], type=pa.string()),
            "price_change_percent": pa.array([r[4] for r in rows], type=pa.string()),
            "weighted_average_price": pa.array([r[5] for r in rows], type=pa.string()),
            "last_price": pa.array([r[6] for r in rows], type=pa.string()),
            "last_quantity": pa.array([r[7] for r in rows], type=pa.string()),
            "open_price": pa.array([r[8] for r in rows], type=pa.string()),
            "high_price": pa.array([r[9] for r in rows], type=pa.string()),
            "low_price": pa.array([r[10] for r in rows], type=pa.string()),
            "base_asset_volume": pa.array([r[11] for r in rows], type=pa.string()),
            "quote_asset_volume": pa.array([r[12] for r in rows], type=pa.string()),
            "statistics_open_time": pa.array([r[13] for r in rows], type=pa.int64()),
            "statistics_close_time": pa.array([r[14] for r in rows], type=pa.int64()),
            "first_trade_id": pa.array([r[15] for r in rows], type=pa.int64()),
            "last_trade_id": pa.array([r[16] for r in rows], type=pa.int64()),
            "total_trades": pa.array([r[17] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_ticker(p))
    assert [e.event_time_ms for e in out] == [1_000, 2_000]
    assert out[0].last_price == 103.0
    assert out[1].last_price == 101.0


def test_iter_ticker_detects_disorder_in_later_row_group(tmp_path: Path) -> None:
    p = tmp_path / "ticker_rg.parquet"

    rows = [
        (1_000_000_000_000_000_000, 1_000, "BTCUSDT", "1.0", "0.1", "100.0", "101.0", "0.5", "99.0", "102.0", "98.0", "10.0", "1000.0", 0, 1_000, 1, 2, 10),
        (2_000_000_000_000_000_000, 2_000, "BTCUSDT", "2.0", "0.2", "100.0", "103.0", "0.5", "99.0", "104.0", "98.0", "10.0", "1000.0", 0, 2_000, 1, 3, 11),
        (3_000_000_000_000_000_000, 1_500, "BTCUSDT", "1.5", "0.15", "100.0", "102.0", "0.5", "99.0", "103.0", "98.0", "10.0", "1000.0", 0, 1_500, 1, 4, 12),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "price_change": pa.array([r[3] for r in rows], type=pa.string()),
            "price_change_percent": pa.array([r[4] for r in rows], type=pa.string()),
            "weighted_average_price": pa.array([r[5] for r in rows], type=pa.string()),
            "last_price": pa.array([r[6] for r in rows], type=pa.string()),
            "last_quantity": pa.array([r[7] for r in rows], type=pa.string()),
            "open_price": pa.array([r[8] for r in rows], type=pa.string()),
            "high_price": pa.array([r[9] for r in rows], type=pa.string()),
            "low_price": pa.array([r[10] for r in rows], type=pa.string()),
            "base_asset_volume": pa.array([r[11] for r in rows], type=pa.string()),
            "quote_asset_volume": pa.array([r[12] for r in rows], type=pa.string()),
            "statistics_open_time": pa.array([r[13] for r in rows], type=pa.int64()),
            "statistics_close_time": pa.array([r[14] for r in rows], type=pa.int64()),
            "first_trade_id": pa.array([r[15] for r in rows], type=pa.int64()),
            "last_trade_id": pa.array([r[16] for r in rows], type=pa.int64()),
            "total_trades": pa.array([r[17] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p, row_group_size=1)

    out = list(iter_ticker(p))
    assert [e.event_time_ms for e in out] == [1_000, 1_500, 2_000]


def test_iter_ticker_sorts_equal_event_time_by_received_time(tmp_path: Path) -> None:
    p = tmp_path / "ticker_tie.parquet"

    rows = [
        (3_000_000_000_000_000_000, 2_000, "BTCUSDT", "0.5", "0.05", "100.0", "100.5", "0.4", "99.0", "101.0", "98.0", "9.0", "900.0", 0, 2_000, 1, 1, 9),
        (2_000_000_000_000_000_000, 1_000, "BTCUSDT", "1.0", "0.1", "100.0", "101.0", "0.5", "99.0", "102.0", "98.0", "10.0", "1000.0", 0, 1_000, 1, 2, 10),
        (1_000_000_000_000_000_000, 1_000, "BTCUSDT", "2.0", "0.2", "100.0", "103.0", "1.5", "99.0", "104.0", "98.0", "11.0", "1100.0", 0, 1_000, 1, 3, 11),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "price_change": pa.array([r[3] for r in rows], type=pa.string()),
            "price_change_percent": pa.array([r[4] for r in rows], type=pa.string()),
            "weighted_average_price": pa.array([r[5] for r in rows], type=pa.string()),
            "last_price": pa.array([r[6] for r in rows], type=pa.string()),
            "last_quantity": pa.array([r[7] for r in rows], type=pa.string()),
            "open_price": pa.array([r[8] for r in rows], type=pa.string()),
            "high_price": pa.array([r[9] for r in rows], type=pa.string()),
            "low_price": pa.array([r[10] for r in rows], type=pa.string()),
            "base_asset_volume": pa.array([r[11] for r in rows], type=pa.string()),
            "quote_asset_volume": pa.array([r[12] for r in rows], type=pa.string()),
            "statistics_open_time": pa.array([r[13] for r in rows], type=pa.int64()),
            "statistics_close_time": pa.array([r[14] for r in rows], type=pa.int64()),
            "first_trade_id": pa.array([r[15] for r in rows], type=pa.int64()),
            "last_trade_id": pa.array([r[16] for r in rows], type=pa.int64()),
            "total_trades": pa.array([r[17] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_ticker(p))
    assert [e.received_time_ns for e in out] == [
        1_000_000_000_000_000_000,
        2_000_000_000_000_000_000,
        3_000_000_000_000_000_000,
    ]


def test_iter_ticker_sort_row_limit_blocks_large_in_memory_sort(tmp_path: Path) -> None:
    p = tmp_path / "ticker_limit.parquet"
    rows = [
        (2, 2_000, "BTCUSDT", "1.0", "0.1", "100.0", "101.0", "0.5", "99.0", "102.0", "98.0", "10.0", "1000.0", 0, 2_000, 1, 2, 10),
        (1, 1_000, "BTCUSDT", "2.0", "0.2", "100.0", "103.0", "1.5", "99.0", "104.0", "98.0", "11.0", "1100.0", 0, 1_000, 1, 3, 11),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "price_change": pa.array([r[3] for r in rows], type=pa.string()),
            "price_change_percent": pa.array([r[4] for r in rows], type=pa.string()),
            "weighted_average_price": pa.array([r[5] for r in rows], type=pa.string()),
            "last_price": pa.array([r[6] for r in rows], type=pa.string()),
            "last_quantity": pa.array([r[7] for r in rows], type=pa.string()),
            "open_price": pa.array([r[8] for r in rows], type=pa.string()),
            "high_price": pa.array([r[9] for r in rows], type=pa.string()),
            "low_price": pa.array([r[10] for r in rows], type=pa.string()),
            "base_asset_volume": pa.array([r[11] for r in rows], type=pa.string()),
            "quote_asset_volume": pa.array([r[12] for r in rows], type=pa.string()),
            "statistics_open_time": pa.array([r[13] for r in rows], type=pa.int64()),
            "statistics_close_time": pa.array([r[14] for r in rows], type=pa.int64()),
            "first_trade_id": pa.array([r[15] for r in rows], type=pa.int64()),
            "last_trade_id": pa.array([r[16] for r in rows], type=pa.int64()),
            "total_trades": pa.array([r[17] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    try:
        list(iter_ticker_advanced(p, sort_mode="always", sort_row_limit=1))
        assert False, "expected MemoryError due to sort_row_limit"
    except MemoryError as e:
        assert "iter_ticker_advanced" in str(e)
