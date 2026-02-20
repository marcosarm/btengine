from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from btengine.data.cryptohftdata import iter_open_interest, iter_open_interest_for_day
from btengine.data.cryptohftdata.open_interest import iter_open_interest_advanced


def test_iter_open_interest_sorts_and_casts(tmp_path: Path) -> None:
    p = tmp_path / "open_interest.parquet"

    # Two rows, deliberately out of order by timestamp.
    rows = [
        (2_000_000_000_000_000_000, "BTCUSDT", "10.0", "1000.0", 2_000),
        (1_000_000_000_000_000_000, "BTCUSDT", "11.0", "1100.0", 1_000),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[1] for r in rows], type=pa.string()),
            "sum_open_interest": pa.array([r[2] for r in rows], type=pa.string()),
            "sum_open_interest_value": pa.array([r[3] for r in rows], type=pa.string()),
            "timestamp": pa.array([r[4] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_open_interest(p))
    assert [e.event_time_ms for e in out] == [1_000, 2_000]
    assert [e.timestamp_ms for e in out] == [1_000, 2_000]
    assert out[0].sum_open_interest == 11.0
    assert out[1].sum_open_interest == 10.0


def test_iter_open_interest_detects_disorder_in_later_row_group(tmp_path: Path) -> None:
    p = tmp_path / "open_interest_rg.parquet"

    rows = [
        (1_000_000_000_000_000_000, "BTCUSDT", "11.0", "1100.0", 1_000),
        (2_000_000_000_000_000_000, "BTCUSDT", "12.0", "1200.0", 2_000),
        (3_000_000_000_000_000_000, "BTCUSDT", "11.5", "1150.0", 1_500),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[1] for r in rows], type=pa.string()),
            "sum_open_interest": pa.array([r[2] for r in rows], type=pa.string()),
            "sum_open_interest_value": pa.array([r[3] for r in rows], type=pa.string()),
            "timestamp": pa.array([r[4] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p, row_group_size=1)

    out = list(iter_open_interest(p))
    assert [e.timestamp_ms for e in out] == [1_000, 1_500, 2_000]


class _DummyLayout:
    def __init__(self, base: Path) -> None:
        self.base = base

    def open_interest(self, *, exchange: str, symbol: str, day: date) -> str:
        return str(self.base / "open_interest.parquet")


def test_iter_open_interest_for_day_fallback_parcial_and_filters_day(tmp_path: Path) -> None:
    d = date(2025, 7, 1)
    day_start_ms = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
    day_end_ms = day_start_ms + 86_400_000

    # Keep open_interest.parquet missing on purpose; only fallback file exists.
    p_alt = tmp_path / "open_interest_parcial.parquet"
    rows = [
        (1_000_000_000_000_000_000, "BTCUSDT", "10.0", "1000.0", day_start_ms - 1_000),
        (1_000_000_000_000_000_001, "BTCUSDT", "11.0", "1100.0", day_start_ms + 2_000),
        (1_000_000_000_000_000_002, "BTCUSDT", "10.5", "1050.0", day_start_ms + 1_000),
        (1_000_000_000_000_000_003, "BTCUSDT", "12.0", "1200.0", day_end_ms + 1_000),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[1] for r in rows], type=pa.string()),
            "sum_open_interest": pa.array([r[2] for r in rows], type=pa.string()),
            "sum_open_interest_value": pa.array([r[3] for r in rows], type=pa.string()),
            "timestamp": pa.array([r[4] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p_alt)

    out = list(
        iter_open_interest_for_day(
            _DummyLayout(tmp_path),
            exchange="binance_futures",
            symbol="BTCUSDT",
            day=d,
            filesystem=None,
        )
    )
    assert [e.timestamp_ms for e in out] == [day_start_ms + 1_000, day_start_ms + 2_000]


def test_iter_open_interest_sort_row_limit_blocks_large_in_memory_sort(tmp_path: Path) -> None:
    p = tmp_path / "open_interest_limit.parquet"
    rows = [
        (2, "BTCUSDT", "10.0", "1000.0", 2_000),
        (1, "BTCUSDT", "11.0", "1100.0", 1_000),
    ]
    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[1] for r in rows], type=pa.string()),
            "sum_open_interest": pa.array([r[2] for r in rows], type=pa.string()),
            "sum_open_interest_value": pa.array([r[3] for r in rows], type=pa.string()),
            "timestamp": pa.array([r[4] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    try:
        list(iter_open_interest_advanced(p, sort_mode="always", sort_row_limit=1))
        assert False, "expected MemoryError due to sort_row_limit"
    except MemoryError as e:
        assert "iter_open_interest_advanced" in str(e)
