from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from btengine.data.cryptohftdata import iter_mark_price


def test_iter_mark_price_sorts_and_casts(tmp_path: Path) -> None:
    p = tmp_path / "mark_price.parquet"

    # Two rows, deliberately out of order by event_time.
    rows = [
        (2_000_000_000_000_000_000, 2_000, "BTCUSDT", "101.0", "100.5", "0.01", 2_000),
        (1_000_000_000_000_000_000, 1_000, "BTCUSDT", "99.0", "99.5", "0.02", 1_000),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "mark_price": pa.array([r[3] for r in rows], type=pa.string()),
            "index_price": pa.array([r[4] for r in rows], type=pa.string()),
            "funding_rate": pa.array([r[5] for r in rows], type=pa.string()),
            "next_funding_time": pa.array([r[6] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_mark_price(p))
    assert [e.event_time_ms for e in out] == [1_000, 2_000]
    assert out[0].mark_price == 99.0
    assert out[0].funding_rate == 0.02
    assert out[1].mark_price == 101.0


def test_iter_mark_price_keeps_already_sorted_order(tmp_path: Path) -> None:
    p = tmp_path / "mark_price.parquet"

    rows = [
        (1_000_000_000_000_000_000, 1_000, "BTCUSDT", "99.0", "99.5", "0.02", 1_000),
        (2_000_000_000_000_000_000, 2_000, "BTCUSDT", "101.0", "100.5", "0.01", 2_000),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "mark_price": pa.array([r[3] for r in rows], type=pa.string()),
            "index_price": pa.array([r[4] for r in rows], type=pa.string()),
            "funding_rate": pa.array([r[5] for r in rows], type=pa.string()),
            "next_funding_time": pa.array([r[6] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    out = list(iter_mark_price(p))
    assert [e.event_time_ms for e in out] == [1_000, 2_000]
    assert out[0].next_funding_time_ms == 1_000
    assert out[1].next_funding_time_ms == 2_000


def test_iter_mark_price_detects_disorder_in_later_row_group(tmp_path: Path) -> None:
    p = tmp_path / "mark_price_rg.parquet"

    rows = [
        (1_000_000_000_000_000_000, 1_000, "BTCUSDT", "99.0", "99.5", "0.02", 1_000),
        (2_000_000_000_000_000_000, 2_000, "BTCUSDT", "101.0", "100.5", "0.01", 2_000),
        (3_000_000_000_000_000_000, 1_500, "BTCUSDT", "100.0", "100.0", "0.03", 1_500),
    ]

    table = pa.table(
        {
            "received_time": pa.array([r[0] for r in rows], type=pa.int64()),
            "event_time": pa.array([r[1] for r in rows], type=pa.int64()),
            "symbol": pa.array([r[2] for r in rows], type=pa.string()),
            "mark_price": pa.array([r[3] for r in rows], type=pa.string()),
            "index_price": pa.array([r[4] for r in rows], type=pa.string()),
            "funding_rate": pa.array([r[5] for r in rows], type=pa.string()),
            "next_funding_time": pa.array([r[6] for r in rows], type=pa.int64()),
        }
    )
    pq.write_table(table, p, row_group_size=1)

    out = list(iter_mark_price(p))
    assert [e.event_time_ms for e in out] == [1_000, 1_500, 2_000]
