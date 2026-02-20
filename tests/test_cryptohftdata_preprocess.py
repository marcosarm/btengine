from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from btengine.data.cryptohftdata.preprocess import preprocess_parquet_file


def test_preprocess_trades_sorts_and_dedups(tmp_path: Path) -> None:
    p_in = tmp_path / "trades_in.parquet"
    p_out = tmp_path / "trades_out.parquet"

    table = pa.table(
        {
            "received_time": pa.array([2000, 1000, 1500], type=pa.int64()),
            "event_time": pa.array([2, 1, 1], type=pa.int64()),
            "trade_time": pa.array([2, 1, 1], type=pa.int64()),
            "symbol": pa.array(["BTCUSDT", "BTCUSDT", "BTCUSDT"], type=pa.string()),
            "trade_id": pa.array([20, 10, 10], type=pa.int64()),
            "price": pa.array(["101.0", "100.0", "100.0"], type=pa.string()),
            "quantity": pa.array(["1.0", "1.0", "1.0"], type=pa.string()),
            "is_buyer_maker": pa.array([True, True, True], type=pa.bool_()),
        }
    )
    pq.write_table(table, p_in)

    res = preprocess_parquet_file(p_in, p_out, kind="trades")
    out = pq.read_table(p_out)

    tt = out["trade_time"].to_pylist()
    tid = out["trade_id"].to_pylist()
    assert tt == [1, 2]
    assert tid == [10, 20]
    assert res.rows_in == 3
    assert res.rows_out == 2
    assert res.dropped_duplicates == 1
    assert res.out_of_order_before == 1
    assert res.out_of_order_after == 0


def test_preprocess_rejects_invalid_kind(tmp_path: Path) -> None:
    p_in = tmp_path / "x.parquet"
    p_out = tmp_path / "y.parquet"
    pq.write_table(pa.table({"a": pa.array([1], type=pa.int64())}), p_in)

    with pytest.raises(ValueError):
        preprocess_parquet_file(p_in, p_out, kind="foo")  # type: ignore[arg-type]


def test_preprocess_respects_max_rows_in_memory(tmp_path: Path) -> None:
    p_in = tmp_path / "trades_in.parquet"
    p_out = tmp_path / "trades_out.parquet"
    table = pa.table(
        {
            "received_time": pa.array([1000, 1001], type=pa.int64()),
            "event_time": pa.array([1, 2], type=pa.int64()),
            "trade_time": pa.array([1, 2], type=pa.int64()),
            "symbol": pa.array(["BTCUSDT", "BTCUSDT"], type=pa.string()),
            "trade_id": pa.array([1, 2], type=pa.int64()),
            "price": pa.array(["100.0", "100.1"], type=pa.string()),
            "quantity": pa.array(["1.0", "1.0"], type=pa.string()),
            "is_buyer_maker": pa.array([True, False], type=pa.bool_()),
        }
    )
    pq.write_table(table, p_in)

    with pytest.raises(MemoryError, match="max_rows_in_memory"):
        preprocess_parquet_file(p_in, p_out, kind="trades", max_rows_in_memory=1)
