from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq


def _load_validate_module():
    root = Path(__file__).resolve().parents[1]
    scripts_dir = root / "scripts"
    src_dir = root / "src"

    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    return importlib.import_module("validate_s3_dataset")


def test_inspect_parquet_handles_empty_time_column(tmp_path: Path) -> None:
    m = _load_validate_module()
    p = tmp_path / "empty.parquet"
    table = pa.table({"event_time": pa.array([], type=pa.int64())})
    pq.write_table(table, p)

    info = m.inspect_parquet(
        str(p),
        fs=pafs.LocalFileSystem(),
        time_col="event_time",
        orderbook_ids=False,
    )
    assert info.rows == 0
    assert info.first_event_time_ms is None
    assert info.last_event_time_ms is None


def test_inspect_parquet_handles_empty_orderbook_ids(tmp_path: Path) -> None:
    m = _load_validate_module()
    p = tmp_path / "empty_ob.parquet"
    table = pa.table(
        {
            "event_time": pa.array([], type=pa.int64()),
            "final_update_id": pa.array([], type=pa.int64()),
            "prev_final_update_id": pa.array([], type=pa.int64()),
        }
    )
    pq.write_table(table, p)

    info = m.inspect_parquet(
        str(p),
        fs=pafs.LocalFileSystem(),
        time_col="event_time",
        orderbook_ids=True,
    )
    assert info.rows == 0
    assert info.first_event_time_ms is None
    assert info.last_event_time_ms is None
    assert info.first_final_update_id is None
    assert info.last_final_update_id is None
    assert info.first_prev_final_update_id is None

