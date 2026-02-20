from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ._arrow import resolve_filesystem_and_path

DatasetKind = Literal["trades", "orderbook", "mark_price", "ticker", "open_interest", "liquidations"]

_SORT_COLS: dict[str, list[str]] = {
    "trades": ["trade_time", "trade_id"],
    "orderbook": ["final_update_id", "side", "price"],
    "mark_price": ["event_time"],
    "ticker": ["event_time"],
    "open_interest": ["timestamp"],
    "liquidations": ["event_time", "trade_time"],
}

_DEDUP_KEY_COLS: dict[str, list[str]] = {
    "trades": ["symbol", "trade_id", "trade_time"],
    "orderbook": ["symbol", "final_update_id", "side", "price"],
    "mark_price": ["symbol", "event_time"],
    "ticker": ["symbol", "event_time"],
    "open_interest": ["symbol", "timestamp"],
    "liquidations": ["symbol", "event_time", "trade_time", "side", "price", "quantity"],
}


@dataclass(frozen=True, slots=True)
class PreprocessResult:
    kind: str
    input_path: str
    output_path: str
    rows_in: int
    rows_out: int
    dropped_duplicates: int
    out_of_order_before: int
    out_of_order_after: int
    sort_columns: tuple[str, ...]
    dedup_columns: tuple[str, ...]


def _require_columns(df: pd.DataFrame, cols: list[str], *, context: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{context}: missing required columns: {missing}")


def _count_out_of_order(df: pd.DataFrame, *, sort_col: str) -> int:
    if sort_col not in df.columns or len(df) <= 1:
        return 0
    s = pd.to_numeric(df[sort_col], errors="coerce")
    diff = s.diff()
    out = int((diff < 0).sum())
    return out


def preprocess_parquet_file(
    input_path: str | Path,
    output_path: str | Path,
    *,
    kind: DatasetKind,
    dedup: bool = True,
    keep: Literal["first", "last"] = "last",
    compression: str = "zstd",
    max_rows_in_memory: int | None = 2_000_000,
) -> PreprocessResult:
    kind_s = str(kind)
    if kind_s not in _SORT_COLS:
        raise ValueError(f"invalid kind: {kind!r}")

    sort_cols = list(_SORT_COLS[kind_s])
    dedup_cols = list(_DEDUP_KEY_COLS[kind_s])

    in_fs, in_resolved = resolve_filesystem_and_path(input_path)
    out_fs, out_resolved = resolve_filesystem_and_path(output_path)

    pf = pq.ParquetFile(in_resolved, filesystem=in_fs)
    rows_in_meta = int(pf.metadata.num_rows or 0)
    if max_rows_in_memory is not None:
        limit = int(max_rows_in_memory)
        if limit > 0 and rows_in_meta > limit:
            raise MemoryError(
                f"preprocess_parquet_file({kind_s}): requires loading {rows_in_meta} rows, "
                f"exceeds max_rows_in_memory={limit}. Use smaller windows or increase the limit."
            )

    table = pf.read()
    df = table.to_pandas()

    _require_columns(df, sort_cols, context=f"{kind_s} sort")
    out_of_order_before = _count_out_of_order(df, sort_col=sort_cols[0])

    # Stable sort keeps deterministic ordering for same-key rows.
    df = df.sort_values(by=sort_cols, kind="mergesort")

    if dedup:
        key_cols = [c for c in dedup_cols if c in df.columns]
        if key_cols:
            df = df.drop_duplicates(subset=key_cols, keep=keep)

    # Re-sort after dedup to guarantee monotonic output.
    df = df.sort_values(by=sort_cols, kind="mergesort")
    out_of_order_after = _count_out_of_order(df, sort_col=sort_cols[0])

    out_table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(out_table, out_resolved, filesystem=out_fs, compression=compression)

    rows_in = int(table.num_rows)
    rows_out = int(out_table.num_rows)
    dropped = max(0, rows_in - rows_out)

    return PreprocessResult(
        kind=kind_s,
        input_path=str(input_path),
        output_path=str(output_path),
        rows_in=rows_in,
        rows_out=rows_out,
        dropped_duplicates=dropped,
        out_of_order_before=int(out_of_order_before),
        out_of_order_after=int(out_of_order_after),
        sort_columns=tuple(sort_cols),
        dedup_columns=tuple(c for c in dedup_cols if c in df.columns),
    )
