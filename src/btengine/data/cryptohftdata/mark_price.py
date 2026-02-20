from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterator, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as fs
import pyarrow.parquet as pq

from ...types import MarkPrice
from ._arrow import (
    ensure_in_memory_sort_within_row_limit,
    parquet_column_is_monotonic_non_decreasing,
    resolve_filesystem_and_path,
    resolve_path,
    resolve_sort_row_limit,
)
from .paths import CryptoHftLayout


def iter_mark_price(parquet_path: str | Path, *, filesystem: fs.FileSystem | None = None) -> Iterator[MarkPrice]:
    """Iterate MarkPrice events from a CryptoHFTData `mark_price.parquet` file.

    Events are yielded in ascending `event_time`. If the parquet is not stored
    in monotonic order, this function auto-detects and sorts when needed.
    """

    sort_mode: Literal["auto", "always", "never"] = "auto"
    yield from iter_mark_price_advanced(parquet_path, filesystem=filesystem, sort_mode=sort_mode)


def iter_mark_price_advanced(
    parquet_path: str | Path,
    *,
    filesystem: fs.FileSystem | None = None,
    sort_mode: Literal["auto", "always", "never"] = "auto",
    sort_row_limit: int | None = None,
) -> Iterator[MarkPrice]:
    """Advanced variant with explicit sort control."""

    if filesystem is None:
        filesystem, resolved_path = resolve_filesystem_and_path(parquet_path)
    else:
        resolved_path = resolve_path(parquet_path)
    pf = pq.ParquetFile(resolved_path, filesystem=filesystem)

    cols = [
        "received_time",
        "event_time",
        "symbol",
        "mark_price",
        "index_price",
        "funding_rate",
        "next_funding_time",
    ]

    needs_sort = sort_mode == "always"
    if sort_mode == "auto":
        needs_sort = not parquet_column_is_monotonic_non_decreasing(pf, "event_time")

    if sort_mode == "never":
        needs_sort = False

    if needs_sort:
        effective_sort_row_limit = resolve_sort_row_limit(sort_row_limit)
        ensure_in_memory_sort_within_row_limit(
            pf,
            row_limit=effective_sort_row_limit,
            context="iter_mark_price_advanced",
        )
        table = pf.read(columns=cols)
        table = table.set_column(table.schema.get_field_index("mark_price"), "mark_price", pc.cast(table["mark_price"], pa.float64()))
        table = table.set_column(
            table.schema.get_field_index("index_price"), "index_price", pc.cast(table["index_price"], pa.float64())
        )
        table = table.set_column(
            table.schema.get_field_index("funding_rate"), "funding_rate", pc.cast(table["funding_rate"], pa.float64())
        )
        sort_idx = pc.sort_indices(table["event_time"])
        table = table.take(sort_idx)

        received = table["received_time"].to_numpy(zero_copy_only=False)
        event_time = table["event_time"].to_numpy(zero_copy_only=False)
        symbol = table["symbol"].to_numpy(zero_copy_only=False)

        mark = table["mark_price"].to_numpy(zero_copy_only=False)
        index = table["index_price"].to_numpy(zero_copy_only=False)
        funding = table["funding_rate"].to_numpy(zero_copy_only=False)
        next_ft = table["next_funding_time"].to_numpy(zero_copy_only=False)

        for i in range(len(received)):
            yield MarkPrice(
                received_time_ns=int(received[i]),
                event_time_ms=int(event_time[i]),
                symbol=str(symbol[i]),
                mark_price=float(mark[i]),
                index_price=float(index[i]),
                funding_rate=float(funding[i]),
                next_funding_time_ms=int(next_ft[i]),
            )
        return

    for rg in range(pf.num_row_groups):
        table = pf.read_row_group(rg, columns=cols)

        received = table["received_time"].to_numpy(zero_copy_only=False)
        event_time = table["event_time"].to_numpy(zero_copy_only=False)
        symbol = table["symbol"].to_numpy(zero_copy_only=False)

        mark = pc.cast(table["mark_price"], pa.float64()).to_numpy(zero_copy_only=False)
        index = pc.cast(table["index_price"], pa.float64()).to_numpy(zero_copy_only=False)
        funding = pc.cast(table["funding_rate"], pa.float64()).to_numpy(zero_copy_only=False)
        next_ft = table["next_funding_time"].to_numpy(zero_copy_only=False)

        for i in range(len(received)):
            yield MarkPrice(
                received_time_ns=int(received[i]),
                event_time_ms=int(event_time[i]),
                symbol=str(symbol[i]),
                mark_price=float(mark[i]),
                index_price=float(index[i]),
                funding_rate=float(funding[i]),
                next_funding_time_ms=int(next_ft[i]),
            )


def iter_mark_price_for_day(
    layout: CryptoHftLayout,
    *,
    exchange: str,
    symbol: str,
    day: date,
    filesystem: fs.FileSystem | None = None,
) -> Iterator[MarkPrice]:
    uri = layout.mark_price(exchange=exchange, symbol=symbol, day=day)
    yield from iter_mark_price(uri, filesystem=filesystem)
