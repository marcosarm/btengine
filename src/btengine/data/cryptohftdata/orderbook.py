from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterator, Literal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as fs
import pyarrow.parquet as pq

from ...types import DepthUpdate
from ._arrow import (
    ensure_in_memory_sort_within_row_limit,
    resolve_filesystem_and_path,
    resolve_path,
    resolve_sort_row_limit,
)
from .paths import CryptoHftLayout


@dataclass(frozen=True, slots=True)
class CryptoHftOrderbookRow:
    received_time_ns: int
    event_time_ms: int
    transaction_time_ms: int
    symbol: str
    event_type: str
    first_update_id: int
    final_update_id: int
    prev_final_update_id: int
    side: str
    price: float
    quantity: float


def _orderbook_needs_sort_auto(pf: pq.ParquetFile) -> bool:
    """Heuristic to detect whether `final_update_id` sorting is needed.

    Scans all row groups (not only the first) to catch late disorder and
    interleaving.
    """

    prev_last_final: int | None = None
    # Track closed contiguous-id segments to detect exact "re-entry" patterns:
    # e.g. 10,10,11,11,10,10 (same final_update_id appears again later).
    closed_segment_ids: set[int] = set()
    current_segment_id: int | None = None

    for rg in range(pf.num_row_groups):
        sample = pf.read_row_group(rg, columns=["final_update_id"])
        arr = sample["final_update_id"].to_numpy(zero_copy_only=False)
        if len(arr) == 0:
            continue

        if prev_last_final is not None and int(arr[0]) < int(prev_last_final):
            return True

        if len(arr) > 1 and not bool(np.all(arr[1:] >= arr[:-1])):
            return True

        # Process only segment boundaries (not every row) to keep this cheap.
        segment_starts = np.flatnonzero(arr[1:] != arr[:-1]) + 1
        if len(segment_starts):
            segment_vals = np.concatenate((arr[:1], arr[segment_starts]))
        else:
            segment_vals = arr[:1]

        for v in segment_vals:
            fid = int(v)
            if current_segment_id is not None and fid != int(current_segment_id):
                closed_segment_ids.add(int(current_segment_id))
            if fid in closed_segment_ids:
                return True
            current_segment_id = fid

        prev_last_final = int(arr[-1])

    return False


def iter_depth_updates(parquet_path: str | Path, *, filesystem: fs.FileSystem | None = None) -> Iterator[DepthUpdate]:
    """Iterate DepthUpdate messages from a flattened orderbook parquet file.

    Expected schema (based on sample):
    - received_time (int64, ns)
    - event_time (int64, ms)
    - transaction_time (int64, ms)
    - symbol (string)
    - event_type (string) -> usually "update"
    - first_update_id (int64)
    - final_update_id (int64)
    - prev_final_update_id (int64)
    - side (string) -> "bid"/"ask"
    - price (string)
    - quantity (string)

    Important: Some datasets are not physically stored grouped by `final_update_id`
    (rows for the same depth message are interleaved). This iterator will detect
    that and sort by `final_update_id` when needed (see `iter_depth_updates_advanced`
    for explicit control). Sorting may require reading the whole hour file into
    memory.
    """

    sort_mode: Literal["auto", "always", "never"] = "auto"
    yield from iter_depth_updates_advanced(parquet_path, filesystem=filesystem, sort_mode=sort_mode)


def iter_depth_updates_advanced(
    parquet_path: str | Path,
    *,
    filesystem: fs.FileSystem | None = None,
    sort_mode: Literal["auto", "always", "never"] = "auto",
    sort_row_limit: int | None = None,
) -> Iterator[DepthUpdate]:
    """Iterate DepthUpdate messages, optionally sorting by `final_update_id`.

    Why sorting matters:
    - Some parquet files may not be stored grouped by `final_update_id` (rows
      for the same message are interleaved).
    - Sorting by `final_update_id` restores the Binance depth stream order.
    """

    if filesystem is None:
        filesystem, resolved_path = resolve_filesystem_and_path(parquet_path)
    else:
        resolved_path = resolve_path(parquet_path)

    pf = pq.ParquetFile(resolved_path, filesystem=filesystem)

    cols = [
        "received_time",
        "event_time",
        "transaction_time",
        "symbol",
        "event_type",
        "first_update_id",
        "final_update_id",
        "prev_final_update_id",
        "side",
        "price",
        "quantity",
    ]

    # Decide whether sorting is needed.
    needs_sort = sort_mode == "always"
    if sort_mode == "auto":
        needs_sort = _orderbook_needs_sort_auto(pf)

    if sort_mode == "never":
        needs_sort = False

    if needs_sort:
        effective_sort_row_limit = resolve_sort_row_limit(sort_row_limit)
        ensure_in_memory_sort_within_row_limit(
            pf,
            row_limit=effective_sort_row_limit,
            context="iter_depth_updates_advanced",
        )
        yield from _iter_depth_updates_sorted(pf, cols=cols)
    else:
        yield from _iter_depth_updates_streaming(pf, cols=cols)


def _iter_depth_updates_sorted(pf: pq.ParquetFile, *, cols: list[str]) -> Iterator[DepthUpdate]:
    table = pf.read(columns=cols)

    # Cast price/quantity are stored as strings; cast using Arrow kernels.
    table = table.set_column(table.schema.get_field_index("price"), "price", pc.cast(table["price"], pa.float64()))
    table = table.set_column(
        table.schema.get_field_index("quantity"), "quantity", pc.cast(table["quantity"], pa.float64())
    )

    # Sort to restore depth update sequence.
    # Include original file row index as secondary key so that updates within the
    # same final_update_id keep their original order deterministically.
    row_idx = pa.array(np.arange(table.num_rows, dtype=np.int64))
    table = table.append_column("__row_idx", row_idx)
    sort_idx = pc.sort_indices(
        table,
        sort_keys=[
            ("final_update_id", "ascending"),
            ("__row_idx", "ascending"),
        ],
    )
    table = table.take(sort_idx)

    received = table["received_time"].to_numpy(zero_copy_only=False)
    event_time = table["event_time"].to_numpy(zero_copy_only=False)
    tx_time = table["transaction_time"].to_numpy(zero_copy_only=False)
    symbol = table["symbol"].to_numpy(zero_copy_only=False)
    first_id = table["first_update_id"].to_numpy(zero_copy_only=False)
    final_id = table["final_update_id"].to_numpy(zero_copy_only=False)
    prev_final_id = table["prev_final_update_id"].to_numpy(zero_copy_only=False)
    side = table["side"].to_numpy(zero_copy_only=False)
    price = table["price"].to_numpy(zero_copy_only=False)
    qty = table["quantity"].to_numpy(zero_copy_only=False)

    if len(final_id) == 0:
        return

    start = 0
    for i in range(1, len(final_id) + 1):
        boundary = i == len(final_id) or final_id[i] != final_id[i - 1]
        if not boundary:
            continue

        bid_updates: list[tuple[float, float]] = []
        ask_updates: list[tuple[float, float]] = []

        for j in range(start, i):
            s = str(side[j])
            p = float(price[j])
            q = float(qty[j])
            if s == "bid":
                bid_updates.append((p, q))
            elif s == "ask":
                ask_updates.append((p, q))
            else:
                raise ValueError(f"invalid side value in parquet: {s!r}")

        yield DepthUpdate(
            received_time_ns=int(received[start]),
            event_time_ms=int(event_time[start]),
            transaction_time_ms=int(tx_time[start]),
            symbol=str(symbol[start]),
            first_update_id=int(first_id[start]),
            final_update_id=int(final_id[start]),
            prev_final_update_id=int(prev_final_id[start]),
            bid_updates=bid_updates,
            ask_updates=ask_updates,
        )

        start = i


def _iter_depth_updates_streaming(pf: pq.ParquetFile, *, cols: list[str]) -> Iterator[DepthUpdate]:
    cur_final_id: int | None = None
    cur_received_ns = 0
    cur_event_ms = 0
    cur_tx_ms = 0
    cur_symbol = ""
    cur_first_id = 0
    cur_prev_final_id = 0
    cur_bid_updates: list[tuple[float, float]] = []
    cur_ask_updates: list[tuple[float, float]] = []

    for rg in range(pf.num_row_groups):
        table = pf.read_row_group(rg, columns=cols)

        received = table["received_time"].to_numpy(zero_copy_only=False)
        event_time = table["event_time"].to_numpy(zero_copy_only=False)
        tx_time = table["transaction_time"].to_numpy(zero_copy_only=False)
        symbol = table["symbol"].to_numpy(zero_copy_only=False)
        first_id = table["first_update_id"].to_numpy(zero_copy_only=False)
        final_id = table["final_update_id"].to_numpy(zero_copy_only=False)
        prev_final_id = table["prev_final_update_id"].to_numpy(zero_copy_only=False)
        side = table["side"].to_numpy(zero_copy_only=False)

        price = pc.cast(table["price"], pa.float64()).to_numpy(zero_copy_only=False)
        qty = pc.cast(table["quantity"], pa.float64()).to_numpy(zero_copy_only=False)

        if len(final_id) == 0:
            continue

        start = 0
        for i in range(1, len(final_id) + 1):
            is_boundary = i == len(final_id) or final_id[i] != final_id[i - 1]
            if not is_boundary:
                continue

            seg_final = int(final_id[start])

            if cur_final_id is None:
                cur_final_id = seg_final
                cur_received_ns = int(received[start])
                cur_event_ms = int(event_time[start])
                cur_tx_ms = int(tx_time[start])
                cur_symbol = str(symbol[start])
                cur_first_id = int(first_id[start])
                cur_prev_final_id = int(prev_final_id[start])
            elif seg_final != cur_final_id:
                yield DepthUpdate(
                    received_time_ns=cur_received_ns,
                    event_time_ms=cur_event_ms,
                    transaction_time_ms=cur_tx_ms,
                    symbol=cur_symbol,
                    first_update_id=cur_first_id,
                    final_update_id=int(cur_final_id),
                    prev_final_update_id=cur_prev_final_id,
                    bid_updates=cur_bid_updates,
                    ask_updates=cur_ask_updates,
                )

                cur_final_id = seg_final
                cur_received_ns = int(received[start])
                cur_event_ms = int(event_time[start])
                cur_tx_ms = int(tx_time[start])
                cur_symbol = str(symbol[start])
                cur_first_id = int(first_id[start])
                cur_prev_final_id = int(prev_final_id[start])
                cur_bid_updates = []
                cur_ask_updates = []

            for j in range(start, i):
                s = str(side[j])
                p = float(price[j])
                q = float(qty[j])
                if s == "bid":
                    cur_bid_updates.append((p, q))
                elif s == "ask":
                    cur_ask_updates.append((p, q))
                else:
                    raise ValueError(f"invalid side value in parquet: {s!r}")

            start = i

    if cur_final_id is not None:
        yield DepthUpdate(
            received_time_ns=cur_received_ns,
            event_time_ms=cur_event_ms,
            transaction_time_ms=cur_tx_ms,
            symbol=cur_symbol,
            first_update_id=cur_first_id,
            final_update_id=int(cur_final_id),
            prev_final_update_id=cur_prev_final_id,
            bid_updates=cur_bid_updates,
            ask_updates=cur_ask_updates,
        )


def iter_depth_updates_for_day(
    layout: CryptoHftLayout,
    *,
    exchange: str,
    symbol: str,
    day: date,
    filesystem: fs.FileSystem | None = None,
    hours: range = range(24),
    skip_missing: bool = False,
) -> Iterator[DepthUpdate]:
    """Iterate depth updates for a full day by concatenating `orderbook_{HH}.parquet`."""

    for h in hours:
        uri = layout.orderbook(exchange=exchange, symbol=symbol, day=day, hour=h)
        try:
            yield from iter_depth_updates(uri, filesystem=filesystem)
        except FileNotFoundError:
            if skip_missing:
                continue
            raise
