from __future__ import annotations

import heapq
from typing import Iterable, Iterator, Protocol, TypeVar


class HasEventTimeMs(Protocol):
    event_time_ms: int


T = TypeVar("T", bound=HasEventTimeMs)


_EVENT_ID_ATTRS: tuple[str, ...] = (
    "transaction_time_ms",
    "trade_time_ms",
    "timestamp_ms",
    "final_update_id",
    "trade_id",
    "next_funding_time_ms",
)


def _received_time_ns_or_default(ev: object) -> int:
    x = getattr(ev, "received_time_ns", None)
    if x is None:
        # Keep stream-order fallback for event-like objects without receive timestamp.
        return 2**63 - 1
    return int(x)


def _event_meta_id_or_none(ev: object) -> int | None:
    """Deterministic event-id fallback used before stream-order fallback."""
    for attr in _EVENT_ID_ATTRS:
        x = getattr(ev, attr, None)
        if x is None:
            continue
        try:
            return int(x)
        except Exception:
            continue

    return None


def merge_event_streams(*streams: Iterable[T]) -> Iterator[T]:
    """Merge multiple event streams ordered by `event_time_ms`.

    This keeps only one event buffered per stream (k-way merge).
    For same `event_time_ms`, events are tie-broken by `received_time_ns`
    when available, then deterministic event ids, then stream order.
    """

    heap: list[tuple[int, int, int, int, int, T, Iterator[T]]] = []
    seq = 0

    for stream in streams:
        it = iter(stream)
        first = next(it, None)
        if first is None:
            continue
        tie_id = _event_meta_id_or_none(first)
        has_tie_id = 0 if tie_id is not None else 1
        tie_id_value = int(tie_id) if tie_id is not None else 0
        heapq.heappush(
            heap,
            (
                int(first.event_time_ms),
                _received_time_ns_or_default(first),
                has_tie_id,
                tie_id_value,
                seq,
                first,
                it,
            ),
        )
        seq += 1

    while heap:
        _, _, _, _, s, ev, it = heapq.heappop(heap)
        yield ev

        nxt = next(it, None)
        if nxt is not None:
            tie_id = _event_meta_id_or_none(nxt)
            has_tie_id = 0 if tie_id is not None else 1
            tie_id_value = int(tie_id) if tie_id is not None else 0
            heapq.heappush(
                heap,
                (
                    int(nxt.event_time_ms),
                    _received_time_ns_or_default(nxt),
                    has_tie_id,
                    tie_id_value,
                    s,
                    nxt,
                    it,
                ),
            )


def slice_event_stream(
    events: Iterable[T],
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> Iterator[T]:
    """Slice a (time-ordered) event stream by `event_time_ms`.

    Semantics:
    - If `start_ms` is provided, events with `event_time_ms < start_ms` are skipped.
    - If `end_ms` is provided, iteration stops when `event_time_ms >= end_ms`.

    This function assumes `events` are ordered by `event_time_ms` to allow early
    termination once `end_ms` is reached.
    """

    if start_ms is None and end_ms is None:
        yield from events
        return

    for ev in events:
        t = int(ev.event_time_ms)
        if start_ms is not None and t < start_ms:
            continue
        if end_ms is not None and t >= end_ms:
            break
        yield ev
