from __future__ import annotations

import heapq
from typing import Iterable, Iterator, Protocol, TypeVar


class HasEventTimeMs(Protocol):
    event_time_ms: int


T = TypeVar("T", bound=HasEventTimeMs)

_EVENT_TYPE_PRIORITY: dict[str, int] = {
    "DepthUpdate": 0,
    "Trade": 1,
    "MarkPrice": 2,
    "Ticker": 3,
    "OpenInterest": 4,
    "Liquidation": 5,
}

_EVENT_ID_ATTRS: tuple[str, ...] = (
    "transaction_time_ms",
    "trade_time_ms",
    "final_update_id",
    "trade_id",
    "timestamp_ms",
    "next_funding_time_ms",
)


def _received_time_ns_or_default(ev: object) -> int:
    x = getattr(ev, "received_time_ns", None)
    if x is None:
        # Keep old behavior (stream order tie-break) for event-like objects
        # without receive timestamp.
        return 2**63 - 1
    return int(x)


def _event_tie_break_key(ev: object) -> tuple[int, int, str]:
    kind = type(ev).__name__
    pri = int(_EVENT_TYPE_PRIORITY.get(kind, 99))
    for attr in _EVENT_ID_ATTRS:
        x = getattr(ev, attr, None)
        if x is None:
            continue
        try:
            return pri, int(x), kind
        except Exception:
            continue
    return pri, 0, kind


def merge_event_streams(*streams: Iterable[T]) -> Iterator[T]:
    """Merge multiple event streams ordered by `event_time_ms`.

    This keeps only one event buffered per stream (k-way merge).
    For same `event_time_ms`, events are tie-broken by `received_time_ns`
    when available, then deterministic event metadata, then stream order.
    """

    heap: list[tuple[int, int, int, int, str, int, T, Iterator[T]]] = []
    seq = 0

    for stream in streams:
        it = iter(stream)
        first = next(it, None)
        if first is None:
            continue
        pri, tie_id, kind = _event_tie_break_key(first)
        heapq.heappush(
            heap,
            (int(first.event_time_ms), _received_time_ns_or_default(first), pri, tie_id, kind, seq, first, it),
        )
        seq += 1

    while heap:
        _, _, _, _, _, s, ev, it = heapq.heappop(heap)
        yield ev

        nxt = next(it, None)
        if nxt is not None:
            pri, tie_id, kind = _event_tie_break_key(nxt)
            heapq.heappush(
                heap,
                (int(nxt.event_time_ms), _received_time_ns_or_default(nxt), pri, tie_id, kind, s, nxt, it),
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
