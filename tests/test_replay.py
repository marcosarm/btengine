from dataclasses import dataclass

from btengine.replay import merge_event_streams, slice_event_stream
from btengine.types import Trade


def _trade(t: int) -> Trade:
    return Trade(
        received_time_ns=0,
        event_time_ms=t,
        trade_time_ms=t,
        symbol="BTCUSDT",
        trade_id=t,
        price=100.0,
        quantity=1.0,
        is_buyer_maker=True,
    )


def test_slice_event_stream_no_window_yields_all():
    events = [_trade(1), _trade(2), _trade(3)]
    out = list(slice_event_stream(events))
    assert [e.event_time_ms for e in out] == [1, 2, 3]


def test_slice_event_stream_start_only_skips_prefix():
    events = [_trade(1), _trade(2), _trade(3)]
    out = list(slice_event_stream(events, start_ms=2))
    assert [e.event_time_ms for e in out] == [2, 3]


def test_slice_event_stream_end_only_stops_early():
    events = [_trade(1), _trade(2), _trade(3)]
    out = list(slice_event_stream(events, end_ms=3))
    assert [e.event_time_ms for e in out] == [1, 2]


def test_slice_event_stream_start_end_slices_half_open_interval():
    events = [_trade(1), _trade(2), _trade(3), _trade(4)]
    out = list(slice_event_stream(events, start_ms=2, end_ms=4))
    assert [e.event_time_ms for e in out] == [2, 3]


@dataclass(frozen=True)
class _Ev:
    event_time_ms: int
    tag: str


def test_merge_event_streams_orders_by_time():
    s1 = [_Ev(1, "a1"), _Ev(3, "a3")]
    s2 = [_Ev(2, "b2"), _Ev(4, "b4")]
    out = list(merge_event_streams(s1, s2))
    assert [(e.event_time_ms, e.tag) for e in out] == [(1, "a1"), (2, "b2"), (3, "a3"), (4, "b4")]


def test_merge_event_streams_stable_tie_breaker_by_stream_order():
    s1 = [_Ev(1_000, "s1_first"), _Ev(2_000, "s1_tie")]
    s2 = [_Ev(1_500, "s2_mid"), _Ev(2_000, "s2_tie")]
    out = list(merge_event_streams(s1, s2))
    # Tie at 2000 keeps stream order (s1 before s2).
    assert [(e.event_time_ms, e.tag) for e in out] == [
        (1_000, "s1_first"),
        (1_500, "s2_mid"),
        (2_000, "s1_tie"),
        (2_000, "s2_tie"),
    ]


@dataclass(frozen=True)
class _EvRecv:
    event_time_ms: int
    received_time_ns: int
    tag: str


def test_merge_event_streams_tie_breaker_prefers_received_time_when_available():
    s1 = [_EvRecv(1_000, 200, "s1_late_recv")]
    s2 = [_EvRecv(1_000, 100, "s2_early_recv")]
    out = list(merge_event_streams(s1, s2))
    assert [(e.event_time_ms, e.tag) for e in out] == [
        (1_000, "s2_early_recv"),
        (1_000, "s1_late_recv"),
    ]


@dataclass(frozen=True)
class _EvNoRecvWithId:
    event_time_ms: int
    final_update_id: int
    tag: str


def test_merge_event_streams_tie_breaker_without_received_time_uses_event_metadata():
    s1 = [_EvNoRecvWithId(1_000, 200, "s1_late_id")]
    s2 = [_EvNoRecvWithId(1_000, 100, "s2_early_id")]
    out = list(merge_event_streams(s1, s2))
    assert [(e.event_time_ms, e.tag) for e in out] == [
        (1_000, "s2_early_id"),
        (1_000, "s1_late_id"),
    ]


@dataclass(frozen=True)
class _EvTypeZ:
    event_time_ms: int
    tag: str


@dataclass(frozen=True)
class _EvTypeA:
    event_time_ms: int
    tag: str


def test_merge_event_streams_same_timestamp_no_type_bias_falls_back_to_stream_order():
    s1 = [_EvTypeZ(1_000, "s1_first")]
    s2 = [_EvTypeA(1_000, "s2_second")]
    out = list(merge_event_streams(s1, s2))
    assert [(e.event_time_ms, e.tag) for e in out] == [
        (1_000, "s1_first"),
        (1_000, "s2_second"),
    ]
