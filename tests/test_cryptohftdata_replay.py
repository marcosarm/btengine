from datetime import date

import pytest

from btengine.data.cryptohftdata import replay as replay_mod
from btengine.types import OpenInterest


class _DummyLayout:
    pass


def _oi(ts_ms: int, recv_ms: int) -> OpenInterest:
    return OpenInterest(
        received_time_ns=int(recv_ms) * 1_000_000,
        event_time_ms=int(ts_ms),
        timestamp_ms=int(ts_ms),
        symbol="BTCUSDT",
        sum_open_interest=1.0,
        sum_open_interest_value=1.0,
    )


def test_build_day_stream_open_interest_fixed_delay(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_open_interest_for_day",
        lambda *args, **kwargs: iter([_oi(1_000, 1_100), _oi(2_000, 2_100)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=False,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=True,
        include_liquidations=False,
        open_interest_delay_ms=500,
        open_interest_alignment_mode="fixed_delay",
    )
    out = list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))
    assert [e.event_time_ms for e in out] == [1_500, 2_500]


def test_build_day_stream_open_interest_causal_asof_quantile(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_open_interest_for_day",
        lambda *args, **kwargs: iter([_oi(1_000, 1_100), _oi(2_000, 3_500), _oi(3_000, 13_000)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=False,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=True,
        include_liquidations=False,
        open_interest_alignment_mode="causal_asof",
        open_interest_availability_quantile=0.5,  # median lag => 1500ms
    )
    out = list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))
    assert [e.event_time_ms for e in out] == [2_500, 3_500, 4_500]


def test_build_day_stream_open_interest_causal_asof_respects_max_delay(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_open_interest_for_day",
        lambda *args, **kwargs: iter([_oi(1_000, 1_100), _oi(2_000, 3_500), _oi(3_000, 13_000)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=False,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=True,
        include_liquidations=False,
        open_interest_alignment_mode="causal_asof",
        open_interest_availability_quantile=0.9,  # would be very large without cap
        open_interest_max_delay_ms=2_000,
    )
    out = list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))
    assert [e.event_time_ms for e in out] == [3_000, 4_000, 5_000]


def test_build_day_stream_open_interest_causal_asof_rejects_invalid_quantile(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_open_interest_for_day",
        lambda *args, **kwargs: iter([_oi(1_000, 1_100)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=False,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=True,
        include_liquidations=False,
        open_interest_alignment_mode="causal_asof",
        open_interest_availability_quantile=1.1,
    )
    with pytest.raises(ValueError):
        list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))


def test_build_day_stream_open_interest_fixed_delay_rejects_negative_delay(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_open_interest_for_day",
        lambda *args, **kwargs: iter([_oi(1_000, 1_100)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=False,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=True,
        include_liquidations=False,
        open_interest_alignment_mode="fixed_delay",
        open_interest_delay_ms=-1,
    )
    with pytest.raises(ValueError):
        list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))
