from datetime import date

import pytest

from btengine.data.cryptohftdata import replay as replay_mod
from btengine.types import OpenInterest, Trade


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


def _trade(ts_ms: int, recv_ms: int, *, trade_id: int) -> Trade:
    return Trade(
        received_time_ns=int(recv_ms) * 1_000_000,
        event_time_ms=int(ts_ms),
        trade_time_ms=int(ts_ms),
        symbol="BTCUSDT",
        trade_id=int(trade_id),
        price=100.0,
        quantity=1.0,
        is_buyer_maker=True,
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
        open_interest_availability_quantile=0.5,
    )
    out = list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))
    # causal_asof now uses rolling past-only quantile:
    # lags are [100, 1500, 10000]
    # ev1: no history -> delay 0
    # ev2: q([100]) = 100
    # ev3: q([100,1500]) = 800
    assert [e.event_time_ms for e in out] == [1_000, 2_100, 3_800]


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
        open_interest_availability_quantile=0.9,
        open_interest_max_delay_ms=2_000,
    )
    out = list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))
    assert [e.event_time_ms for e in out] == [1_000, 2_100, 4_360]


def test_build_day_stream_open_interest_causal_asof_global_keeps_global_quantile(monkeypatch):
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
        open_interest_alignment_mode="causal_asof_global",
        open_interest_availability_quantile=0.5,  # median lag over full day => 1500ms
    )
    out = list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))
    assert [e.event_time_ms for e in out] == [2_500, 3_500, 4_500]


def test_build_day_stream_open_interest_causal_asof_uses_precalibrated_floor(monkeypatch):
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
        open_interest_availability_quantile=0.5,
        open_interest_calibrated_delay_ms=2_000,
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


def test_build_day_stream_open_interest_rejects_negative_precalibrated_delay(monkeypatch):
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
        open_interest_calibrated_delay_ms=-1,
    )
    with pytest.raises(ValueError):
        list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))


def test_build_day_stream_trade_alignment_fixed_delay(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_trades_for_day",
        lambda *args, **kwargs: iter([_trade(1_000, 1_100, trade_id=1), _trade(2_000, 2_100, trade_id=2)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=True,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=False,
        include_liquidations=False,
        stream_alignment_mode="fixed_delay",
        trade_delay_ms=250,
    )
    out = list(
        replay_mod.build_day_stream(
            _DummyLayout(),
            cfg=cfg,
            symbol="BTCUSDT",
            day=date(2025, 7, 20),
            filesystem=None,
        )
    )
    assert [e.event_time_ms for e in out] == [1_250, 2_250]


def test_build_day_stream_trade_alignment_causal_asof_rolling(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_trades_for_day",
        lambda *args, **kwargs: iter(
            [
                _trade(1_000, 1_100, trade_id=1),  # lag=100
                _trade(2_000, 2_500, trade_id=2),  # lag=500
                _trade(3_000, 4_500, trade_id=3),  # lag=1500
            ]
        ),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=True,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=False,
        include_liquidations=False,
        stream_alignment_mode="causal_asof",
        stream_alignment_quantile=0.5,
        stream_alignment_history_size=1024,
    )
    out = list(
        replay_mod.build_day_stream(
            _DummyLayout(),
            cfg=cfg,
            symbol="BTCUSDT",
            day=date(2025, 7, 20),
            filesystem=None,
        )
    )
    # causal, past-only:
    # ev1 -> no history => +0
    # ev2 -> q([100])=100
    # ev3 -> q([100,500])=300
    assert [e.event_time_ms for e in out] == [1_000, 2_100, 3_300]


def test_build_day_stream_trade_alignment_respects_min_max_delay(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_trades_for_day",
        lambda *args, **kwargs: iter([_trade(1_000, 1_010, trade_id=1), _trade(2_000, 2_100, trade_id=2)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=True,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=False,
        include_liquidations=False,
        stream_alignment_mode="fixed_delay",
        trade_delay_ms=10,
        stream_alignment_min_delay_ms=50,
        stream_alignment_max_delay_ms=80,
    )
    out = list(
        replay_mod.build_day_stream(
            _DummyLayout(),
            cfg=cfg,
            symbol="BTCUSDT",
            day=date(2025, 7, 20),
            filesystem=None,
        )
    )
    # fixed 10ms clamped to min=50ms
    assert [e.event_time_ms for e in out] == [1_050, 2_050]


def test_build_day_stream_trade_alignment_rejects_invalid_quantile(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_trades_for_day",
        lambda *args, **kwargs: iter([_trade(1_000, 1_100, trade_id=1)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=True,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=False,
        include_liquidations=False,
        stream_alignment_mode="causal_asof",
        stream_alignment_quantile=2.0,
    )
    with pytest.raises(ValueError):
        list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))


def test_build_day_stream_trade_alignment_rejects_invalid_history_size(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_trades_for_day",
        lambda *args, **kwargs: iter([_trade(1_000, 1_100, trade_id=1)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=True,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=False,
        include_liquidations=False,
        stream_alignment_mode="causal_asof",
        stream_alignment_quantile=0.5,
        stream_alignment_history_size=0,
    )
    with pytest.raises(ValueError):
        list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))


def test_build_day_stream_trade_alignment_causal_asof_clamps_non_monotonic(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_trades_for_day",
        lambda *args, **kwargs: iter(
            [
                _trade(1_000, 3_000, trade_id=1),  # lag=2000
                _trade(2_000, 2_000, trade_id=2),  # lag=0
                _trade(3_000, 3_000, trade_id=3),  # lag=0
            ]
        ),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=True,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=False,
        include_liquidations=False,
        stream_alignment_mode="causal_asof",
        stream_alignment_quantile=0.0,
        stream_alignment_history_size=1024,
    )
    out = list(
        replay_mod.build_day_stream(
            _DummyLayout(),
            cfg=cfg,
            symbol="BTCUSDT",
            day=date(2025, 7, 20),
            filesystem=None,
        )
    )
    times = [e.event_time_ms for e in out]
    assert times == [1_000, 4_000, 4_000]
    assert times == sorted(times)


def test_build_day_stream_trade_alignment_global_row_limit_raises(monkeypatch):
    monkeypatch.setattr(
        replay_mod,
        "iter_trades_for_day",
        lambda *args, **kwargs: iter([_trade(1_000, 1_100, trade_id=1), _trade(2_000, 2_100, trade_id=2)]),
    )

    cfg = replay_mod.CryptoHftDayConfig(
        include_trades=True,
        include_orderbook=False,
        include_mark_price=False,
        include_ticker=False,
        include_open_interest=False,
        include_liquidations=False,
        stream_alignment_mode="causal_asof_global",
        stream_alignment_quantile=0.5,
        stream_alignment_global_row_limit=1,
    )
    with pytest.raises(MemoryError):
        list(replay_mod.build_day_stream(_DummyLayout(), cfg=cfg, symbol="BTCUSDT", day=date(2025, 7, 20), filesystem=None))
