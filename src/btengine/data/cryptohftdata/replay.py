from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Iterator, Literal

import pyarrow.fs as fs

from ...replay import merge_event_streams, slice_event_stream
from ...types import DepthUpdate, Liquidation, MarkPrice, OpenInterest, Ticker, Trade
from .liquidations import iter_liquidations_for_day
from .mark_price import iter_mark_price_for_day
from .open_interest import iter_open_interest_for_day
from .orderbook import iter_depth_updates_for_day
from .paths import CryptoHftLayout
from .ticker import iter_ticker_for_day
from .trades import iter_trades_for_day


@dataclass(frozen=True, slots=True)
class CryptoHftDayConfig:
    exchange: str = "binance_futures"
    include_trades: bool = True
    include_orderbook: bool = True
    include_mark_price: bool = False
    include_ticker: bool = False
    include_open_interest: bool = False
    include_liquidations: bool = False
    open_interest_delay_ms: int = 0
    # Optional externally-calibrated delay (ms) measured offline.
    open_interest_calibrated_delay_ms: int | None = None
    open_interest_alignment_mode: Literal["fixed_delay", "causal_asof", "causal_asof_global"] = "fixed_delay"
    open_interest_availability_quantile: float = 0.8
    open_interest_min_delay_ms: int = 0
    open_interest_max_delay_ms: int | None = None
    orderbook_hours: range = range(24)
    orderbook_skip_missing: bool = True
    skip_missing_daily_files: bool = False
    stream_start_ms: int | None = None
    stream_end_ms: int | None = None


def _shift_open_interest_event(ev: OpenInterest, *, delay_ms: int) -> OpenInterest:
    return OpenInterest(
        received_time_ns=int(ev.received_time_ns),
        event_time_ms=int(ev.timestamp_ms) + int(delay_ms),
        timestamp_ms=int(ev.timestamp_ms),
        symbol=str(ev.symbol),
        sum_open_interest=float(ev.sum_open_interest),
        sum_open_interest_value=float(ev.sum_open_interest_value),
    )


def _quantile_interpolated(values_sorted: list[int], q: float) -> int:
    if not values_sorted:
        return 0
    if len(values_sorted) == 1:
        return int(values_sorted[0])
    x = float(q) * float(len(values_sorted) - 1)
    lo = int(x)
    hi = min(lo + 1, len(values_sorted) - 1)
    if lo == hi:
        return int(values_sorted[lo])
    frac = float(x - lo)
    y = (1.0 - frac) * float(values_sorted[lo]) + frac * float(values_sorted[hi])
    return int(round(y))


def _clamp_open_interest_delay(delay_ms: int, *, cfg: CryptoHftDayConfig) -> int:
    min_delay = int(cfg.open_interest_min_delay_ms or 0)
    max_delay = cfg.open_interest_max_delay_ms
    if min_delay < 0:
        raise ValueError("open_interest_min_delay_ms must be >= 0")
    if max_delay is not None and int(max_delay) < 0:
        raise ValueError("open_interest_max_delay_ms must be >= 0 when provided")
    if max_delay is not None and int(max_delay) < int(min_delay):
        raise ValueError("open_interest_max_delay_ms must be >= open_interest_min_delay_ms")

    out = int(delay_ms)
    if out < min_delay:
        out = min_delay
    if max_delay is not None and out > int(max_delay):
        out = int(max_delay)
    return out


def _resolve_open_interest_base_delay(*, cfg: CryptoHftDayConfig, fixed_delay_ms: int) -> int:
    out = int(fixed_delay_ms)
    calibrated = cfg.open_interest_calibrated_delay_ms
    if calibrated is not None:
        if int(calibrated) < 0:
            raise ValueError("open_interest_calibrated_delay_ms must be >= 0 when provided")
        out = max(out, int(calibrated))
    return _clamp_open_interest_delay(out, cfg=cfg)


def _stream_open_interest_delay_rolling(
    evs: list[OpenInterest], *, q: float, cfg: CryptoHftDayConfig, fixed_delay_ms: int
) -> Iterator[OpenInterest]:
    """Yield OI events with a rolling causal availability delay.

    Delay for event i is estimated from lags observed strictly before i.
    This avoids using future (or same-event) lag information.
    """

    base_delay = _resolve_open_interest_base_delay(cfg=cfg, fixed_delay_ms=fixed_delay_ms)
    lags_seen: list[int] = []

    for ev in evs:
        if lags_seen:
            q_delay = _quantile_interpolated(lags_seen, q)
            delay = max(int(base_delay), int(q_delay))
            delay = _clamp_open_interest_delay(delay, cfg=cfg)
        else:
            delay = int(base_delay)

        yield _shift_open_interest_event(ev, delay_ms=delay)

        recv_ms = int(int(ev.received_time_ns) // 1_000_000)
        lag = recv_ms - int(ev.timestamp_ms)
        if lag < 0:
            lag = 0
        lags_seen.append(int(lag))
        lags_seen.sort()


def _stream_open_interest_delay_global_quantile(
    evs: list[OpenInterest], *, q: float, cfg: CryptoHftDayConfig, fixed_delay_ms: int
) -> Iterator[OpenInterest]:
    """Yield OI events using one global quantile from the full day.

    Warning: this is not strictly causal and may introduce leakage.
    """

    lags_ms: list[int] = []
    for ev in evs:
        recv_ms = int(int(ev.received_time_ns) // 1_000_000)
        lag = recv_ms - int(ev.timestamp_ms)
        if lag < 0:
            lag = 0
        lags_ms.append(int(lag))
    lags_ms.sort()

    q_delay = _quantile_interpolated(lags_ms, q)
    base_delay = _resolve_open_interest_base_delay(cfg=cfg, fixed_delay_ms=fixed_delay_ms)
    delay = max(int(base_delay), int(q_delay))
    delay = _clamp_open_interest_delay(delay, cfg=cfg)

    for ev in evs:
        yield _shift_open_interest_event(ev, delay_ms=delay)


def _align_open_interest_stream(stream: Iterable[OpenInterest], *, cfg: CryptoHftDayConfig) -> Iterable[OpenInterest]:
    mode = str(cfg.open_interest_alignment_mode)
    fixed_delay = int(cfg.open_interest_delay_ms or 0)
    if fixed_delay < 0:
        raise ValueError("open_interest_delay_ms must be >= 0")

    if mode == "fixed_delay":
        delay = _resolve_open_interest_base_delay(cfg=cfg, fixed_delay_ms=fixed_delay)
        for ev in stream:
            yield _shift_open_interest_event(ev, delay_ms=delay)
        return

    if mode == "causal_asof":
        q = float(cfg.open_interest_availability_quantile)
        if not (0.0 <= q <= 1.0):
            raise ValueError("open_interest_availability_quantile must be in [0, 1]")

        evs = list(stream)
        yield from _stream_open_interest_delay_rolling(evs, q=q, cfg=cfg, fixed_delay_ms=fixed_delay)
        return

    if mode == "causal_asof_global":
        q = float(cfg.open_interest_availability_quantile)
        if not (0.0 <= q <= 1.0):
            raise ValueError("open_interest_availability_quantile must be in [0, 1]")
        evs = list(stream)
        yield from _stream_open_interest_delay_global_quantile(evs, q=q, cfg=cfg, fixed_delay_ms=fixed_delay)
        return

    raise ValueError("open_interest_alignment_mode must be 'fixed_delay', 'causal_asof' or 'causal_asof_global'")


def build_day_stream(
    layout: CryptoHftLayout,
    *,
    cfg: CryptoHftDayConfig,
    symbol: str,
    day: date,
    filesystem: fs.FileSystem | None = None,
) -> Iterable[DepthUpdate | Trade | MarkPrice | Ticker | OpenInterest | Liquidation]:
    """Build a merged event stream for one symbol on one day.

    If `cfg.stream_start_ms`/`cfg.stream_end_ms` are provided, each underlying
    stream is time-sliced before merging (useful to focus on specific hours).
    """

    streams: list[Iterable[DepthUpdate | Trade | MarkPrice | Ticker | OpenInterest | Liquidation]] = []
    start_ms = cfg.stream_start_ms
    end_ms = cfg.stream_end_ms

    def _safe(stream: Iterable[DepthUpdate | Trade | MarkPrice | Ticker | OpenInterest | Liquidation]):
        try:
            yield from stream
        except FileNotFoundError:
            if cfg.skip_missing_daily_files:
                return
            raise

    if cfg.include_orderbook:
        ob = (
            iter_depth_updates_for_day(
                layout,
                exchange=cfg.exchange,
                symbol=symbol,
                day=day,
                filesystem=filesystem,
                hours=cfg.orderbook_hours,
                skip_missing=cfg.orderbook_skip_missing,
            )
        )
        if start_ms is not None or end_ms is not None:
            ob = slice_event_stream(ob, start_ms=start_ms, end_ms=end_ms)
        streams.append(ob)

    if cfg.include_trades:
        tr = iter_trades_for_day(layout, exchange=cfg.exchange, symbol=symbol, day=day, filesystem=filesystem)
        if start_ms is not None or end_ms is not None:
            tr = slice_event_stream(tr, start_ms=start_ms, end_ms=end_ms)
        streams.append(_safe(tr))

    if cfg.include_mark_price:
        mp = iter_mark_price_for_day(layout, exchange=cfg.exchange, symbol=symbol, day=day, filesystem=filesystem)
        if start_ms is not None or end_ms is not None:
            mp = slice_event_stream(mp, start_ms=start_ms, end_ms=end_ms)
        streams.append(_safe(mp))

    if cfg.include_ticker:
        tk = iter_ticker_for_day(layout, exchange=cfg.exchange, symbol=symbol, day=day, filesystem=filesystem)
        if start_ms is not None or end_ms is not None:
            tk = slice_event_stream(tk, start_ms=start_ms, end_ms=end_ms)
        streams.append(_safe(tk))

    if cfg.include_open_interest:
        oi = iter_open_interest_for_day(layout, exchange=cfg.exchange, symbol=symbol, day=day, filesystem=filesystem)
        oi = _align_open_interest_stream(oi, cfg=cfg)
        if start_ms is not None or end_ms is not None:
            oi = slice_event_stream(oi, start_ms=start_ms, end_ms=end_ms)
        streams.append(_safe(oi))

    if cfg.include_liquidations:
        liq = iter_liquidations_for_day(layout, exchange=cfg.exchange, symbol=symbol, day=day, filesystem=filesystem)
        if start_ms is not None or end_ms is not None:
            liq = slice_event_stream(liq, start_ms=start_ms, end_ms=end_ms)
        streams.append(_safe(liq))

    return merge_event_streams(*streams)
