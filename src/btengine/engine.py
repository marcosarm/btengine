from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable, Literal, Protocol

from .book_guard import BookGuardConfig, BookGuardedBroker
from .broker import SimBroker
from .execution.orders import Order
from .marketdata.orderbook import L2Book
from .types import DepthUpdate, Liquidation, MarkPrice, OpenInterest, Ticker, Trade

Event = DepthUpdate | Trade | MarkPrice | Ticker | OpenInterest | Liquidation


class Strategy(Protocol):
    """Strategy callback interface for the backtest engine.

    Strategies should be pure decision logic; market state lives in `EngineContext`.
    """

    def on_start(self, ctx: "EngineContext") -> None: ...

    def on_tick(self, now_ms: int, ctx: "EngineContext") -> None: ...

    def on_event(self, event: Event, ctx: "EngineContext") -> None: ...

    def on_end(self, ctx: "EngineContext") -> None: ...


@dataclass(slots=True)
class EngineConfig:
    tick_interval_ms: int = 1_000
    trading_start_ms: int | None = None
    trading_end_ms: int | None = None
    strict_event_time_monotonic: bool = False
    trading_window_mode: Literal["entry_only", "block_all"] = "entry_only"
    allow_reducing_outside_trading_window: bool = True
    broker_time_mode: Literal["before_event", "after_event"] = "after_event"
    book_guard: BookGuardConfig | None = None
    book_guard_symbol: str | None = None
    emit_final_tick: bool = True


@dataclass(slots=True)
class EngineContext:
    config: EngineConfig
    broker: SimBroker
    books: dict[str, L2Book] = field(default_factory=dict)

    now_ms: int = 0

    # Latest MarkPrice per symbol.
    mark: dict[str, MarkPrice] = field(default_factory=dict)
    ticker: dict[str, Ticker] = field(default_factory=dict)
    open_interest: dict[str, OpenInterest] = field(default_factory=dict)
    liquidation: dict[str, Liquidation] = field(default_factory=dict)

    # Funding bookkeeping.
    _last_funding_applied_ms: dict[str, int] = field(default_factory=dict, init=False, repr=False)

    def is_trading_time(self) -> bool:
        if self.config.trading_start_ms is not None and self.now_ms < self.config.trading_start_ms:
            return False
        if self.config.trading_end_ms is not None and self.now_ms > self.config.trading_end_ms:
            return False
        return True

    def book(self, symbol: str) -> L2Book:
        b = self.books.get(symbol)
        if b is None:
            b = L2Book()
            self.books[symbol] = b
        return b

    def apply_funding_if_due(self, mp: MarkPrice) -> float:
        """Apply funding at funding timestamps when due.

        Returns funding pnl applied (USDT), or 0.0 if nothing happened.
        """

        if mp.next_funding_time_ms <= 0:
            return 0.0

        # Apply at the first mark-price event at/after the funding time. This
        # matches the dataset shape where the funding timestamp appears in
        # `next_funding_time_ms`.
        if mp.event_time_ms < mp.next_funding_time_ms:
            return 0.0

        last_applied = self._last_funding_applied_ms.get(mp.symbol, -1)
        if mp.next_funding_time_ms <= last_applied:
            return 0.0

        self._last_funding_applied_ms[mp.symbol] = mp.next_funding_time_ms
        return self.broker.portfolio.apply_funding(mp.symbol, mp.mark_price, mp.funding_rate)


@dataclass(slots=True)
class BacktestResult:
    ctx: EngineContext


class _TradingWindowBroker:
    """Proxy broker that blocks submits outside the trading window."""

    def __init__(self, inner: SimBroker, ctx: EngineContext) -> None:
        self._inner = inner
        self._ctx = ctx

    @property
    def portfolio(self):
        return self._inner.portfolio

    @property
    def fills(self):
        return self._inner.fills

    def has_open_orders(self) -> bool:
        return self._inner.has_open_orders()

    def on_time(self, now_ms: int) -> None:
        return self._inner.on_time(now_ms)

    def on_depth_update(self, update: DepthUpdate, book: L2Book) -> None:
        return self._inner.on_depth_update(update, book)

    def on_trade(self, trade: Trade, now_ms: int) -> None:
        return self._inner.on_trade(trade, now_ms=now_ms)

    def submit(self, order: Order, book: L2Book, now_ms: int) -> None:
        if order.reduce_only and not self._is_reducing_order(order):
            return
        if self._ctx.is_trading_time():
            return self._inner.submit(order, book, now_ms)
        mode = self._ctx.config.trading_window_mode
        if mode not in ("entry_only", "block_all"):
            raise ValueError("trading_window_mode must be 'entry_only' or 'block_all'")
        if mode == "entry_only" and (not self._ctx.config.allow_reducing_outside_trading_window):
            mode = "block_all"
        if mode == "entry_only" and self._is_reducing_order(order):
            return self._inner.submit(order, book, now_ms)
        return

    def _is_reducing_order(self, order: Order) -> bool:
        pos = self._inner.portfolio.positions.get(order.symbol)
        if pos is None or pos.qty == 0.0:
            return False
        qty = float(order.quantity)
        if qty <= 0.0:
            return False
        if pos.qty > 0.0 and order.side == "sell":
            return qty <= float(pos.qty) + 1e-12
        if pos.qty < 0.0 and order.side == "buy":
            return qty <= abs(float(pos.qty)) + 1e-12
        return False

    def cancel(self, order_id: str, *, now_ms: int | None = None) -> None:
        return self._inner.cancel(order_id, now_ms=now_ms)

    def __getattr__(self, name: str):
        return getattr(self._inner, name)


class BacktestEngine:
    def __init__(self, *, config: EngineConfig, broker: SimBroker | None = None) -> None:
        self.config = config
        self.broker = broker or SimBroker()

    def run(self, events: Iterable[Event], *, strategy: Strategy) -> BacktestResult:
        broker_chain = self.broker
        guard_cfg = self.config.book_guard
        if guard_cfg is not None and bool(guard_cfg.enabled):
            broker_chain = BookGuardedBroker(
                broker_chain,
                symbol=self.config.book_guard_symbol,
                cfg=guard_cfg,
            )

        ctx = EngineContext(config=self.config, broker=broker_chain)
        ctx.broker = _TradingWindowBroker(broker_chain, ctx)

        # Optional methods: let a strategy implement only the hooks it needs.
        on_start = getattr(strategy, "on_start", None)
        on_tick = getattr(strategy, "on_tick", None)
        on_event = getattr(strategy, "on_event", None)
        on_end = getattr(strategy, "on_end", None)

        if callable(on_start):
            on_start(ctx)

        next_tick_ms: int | None = None
        tick_interval = int(self.config.tick_interval_ms or 0)
        broker_time_mode = self.config.broker_time_mode
        if broker_time_mode not in ("before_event", "after_event"):
            raise ValueError("broker_time_mode must be 'before_event' or 'after_event'")
        trading_window_mode = self.config.trading_window_mode
        if trading_window_mode not in ("entry_only", "block_all"):
            raise ValueError("trading_window_mode must be 'entry_only' or 'block_all'")

        def _run_tick(ts: int, *, call_on_time: bool) -> None:
            ctx.now_ms = int(ts)
            if call_on_time:
                ctx.broker.on_time(int(ts))
            on_tick(int(ts), ctx)

        last_event_time_ms: int | None = None

        for ev in events:
            now = int(ev.event_time_ms)
            if last_event_time_ms is not None and now < int(last_event_time_ms):
                if bool(self.config.strict_event_time_monotonic):
                    raise ValueError(
                        f"event_time_ms must be non-decreasing, got {now} after {int(last_event_time_ms)}"
                    )
            last_event_time_ms = now

            # Drive ticks strictly before current event time.
            if tick_interval > 0 and callable(on_tick):
                if next_tick_ms is None:
                    # Anchor ticks to the first observed timestamp.
                    next_tick_ms = now

                while next_tick_ms < now:
                    _run_tick(next_tick_ms, call_on_time=True)
                    next_tick_ms += tick_interval

            ctx.now_ms = now
            if broker_time_mode == "before_event":
                ctx.broker.on_time(now)

            if isinstance(ev, DepthUpdate):
                book = ctx.book(ev.symbol)
                ctx.broker.on_depth_update(ev, book)
            elif isinstance(ev, Trade):
                ctx.broker.on_trade(ev, now_ms=now)
            elif isinstance(ev, MarkPrice):
                ctx.mark[ev.symbol] = ev
                ctx.apply_funding_if_due(ev)
            elif isinstance(ev, Ticker):
                ctx.ticker[ev.symbol] = ev
            elif isinstance(ev, OpenInterest):
                ctx.open_interest[ev.symbol] = ev
            elif isinstance(ev, Liquidation):
                ctx.liquidation[ev.symbol] = ev
            else:
                raise TypeError(f"unsupported event type: {type(ev)}")

            if broker_time_mode == "after_event":
                ctx.broker.on_time(now)

            # If a tick lands exactly on this event timestamp, run it after
            # the event has been applied.
            if tick_interval > 0 and callable(on_tick) and next_tick_ms == now:
                _run_tick(now, call_on_time=False)
                next_tick_ms += tick_interval

            if callable(on_event):
                on_event(ev, ctx)

        # One last tick at the end so strategies can cleanup on grid boundaries.
        if next_tick_ms is not None and callable(on_tick) and bool(self.config.emit_final_tick):
            ctx.now_ms = next_tick_ms
            ctx.broker.on_time(next_tick_ms)
            on_tick(next_tick_ms, ctx)

        if callable(on_end):
            on_end(ctx)

        return BacktestResult(ctx=ctx)
