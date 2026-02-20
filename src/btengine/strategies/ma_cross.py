from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from ..engine import EngineContext
from ..execution.orders import Order
from ..types import MarkPrice, Trade


@dataclass(slots=True)
class Bar:
    start_ms: int
    open: float
    high: float
    low: float
    close: float


@dataclass(slots=True)
class BarBuilder:
    """Timeframe bar builder based on incoming prices.

    A bar is considered "closed" when we observe the first tick of the next bar.
    """

    tf_ms: int
    fill_missing: bool = False

    _bar_id: int | None = None
    _bar: Bar | None = None

    def on_price(self, t_ms: int, price: float) -> list[Bar]:
        t = int(t_ms)
        p = float(price)
        if self.tf_ms <= 0:
            raise ValueError("tf_ms must be > 0")

        bid = t // int(self.tf_ms)
        closed: list[Bar] = []

        if self._bar_id is None:
            self._bar_id = bid
            start = bid * int(self.tf_ms)
            self._bar = Bar(start_ms=int(start), open=p, high=p, low=p, close=p)
            return closed

        assert self._bar is not None
        if bid == self._bar_id:
            b = self._bar
            b.high = max(b.high, p)
            b.low = min(b.low, p)
            b.close = p
            return closed

        # New bar(s) started; close current.
        closed.append(self._bar)

        # Fill missing bars if requested (repeat last close).
        if self.fill_missing and bid > int(self._bar_id) + 1:
            last_close = float(self._bar.close)
            for mid in range(int(self._bar_id) + 1, int(bid)):
                start = mid * int(self.tf_ms)
                closed.append(
                    Bar(
                        start_ms=int(start),
                        open=last_close,
                        high=last_close,
                        low=last_close,
                        close=last_close,
                    )
                )

        # Start new bar with current tick.
        self._bar_id = bid
        start = bid * int(self.tf_ms)
        self._bar = Bar(start_ms=int(start), open=p, high=p, low=p, close=p)
        return closed


@dataclass(slots=True)
class MaCrossStrategy:
    symbol: str
    qty: float
    tf_ms: int = 300_000  # 5m
    ma_len: int = 9
    rule: Literal["cross", "state"] = "cross"  # cross = only trade on cross, state = always target side
    mode: Literal["long_short", "long_only"] = "long_short"
    price_source: Literal["mark", "trade"] = "mark"
    fill_missing_bars: bool = False
    eps_qty: float = 1e-12

    bars: list[Bar] = field(default_factory=list)
    closes: list[float] = field(default_factory=list)
    prev_diff: float | None = None

    equity_curve: list[tuple[int, float]] = field(default_factory=list)

    _bar_builder: BarBuilder | None = None

    def on_start(self, ctx: EngineContext) -> None:
        if self.qty <= 0:
            raise ValueError("qty must be > 0")
        if self.ma_len <= 0:
            raise ValueError("ma_len must be > 0")
        self._bar_builder = BarBuilder(tf_ms=int(self.tf_ms), fill_missing=bool(self.fill_missing_bars))

    def _pos_qty(self, ctx: EngineContext) -> float:
        p = ctx.broker.portfolio.positions.get(self.symbol)
        return float(p.qty) if p is not None else 0.0

    def _ensure_book_ready(self, ctx: EngineContext) -> bool:
        book = ctx.books.get(self.symbol)
        if book is None:
            return False
        return book.best_bid() is not None and book.best_ask() is not None

    def _set_target(self, ctx: EngineContext, *, target_qty: float, reason: str) -> None:
        if not self._ensure_book_ready(ctx):
            return
        cur = self._pos_qty(ctx)
        delta = float(target_qty) - float(cur)
        if abs(delta) <= float(self.eps_qty):
            return

        side = "buy" if delta > 0.0 else "sell"
        q = abs(delta)
        book = ctx.books[self.symbol]
        ctx.broker.submit(
            Order(
                id=f"ma_{reason}_{int(ctx.now_ms)}",
                symbol=self.symbol,
                side=side,
                order_type="market",
                quantity=q,
            ),
            book,
            now_ms=int(ctx.now_ms),
        )

    def _on_closed_bar(self, b: Bar, ctx: EngineContext) -> None:
        self.bars.append(b)
        self.closes.append(float(b.close))

        if len(self.closes) < int(self.ma_len):
            return

        w = self.closes[-int(self.ma_len) :]
        ma = sum(w) / float(len(w))
        diff = float(b.close) - float(ma)

        # Decide desired direction.
        desired: Literal["long", "short", "flat"] | None = None
        if self.rule == "state":
            desired = "long" if diff >= 0.0 else "short"
        else:  # cross
            if self.prev_diff is not None:
                if self.prev_diff <= 0.0 and diff > 0.0:
                    desired = "long"
                elif self.prev_diff >= 0.0 and diff < 0.0:
                    desired = "short"
            else:
                # First eligible bar: pick a side, but it's still conservative
                # because it only uses completed history.
                desired = "long" if diff > 0.0 else ("short" if diff < 0.0 else None)

        self.prev_diff = diff

        if desired is None:
            return
        if self.mode == "long_only" and desired == "short":
            desired = "flat"

        if desired == "long":
            self._set_target(ctx, target_qty=float(self.qty), reason="long")
        elif desired == "short":
            self._set_target(ctx, target_qty=-float(self.qty), reason="short")
        else:  # flat
            self._set_target(ctx, target_qty=0.0, reason="flat")

    def on_event(self, event: object, ctx: EngineContext) -> None:
        # Equity curve sampled on mark price.
        if isinstance(event, MarkPrice) and event.symbol == self.symbol:
            p = ctx.broker.portfolio.positions.get(self.symbol)
            unreal = 0.0
            if p is not None and p.qty != 0.0:
                unreal = float(p.qty) * (float(event.mark_price) - float(p.avg_price))
            eq = float(ctx.broker.portfolio.realized_pnl_usdt) + unreal
            self.equity_curve.append((int(event.event_time_ms), float(eq)))

        # Bar aggregation.
        if self._bar_builder is None:
            return

        if self.price_source == "mark":
            if not isinstance(event, MarkPrice) or event.symbol != self.symbol:
                return
            t_ms = int(event.event_time_ms)
            price = float(event.mark_price)
        else:  # trade
            if not isinstance(event, Trade) or event.symbol != self.symbol:
                return
            t_ms = int(event.event_time_ms)
            price = float(event.price)

        closed = self._bar_builder.on_price(t_ms, price)
        for b in closed:
            self._on_closed_bar(b, ctx)

    def on_end(self, ctx: EngineContext) -> None:
        # Force-flat at end (optional semantics: if mode is long_short, still go flat at end).
        self._set_target(ctx, target_qty=0.0, reason="end")

