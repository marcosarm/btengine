from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from ..engine import EngineContext
from ..execution.orders import Order
from ..types import DepthUpdate, MarkPrice


@dataclass(slots=True)
class EntryExitStrategy:
    symbol: str
    direction: Literal["long", "short"]
    target_qty: float
    schedule_ms: list[tuple[int, int]]  # [(enter_ms, exit_ms), ...]
    force_close_on_end: bool = True

    # Internal state.
    _cycle: int = 0
    _in_position: bool = False
    equity_curve: list[tuple[int, float]] = field(default_factory=list)

    def _pos_qty(self, ctx: EngineContext) -> float:
        p = ctx.broker.portfolio.positions.get(self.symbol)
        return float(p.qty) if p is not None else 0.0

    def _close_qty(self, ctx: EngineContext) -> float:
        return abs(self._pos_qty(ctx))

    def _submit_entry(self, ctx: EngineContext) -> None:
        side = "buy" if self.direction == "long" else "sell"
        book = ctx.book(self.symbol)
        ctx.broker.submit(
            Order(
                id=f"entry_{self._cycle}",
                symbol=self.symbol,
                side=side,
                order_type="market",
                quantity=float(self.target_qty),
            ),
            book,
            now_ms=int(ctx.now_ms),
        )

        # Market fills are immediate when there is depth.
        self._in_position = (self._pos_qty(ctx) != 0.0)

    def _submit_exit(self, ctx: EngineContext) -> None:
        q = self._close_qty(ctx)
        if q <= 0.0:
            self._in_position = False
            return

        side = "sell" if self._pos_qty(ctx) > 0.0 else "buy"
        book = ctx.book(self.symbol)
        ctx.broker.submit(
            Order(
                id=f"exit_{self._cycle}",
                symbol=self.symbol,
                side=side,
                order_type="market",
                quantity=float(q),
            ),
            book,
            now_ms=int(ctx.now_ms),
        )
        self._in_position = (self._pos_qty(ctx) != 0.0)

    def on_event(self, event: object, ctx: EngineContext) -> None:
        # Equity curve (PnL) sampled on mark price.
        if isinstance(event, MarkPrice) and event.symbol == self.symbol:
            p = ctx.broker.portfolio.positions.get(self.symbol)
            unreal = 0.0
            if p is not None and p.qty != 0.0:
                unreal = float(p.qty) * (float(event.mark_price) - float(p.avg_price))
            eq = float(ctx.broker.portfolio.realized_pnl_usdt) + unreal
            self.equity_curve.append((int(event.event_time_ms), float(eq)))
            return

        if not isinstance(event, DepthUpdate) or event.symbol != self.symbol:
            return

        if self._cycle >= len(self.schedule_ms):
            return

        enter_ms, exit_ms = self.schedule_ms[self._cycle]
        now = int(ctx.now_ms)

        # Wait until book is formed.
        book = ctx.book(self.symbol)
        if book.best_bid() is None or book.best_ask() is None:
            return

        if not self._in_position and now >= int(enter_ms):
            self._submit_entry(ctx)
            return

        if self._in_position and now >= int(exit_ms):
            self._submit_exit(ctx)
            if not self._in_position:
                self._cycle += 1

    def on_end(self, ctx: EngineContext) -> None:
        if not self.force_close_on_end:
            return
        if self._close_qty(ctx) <= 0.0:
            return
        self._submit_exit(ctx)

