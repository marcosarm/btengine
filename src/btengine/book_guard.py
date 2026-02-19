from __future__ import annotations

from dataclasses import dataclass, field

from .broker import SimBroker
from .execution.orders import Order
from .marketdata.orderbook import L2Book
from .types import DepthUpdate, Trade


def _reset_book_in_place(book: L2Book) -> None:
    # Clear heaps and presence sets to avoid stale best-level artifacts.
    book.bids.clear()
    book.asks.clear()
    book._bid_heap.clear()  # type: ignore[attr-defined]
    book._ask_heap.clear()  # type: ignore[attr-defined]
    if hasattr(book, "_bid_present"):
        book._bid_present.clear()  # type: ignore[attr-defined]
    if hasattr(book, "_ask_present"):
        book._ask_present.clear()  # type: ignore[attr-defined]


@dataclass(slots=True)
class BookGuardConfig:
    enabled: bool = False
    max_spread: float | None = None
    max_spread_bps: float | None = 5.0
    cooldown_ms: int = 1_000
    warmup_depth_updates: int = 1_000
    max_staleness_ms: int = 500
    reset_on_mismatch: bool = True
    reset_on_crossed: bool = True
    reset_on_missing_side: bool = False
    reset_on_spread: bool = False
    reset_on_stale: bool = False


@dataclass(slots=True)
class BookGuardStats:
    resets: int = 0
    mismatch_trips: int = 0
    cross_trips: int = 0
    missing_side_trips: int = 0
    spread_trips: int = 0
    stale_trips: int = 0

    blocked_submits: int = 0
    blocked_submit_reason: dict[str, int] = field(default_factory=dict)


class BookGuardedBroker:
    """SimBroker wrapper that blocks submits when the local book looks invalid.

    When `symbol` is provided, guard checks apply only to that symbol.
    """

    def __init__(self, inner: SimBroker, *, symbol: str | None, cfg: BookGuardConfig) -> None:
        self.inner = inner
        self.symbol = (str(symbol) if symbol is not None else None)
        self.cfg = cfg
        self.stats = BookGuardStats()

        self._blocked_until_ms: dict[str, int] = {}
        self._warmup_remaining: dict[str, int] = {}
        self._last_final_update_id: dict[str, int] = {}
        self._last_depth_event_ms: dict[str, int] = {}

    @property
    def portfolio(self):
        return self.inner.portfolio

    @property
    def fills(self):
        return self.inner.fills

    def has_open_orders(self) -> bool:
        return self.inner.has_open_orders()

    def on_time(self, now_ms: int) -> None:
        return self.inner.on_time(now_ms)

    def cancel(self, order_id: str, *, now_ms: int | None = None) -> None:
        return self.inner.cancel(order_id, now_ms=now_ms)

    def on_trade(self, trade: Trade, now_ms: int) -> None:
        return self.inner.on_trade(trade, now_ms=now_ms)

    def _symbol_applies(self, symbol: str) -> bool:
        if self.symbol is None:
            return True
        return str(symbol) == self.symbol

    def _trip(self, book: L2Book, *, symbol: str, now_ms: int, reason: str) -> None:
        sym = str(symbol)
        if int(self.cfg.cooldown_ms or 0) > 0:
            self._blocked_until_ms[sym] = max(int(self._blocked_until_ms.get(sym, 0)), int(now_ms) + int(self.cfg.cooldown_ms))
        if int(self.cfg.warmup_depth_updates or 0) > 0:
            self._warmup_remaining[sym] = max(
                int(self._warmup_remaining.get(sym, 0)),
                int(self.cfg.warmup_depth_updates),
            )

        reset = False
        if reason == "mismatch":
            reset = bool(self.cfg.reset_on_mismatch)
        elif reason == "crossed":
            reset = bool(self.cfg.reset_on_crossed)
        elif reason == "missing_side":
            reset = bool(self.cfg.reset_on_missing_side)
        elif reason == "spread":
            reset = bool(self.cfg.reset_on_spread)
        elif reason == "stale":
            reset = bool(self.cfg.reset_on_stale)

        if reset:
            _reset_book_in_place(book)
            # Clear maker orders tied to this symbol.
            try:
                for oid, mo in list(self.inner._maker_orders.items()):  # type: ignore[attr-defined]
                    if str(getattr(mo, "symbol", "")) == sym:
                        self.inner._maker_orders.pop(oid, None)  # type: ignore[attr-defined]
            except Exception:
                pass
            self.stats.resets += 1

    def on_depth_update(self, update: DepthUpdate, book: L2Book) -> None:
        if self.cfg.enabled and self._symbol_applies(update.symbol):
            sym = str(update.symbol)
            self._last_depth_event_ms[sym] = int(update.event_time_ms)

            warmup = int(self._warmup_remaining.get(sym, 0))
            if warmup > 0:
                self._warmup_remaining[sym] = warmup - 1

            last_final = self._last_final_update_id.get(sym)
            if last_final is not None and int(update.prev_final_update_id) != int(last_final):
                self.stats.mismatch_trips += 1
                self._trip(book, symbol=sym, now_ms=int(update.event_time_ms), reason="mismatch")

            self._last_final_update_id[sym] = int(update.final_update_id)

        self.inner.on_depth_update(update, book)

        # Also trip on crossed book during depth updates (not only on submit).
        if self.cfg.enabled and self._symbol_applies(update.symbol):
            bid = book.best_bid()
            ask = book.best_ask()
            if bid is not None and ask is not None and float(bid) >= float(ask):
                self.stats.cross_trips += 1
                self._trip(book, symbol=str(update.symbol), now_ms=int(update.event_time_ms), reason="crossed")

    def submit(self, order: Order, book: L2Book, now_ms: int) -> None:
        if (not self.cfg.enabled) or (not self._symbol_applies(order.symbol)):
            return self.inner.submit(order, book, now_ms)

        sym = str(order.symbol)
        now = int(now_ms)

        blocked_until = int(self._blocked_until_ms.get(sym, 0))
        if now < blocked_until:
            self.stats.blocked_submits += 1
            self.stats.blocked_submit_reason["cooldown"] = self.stats.blocked_submit_reason.get("cooldown", 0) + 1
            return

        warmup = int(self._warmup_remaining.get(sym, 0))
        if warmup > 0:
            self.stats.blocked_submits += 1
            self.stats.blocked_submit_reason["warmup"] = self.stats.blocked_submit_reason.get("warmup", 0) + 1
            return

        if int(self.cfg.max_staleness_ms or 0) > 0:
            last_depth_ms = self._last_depth_event_ms.get(sym)
            if last_depth_ms is None or (now - int(last_depth_ms)) > int(self.cfg.max_staleness_ms):
                self.stats.blocked_submits += 1
                self.stats.blocked_submit_reason["stale"] = self.stats.blocked_submit_reason.get("stale", 0) + 1
                self.stats.stale_trips += 1
                self._trip(book, symbol=sym, now_ms=now, reason="stale")
                return

        bid = book.best_bid()
        ask = book.best_ask()
        if bid is None or ask is None:
            self.stats.blocked_submits += 1
            self.stats.blocked_submit_reason["missing_side"] = self.stats.blocked_submit_reason.get("missing_side", 0) + 1
            self.stats.missing_side_trips += 1
            self._trip(book, symbol=sym, now_ms=now, reason="missing_side")
            return

        if float(bid) >= float(ask):
            self.stats.blocked_submits += 1
            self.stats.blocked_submit_reason["crossed"] = self.stats.blocked_submit_reason.get("crossed", 0) + 1
            self.stats.cross_trips += 1
            self._trip(book, symbol=sym, now_ms=now, reason="crossed")
            return

        spread = float(ask) - float(bid)
        if self.cfg.max_spread is not None and spread > float(self.cfg.max_spread):
            self.stats.blocked_submits += 1
            self.stats.blocked_submit_reason["spread"] = self.stats.blocked_submit_reason.get("spread", 0) + 1
            self.stats.spread_trips += 1
            self._trip(book, symbol=sym, now_ms=now, reason="spread")
            return

        if self.cfg.max_spread_bps is not None:
            mid = (float(ask) + float(bid)) / 2.0
            if mid > 0.0:
                spread_bps = (spread / mid) * 10_000.0
                if spread_bps > float(self.cfg.max_spread_bps):
                    self.stats.blocked_submits += 1
                    self.stats.blocked_submit_reason["spread"] = self.stats.blocked_submit_reason.get("spread", 0) + 1
                    self.stats.spread_trips += 1
                    self._trip(book, symbol=sym, now_ms=now, reason="spread")
                    return

        return self.inner.submit(order, book, now_ms)

    def __getattr__(self, name: str):
        # Delegate any other methods/attrs to the underlying SimBroker.
        return getattr(self.inner, name)
