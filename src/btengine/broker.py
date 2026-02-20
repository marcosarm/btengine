from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field

from .execution.orders import Order
from .execution.queue_model import MakerQueueOrder
from .execution.taker import consume_taker_fill
from .marketdata.orderbook import L2Book
from .portfolio import Portfolio
from .types import DepthUpdate, Trade


def _price_key(price: float) -> int:
    # Normalize float prices into a deterministic key for level indexing.
    return int(round(float(price) * 1_000_000_000.0))


@dataclass(frozen=True, slots=True)
class Fill:
    order_id: str
    symbol: str
    side: str  # "buy" | "sell"
    quantity: float
    price: float
    fee_usdt: float
    event_time_ms: int
    liquidity: str  # "maker" | "taker"


@dataclass(slots=True)
class SimBroker:
    """Minimal broker simulator with:
    - taker fills from book depth
    - maker queue model fills from trade tape
    """

    maker_fee_frac: float = 0.0004
    taker_fee_frac: float = 0.0005

    # Realism knobs.
    submit_latency_ms: int = 0
    cancel_latency_ms: int = 0
    # Conservative taker slippage overlay:
    # executed_px +=/- (abs + bps*px + spread_frac*spread)
    taker_slippage_bps: float = 0.0
    taker_slippage_spread_frac: float = 0.0
    taker_slippage_abs: float = 0.0

    # Conservative maker queue modeling.
    maker_queue_ahead_factor: float = 1.0
    maker_queue_ahead_extra_qty: float = 0.0
    maker_trade_participation: float = 1.0

    portfolio: Portfolio = field(default_factory=Portfolio)
    fills: list[Fill] = field(default_factory=list)

    _maker_orders: dict[str, MakerQueueOrder] = field(default_factory=dict, init=False, repr=False)
    _maker_level_index: dict[tuple[str, str, int], list[str]] = field(default_factory=dict, init=False, repr=False)
    _maker_order_level_key: dict[str, tuple[str, str, int]] = field(default_factory=dict, init=False, repr=False)
    _pending_submits: list[tuple[int, int, Order, L2Book]] = field(default_factory=list, init=False, repr=False)
    _pending_cancels: list[tuple[int, int, str]] = field(default_factory=list, init=False, repr=False)
    _pending_submit_cancel_seq_cutoff: dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _pending_submit_cancel_seq_cutoff_by_symbol: dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _seq: int = field(default=0, init=False, repr=False)
    _maker_seq: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        if float(self.taker_slippage_bps) < 0.0:
            raise ValueError("taker_slippage_bps must be >= 0")
        if float(self.taker_slippage_spread_frac) < 0.0:
            raise ValueError("taker_slippage_spread_frac must be >= 0")
        if float(self.taker_slippage_abs) < 0.0:
            raise ValueError("taker_slippage_abs must be >= 0")

    def on_time(self, now_ms: int) -> None:
        """Advance broker time and process any pending submissions/cancels."""

        now = int(now_ms)

        # Cancels first: if a cancel and submit become due at the same time,
        # treat it as cancel arriving first (conservative).
        while self._pending_cancels and self._pending_cancels[0][0] <= now:
            _, _, order_id = heapq.heappop(self._pending_cancels)
            self._cancel_now(order_id)

        while self._pending_submits and self._pending_submits[0][0] <= now:
            _, submit_seq, order, book = heapq.heappop(self._pending_submits)
            cancel_cutoff = int(self._pending_submit_cancel_seq_cutoff.get(order.id, -1))
            cancel_cutoff_sym = int(self._pending_submit_cancel_seq_cutoff_by_symbol.get(str(order.symbol), -1))
            if int(submit_seq) <= max(cancel_cutoff, cancel_cutoff_sym):
                # Lazy-canceled before activation.
                continue
            self._submit_now(order, book, now)

    def submit(self, order: Order, book: L2Book, now_ms: int) -> None:
        """Submit an order into the simulator.

        When `submit_latency_ms > 0`, orders are queued and activated later via `on_time()`.
        """

        if self.submit_latency_ms and self.submit_latency_ms > 0:
            self._seq += 1
            due = int(now_ms) + int(self.submit_latency_ms)
            heapq.heappush(self._pending_submits, (due, self._seq, order, book))
            return

        self._submit_now(order, book, int(now_ms))

    def _submit_now(self, order: Order, book: L2Book, now_ms: int) -> None:
        if order.order_type == "market":
            self._fill_taker(order, book, now_ms, limit_price=None)
            return

        if order.order_type != "limit":
            raise ValueError(f"unsupported order_type: {order.order_type!r}")
        if order.price is None:
            raise ValueError("limit order requires price")

        limit_px = float(order.price)
        best_bid = book.best_bid()
        best_ask = book.best_ask()

        def crosses() -> bool:
            # Buy crosses if it reaches the ask; sell crosses if it reaches the bid.
            if order.side == "buy":
                return best_ask is not None and limit_px >= float(best_ask)
            return best_bid is not None and limit_px <= float(best_bid)

        if order.post_only:
            # Post-only orders that would execute immediately should be rejected.
            if crosses():
                return
            self._open_maker(order, book)
            return

        # Non-post-only limit: IOC acts as taker up to the limit.
        if order.time_in_force == "IOC":
            self._fill_taker(order, book, now_ms, limit_price=limit_px)
            return

        # GTC limit without post-only:
        # - if it crosses the spread, it should execute immediately (taker)
        # - otherwise, it rests (maker)
        if crosses():
            _, filled_qty = self._fill_taker(order, book, now_ms, limit_price=limit_px)
            remaining = float(order.quantity) - float(filled_qty)
            if remaining > 0.0:
                # In real exchanges, the unfilled portion of a limit order remains on the book.
                self._open_maker(
                    Order(
                        id=order.id,
                        symbol=order.symbol,
                        side=order.side,
                        order_type="limit",
                        quantity=remaining,
                        price=limit_px,
                        time_in_force="GTC",
                        post_only=False,
                        created_time_ms=int(order.created_time_ms),
                    ),
                    book,
                )
            return

        self._open_maker(order, book)

    def _open_maker(self, order: Order, book: L2Book) -> None:
        # Visible qty at the level on our side.
        if order.side == "buy":
            q_ahead = float(book.bids.get(float(order.price), 0.0))
        else:
            q_ahead = float(book.asks.get(float(order.price), 0.0))

        if self.maker_queue_ahead_factor < 0.0:
            raise ValueError("maker_queue_ahead_factor must be >= 0")
        if self.maker_queue_ahead_extra_qty < 0.0:
            raise ValueError("maker_queue_ahead_extra_qty must be >= 0")

        q_ahead = q_ahead * float(self.maker_queue_ahead_factor) + float(self.maker_queue_ahead_extra_qty)

        self._maker_orders[order.id] = MakerQueueOrder(
            symbol=order.symbol,
            side=order.side,
            price=float(order.price),
            quantity=float(order.quantity),
            queue_ahead_qty=q_ahead,
            trade_participation=float(self.maker_trade_participation),
            priority_seq=int(self._maker_seq),
        )
        key = (str(order.symbol), str(order.side), _price_key(float(order.price)))
        self._maker_level_index.setdefault(key, []).append(order.id)
        self._maker_order_level_key[order.id] = key
        self._maker_seq += 1

    def _fill_taker(self, order: Order, book: L2Book, now_ms: int, *, limit_price: float | None) -> tuple[float, float]:
        pre_best_bid = book.best_bid()
        pre_best_ask = book.best_ask()
        avg_px, filled_qty = consume_taker_fill(
            book,
            side=order.side,
            quantity=float(order.quantity),
            limit_price=limit_price,
        )
        if filled_qty <= 0.0 or math.isnan(avg_px):
            return avg_px, 0.0

        exec_px = self._apply_taker_slippage(
            side=str(order.side),
            raw_exec_price=float(avg_px),
            best_bid=pre_best_bid,
            best_ask=pre_best_ask,
            limit_price=limit_price,
        )
        fee = filled_qty * exec_px * self.taker_fee_frac
        self.portfolio.apply_fill(order.symbol, order.side, filled_qty, exec_px, fee_usdt=fee)
        self.fills.append(
            Fill(
                order_id=order.id,
                symbol=order.symbol,
                side=order.side,
                quantity=filled_qty,
                price=exec_px,
                fee_usdt=fee,
                event_time_ms=now_ms,
                liquidity="taker",
            )
        )
        return exec_px, filled_qty

    def _apply_taker_slippage(
        self,
        *,
        side: str,
        raw_exec_price: float,
        best_bid: float | None,
        best_ask: float | None,
        limit_price: float | None,
    ) -> float:
        if raw_exec_price <= 0.0:
            return float(raw_exec_price)

        spread = 0.0
        if best_bid is not None and best_ask is not None and float(best_ask) >= float(best_bid):
            spread = float(best_ask) - float(best_bid)

        price_slippage = (
            float(self.taker_slippage_abs)
            + float(raw_exec_price) * float(self.taker_slippage_bps) / 10_000.0
            + float(spread) * float(self.taker_slippage_spread_frac)
        )
        if price_slippage <= 0.0:
            out = float(raw_exec_price)
        elif str(side) == "buy":
            out = float(raw_exec_price) + float(price_slippage)
        elif str(side) == "sell":
            out = max(0.0, float(raw_exec_price) - float(price_slippage))
        else:
            out = float(raw_exec_price)

        # Preserve limit-order semantics even with conservative overlays:
        # a buy cannot execute above limit and a sell cannot execute below limit.
        if limit_price is not None:
            lp = float(limit_price)
            if str(side) == "buy":
                out = min(float(out), lp)
            elif str(side) == "sell":
                out = max(float(out), lp)

        return float(out)

    def on_depth_update(self, update: DepthUpdate, book: L2Book) -> None:
        # Update book first.
        book.apply_depth_update(update.bid_updates, update.ask_updates)

        # Progress maker queues only for touched levels on the same side.
        for p, q in update.bid_updates:
            self._on_depth_level_qty(symbol=str(update.symbol), maker_side="buy", price=float(p), new_qty=float(q))
        for p, q in update.ask_updates:
            self._on_depth_level_qty(symbol=str(update.symbol), maker_side="sell", price=float(p), new_qty=float(q))

    def on_trade(self, trade: Trade, now_ms: int) -> None:
        # For one trade, only one side/price level can fill makers.
        maker_side = "buy" if bool(trade.is_buyer_maker) else "sell"
        key = (str(trade.symbol), maker_side, _price_key(float(trade.price)))
        bucket = self._maker_level_index.get(key)
        if not bucket:
            return

        remaining_trade_qty = float(trade.quantity)
        active_ids: list[str] = []

        for order_id in bucket:
            mo = self._maker_orders.get(order_id)
            if mo is None:
                continue

            if remaining_trade_qty > 0.0:
                fill_qty, consumed_qty = mo.on_trade_budgeted(trade, max_trade_qty=remaining_trade_qty)
                if consumed_qty > 0.0:
                    remaining_trade_qty = max(0.0, remaining_trade_qty - float(consumed_qty))
                if fill_qty > 0.0:
                    fee = fill_qty * trade.price * self.maker_fee_frac
                    self.portfolio.apply_fill(mo.symbol, mo.side, fill_qty, trade.price, fee_usdt=fee)
                    self.fills.append(
                        Fill(
                            order_id=order_id,
                            symbol=mo.symbol,
                            side=mo.side,
                            quantity=fill_qty,
                            price=trade.price,
                            fee_usdt=fee,
                            event_time_ms=now_ms,
                            liquidity="maker",
                        )
                    )

            if mo.is_filled():
                self._maker_orders.pop(order_id, None)
                self._maker_order_level_key.pop(order_id, None)
            else:
                active_ids.append(order_id)

        if active_ids:
            self._maker_level_index[key] = active_ids
        else:
            self._maker_level_index.pop(key, None)

    def cancel(self, order_id: str, *, now_ms: int | None = None) -> None:
        """Cancel an open maker order.

        If `cancel_latency_ms > 0` and `now_ms` is provided, cancellation will be delayed
        and applied via `on_time()`.
        """

        if self.cancel_latency_ms and self.cancel_latency_ms > 0 and now_ms is not None:
            self._seq += 1
            due = int(now_ms) + int(self.cancel_latency_ms)
            heapq.heappush(self._pending_cancels, (due, self._seq, order_id))
            return

        self._cancel_now(order_id)

    def _cancel_now(self, order_id: str) -> None:
        self._maker_orders.pop(order_id, None)
        self._remove_order_from_level_index(order_id)
        # Also cancel any order that has been submitted but not yet activated.
        # This is lazy: when pending submits are popped in `on_time`, entries
        # with submit_seq <= cutoff for this order_id are discarded.
        self._pending_submit_cancel_seq_cutoff[order_id] = max(
            int(self._pending_submit_cancel_seq_cutoff.get(order_id, -1)),
            int(self._seq),
        )

    def cancel_symbol_orders(
        self,
        symbol: str,
        *,
        cancel_active_makers: bool = True,
        cancel_pending_submits: bool = True,
    ) -> None:
        """Cancel broker state tied to one symbol.

        - `cancel_active_makers`: remove active maker orders for the symbol.
        - `cancel_pending_submits`: lazily cancel pending submits for the symbol
          that were enqueued up to the current sequence watermark.
        """

        sym = str(symbol)

        if cancel_active_makers:
            for order_id, mo in list(self._maker_orders.items()):
                if str(mo.symbol) == sym:
                    self._cancel_now(order_id)

        if cancel_pending_submits:
            self._pending_submit_cancel_seq_cutoff_by_symbol[sym] = max(
                int(self._pending_submit_cancel_seq_cutoff_by_symbol.get(sym, -1)),
                int(self._seq),
            )

    def has_pending_orders(self, symbol: str | None = None) -> bool:
        """Return whether there are pending submits not canceled yet."""

        if symbol is None:
            for _, submit_seq, order, _ in self._pending_submits:
                cancel_cutoff = int(self._pending_submit_cancel_seq_cutoff.get(order.id, -1))
                cancel_cutoff_sym = int(self._pending_submit_cancel_seq_cutoff_by_symbol.get(str(order.symbol), -1))
                if int(submit_seq) > max(cancel_cutoff, cancel_cutoff_sym):
                    return True
            return False

        sym = str(symbol)
        for _, submit_seq, order, _ in self._pending_submits:
            if str(order.symbol) != sym:
                continue
            cancel_cutoff = int(self._pending_submit_cancel_seq_cutoff.get(order.id, -1))
            cancel_cutoff_sym = int(self._pending_submit_cancel_seq_cutoff_by_symbol.get(sym, -1))
            if int(submit_seq) > max(cancel_cutoff, cancel_cutoff_sym):
                return True
        return False

    def _on_depth_level_qty(self, *, symbol: str, maker_side: str, price: float, new_qty: float) -> None:
        key = (str(symbol), str(maker_side), _price_key(float(price)))
        bucket = self._maker_level_index.get(key)
        if not bucket:
            return

        active_ids: list[str] = []
        for order_id in bucket:
            mo = self._maker_orders.get(order_id)
            if mo is None:
                continue
            mo.on_book_qty_update(float(new_qty))
            if mo.is_filled():
                self._maker_orders.pop(order_id, None)
                self._maker_order_level_key.pop(order_id, None)
                continue
            active_ids.append(order_id)

        if active_ids:
            self._maker_level_index[key] = active_ids
        else:
            self._maker_level_index.pop(key, None)

    def _remove_order_from_level_index(self, order_id: str) -> None:
        key = self._maker_order_level_key.pop(order_id, None)
        if key is None:
            return
        bucket = self._maker_level_index.get(key)
        if not bucket:
            return
        out = [oid for oid in bucket if oid != order_id]
        if out:
            self._maker_level_index[key] = out
        else:
            self._maker_level_index.pop(key, None)

    def has_open_orders(self) -> bool:
        return bool(self._maker_orders) or self.has_pending_orders()
