from __future__ import annotations

import heapq
import math

from ..marketdata.orderbook import L2Book
from ..types import Side


def simulate_taker_fill(
    book: L2Book,
    side: Side,
    quantity: float,
    *,
    limit_price: float | None = None,
) -> tuple[float, float]:
    """Simulate a taker fill using L2 book depth.

    Returns (avg_price, filled_qty).
    - For `side="buy"` consumes asks from low to high.
    - For `side="sell"` consumes bids from high to low.
    - If `limit_price` is provided, the fill will not cross it (IOC-like).
    """

    if quantity <= 0:
        raise ValueError("quantity must be > 0")

    if side == "buy":
        price_heap = [float(p) for p, q in book.asks.items() if float(q) > 0.0]
        heapq.heapify(price_heap)
        side_map = book.asks

        def next_price() -> float | None:
            if not price_heap:
                return None
            return float(heapq.heappop(price_heap))

        def crosses(p: float) -> bool:
            return limit_price is not None and p > limit_price
    elif side == "sell":
        price_heap = [-float(p) for p, q in book.bids.items() if float(q) > 0.0]
        heapq.heapify(price_heap)
        side_map = book.bids

        def next_price() -> float | None:
            if not price_heap:
                return None
            return -float(heapq.heappop(price_heap))

        def crosses(p: float) -> bool:
            return limit_price is not None and p < limit_price
    else:
        raise ValueError(f"invalid side: {side!r}")

    remaining = float(quantity)
    filled = 0.0
    cost = 0.0

    while remaining > 0.0:
        price = next_price()
        if price is None:
            break
        lvl_qty = float(side_map.get(float(price), 0.0))
        if remaining <= 0:
            break
        if lvl_qty <= 0:
            continue
        if crosses(price):
            break

        take = lvl_qty if lvl_qty <= remaining else remaining
        filled += take
        cost += take * price
        remaining -= take

    if filled <= 0:
        return math.nan, 0.0

    return cost / filled, filled


def consume_taker_fill(
    book: L2Book,
    side: Side,
    quantity: float,
    *,
    limit_price: float | None = None,
    eps_qty: float = 1e-12,
) -> tuple[float, float]:
    """Simulate a taker fill and apply self-impact to the in-memory `book`.

    This is identical to `simulate_taker_fill(...)` but mutates `book` by
    decrementing quantities on the opposite side for the consumed levels.

    Implementation note:
    - Uses `best_bid()`/`best_ask()` iteratively, so consumption cost is closer
      to O(k log n) for k consumed levels (instead of sorting the full side).
    """

    if quantity <= 0:
        raise ValueError("quantity must be > 0")

    remaining = float(quantity)
    filled = 0.0
    cost = 0.0

    if side == "buy":
        def crosses(p: float) -> bool:
            return limit_price is not None and p > limit_price

        best_level = book.best_ask
        side_map = book.asks
        book_side = "ask"

    elif side == "sell":
        def crosses(p: float) -> bool:
            return limit_price is not None and p < limit_price

        best_level = book.best_bid
        side_map = book.bids
        book_side = "bid"

    else:
        raise ValueError(f"invalid side: {side!r}")

    while remaining > 0.0:
        price = best_level()
        if price is None:
            break
        if crosses(price):
            break

        lvl_qty = float(side_map.get(float(price), 0.0))
        if lvl_qty <= eps_qty:
            side_map.pop(float(price), None)
            continue

        take = lvl_qty if lvl_qty <= remaining else remaining
        filled += take
        cost += take * price
        remaining -= take

        new_qty = float(lvl_qty) - float(take)
        if new_qty <= eps_qty:
            book.apply_level(book_side, float(price), 0.0)
        else:
            book.apply_level(book_side, float(price), new_qty)

    if filled <= 0:
        return math.nan, 0.0

    return cost / filled, filled
