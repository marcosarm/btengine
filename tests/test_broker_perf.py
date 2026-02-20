from __future__ import annotations

import time

from btengine.broker import SimBroker
from btengine.execution.orders import Order
from btengine.marketdata import L2Book
from btengine.types import DepthUpdate, Trade


def _setup_broker_with_unique_makers(n_orders: int) -> tuple[SimBroker, L2Book, float]:
    book = L2Book()
    # Keep ask far enough so post-only bid orders never cross.
    book.apply_level("ask", 10_000.0, 1.0)

    # Build unique bid levels so only one maker sits on each price.
    px0 = 100.0
    step = 0.01
    for i in range(n_orders):
        p = px0 - float(i) * step
        # Large visible qty keeps queue-ahead high and avoids immediate fills.
        book.apply_level("bid", p, 1_000_000.0)

    broker = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0)
    for i in range(n_orders):
        p = px0 - float(i) * step
        broker.submit(
            Order(
                id=f"m{i}",
                symbol="BTCUSDT",
                side="buy",
                order_type="limit",
                quantity=1.0,
                price=p,
                post_only=True,
            ),
            book,
            now_ms=0,
        )
    return broker, book, px0


def _bench_on_depth(n_orders: int, n_updates: int) -> float:
    broker, book, px = _setup_broker_with_unique_makers(n_orders)
    t0 = time.perf_counter()
    for i in range(n_updates):
        broker.on_depth_update(
            DepthUpdate(
                received_time_ns=0,
                event_time_ms=i + 1,
                transaction_time_ms=i + 1,
                symbol="BTCUSDT",
                first_update_id=i + 1,
                final_update_id=i + 1,
                prev_final_update_id=i,
                bid_updates=[(px, 1_000_000.0)],
                ask_updates=[],
            ),
            book,
        )
    return time.perf_counter() - t0


def _bench_on_trade(n_orders: int, n_trades: int) -> float:
    broker, _book, px = _setup_broker_with_unique_makers(n_orders)
    t0 = time.perf_counter()
    for i in range(n_trades):
        broker.on_trade(
            Trade(
                received_time_ns=0,
                event_time_ms=i + 1,
                trade_time_ms=i + 1,
                symbol="BTCUSDT",
                trade_id=i + 1,
                price=px,
                quantity=1.0,
                is_buyer_maker=True,  # sell aggressor hits bids
            ),
            now_ms=i + 1,
        )
    return time.perf_counter() - t0


def test_broker_on_depth_update_scales_sublinearly_with_total_makers():
    # Only one level is touched on each update. Runtime should not blow up
    # with total makers after indexed dispatch by (symbol, side, price).
    small = _bench_on_depth(n_orders=200, n_updates=1_500)
    large = _bench_on_depth(n_orders=4_000, n_updates=1_500)
    ratio = large / max(small, 1e-9)
    assert ratio < 8.0


def test_broker_on_trade_scales_sublinearly_with_total_makers():
    # Trade is always at one price level; matching should be bucket-local.
    small = _bench_on_trade(n_orders=200, n_trades=1_500)
    large = _bench_on_trade(n_orders=4_000, n_trades=1_500)
    ratio = large / max(small, 1e-9)
    assert ratio < 8.0

