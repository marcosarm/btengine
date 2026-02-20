from __future__ import annotations

from btengine.book_guard import BookGuardConfig, BookGuardedBroker
from btengine.broker import SimBroker
from btengine.execution.orders import Order
from btengine.marketdata import L2Book
from btengine.types import DepthUpdate


def test_book_guarded_broker_blocks_submit_when_book_missing_side():
    inner = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0)
    guard = BookGuardedBroker(
        inner,
        symbol="BTCUSDT",
        cfg=BookGuardConfig(
            enabled=True,
            max_staleness_ms=0,
            cooldown_ms=0,
            warmup_depth_updates=0,
            max_spread_bps=None,
        ),
    )

    # No bid/ask in the book -> submit should be blocked.
    book = L2Book()
    guard.submit(
        Order(id="m1", symbol="BTCUSDT", side="buy", order_type="market", quantity=0.1),
        book,
        now_ms=1_000,
    )

    assert guard.stats.blocked_submits == 1
    assert guard.stats.blocked_submit_reason.get("missing_side", 0) == 1
    assert inner.portfolio.positions.get("BTCUSDT") is None


def test_book_guarded_broker_resets_book_on_prev_final_id_mismatch():
    inner = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0)
    guard = BookGuardedBroker(
        inner,
        symbol="BTCUSDT",
        cfg=BookGuardConfig(
            enabled=True,
            cooldown_ms=0,
            warmup_depth_updates=0,
            max_staleness_ms=0,
            max_spread_bps=None,
            reset_on_mismatch=True,
        ),
    )

    book = L2Book()

    # First update establishes continuity and both sides in the book.
    guard.on_depth_update(
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=1_000,
            transaction_time_ms=1_000,
            symbol="BTCUSDT",
            first_update_id=1,
            final_update_id=10,
            prev_final_update_id=9,
            bid_updates=[(99.0, 1.0)],
            ask_updates=[(101.0, 1.0)],
        ),
        book,
    )
    assert book.best_ask() == 101.0

    # Mismatch: prev_final_update_id should be 10 but we provide 999.
    # With reset_on_mismatch=True, previous levels are cleared before applying this update.
    guard.on_depth_update(
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=1_100,
            transaction_time_ms=1_100,
            symbol="BTCUSDT",
            first_update_id=11,
            final_update_id=11,
            prev_final_update_id=999,
            bid_updates=[(98.0, 2.0)],
            ask_updates=[],
        ),
        book,
    )

    assert guard.stats.mismatch_trips == 1
    assert guard.stats.resets == 1
    assert book.best_bid() == 98.0
    assert book.best_ask() is None
