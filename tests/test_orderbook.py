import math
import builtins

from btengine.execution.taker import simulate_taker_fill
from btengine.marketdata import L2Book


def test_best_bid_ask_and_mid():
    book = L2Book()
    book.apply_depth_update(bid_updates=[(100.0, 1.0)], ask_updates=[(101.0, 2.0)])

    assert book.best_bid() == 100.0
    assert book.best_ask() == 101.0
    assert book.mid_price() == 100.5


def test_impact_vwap_partial_fill():
    book = L2Book()
    # Asks: 1 @ 100, 1 @ 101
    book.apply_depth_update(bid_updates=[], ask_updates=[(100.0, 1.0), (101.0, 1.0)])

    # Buy notional 150 => take 1 @ 100 (=100) + 0.4950495 @ 101 (=50)
    vwap = book.impact_vwap("buy", 150.0, max_levels=10)
    assert not math.isnan(vwap)
    assert abs(vwap - (150.0 / (1.0 + 50.0 / 101.0))) < 1e-9


def test_impact_vwap_insufficient_depth_returns_nan():
    book = L2Book()
    book.apply_depth_update(bid_updates=[], ask_updates=[(100.0, 0.5)])
    vwap = book.impact_vwap("buy", 100.0, max_levels=10)
    assert math.isnan(vwap)


def test_impact_vwap_retries_with_full_depth_when_max_levels_limits():
    book = L2Book()
    # Asks: 1 @ 100, 1 @ 101, 1 @ 102
    book.apply_depth_update(bid_updates=[], ask_updates=[(100.0, 1.0), (101.0, 1.0), (102.0, 1.0)])

    # With max_levels=1 the first level is insufficient, but full depth is enough.
    vwap = book.impact_vwap("buy", 150.0, max_levels=1)
    assert not math.isnan(vwap)
    assert abs(vwap - (150.0 / (1.0 + 50.0 / 101.0))) < 1e-9


def test_simulate_taker_fill_does_not_use_global_sorted(monkeypatch):
    book = L2Book()
    book.apply_depth_update(bid_updates=[(99.0, 1.0)], ask_updates=[(100.0, 1.0), (101.0, 1.0)])

    def _fail_sorted(*args, **kwargs):
        raise AssertionError("simulate_taker_fill should not call sorted()")

    monkeypatch.setattr(builtins, "sorted", _fail_sorted)

    avg, qty = simulate_taker_fill(book, side="buy", quantity=1.5)
    assert abs(qty - 1.5) < 1e-12
    assert abs(avg - (100.0 * 1.0 + 101.0 * 0.5) / 1.5) < 1e-12


def test_orderbook_repeated_level_updates_do_not_duplicate_heap_entries():
    book = L2Book()

    for _ in range(10_000):
        book.apply_level("ask", 101.0, 1.0)
    assert len(book._ask_heap) == 1

    # Remove and re-add the same level repeatedly.
    for _ in range(1_000):
        book.apply_level("ask", 101.0, 0.0)
        book.apply_level("ask", 101.0, 1.0)
    assert len(book._ask_heap) == 1
    assert book.best_ask() == 101.0
