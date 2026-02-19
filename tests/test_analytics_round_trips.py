from btengine.analytics import max_drawdown, round_trips_from_fills, summarize_round_trips
from btengine.broker import Fill
from btengine.portfolio import Portfolio


def test_round_trips_from_fills_single_long_round_trip():
    fills = [
        Fill(
            order_id="entry",
            symbol="BTCUSDT",
            side="buy",
            quantity=1.0,
            price=100.0,
            fee_usdt=0.0,
            event_time_ms=0,
            liquidity="taker",
        ),
        Fill(
            order_id="exit",
            symbol="BTCUSDT",
            side="sell",
            quantity=1.0,
            price=110.0,
            fee_usdt=0.0,
            event_time_ms=1_000,
            liquidity="taker",
        ),
    ]

    trades = round_trips_from_fills(fills)
    assert len(trades) == 1
    t = trades[0]
    assert t.symbol == "BTCUSDT"
    assert t.direction == "long"
    assert t.open_time_ms == 0
    assert t.close_time_ms == 1_000
    assert abs(t.net_pnl_usdt - 10.0) < 1e-12
    assert abs(t.gross_pnl_usdt - 10.0) < 1e-12
    assert abs(t.fees_usdt - 0.0) < 1e-12

    s = summarize_round_trips(trades)
    assert s.trades == 1
    assert s.wins == 1
    assert s.losses == 0
    assert abs(s.net_pnl_usdt - 10.0) < 1e-12


def test_round_trips_from_fills_flip_closes_first_trade_and_opens_new():
    fills = [
        Fill(
            order_id="f1",
            symbol="BTCUSDT",
            side="buy",
            quantity=1.0,
            price=100.0,
            fee_usdt=0.0,
            event_time_ms=0,
            liquidity="taker",
        ),
        # Flip: sell 2 closes the long and opens a short of 1.
        Fill(
            order_id="f2",
            symbol="BTCUSDT",
            side="sell",
            quantity=2.0,
            price=110.0,
            fee_usdt=0.0,
            event_time_ms=1_000,
            liquidity="taker",
        ),
    ]

    trades = round_trips_from_fills(fills)
    assert len(trades) == 1
    assert trades[0].direction == "long"
    assert abs(trades[0].net_pnl_usdt - 10.0) < 1e-12


def test_round_trips_include_fees_in_net_pnl():
    fills = [
        Fill(
            order_id="entry",
            symbol="BTCUSDT",
            side="buy",
            quantity=1.0,
            price=100.0,
            fee_usdt=0.1,
            event_time_ms=0,
            liquidity="taker",
        ),
        Fill(
            order_id="exit",
            symbol="BTCUSDT",
            side="sell",
            quantity=1.0,
            price=110.0,
            fee_usdt=0.1,
            event_time_ms=1_000,
            liquidity="taker",
        ),
    ]

    trades = round_trips_from_fills(fills)
    assert len(trades) == 1
    assert abs(trades[0].fees_usdt - 0.2) < 1e-12
    assert abs(trades[0].gross_pnl_usdt - 10.0) < 1e-12
    assert abs(trades[0].net_pnl_usdt - 9.8) < 1e-12


def test_max_drawdown():
    eq = [(0, 0.0), (1, 10.0), (2, 5.0), (3, 12.0), (4, 7.0)]
    assert abs(max_drawdown(eq) - (-5.0)) < 1e-12


def test_round_trips_are_fill_only_and_exclude_funding():
    fills = [
        Fill(
            order_id="entry",
            symbol="BTCUSDT",
            side="buy",
            quantity=1.0,
            price=100.0,
            fee_usdt=0.0,
            event_time_ms=0,
            liquidity="taker",
        ),
        Fill(
            order_id="exit",
            symbol="BTCUSDT",
            side="sell",
            quantity=1.0,
            price=100.0,
            fee_usdt=0.0,
            event_time_ms=1_000,
            liquidity="taker",
        ),
    ]

    # Funding can affect portfolio PnL, but round_trips_from_fills ignores it by design.
    pf = Portfolio()
    pf.apply_fill("BTCUSDT", "buy", qty=1.0, price=100.0, fee_usdt=0.0)
    funding_pnl = pf.apply_funding("BTCUSDT", mark_price=100.0, funding_rate=0.01)
    assert abs(funding_pnl - (-1.0)) < 1e-12

    trades = round_trips_from_fills(fills)
    assert len(trades) == 1
    assert abs(trades[0].net_pnl_usdt - 0.0) < 1e-12


def test_max_drawdown_uses_the_provided_curve_only():
    eq_a = [(0, 0.0), (1, 2.0), (2, 1.0)]
    eq_b = [(0, 0.0), (1, 10.0), (2, 9.0), (3, 8.0)]
    assert abs(max_drawdown(eq_a) - (-1.0)) < 1e-12
    assert abs(max_drawdown(eq_b) - (-2.0)) < 1e-12
