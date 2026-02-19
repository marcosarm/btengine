from btengine.portfolio import Portfolio


def test_apply_fill_realizes_pnl_on_reduction() -> None:
    pf = Portfolio()
    pf.apply_fill("BTCUSDT", "buy", qty=2.0, price=100.0, fee_usdt=0.0)
    pf.apply_fill("BTCUSDT", "sell", qty=0.5, price=110.0, fee_usdt=0.0)

    pos = pf.positions["BTCUSDT"]
    assert abs(pos.qty - 1.5) < 1e-12
    assert abs(pos.avg_price - 100.0) < 1e-12
    assert abs(pf.realized_pnl_usdt - 5.0) < 1e-12


def test_apply_fill_realizes_pnl_on_close() -> None:
    pf = Portfolio()
    pf.apply_fill("BTCUSDT", "buy", qty=1.0, price=100.0, fee_usdt=0.0)
    pf.apply_fill("BTCUSDT", "sell", qty=1.0, price=90.0, fee_usdt=0.0)

    pos = pf.positions["BTCUSDT"]
    assert abs(pos.qty - 0.0) < 1e-12
    assert abs(pos.avg_price - 0.0) < 1e-12
    assert abs(pf.realized_pnl_usdt - (-10.0)) < 1e-12


def test_apply_fill_realizes_pnl_on_flip() -> None:
    pf = Portfolio()
    pf.apply_fill("BTCUSDT", "buy", qty=1.0, price=100.0, fee_usdt=0.0)
    # Sell 2.0 closes the long 1.0 and opens short 1.0.
    pf.apply_fill("BTCUSDT", "sell", qty=2.0, price=110.0, fee_usdt=0.0)

    pos = pf.positions["BTCUSDT"]
    assert abs(pos.qty - (-1.0)) < 1e-12
    assert abs(pos.avg_price - 110.0) < 1e-12
    assert abs(pf.realized_pnl_usdt - 10.0) < 1e-12


def test_apply_funding_positive_rate_penalizes_long_and_benefits_short() -> None:
    long_pf = Portfolio()
    long_pf.apply_fill("BTCUSDT", "buy", qty=1.0, price=100.0, fee_usdt=0.0)
    long_funding = long_pf.apply_funding("BTCUSDT", mark_price=100.0, funding_rate=0.01)
    assert abs(long_funding - (-1.0)) < 1e-12
    assert abs(long_pf.realized_pnl_usdt - (-1.0)) < 1e-12

    short_pf = Portfolio()
    short_pf.apply_fill("BTCUSDT", "sell", qty=1.0, price=100.0, fee_usdt=0.0)
    short_funding = short_pf.apply_funding("BTCUSDT", mark_price=100.0, funding_rate=0.01)
    assert abs(short_funding - 1.0) < 1e-12
    assert abs(short_pf.realized_pnl_usdt - 1.0) < 1e-12
