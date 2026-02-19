from btengine.book_guard import BookGuardConfig
from btengine.engine import BacktestEngine, EngineConfig
from btengine.broker import SimBroker
from btengine.execution.orders import Order
from btengine.types import DepthUpdate, Liquidation, MarkPrice, OpenInterest, Ticker


class NoopStrategy:
    pass


def test_engine_applies_funding_once_per_timestamp():
    broker = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0)

    # Open a short position (qty negative). Positive funding => shorts receive.
    broker.portfolio.apply_fill("BTCUSDT", "sell", qty=1.0, price=100.0, fee_usdt=0.0)

    engine = BacktestEngine(config=EngineConfig(tick_interval_ms=0), broker=broker)

    events = [
        MarkPrice(
            received_time_ns=0,
            event_time_ms=1_000,
            symbol="BTCUSDT",
            mark_price=100.0,
            index_price=100.0,
            funding_rate=0.01,
            next_funding_time_ms=1_000,
        ),
        # Same funding timestamp should not apply twice.
        MarkPrice(
            received_time_ns=0,
            event_time_ms=1_001,
            symbol="BTCUSDT",
            mark_price=101.0,
            index_price=101.0,
            funding_rate=0.02,
            next_funding_time_ms=1_000,
        ),
    ]

    res = engine.run(events, strategy=NoopStrategy())

    assert abs(res.ctx.broker.portfolio.realized_pnl_usdt - 1.0) < 1e-12


def test_engine_stores_latest_aux_events_in_context():
    engine = BacktestEngine(config=EngineConfig(tick_interval_ms=0), broker=SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0))

    events = [
        Ticker(
            received_time_ns=0,
            event_time_ms=1_000,
            symbol="BTCUSDT",
            price_change=1.0,
            price_change_percent=0.1,
            weighted_average_price=100.0,
            last_price=101.0,
            last_quantity=0.5,
            open_price=99.0,
            high_price=102.0,
            low_price=98.0,
            base_asset_volume=10.0,
            quote_asset_volume=1000.0,
            statistics_open_time_ms=0,
            statistics_close_time_ms=1_000,
            first_trade_id=1,
            last_trade_id=2,
            total_trades=10,
        ),
        OpenInterest(
            received_time_ns=0,
            event_time_ms=2_000,
            timestamp_ms=2_000,
            symbol="BTCUSDT",
            sum_open_interest=11.0,
            sum_open_interest_value=1100.0,
        ),
        Liquidation(
            received_time_ns=0,
            event_time_ms=3_000,
            symbol="BTCUSDT",
            side="BUY",
            order_type="LIMIT",
            time_in_force="IOC",
            quantity=0.1,
            price=100.0,
            average_price=100.0,
            order_status="FILLED",
            last_filled_quantity=0.1,
            filled_quantity=0.1,
            trade_time_ms=3_000,
        ),
    ]

    res = engine.run(events, strategy=NoopStrategy())
    ctx = res.ctx
    assert ctx.ticker["BTCUSDT"].event_time_ms == 1_000
    assert ctx.open_interest["BTCUSDT"].event_time_ms == 2_000
    assert ctx.liquidation["BTCUSDT"].event_time_ms == 3_000


class _SubmitOnDepth:
    def __init__(self) -> None:
        self.submits = 0

    def on_event(self, event: object, ctx) -> None:
        if not isinstance(event, DepthUpdate):
            return
        book = ctx.book(event.symbol)
        ctx.broker.submit(
            Order(id=f"o{self.submits}", symbol=event.symbol, side="buy", order_type="market", quantity=1.0),
            book,
            now_ms=ctx.now_ms,
        )
        self.submits += 1


def test_engine_blocks_submits_outside_trading_window():
    broker = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0)
    engine = BacktestEngine(
        config=EngineConfig(tick_interval_ms=0, trading_start_ms=500, trading_end_ms=1_500),
        broker=broker,
    )

    events = [
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=0,
            transaction_time_ms=0,
            symbol="BTCUSDT",
            first_update_id=1,
            final_update_id=1,
            prev_final_update_id=0,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=1_000,
            transaction_time_ms=1_000,
            symbol="BTCUSDT",
            first_update_id=2,
            final_update_id=2,
            prev_final_update_id=1,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=2_000,
            transaction_time_ms=2_000,
            symbol="BTCUSDT",
            first_update_id=3,
            final_update_id=3,
            prev_final_update_id=2,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
    ]

    res = engine.run(events, strategy=_SubmitOnDepth())
    fills = res.ctx.broker.fills
    assert len(fills) == 1
    assert fills[0].event_time_ms == 1_000
    pos = res.ctx.broker.portfolio.positions["BTCUSDT"]
    assert abs(pos.qty - 1.0) < 1e-12


class _TickRecorder:
    def __init__(self) -> None:
        self.ticks: list[int] = []

    def on_tick(self, now_ms: int, ctx) -> None:
        self.ticks.append(int(now_ms))


def test_engine_ticks_anchor_to_first_event_time():
    engine = BacktestEngine(config=EngineConfig(tick_interval_ms=1000), broker=SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0))

    events = [
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=1_500,
            transaction_time_ms=1_500,
            symbol="BTCUSDT",
            first_update_id=1,
            final_update_id=1,
            prev_final_update_id=0,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=2_600,
            transaction_time_ms=2_600,
            symbol="BTCUSDT",
            first_update_id=2,
            final_update_id=2,
            prev_final_update_id=1,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
    ]

    strat = _TickRecorder()
    engine.run(events, strategy=strat)

    assert strat.ticks == [1_500, 2_500, 3_500]


class _EntryThenForceCloseOnEnd:
    def on_event(self, event: object, ctx) -> None:
        if isinstance(event, DepthUpdate) and event.event_time_ms == 1_000:
            ctx.broker.submit(
                Order(id="open", symbol=event.symbol, side="buy", order_type="market", quantity=1.0),
                ctx.book(event.symbol),
                now_ms=ctx.now_ms,
            )

    def on_end(self, ctx) -> None:
        ctx.broker.submit(
            Order(id="close", symbol="BTCUSDT", side="sell", order_type="market", quantity=1.0),
            ctx.book("BTCUSDT"),
            now_ms=ctx.now_ms,
        )


def test_engine_allows_reducing_submit_outside_trading_window():
    broker = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0)
    engine = BacktestEngine(
        config=EngineConfig(tick_interval_ms=0, trading_start_ms=500, trading_end_ms=1_500),
        broker=broker,
    )

    events = [
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=1_000,
            transaction_time_ms=1_000,
            symbol="BTCUSDT",
            first_update_id=1,
            final_update_id=1,
            prev_final_update_id=0,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=2_000,
            transaction_time_ms=2_000,
            symbol="BTCUSDT",
            first_update_id=2,
            final_update_id=2,
            prev_final_update_id=1,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
    ]

    res = engine.run(events, strategy=_EntryThenForceCloseOnEnd())
    fills = res.ctx.broker.fills
    assert [f.order_id for f in fills] == ["open", "close"]
    pos = res.ctx.broker.portfolio.positions["BTCUSDT"]
    assert abs(pos.qty) < 1e-12


def test_engine_block_all_mode_blocks_reducing_submit_outside_trading_window():
    broker = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0)
    engine = BacktestEngine(
        config=EngineConfig(
            tick_interval_ms=0,
            trading_start_ms=500,
            trading_end_ms=1_500,
            trading_window_mode="block_all",
        ),
        broker=broker,
    )

    events = [
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=1_000,
            transaction_time_ms=1_000,
            symbol="BTCUSDT",
            first_update_id=1,
            final_update_id=1,
            prev_final_update_id=0,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=2_000,
            transaction_time_ms=2_000,
            symbol="BTCUSDT",
            first_update_id=2,
            final_update_id=2,
            prev_final_update_id=1,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[(100.0, 10.0)],
        ),
    ]

    res = engine.run(events, strategy=_EntryThenForceCloseOnEnd())
    fills = res.ctx.broker.fills
    assert [f.order_id for f in fills] == ["open"]
    pos = res.ctx.broker.portfolio.positions["BTCUSDT"]
    assert abs(pos.qty - 1.0) < 1e-12


class _SubmitLatencyIocAtT900:
    def on_event(self, event: object, ctx) -> None:
        if not isinstance(event, DepthUpdate):
            return
        if event.event_time_ms != 900:
            return
        ctx.broker.submit(
            Order(
                id="lat",
                symbol=event.symbol,
                side="buy",
                order_type="limit",
                quantity=1.0,
                price=100.0,
                time_in_force="IOC",
            ),
            ctx.book(event.symbol),
            now_ms=ctx.now_ms,
        )


def _depth(event_time_ms: int, ask_px: float) -> DepthUpdate:
    return DepthUpdate(
        received_time_ns=0,
        event_time_ms=event_time_ms,
        transaction_time_ms=event_time_ms,
        symbol="BTCUSDT",
        first_update_id=event_time_ms,
        final_update_id=event_time_ms,
        prev_final_update_id=event_time_ms - 1,
        bid_updates=[(99.0, 10.0)],
        ask_updates=[(ask_px, 10.0)],
    )


def test_engine_default_broker_time_mode_after_event():
    broker = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0, submit_latency_ms=100)
    engine = BacktestEngine(config=EngineConfig(tick_interval_ms=0), broker=broker)
    events = [_depth(900, 101.0), _depth(1_000, 100.0)]

    res = engine.run(events, strategy=_SubmitLatencyIocAtT900())
    fills = res.ctx.broker.fills
    assert len(fills) == 1
    assert fills[0].order_id == "lat"
    assert fills[0].event_time_ms == 1_000


def test_engine_broker_time_mode_before_event_legacy_behavior():
    broker = SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0, submit_latency_ms=100)
    engine = BacktestEngine(config=EngineConfig(tick_interval_ms=0, broker_time_mode="before_event"), broker=broker)
    events = [_depth(900, 101.0), _depth(1_000, 100.0)]

    res = engine.run(events, strategy=_SubmitLatencyIocAtT900())
    assert len(res.ctx.broker.fills) == 0


class _SubmitOnDepthForGuard:
    def on_event(self, event: object, ctx) -> None:
        if not isinstance(event, DepthUpdate):
            return
        ctx.broker.submit(
            Order(id="g1", symbol=event.symbol, side="buy", order_type="market", quantity=1.0),
            ctx.book(event.symbol),
            now_ms=ctx.now_ms,
        )


def test_engine_can_enable_core_book_guard_via_config():
    engine = BacktestEngine(
        config=EngineConfig(
            tick_interval_ms=0,
            book_guard=BookGuardConfig(
                enabled=True,
                max_staleness_ms=0,
                cooldown_ms=0,
                warmup_depth_updates=0,
                max_spread_bps=None,
            ),
            book_guard_symbol="BTCUSDT",
        ),
        broker=SimBroker(maker_fee_frac=0.0, taker_fee_frac=0.0),
    )

    events = [
        DepthUpdate(
            received_time_ns=0,
            event_time_ms=1_000,
            transaction_time_ms=1_000,
            symbol="BTCUSDT",
            first_update_id=1,
            final_update_id=1,
            prev_final_update_id=0,
            bid_updates=[(99.0, 10.0)],
            ask_updates=[],
        ),
    ]

    res = engine.run(events, strategy=_SubmitOnDepthForGuard())
    assert len(res.ctx.broker.fills) == 0
    assert int(res.ctx.broker.stats.blocked_submits) == 1
    assert int(res.ctx.broker.stats.blocked_submit_reason.get("missing_side", 0)) == 1
