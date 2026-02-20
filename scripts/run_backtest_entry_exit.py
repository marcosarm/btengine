from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from btengine.analytics import max_drawdown, round_trips_from_fills, summarize_round_trips
from btengine.broker import SimBroker
from btengine.data.cryptohftdata import CryptoHftDayConfig, CryptoHftLayout, S3Config, build_day_stream, make_s3_filesystem
from btengine.engine import BacktestEngine, EngineConfig
from btengine.strategies import EntryExitStrategy
from btengine.util import (
    add_stream_alignment_args,
    add_strict_book_args,
    add_taker_slippage_args,
    load_dotenv,
    stream_alignment_kwargs_from_args,
    strict_book_config_from_args,
    taker_slippage_kwargs_from_args,
)


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_hours(s: str) -> range:
    if "-" in s:
        a, b = s.split("-", 1)
        h0, h1 = int(a), int(b)
        return range(h0, h1 + 1)
    h = int(s)
    return range(h, h + 1)


def _utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).isoformat()


def _write_fills_csv(path: str, fills) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_time_ms", "symbol", "order_id", "side", "qty", "price", "fee_usdt", "liquidity"])
        for x in fills:
            w.writerow([x.event_time_ms, x.symbol, x.order_id, x.side, x.quantity, x.price, x.fee_usdt, x.liquidity])


def _write_equity_csv(path: str, equity_curve: list[tuple[int, float]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_time_ms", "equity_pnl_usdt"])
        for t, eq in equity_curve:
            w.writerow([t, eq])


def main() -> int:
    ap = argparse.ArgumentParser(description="Simple entry/exit setup to sanity-check PnL and basic stats.")
    ap.add_argument("--dotenv", default=str(ROOT / ".env"))
    ap.add_argument("--exchange", default="binance_futures")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--hours", default="12-12")
    ap.add_argument("--include-ticker", action="store_true")
    ap.add_argument("--include-open-interest", action="store_true")
    ap.add_argument("--include-liquidations", action="store_true")
    ap.add_argument("--open-interest-delay-ms", type=int, default=0)
    ap.add_argument(
        "--open-interest-alignment",
        choices=["fixed_delay", "causal_asof", "causal_asof_global"],
        default="fixed_delay",
        help="How to place open_interest on the replay timeline.",
    )
    ap.add_argument(
        "--open-interest-calibrated-delay-ms",
        type=int,
        default=None,
        help="Optional externally-calibrated OI availability delay floor (ms).",
    )
    ap.add_argument(
        "--open-interest-availability-quantile",
        type=float,
        default=0.8,
        help="Quantile in [0,1] used by causal_asof to estimate OI availability delay.",
    )
    ap.add_argument("--open-interest-min-delay-ms", type=int, default=0, help="Lower bound for effective OI delay (ms).")
    ap.add_argument("--open-interest-max-delay-ms", type=int, default=None, help="Upper bound for effective OI delay (ms).")
    ap.add_argument("--skip-missing", action="store_true")

    ap.add_argument("--direction", choices=["long", "short"], default="long")
    ap.add_argument("--qty", type=float, default=0.001)
    ap.add_argument("--enter-offset-s", type=int, default=30, help="Enter offset from window start (seconds).")
    ap.add_argument("--hold-s", type=int, default=60, help="Hold duration before exit (seconds).")
    ap.add_argument("--gap-s", type=int, default=60, help="Gap between cycles (seconds).")
    ap.add_argument("--cycles", type=int, default=1)
    ap.add_argument(
        "--no-force-close-on-end",
        dest="force_close_on_end",
        action="store_false",
        help="Do not force-close an open position at the end of the stream.",
    )
    ap.set_defaults(force_close_on_end=True)

    ap.add_argument("--tick-ms", type=int, default=0, help="Engine tick interval (0 disables ticks).")
    ap.add_argument("--max-events", type=int, default=0, help="0 = no limit")

    ap.add_argument("--maker-fee-frac", type=float, default=0.0004)
    ap.add_argument("--taker-fee-frac", type=float, default=0.0005)
    ap.add_argument("--submit-latency-ms", type=int, default=0)
    ap.add_argument("--cancel-latency-ms", type=int, default=0)
    add_taker_slippage_args(ap)
    add_stream_alignment_args(ap)
    add_strict_book_args(ap, default_max_staleness_ms=250)

    ap.add_argument("--out-fills-csv", default=None)
    ap.add_argument("--out-equity-csv", default=None)
    args = ap.parse_args()

    env = load_dotenv(args.dotenv, override=False).values
    bucket = env.get("S3_BUCKET")
    prefix = env.get("S3_PREFIX")
    if not bucket or not prefix:
        print("ERROR: missing S3_BUCKET or S3_PREFIX in .env", file=sys.stderr)
        return 2

    region = env.get("AWS_REGION") or None
    access_key = env.get("AWS_ACCESS_KEY_ID") or None
    secret_key = env.get("AWS_SECRET_ACCESS_KEY") or None
    session_token = env.get("AWS_SESSION_TOKEN") or None

    fs = make_s3_filesystem(
        S3Config(region=region, access_key=access_key, secret_key=secret_key, session_token=session_token)
    )
    layout = CryptoHftLayout(bucket=bucket, prefix=prefix)

    d = _parse_day(args.day)
    hours = _parse_hours(args.hours)

    day_start_ms = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
    window_start_ms = day_start_ms + int(hours.start) * 3_600_000
    window_end_ms = day_start_ms + int(hours.stop) * 3_600_000

    if args.cycles <= 0:
        print("ERROR: --cycles must be >= 1", file=sys.stderr)
        return 2
    if args.qty <= 0:
        print("ERROR: --qty must be > 0", file=sys.stderr)
        return 2
    if args.enter_offset_s < 0 or args.hold_s <= 0 or args.gap_s < 0:
        print("ERROR: invalid enter/hold/gap parameters", file=sys.stderr)
        return 2

    schedule: list[tuple[int, int]] = []
    step_ms = (int(args.hold_s) + int(args.gap_s)) * 1000
    for i in range(int(args.cycles)):
        enter_ms = window_start_ms + int(args.enter_offset_s) * 1000 + i * step_ms
        exit_ms = enter_ms + int(args.hold_s) * 1000
        if exit_ms >= window_end_ms:
            break
        schedule.append((int(enter_ms), int(exit_ms)))

    if not schedule:
        print("ERROR: schedule is empty (window too small for enter/exit).", file=sys.stderr)
        return 2

    cfg = CryptoHftDayConfig(
        exchange=args.exchange,
        include_trades=True,
        include_orderbook=True,
        include_mark_price=True,
        include_ticker=bool(args.include_ticker),
        include_open_interest=bool(args.include_open_interest),
        include_liquidations=bool(args.include_liquidations),
        open_interest_delay_ms=int(args.open_interest_delay_ms or 0),
        open_interest_calibrated_delay_ms=(
            None if args.open_interest_calibrated_delay_ms is None else int(args.open_interest_calibrated_delay_ms)
        ),
        open_interest_alignment_mode=str(args.open_interest_alignment),  # type: ignore[arg-type]
        open_interest_availability_quantile=float(args.open_interest_availability_quantile),
        open_interest_min_delay_ms=int(args.open_interest_min_delay_ms or 0),
        open_interest_max_delay_ms=(None if args.open_interest_max_delay_ms is None else int(args.open_interest_max_delay_ms)),
        **stream_alignment_kwargs_from_args(args),
        orderbook_hours=hours,
        orderbook_skip_missing=True,
        skip_missing_daily_files=bool(args.skip_missing),
        stream_start_ms=window_start_ms,
        stream_end_ms=window_end_ms,
    )

    stream = build_day_stream(layout, cfg=cfg, symbol=str(args.symbol), day=d, filesystem=fs)
    events = stream

    max_events = int(args.max_events or 0)
    if max_events > 0:
        def _limit(xs):
            for i, ev in enumerate(xs):
                if i >= max_events:
                    break
                yield ev
        events = _limit(events)

    broker = SimBroker(
        maker_fee_frac=float(args.maker_fee_frac),
        taker_fee_frac=float(args.taker_fee_frac),
        submit_latency_ms=int(args.submit_latency_ms),
        cancel_latency_ms=int(args.cancel_latency_ms),
        **taker_slippage_kwargs_from_args(args),
    )
    guard_cfg = strict_book_config_from_args(args)
    engine = BacktestEngine(
        config=EngineConfig(
            tick_interval_ms=int(args.tick_ms),
            book_guard=guard_cfg,
            book_guard_symbol=str(args.symbol),
        ),
        broker=broker,
    )

    strat = EntryExitStrategy(
        symbol=str(args.symbol),
        direction=str(args.direction),  # type: ignore[arg-type]
        target_qty=float(args.qty),
        schedule_ms=schedule,
        force_close_on_end=bool(args.force_close_on_end),
    )
    res = engine.run(events, strategy=strat)

    fills = res.ctx.broker.fills
    trades = round_trips_from_fills(fills)
    summary = summarize_round_trips(trades)

    eq = strat.equity_curve
    mdd = max_drawdown(eq)
    eq_min = min((x for _, x in eq), default=None)
    eq_max = max((x for _, x in eq), default=None)

    print("\n== Window ==")
    print(f"symbol: {args.symbol}")
    print(f"start_ms: {window_start_ms} ({_utc_iso(window_start_ms)})")
    print(f"end_ms:   {window_end_ms} ({_utc_iso(window_end_ms)})")

    print("\n== Schedule (UTC) ==")
    for i, (a, b) in enumerate(schedule):
        print(f"{i}: enter={_utc_iso(a)} exit={_utc_iso(b)}")

    print("\n== Fills ==")
    if not fills:
        print("fills: 0")
    else:
        for f in fills:
            print(
                f"t={_utc_iso(f.event_time_ms)} order={f.order_id} {f.side} "
                f"qty={f.quantity} px={f.price} fee={f.fee_usdt} liq={f.liquidity}"
            )

    print("\n== Portfolio ==")
    print(f"realized_pnl_usdt: {res.ctx.broker.portfolio.realized_pnl_usdt:.6f}")
    print(f"fees_paid_usdt:    {res.ctx.broker.portfolio.fees_paid_usdt:.6f}")
    p = res.ctx.broker.portfolio.positions.get(str(args.symbol))
    if p is None:
        print("final_position: none")
    else:
        print(f"final_position: qty={p.qty} avg_price={p.avg_price}")

    print("\n== Round Trips (from fills) ==")
    print(f"trades: {summary.trades} wins={summary.wins} losses={summary.losses} win_rate={summary.win_rate}")
    print(f"net_pnl_usdt:   {summary.net_pnl_usdt:.6f}")
    print(f"gross_pnl_usdt: {summary.gross_pnl_usdt:.6f}")
    print(f"fees_usdt:      {summary.fees_usdt:.6f}")
    print(f"avg_duration_ms:{summary.avg_duration_ms} max_duration_ms:{summary.max_duration_ms}")

    print("\n== Equity Curve (mark_price) ==")
    print(f"points: {len(eq)} eq_min={eq_min} eq_max={eq_max} max_drawdown={mdd}")

    if args.out_fills_csv:
        _write_fills_csv(str(args.out_fills_csv), fills)
        print(f"\nwrote fills csv: {args.out_fills_csv}")

    if args.out_equity_csv:
        _write_equity_csv(str(args.out_equity_csv), eq)
        print(f"wrote equity csv: {args.out_equity_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
