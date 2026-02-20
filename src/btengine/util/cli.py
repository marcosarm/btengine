from __future__ import annotations

from argparse import ArgumentParser, Namespace

from ..book_guard import BookGuardConfig


def add_strict_book_args(
    ap: ArgumentParser,
    *,
    default_max_staleness_ms: int = 250,
    default_max_spread_bps: float = 5.0,
    default_cooldown_ms: int = 1_000,
    default_warmup_depth_updates: int = 1_000,
) -> None:
    ap.add_argument("--strict-book", action="store_true", help="Block submits on stale/crossed/invalid book.")
    ap.add_argument(
        "--strict-book-max-staleness-ms",
        type=int,
        default=int(default_max_staleness_ms),
        help="Block submits when latest depth is older than N ms.",
    )
    ap.add_argument("--strict-book-max-spread", type=float, default=None, help="Optional max absolute spread.")
    ap.add_argument(
        "--strict-book-max-spread-bps",
        type=float,
        default=float(default_max_spread_bps),
        help="Optional max spread in bps.",
    )
    ap.add_argument(
        "--strict-book-cooldown-ms",
        type=int,
        default=int(default_cooldown_ms),
        help="Block submits for N ms after guard trip.",
    )
    ap.add_argument(
        "--strict-book-warmup-depth-updates",
        type=int,
        default=int(default_warmup_depth_updates),
        help="Block submits for N depth updates after guard trip.",
    )
    ap.add_argument("--strict-book-reset-on-mismatch", action="store_true", default=True)
    ap.add_argument("--strict-book-no-reset-on-mismatch", dest="strict_book_reset_on_mismatch", action="store_false")
    ap.add_argument("--strict-book-reset-on-crossed", action="store_true", default=True)
    ap.add_argument("--strict-book-no-reset-on-crossed", dest="strict_book_reset_on_crossed", action="store_false")
    ap.add_argument("--strict-book-reset-on-missing-side", action="store_true", default=False)
    ap.add_argument("--strict-book-reset-on-spread", action="store_true", default=False)
    ap.add_argument("--strict-book-reset-on-stale", action="store_true", default=False)


def strict_book_config_from_args(args: Namespace) -> BookGuardConfig | None:
    if not bool(getattr(args, "strict_book", False)):
        return None
    return BookGuardConfig(
        enabled=True,
        max_spread=getattr(args, "strict_book_max_spread", None),
        max_spread_bps=getattr(args, "strict_book_max_spread_bps", None),
        max_staleness_ms=int(getattr(args, "strict_book_max_staleness_ms", 0) or 0),
        cooldown_ms=int(getattr(args, "strict_book_cooldown_ms", 0) or 0),
        warmup_depth_updates=int(getattr(args, "strict_book_warmup_depth_updates", 0) or 0),
        reset_on_mismatch=bool(getattr(args, "strict_book_reset_on_mismatch", True)),
        reset_on_crossed=bool(getattr(args, "strict_book_reset_on_crossed", True)),
        reset_on_missing_side=bool(getattr(args, "strict_book_reset_on_missing_side", False)),
        reset_on_spread=bool(getattr(args, "strict_book_reset_on_spread", False)),
        reset_on_stale=bool(getattr(args, "strict_book_reset_on_stale", False)),
    )


def add_stream_alignment_args(ap: ArgumentParser) -> None:
    ap.add_argument(
        "--stream-alignment-mode",
        choices=["none", "fixed_delay", "causal_asof", "causal_asof_global"],
        default="none",
        help="Optional availability-time alignment for trade/mark/ticker/liquidation streams.",
    )
    ap.add_argument(
        "--stream-alignment-quantile",
        type=float,
        default=0.8,
        help="Quantile in [0,1] used by stream_alignment_mode=causal_asof.",
    )
    ap.add_argument(
        "--stream-alignment-min-delay-ms",
        type=int,
        default=0,
        help="Lower bound for effective non-OI stream alignment delay (ms).",
    )
    ap.add_argument(
        "--stream-alignment-max-delay-ms",
        type=int,
        default=None,
        help="Upper bound for effective non-OI stream alignment delay (ms).",
    )
    ap.add_argument(
        "--stream-alignment-history-size",
        type=int,
        default=1024,
        help="Rolling lag history size for stream_alignment_mode=causal_asof.",
    )
    ap.add_argument(
        "--stream-alignment-global-row-limit",
        type=int,
        default=2_000_000,
        help="Max rows materialized in memory for stream_alignment_mode=causal_asof_global (<=0 disables).",
    )
    ap.add_argument("--trade-delay-ms", type=int, default=0, help="Base delay applied to trades stream (ms).")
    ap.add_argument("--mark-price-delay-ms", type=int, default=0, help="Base delay applied to mark_price stream (ms).")
    ap.add_argument("--ticker-delay-ms", type=int, default=0, help="Base delay applied to ticker stream (ms).")
    ap.add_argument("--liquidation-delay-ms", type=int, default=0, help="Base delay applied to liquidations stream (ms).")


def stream_alignment_kwargs_from_args(args: Namespace) -> dict[str, object]:
    return {
        "stream_alignment_mode": str(getattr(args, "stream_alignment_mode", "none")),
        "stream_alignment_quantile": float(getattr(args, "stream_alignment_quantile", 0.8)),
        "stream_alignment_min_delay_ms": int(getattr(args, "stream_alignment_min_delay_ms", 0) or 0),
        "stream_alignment_max_delay_ms": (
            None
            if getattr(args, "stream_alignment_max_delay_ms", None) is None
            else int(getattr(args, "stream_alignment_max_delay_ms"))
        ),
        "stream_alignment_history_size": int(getattr(args, "stream_alignment_history_size", 1024) or 0),
        "stream_alignment_global_row_limit": (
            None
            if int(getattr(args, "stream_alignment_global_row_limit", 2_000_000) or 0) <= 0
            else int(getattr(args, "stream_alignment_global_row_limit"))
        ),
        "trade_delay_ms": int(getattr(args, "trade_delay_ms", 0) or 0),
        "mark_price_delay_ms": int(getattr(args, "mark_price_delay_ms", 0) or 0),
        "ticker_delay_ms": int(getattr(args, "ticker_delay_ms", 0) or 0),
        "liquidation_delay_ms": int(getattr(args, "liquidation_delay_ms", 0) or 0),
    }


def add_taker_slippage_args(ap: ArgumentParser) -> None:
    ap.add_argument(
        "--taker-slippage-bps",
        type=float,
        default=0.0,
        help="Conservative taker overlay in bps (applied against trader side).",
    )
    ap.add_argument(
        "--taker-slippage-spread-frac",
        type=float,
        default=0.0,
        help="Additional taker overlay as a fraction of current spread (e.g. 0.5 = half spread).",
    )
    ap.add_argument(
        "--taker-slippage-abs",
        type=float,
        default=0.0,
        help="Additional absolute taker overlay in quote units.",
    )


def taker_slippage_kwargs_from_args(args: Namespace) -> dict[str, float]:
    return {
        "taker_slippage_bps": float(getattr(args, "taker_slippage_bps", 0.0) or 0.0),
        "taker_slippage_spread_frac": float(getattr(args, "taker_slippage_spread_frac", 0.0) or 0.0),
        "taker_slippage_abs": float(getattr(args, "taker_slippage_abs", 0.0) or 0.0),
    }
