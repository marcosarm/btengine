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

