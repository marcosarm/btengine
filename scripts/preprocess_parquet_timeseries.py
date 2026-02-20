from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from btengine.data.cryptohftdata.preprocess import preprocess_parquet_file


def main() -> int:
    ap = argparse.ArgumentParser(description="Preprocess parquet (sort + dedup) before replay.")
    ap.add_argument("--kind", required=True, choices=["trades", "orderbook", "mark_price", "ticker", "open_interest", "liquidations"])
    ap.add_argument("--input", required=True, help="Input parquet path or URI.")
    ap.add_argument("--output", required=True, help="Output parquet path or URI.")
    ap.add_argument("--no-dedup", action="store_true", help="Disable deduplication; keep only sorting.")
    ap.add_argument("--keep", choices=["first", "last"], default="last", help="Duplicate retention rule when dedup is enabled.")
    ap.add_argument("--compression", default="zstd", help="Parquet compression codec (default: zstd).")
    ap.add_argument(
        "--max-rows-in-memory",
        type=int,
        default=2_000_000,
        help="Fail-fast row limit for in-memory preprocessing (<=0 disables).",
    )
    args = ap.parse_args()

    res = preprocess_parquet_file(
        input_path=str(args.input),
        output_path=str(args.output),
        kind=str(args.kind),  # type: ignore[arg-type]
        dedup=not bool(args.no_dedup),
        keep=str(args.keep),  # type: ignore[arg-type]
        compression=str(args.compression),
        max_rows_in_memory=(None if int(args.max_rows_in_memory or 0) <= 0 else int(args.max_rows_in_memory)),
    )

    print("== Preprocess Summary ==")
    print(f"kind: {res.kind}")
    print(f"input: {res.input_path}")
    print(f"output: {res.output_path}")
    print(f"sort_columns: {list(res.sort_columns)}")
    print(f"dedup_columns: {list(res.dedup_columns)}")
    print(f"rows_in: {res.rows_in}")
    print(f"rows_out: {res.rows_out}")
    print(f"dropped_duplicates: {res.dropped_duplicates}")
    print(f"out_of_order_before: {res.out_of_order_before}")
    print(f"out_of_order_after: {res.out_of_order_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
