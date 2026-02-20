# Scripts (validation and replay)

Scripts live in `scripts/` and are intended to:
- inspect local Parquets
- validate the S3 dataset (existence, schema, ranges)
- replay data through `btengine` for sanity checks

Strategy examples live in consumer repos (not in btengine).

## `validate_s3_dataset.py`

File: `scripts/validate_s3_dataset.py`

Purpose:
- check S3 files exist for a given day
- inspect Parquet quickly: rows, row groups, Arrow schema, min/max timestamps
- for orderbook: min/max `final_update_id` + continuity per hour

Examples:

```bash
# BTCUSDT (full day)
python scripts\\validate_s3_dataset.py --day 2025-07-01 --symbols BTCUSDT --hours 0-23

# Hour 12 only
python scripts\\validate_s3_dataset.py --day 2025-07-01 --symbols BTCUSDT --hours 12-12

# Multiple symbols
python scripts\\validate_s3_dataset.py --day 2025-07-01 --symbols BTCUSDT,BTCUSDT_260626 --hours 12-12 --skip-missing
```

Notes:
- reads `.env` from repo root (use `.env.example` as template)
- does not print credentials
- `--skip-missing` treats `FileNotFoundError` as "MISSING" and continues

## `run_backtest_replay.py`

File: `scripts/run_backtest_replay.py`

Purpose:
- build S3 streams (per symbol)
- merge multi-symbol streams
- run `BacktestEngine` with a no-op strategy
- print summary (books, PnL, event counts)

Example:

```bash
python scripts\\run_backtest_replay.py --day 2025-07-01 --symbols BTCUSDT --mark-price-symbols BTCUSDT --hours 12-12 --max-events 200000
```

The script:
- derives a `[start,end)` window from `--hours` (UTC)
- applies the same window to trades/orderbook/mark_price via `CryptoHftDayConfig.stream_start_ms/stream_end_ms`

Optional streams:

```bash
python scripts\\run_backtest_replay.py --day 2025-07-01 --symbols BTCUSDT --hours 12-12 --include-ticker --include-open-interest --include-liquidations
```

If exploring partial coverage:

```bash
python scripts\\run_backtest_replay.py --day 2025-07-01 --symbols BTCUSDT,BTCUSDT_260626 --hours 12-12 --skip-missing
```

Execution knobs (for strategies that submit orders):
- `--submit-latency-ms`
- `--cancel-latency-ms`
- `--maker-queue-ahead-factor`
- `--maker-queue-ahead-extra-qty`
- `--maker-trade-participation`

Data knobs (anti-lookahead):
- `--open-interest-delay-ms`
- `--open-interest-alignment` (`fixed_delay`, `causal_asof`, `causal_asof_global`)
- `--open-interest-calibrated-delay-ms`
- `--open-interest-availability-quantile`
- `--open-interest-min-delay-ms`
- `--open-interest-max-delay-ms`

Recommended OI preset for realism:

```bash
python scripts\\run_backtest_replay.py --day 2025-07-01 --symbols BTCUSDT --mark-price-symbols BTCUSDT --hours 12-12 --include-ticker --include-open-interest --include-liquidations --strict-book --open-interest-alignment causal_asof --open-interest-availability-quantile 0.5 --open-interest-min-delay-ms 5000 --open-interest-max-delay-ms 300000
```

Notes:
- `causal_asof` now uses rolling past-only quantile (no same-day lookahead).
- `causal_asof_global` keeps the legacy behavior (global day quantile) and may leak future information.
- `open-interest-calibrated-delay-ms` works as an external pre-calibrated delay floor.
- `max-delay=300000` (5 min) avoids excessively stale OI snapshots in this dataset.
- deterministic equivalent: `--open-interest-alignment fixed_delay --open-interest-delay-ms 300000`.

## `run_backtest_entry_exit.py`

File: `scripts/run_backtest_entry_exit.py`

Purpose:
- run a simple entry/exit setup (market orders)
- compute realized PnL and fees
- print basic stats:
  - round trips reconstructed from fills (wins/losses, net/gross, duration)
  - equity curve sampled on `mark_price` + max drawdown

Example:

```bash
python scripts\\run_backtest_entry_exit.py --day 2025-07-01 --symbol BTCUSDT --hours 12-13 --direction long --qty 0.001 --enter-offset-s 30 --hold-s 60 --gap-s 60 --cycles 3 --out-fills-csv fills.csv --out-equity-csv equity.csv
```

## `run_backtest_ma_cross.py`

File: `scripts/run_backtest_ma_cross.py`

Purpose:
- build bars by timeframe (e.g. 5m)
- compute MA(N) and generate signals:
  - `rule=cross`: trade only on cross (price vs MA)
  - `rule=state`: stay long if price>=MA, short if price<MA
- execute market orders to reach target position
- print PnL, fees, round trips and equity curve + max drawdown

Example:

```bash
python scripts\\run_backtest_ma_cross.py --day 2025-07-01 --symbol BTCUSDT --hours 12-13 --tf-min 5 --ma-len 9 --price-source mark --rule cross --mode long_short --qty 0.001 --out-fills-csv fills.csv --out-equity-csv equity.csv
```

## `run_backtest_batch.py`

File: `scripts/run_backtest_batch.py`

Purpose:
- run a setup across multiple days (entry_exit or ma_cross)
- validate temporal continuity per day
- consolidate metrics and PnL into CSV
- optional book guard (`--strict-book`) to reduce impact of bad data

Example (5 days, MA9 5m, full day):

```bash
python scripts\\run_backtest_batch.py --start-day 2025-07-20 --days 5 --symbol BTCUSDT --hours 0-23 --setup ma_cross --tf-min 5 --ma-len 9 --price-source mark --rule cross --mode long_short --qty 0.001 --include-ticker --include-open-interest --include-liquidations --strict-book --out-csv batch_5d.csv
```

Guard knobs:
- `--strict-book-max-spread`
- `--strict-book-max-spread-bps`
- `--strict-book-max-staleness-ms`
- `--strict-book-cooldown-ms`
- `--strict-book-warmup-depth-updates`
- `--strict-book-reset-on-mismatch` / `--strict-book-no-reset-on-mismatch`
- `--strict-book-reset-on-crossed` / `--strict-book-no-reset-on-crossed`

Default guard profile used by scripts:
- `max_staleness_ms=250`
- `max_spread_bps=5.0`
- `cooldown_ms=1000`
- `warmup_depth_updates=1000`

CSV columns:
- temporal: `out_of_order`, `duplicates`, `outside_window`
- continuity: `final_id_nonmonotonic`, `prev_id_mismatch`
- book sanity: `book_crossed`, `book_missing_side`, `spread_*`
- guard: `strict_guard_*`, `strict_blocked_submits`, `strict_blocked_submit_reason`
- results: `round_trips`, `net_pnl_usdt`, `fees_usdt`, `max_drawdown_usdt`

## `analyze_replay_temporal.py`

File: `scripts/analyze_replay_temporal.py`

Purpose:
- validate temporal ordering (per stream and merge)
- check `[start,end)` window
- check orderbook continuity (`prev_final_update_id` vs `final_update_id`)
- sanity-check the book (spread and crossed book)

Example:

```bash
python scripts\\analyze_replay_temporal.py --day 2025-07-01 --symbols BTCUSDT --hours 12-12 --include-ticker --include-open-interest --include-liquidations --skip-missing --max-events 0 --book-check-every 5000
```

To simulate open interest delay:

```bash
python scripts\\analyze_replay_temporal.py --day 2025-07-01 --symbols BTCUSDT --hours 12-12 --include-open-interest --open-interest-delay-ms 5000
```

Recommended temporal check with the same OI preset:

```bash
python scripts\\analyze_replay_temporal.py --day 2025-07-01 --symbols BTCUSDT --mark-price-symbols BTCUSDT --hours 12-12 --include-ticker --include-open-interest --include-liquidations --open-interest-alignment causal_asof --open-interest-availability-quantile 0.5 --open-interest-min-delay-ms 5000 --open-interest-max-delay-ms 300000 --cross-check-fresh-book-ms 250 --cross-check-mid-band-bps 30 --book-check-every 5000 --max-events 0
```

Optionally control the window explicitly:

```bash
python scripts\\run_backtest_replay.py --day 2025-07-01 --symbols BTCUSDT --hours 0-23 --start-utc 2025-07-01T12:00:00Z --end-utc 2025-07-01T13:00:00Z
```

## `inspect_orderbook_parquet.py` (local)

File: `scripts/inspect_orderbook_parquet.py`

Purpose:
- print Parquet schema and metadata
- check `event_time` range
- check monotonicity (`event_time`, `final_update_id`)
- rows per message (group by `final_update_id`)
- basic continuity via `prev_final_update_id`

Example:

```bash
python scripts\\inspect_orderbook_parquet.py C:\\Users\\marco\\Downloads\\orderbook_00.parquet
```

## `replay_orderbook.py` (local)

File: `scripts/replay_orderbook.py`

Purpose:
- apply L2 deltas into an in-memory `L2Book`
- print periodic snapshots (best bid/ask, mid, impact VWAP)

Example:

```bash
python scripts\\replay_orderbook.py C:\\Users\\marco\\Downloads\\orderbook_00.parquet --max-messages 2000
```
