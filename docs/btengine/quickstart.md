# Quickstart

## 1) Install dependencies

In a virtualenv:

```bash
pip install -e .
pip install -e ".[dev]"
```

Run tests:

```bash
pytest -q
```

## 1b) Consume btengine in another repo

Git pin (private, SSH):

```bash
pip install "git+ssh://git@github.com/marcosarm/btengine.git@<commit-or-tag>"
```

Local editable:

```bash
pip install -e C:\\path\\to\\btengine
```

Full reuse guide:
- `docs/btengine/reuse_in_other_projects.md`

## 2) Configure S3 access (CryptoHFTData)

Use `.env.example` as a template and create a local `.env`.

```dotenv
AWS_REGION=ap-northeast-1
S3_BUCKET=amzn-tdata
S3_PREFIX=hftdata

# Optional (if you are not using default AWS credentials chain):
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

## 3) Validate dataset quickly (S3)

Example: validate `BTCUSDT` on `2025-07-01` (UTC) for hour 12:

```bash
python scripts\\validate_s3_dataset.py --day 2025-07-01 --symbols BTCUSDT --hours 12-12
```

## 4) Replay data in the engine (no strategy)

Example:

```bash
python scripts\\run_backtest_replay.py --day 2025-07-01 --symbols BTCUSDT --mark-price-symbols BTCUSDT --hours 12-12 --max-events 200000 --include-ticker --include-open-interest --include-liquidations
```

The script derives a `[start,end)` window from `--hours` and applies it to
trades/orderbook/mark_price via `CryptoHftDayConfig.stream_start_ms/stream_end_ms`.

## 4b) Simple entry/exit setup (PnL sanity check)

```bash
python scripts\\run_backtest_entry_exit.py --day 2025-07-01 --symbol BTCUSDT --hours 12-12 --direction long --qty 0.001 --enter-offset-s 30 --hold-s 60
```

## 4c) Simple MA(9) strategy on 5m bars

```bash
python scripts\\run_backtest_ma_cross.py --day 2025-07-01 --symbol BTCUSDT --hours 12-13 --tf-min 5 --ma-len 9 --price-source mark --rule cross --mode long_short --qty 0.001
```

## 4d) Multi-day batch with book guard

```bash
python scripts\\run_backtest_batch.py --start-day 2025-07-20 --days 5 --symbol BTCUSDT --hours 0-23 --setup ma_cross --tf-min 5 --ma-len 9 --price-source mark --rule cross --mode long_short --qty 0.001 --include-ticker --include-open-interest --include-liquidations --strict-book --out-csv batch_5d.csv
```

Useful guard knobs:
- `--strict-book-max-spread`
- `--strict-book-max-spread-bps`
- `--strict-book-max-staleness-ms`
- `--strict-book-cooldown-ms`
- `--strict-book-warmup-depth-updates`

## 4e) Strategy examples (external)

Full strategies live in consumer repos, not in btengine.
Example: `C:\\4mti\\Projetos\\tbot_funding_arb`.

## 5) Minimal code example

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from btengine.broker import SimBroker
from btengine.data.cryptohftdata import (
    CryptoHftDayConfig,
    CryptoHftLayout,
    S3Config,
    build_day_stream,
    make_s3_filesystem,
)
from btengine.engine import BacktestEngine, EngineConfig, EngineContext
from btengine.execution.orders import Order
from btengine.types import DepthUpdate, MarkPrice, Trade


def day_start_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


@dataclass
class DemoStrategy:
    symbol: str
    did_submit: bool = False

    def on_event(self, event: DepthUpdate | Trade | MarkPrice, ctx: EngineContext) -> None:
        # Submit a market order once the book exists.
        if self.did_submit:
            return
        book = ctx.books.get(self.symbol)
        if book is None:
            return

        ctx.broker.submit(
            Order(id="mkt1", symbol=self.symbol, side="buy", order_type="market", quantity=0.001),
            book,
            now_ms=ctx.now_ms,
        )
        self.did_submit = True


def main() -> None:
    bucket = "amzn-tdata"
    prefix = "hftdata"
    fs = make_s3_filesystem(S3Config(region="ap-northeast-1"))
    layout = CryptoHftLayout(bucket=bucket, prefix=prefix)

    d = date(2025, 7, 1)
    start = day_start_ms(d) + 12 * 3_600_000
    end = start + 3_600_000

    cfg = CryptoHftDayConfig(
        exchange="binance_futures",
        include_orderbook=True,
        include_trades=True,
        include_mark_price=True,
        orderbook_hours=range(12, 13),
        orderbook_skip_missing=True,
        stream_start_ms=start,
        stream_end_ms=end,
    )

    events = build_day_stream(layout, cfg=cfg, symbol="BTCUSDT", day=d, filesystem=fs)

    broker = SimBroker()
    engine = BacktestEngine(config=EngineConfig(tick_interval_ms=1000), broker=broker)
    res = engine.run(events, strategy=DemoStrategy(symbol="BTCUSDT"))

    print("fills:", len(res.ctx.broker.fills))
    print("realized_pnl_usdt:", res.ctx.broker.portfolio.realized_pnl_usdt)


if __name__ == "__main__":
    main()
```
