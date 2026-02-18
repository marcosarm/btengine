# Context for btengine

This file is a quick start context for a new Codex instance working on btengine.
The engine is isolated here; strategy code lives in consumer repos.

## Goals
- reusable, event-driven backtest engine
- stable public API for consumers
- no strategy code inside this repo

## Where to start (key files)
- `README.md`
- `docs\\btengine\\README.md`
- `docs\\btengine\\quickstart.md`
- `docs\\btengine\\api_reference.md`
- `docs\\btengine\\scripts.md`

## Install and tests

```bash
pip install -e .
pip install -e ".[dev]"
pytest -q
```

## Environment (S3 / CryptoHFTData)
- Use `.env.example` to create a local `.env`.
- Do not commit `.env`.

Required keys:
- `AWS_REGION`
- `S3_BUCKET`
- `S3_PREFIX`

Optional (if not using default AWS credentials chain):
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`

## Core API surface
- `btengine.engine`: `BacktestEngine`, `EngineConfig`, `EngineContext`
- `btengine.broker`: `SimBroker`, `Fill`
- `btengine.execution.orders`: `Order`
- `btengine.types`: `DepthUpdate`, `Trade`, `MarkPrice`, `Ticker`, `OpenInterest`, `Liquidation`, `Side`
- `btengine.replay`: `merge_event_streams`, `slice_event_stream`

## Scripts (sanity checks)
- `scripts\\validate_s3_dataset.py`
- `scripts\\run_backtest_replay.py`
- `scripts\\run_backtest_entry_exit.py`
- `scripts\\run_backtest_ma_cross.py`
- `scripts\\run_backtest_batch.py`
- `scripts\\analyze_replay_temporal.py`

## Consumer example
Strategy example lives in a separate repo:
- `C:\\4mti\\Projetos\\tbot_funding_arb`

Keep the boundary clean: no strategy code in btengine.
