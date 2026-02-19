# btengine

btengine is a small, event-driven backtest/simulation engine for crypto market data
(L2 orderbook + trades + mark price), with an adapter for CryptoHFTData (Parquet in S3).

Goals:
- reusable engine, strategy-agnostic
- stable public API for consumer repos
- realistic-enough fills (taker via L2 consumption, maker via queue model)

What it is not (yet):
- full exchange-grade matching engine
- detailed microstructure/latency modeling

## Quickstart

```bash
pip install -e .
pip install -e ".[dev]"
pytest -q
```

Private install (consumer repo, SSH):

```bash
pip install "git+ssh://git@github.com/marcosarm/btengine.git@<tag>"
```

## S3 / CryptoHFTData

Use `.env.example` to create a local `.env`.

Required keys:
- `AWS_REGION`
- `S3_BUCKET`
- `S3_PREFIX`

Optional (if not using default AWS credentials chain):
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`

## Useful scripts

- `scripts/validate_s3_dataset.py`
- `scripts/run_backtest_replay.py`
- `scripts/run_backtest_entry_exit.py`
- `scripts/run_backtest_ma_cross.py`
- `scripts/run_backtest_batch.py`
- `scripts/analyze_replay_temporal.py`

## Recommended OI preset (anti-lookahead)

For realistic open-interest alignment, prefer causal availability with a hard cap:

```bash
--open-interest-alignment causal_asof \
--open-interest-availability-quantile 0.5 \
--open-interest-min-delay-ms 5000 \
--open-interest-max-delay-ms 300000
```

Equivalent deterministic fallback:

```bash
--open-interest-alignment fixed_delay --open-interest-delay-ms 300000
```

## Docs

- `docs/btengine/quickstart.md`
- `docs/btengine/reuse_in_other_projects.md`
- `docs/btengine/core_concepts.md`
- `docs/btengine/crypto_hftdata.md`
- `docs/btengine/execution_model.md`
- `docs/btengine/api_reference.md`
- `docs/btengine/scripts.md`
- `docs/btengine/implementation_plan.md`

## Repo layout

- `src/btengine/engine.py`
- `src/btengine/types.py`
- `src/btengine/replay.py`
- `src/btengine/marketdata/orderbook.py`
- `src/btengine/broker.py`, `src/btengine/execution/*`
- `src/btengine/data/cryptohftdata/*`

## Strategies live elsewhere

Example consumer repo: `C:\4mti\Projetos\tbot_funding_arb`.

## Context

See `CONTEXT.md` and `AGENTS.md`.
