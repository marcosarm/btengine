# AGENTS.md

This repo is the btengine backtest engine only.
Do not add strategy code here; use consumer repos (example: `C:\\4mti\\Projetos\\tbot_funding_arb`).

## Key locations
- Engine code: `src\\btengine\\`
- Scripts: `scripts\\`
- Tests: `tests\\`
- Docs: `docs\\btengine\\`
- Context: `CONTEXT.md`

## Install and tests
```bash
pip install -e .
pip install -e ".[dev]"
pytest -q
```

## Environment
- Use `.env.example` to create a local `.env`.
- Do not commit `.env`.
- Required: `AWS_REGION`, `S3_BUCKET`, `S3_PREFIX`

## Typical dev flow
1. Read `docs\\btengine\\README.md` and `docs\\btengine\\api_reference.md`
2. Run unit tests (`pytest -q`)
3. Validate a small S3 window with `scripts\\validate_s3_dataset.py`
4. Replay a small window with `scripts\\run_backtest_replay.py`
