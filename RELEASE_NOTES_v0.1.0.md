# Release notes for v0.1.0

## Highlights
- btengine extracted as a standalone repo (engine-only, no strategy code)
- full docs set (quickstart, scripts, API reference, execution model)
- scripts for dataset validation and replay
- test suite included and passing
- packaging verified (wheel + sdist)

## Included
- Core engine: `btengine.engine`, `btengine.broker`, `btengine.types`, `btengine.replay`
- Execution models: maker/taker, queue model
- Marketdata: L2 book + impact VWAP
- Data adapter: CryptoHFTData (S3 parquet)

## Notes
- Strategy examples are external (consumer repos)
- S3 credentials loaded via `.env` (not committed)

## Install
```bash
pip install "git+https://github.com/marcosarm/btengine.git@v0.1.0"
```
