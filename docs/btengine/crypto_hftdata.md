# Adapter CryptoHFTData (S3 + Parquet)

## Layout (paths)

O dataset segue particionamento por dia (UTC) e, para orderbook, por hora:

- Trades: `trades/{exchange}/{symbol}/YYYY/MM/DD/trades.parquet`
- Mark price: `mark_price/{exchange}/{symbol}/YYYY/MM/DD/mark_price.parquet`
- Orderbook: `orderbook/{exchange}/{symbol}/YYYY/MM/DD/orderbook_{HH}.parquet`
- Ticker: `ticker/{exchange}/{symbol}/YYYY/MM/DD/ticker.parquet`
- Open interest: `open_interest/{exchange}/{symbol}/YYYY/MM/DD/open_interest.parquet`
- Liquidations: `liquidations/{exchange}/{symbol}/YYYY/MM/DD/liquidations.parquet`

No codigo:

- builder de paths: `btengine.data.cryptohftdata.CryptoHftLayout` (`src/btengine/data/cryptohftdata/paths.py`)
- leitura/replay: `btengine.data.cryptohftdata.build_day_stream` (`src/btengine/data/cryptohftdata/replay.py`)
  - streams adicionais: `iter_ticker*`, `iter_open_interest*`, `iter_liquidations*`

## Conectando no S3 via PyArrow

O adapter usa `pyarrow.fs.S3FileSystem`. Para criar:

- `btengine.data.cryptohftdata.S3Config`
- `btengine.data.cryptohftdata.make_s3_filesystem`

Os scripts do repo leem um `.env` via `btengine.util.load_dotenv` e montam o filesystem com:

- `AWS_REGION`
- `S3_BUCKET`
- `S3_PREFIX`
- opcionais: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`

Exemplo de uso (simplificado):

```python
from btengine.data.cryptohftdata import CryptoHftLayout, S3Config, make_s3_filesystem

fs = make_s3_filesystem(S3Config(region="ap-northeast-1"))
layout = CryptoHftLayout(bucket="amzn-tdata", prefix="hftdata")
```

## Schemas (resumo)

Os Parquets reais podem ter colunas extras, mas o adapter espera (baseado nas amostras em `cryptohftdata_amostras.md`):

Trades (`trades.parquet`):

- `received_time` (int64, ns)
- `event_time` (int64, ms) (na pratica pode existir, mas o adapter usa `trade_time` como tempo canonico)
- `trade_time` (int64, ms)
- `symbol` (string)
- `trade_id` (int64)
- `price` (string/float) -> e convertido para `float64`
- `quantity` (string/float) -> e convertido para `float64`
- `is_buyer_maker` (bool)

Orderbook (`orderbook_{HH}.parquet`, representacao flattened):

- `received_time` (int64, ns)
- `event_time` (int64, ms)
- `transaction_time` (int64, ms)
- `symbol` (string)
- `first_update_id` (int64)
- `final_update_id` (int64)
- `prev_final_update_id` (int64)
- `side` ("bid"/"ask")
- `price` (string) -> cast float64
- `quantity` (string) -> cast float64

Mark price (`mark_price.parquet`):

- `received_time` (int64, ns)
- `event_time` (int64, ms)
- `symbol` (string)
- `mark_price` (string/float) -> cast float64
- `index_price` (string/float) -> cast float64
- `funding_rate` (string/float) -> cast float64
- `next_funding_time` (int64, ms)

## Ordenacao e "interleaving" (cuidado importante)

Na pratica, nem sempre os Parquets estao armazenados fisicamente ordenados pelo tempo.

Consequencias:

- orderbook pode ter linhas de um mesmo `final_update_id` intercaladas com outros `final_update_id`
- trades podem nao estar monotonicos em `trade_time`

O adapter faz:

- orderbook: detecta (heuristica) e, se necessario, ordena por `final_update_id` para reconstruir mensagens coerentes (`iter_depth_updates`)
- trades: detecta e, se necessario, ordena por `trade_time` (`iter_trades`)
- mark_price: detecta e, se necessario, ordena por `event_time` (`iter_mark_price`)
- ticker/liquidations/open_interest: mesmo principio (auto-sort quando necessario)

Tradeoff:

- a deteccao de ordenacao avalia row groups ao longo do arquivo (nao apenas o primeiro), para reduzir falso "ordenado"
- ordenar costuma exigir ler o arquivo inteiro na memoria (por hora, no caso do orderbook)
- isso e mais custoso, mas evita bugs de replay (mensagens quebradas / viagem no tempo)

Mitigacao de memoria (novo):

- os readers `iter_*_advanced(..., sort_row_limit=...)` aceitam limite de linhas
  para proteger contra sort in-memory muito grande
- default atual: `10_000_000` linhas
- se `sort_row_limit` nao for informado, os readers tentam `BTENGINE_SORT_ROW_LIMIT`
  do ambiente antes do fallback no codigo
- se exceder esse limite no caminho que exige sort, o reader levanta `MemoryError`
  com orientacao para reduzir janela temporal ou pre-processar/parquet pre-ordenado

Exemplos:

```python
from btengine.data.cryptohftdata.trades import iter_trades_advanced
from btengine.data.cryptohftdata.orderbook import iter_depth_updates_advanced

trades = iter_trades_advanced("trades.parquet", sort_mode="auto", sort_row_limit=2_000_000)
depth = iter_depth_updates_advanced("orderbook_12.parquet", sort_mode="auto", sort_row_limit=1_000_000)
```

Exemplo via `.env`:

```dotenv
BTENGINE_SORT_ROW_LIMIT=20000000
```

## Horas faltantes (orderbook)

Nem todo dia tem as 24 horas de `orderbook_{HH}.parquet`.

Para replay robusto:

- use `CryptoHftDayConfig.orderbook_skip_missing=True` (default)
- restrinja `orderbook_hours` para janelas que voce sabe que existem, quando for debug

## Open interest (arquivo parcial)

Em alguns dias, o open interest pode aparecer como `open_interest_parcial.parquet` (multi-dia).
O adapter `iter_open_interest_for_day(...)`:

- tenta `open_interest.parquet` e faz fallback para `open_interest_parcial.parquet` se necessario
- filtra as linhas para o dia solicitado (UTC) com base na coluna `timestamp`
- ordena por `timestamp` apos filtrar

Para evitar lookahead, voce pode atrasar a disponibilidade do snapshot via `CryptoHftDayConfig.open_interest_delay_ms` (ver `docs/btengine/scripts.md`).

## Alinhamento temporal opcional (trade/mark/ticker/liquidation)

Para reduzir mismatch de clocks entre streams assincronos, o replay agora suporta
alinhar `event_time_ms` de `trades`, `mark_price`, `ticker` e `liquidations`:

- `stream_alignment_mode`: `none` (default), `fixed_delay`, `causal_asof`, `causal_asof_global`
- delays base por stream:
  - `trade_delay_ms`
  - `mark_price_delay_ms`
  - `ticker_delay_ms`
  - `liquidation_delay_ms`
- limites:
  - `stream_alignment_min_delay_ms`
  - `stream_alignment_max_delay_ms`
- em `causal_asof`:
  - `stream_alignment_quantile`
  - `stream_alignment_history_size` (janela rolling, causal, sem olhar futuro)
- em `causal_asof_global`:
  - `stream_alignment_global_row_limit` limita materializacao in-memory
  - se exceder, o replay levanta `MemoryError` com orientacao de janela menor/modo alternativo

Garantia adicional:

- apos o shift temporal, o adapter faz clamp monotono por stream para evitar `event_time_ms` regredir.

Uso recomendado:

- manter `none` para baseline bruto;
- usar `fixed_delay` em producao quando voce ja tem atraso calibrado offline;
- usar `causal_asof` para ajuste adaptativo sem leakage.

## Preprocess (sort + dedup) antes do replay

Script novo para pre-ordenar parquet e remover duplicatas por chave de evento:

```bash
python scripts\\preprocess_parquet_timeseries.py --kind trades --input trades.parquet --output trades.sorted.parquet
python scripts\\preprocess_parquet_timeseries.py --kind orderbook --input orderbook_12.parquet --output orderbook_12.sorted.parquet
```

Isso ajuda a reduzir regressao temporal e ruido de duplicatas antes de rodar o motor.

Knob de memoria:

- `--max-rows-in-memory` (default `2_000_000`; `<=0` desabilita fail-fast)

## Validacao do dataset

O repo inclui `scripts/validate_s3_dataset.py` para checar existencia, schema, contagem de linhas e range de timestamps.

Ver: `docs/btengine/scripts.md`
