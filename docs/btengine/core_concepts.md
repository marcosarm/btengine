# Conceitos centrais (eventos, tempo, streams)

## Eventos

O `btengine` trabalha com um stream unico de eventos por simbolo (ou multi-simbolo), onde cada evento tem `event_time_ms` (epoch ms, UTC) e tipos principais:

- `DepthUpdate`: delta L2 (bids/asks) agregado por `final_update_id`
- `Trade`: negocio individual (trade tape)
- `MarkPrice`: mark/index/funding_rate (tipicamente 1Hz)
- `Ticker`: ticker agregado (estatisticas tipo 24h rolling window)
- `OpenInterest`: snapshots de open interest (baixa frequencia)
- `Liquidation`: eventos de liquidacao (force orders)

Definicoes (ver `src/btengine/types.py`):

- `received_time_ns`: quando o dado foi recebido/ingestado (ns)
- `event_time_ms`: timestamp do evento na exchange (ms) e o "tempo" do backtest
- `transaction_time_ms` (depth): timestamp adicional (ms), quando disponivel
- `trade_time_ms` (trade): timestamp do trade (ms). No adapter CryptoHFTData, ele vira o `event_time_ms` canonico do Trade
- `next_funding_time_ms` (mark_price): proximo horario de funding (ms)
- `timestamp_ms` (open_interest): timestamp medido do snapshot (ms). `event_time_ms` pode ser igual ou `timestamp_ms + delay` dependendo da configuracao.

No `EngineContext`, os ultimos valores por simbolo sao mantidos em:

- `ctx.mark[symbol] -> MarkPrice`
- `ctx.ticker[symbol] -> Ticker`
- `ctx.open_interest[symbol] -> OpenInterest`
- `ctx.liquidation[symbol] -> Liquidation` (ultimo evento)

## Stream ordering e merge

`btengine.replay.merge_event_streams()` faz um k-way merge de multiplos streams assumindo que cada um esta ordenado por `event_time_ms`.
Em empate de `event_time_ms`, o desempate usa:

- `received_time_ns` (quando disponivel)
- metadados deterministicos do evento (prioridade por tipo + ids conhecidos)
- ordem do stream (fallback final)

Isso reduz vies de causalidade em empates sem perder determinismo.

No adapter CryptoHFTData:

- orderbook: pode precisar ordenar por `final_update_id` para reconstruir a ordem correta do stream (ver `iter_depth_updates`)
- trades: pode precisar ordenar por `trade_time`
- mark_price: detecta e, se necessario, ordena por `event_time`

Se um stream nao estiver ordenado, o merge pode produzir "viagem no tempo" e quebrar invariantes do motor.

No `BacktestEngine`, existe um modo opcional para bloquear isso imediatamente:

- `EngineConfig.strict_event_time_monotonic=True`
  - levanta erro se aparecer um evento com `event_time_ms` menor que o anterior
  - util para fail-fast em pipelines de dados com risco de desordem

Observacao sobre open interest:

- modo recomendado: `open_interest_alignment_mode=causal_asof` (rolling past-only, sem olhar futuro)
- modo legado disponivel para comparacao: `causal_asof_global` (usa quantil global do dia e pode introduzir leakage)
- `open_interest_calibrated_delay_ms` pode ser usado como piso externo pre-calibrado (offline)

## Janelas de tempo (stream vs trading)

Existem dois conceitos de janela:

1) Janela do stream (dados que entram no motor)

- `btengine.data.cryptohftdata.CryptoHftDayConfig.stream_start_ms`
- `btengine.data.cryptohftdata.CryptoHftDayConfig.stream_end_ms`

Quando setados, o `build_day_stream()` fatia trades/orderbook/mark_price antes de fazer o merge (bom para focar em horas especificas).

2) Janela de trading (quando a estrategia pode operar)

- `btengine.engine.EngineConfig.trading_start_ms`
- `btengine.engine.EngineConfig.trading_end_ms`
- `btengine.engine.EngineConfig.trading_window_mode` (`entry_only` | `block_all`)
- `btengine.engine.EngineConfig.allow_reducing_outside_trading_window`
- `EngineContext.is_trading_time()`

Nota: o engine bloqueia `ctx.broker.submit(...)` fora dessa janela (mas os callbacks ainda rodam para warmup/estado).
- modo `entry_only`: bloqueia novas entradas e permite ordens redutoras (se `allow_reducing_outside_trading_window=True`)
- modo `block_all`: bloqueia qualquer submit fora da janela

Um padrao comum e:

- alimentar o motor com um periodo de warmup (para construir book/indicadores)
- habilitar operacao apenas depois de `trading_start_ms`

## Ticks (clock discreto)

`EngineConfig.tick_interval_ms` ativa ticks discretos. O engine:

- ancora o primeiro tick no primeiro evento observado
- o primeiro tick e exatamente o timestamp do primeiro evento (nao ha "floor" para alinhar na grade)
- chama `strategy.on_tick(tick_ms, ctx)` em grade fixa ate o timestamp do evento atual

Isso e util para estrategias que rodam logica periodica (ex: a cada 1s) sem depender de cada evento.

## Ordem de processamento no Engine

No `BacktestEngine.run()` (`src/btengine/engine.py`):

1) atualiza `ctx.now_ms = event.event_time_ms`
2) dispara ticks ate `now_ms` (se habilitado)
3) aplica o evento ao estado:
   - `DepthUpdate`: atualiza book e permite progressao de ordens maker
   - `Trade`: permite fills maker via trade tape
   - `MarkPrice`: atualiza ultimo mark e aplica funding se devido
4) chama `strategy.on_event(event, ctx)` (se existir)

Isso significa que dentro de `on_event` o `ctx.books[symbol]` ja reflete o evento aplicado.

`EngineConfig.broker_time_mode` controla quando o broker processa latencias/cancelamentos (`on_time`):
- `before_event`: antes de aplicar o evento
- `after_event` (padrao): depois de aplicar o evento

Observacao de guard estrito:
- quando `BookGuardedBroker` trippa (mismatch/crossed/stale/etc), ordens pendentes
  antigas do simbolo tambem sao invalidadas para evitar ativacao durante cooldown/warmup.
