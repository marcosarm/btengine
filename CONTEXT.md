# Contexto do Projeto Btengine

## Objetivo

- Disponibilizar um motor de backtest event-driven reutilizável.
- Manter API estável para projetos consumidores (como `tbot_funding_arb`).
- Conter apenas infraestrutura de simulação e execução, sem estratégia de negócio.

## Interface esperada

- `EngineConfig`, `BacktestEngine`, `EngineContext`
- Tipos de mercado em `btengine.types`
- Broker/execution: `btengine.broker` e `btengine.execution.*`

## Consumidores

- Estratégia de funding/basis está no repositório `tbot_funding_arb` (pacote `funding`).
- O consumidor não deve criar dependências reversas para dentro do motor.

## Bootstrap

```bash
# Instalação local para desenvolvimento:
pip install -e C:\4mti\Projetos\btengine
```

## Publicação/integração

- Preferível publicar por Git tags ou commits fixos.
- Consumidores devem fixar versão em `pyproject.toml` para reprodutibilidade.
