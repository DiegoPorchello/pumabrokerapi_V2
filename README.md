# pumabroker — Biblioteca Python para Puma Broker

Integração em tempo real com a plataforma **trade.pumabroker.com** via
WebSocket e REST, baseada em engenharia reversa do protocolo real
(capturado via DevTools em 15/06/2026).

---

## Protocolo Descoberto (100% real — sem invenção)

### 3 conexões simultâneas

```
[Cliente]
  ├── REST HTTP ────────────► trade.pumabroker.com
  │     ├── GET /me           (perfil)
  │     ├── GET /accounts     (contas)
  │     ├── GET /balance      (saldo)
  │     ├── GET /settings     (configurações)
  │     └── GET /active       (ativos disponíveis)
  │
  ├── WS2 WebSocket puro ───► wss://wsm5.pumabroker.com/
  │     Frame recebido (real):
  │     {"type":"bar_update","symbol":"EURUSD","interval":"5",
  │      "bar":{"time":1781556900,"open":1.15884,"high":1.159,
  │             "low":1.15883,"close":1.15896,"volume":190.0}}
  │     {"type":"server_time","timestamp":1781557272731}
  │
  └── WS3 Socket.IO v4 ─────► wss://trade.pumabroker.com/socket.io/
        Namespaces: /trades, /otc
        Handshake (real):
          0{"sid":"AAxrTo2u...","pingInterval":25000,"pingTimeout":20000}
        Subscribe (real):
          42/trades,["subscribe","28318"]
        tradeUpdate (real — capturado após COMPRA):
          42/trades,["tradeUpdate",{
            "id":"5186325",
            "symbol":"BETHUSDT",
            "direction":"CALL",     ← MAIÚSCULAS
            "amount":2,
            "entryPrice":"911.31632",
            "exitPrice":null,
            "profit":0,
            "payout":0.87,          ← 87% de retorno
            "status":"ACTIVE",
            "isDemo":false
          }]
```

### Autenticação

```
Cookie: server_name_session=cd0dc3ba351b950fc7621ef63b19d855
```

Sem login programático — usar token da sessão do navegador.

---

## Instalação

```bash
pip install -r requirements.txt
```

### Variáveis de ambiente

```bash
# .env
PUMA_SESSION=cd0dc3ba351b950fc7621ef63b19d855
LOG_LEVEL=INFO
```

---

## Como obter o session_token e account_id

```
session_token:
  1. Abra trade.pumabroker.com e faça login
  2. F12 → Application → Cookies → trade.pumabroker.com
  3. Copie o valor de "server_name_session"

account_id:
  1. F12 → Network → Socket → socket.io/?EIO=4...
  2. Aba Messages → procure: 42/trades,["subscribe","XXXXX"]
  3. O número XXXXX é o seu account_id
```

---

## Uso

```python
import asyncio
from pumabroker import PumaBroker, TradeUpdate, BarUpdateEvent

async def main():
    pb = PumaBroker(
        session_token="SEU_TOKEN_AQUI",
        account_id="SEU_ACCOUNT_ID",
    )

    async with pb:
        # Candles em tempo real (wsm5)
        pb.on_bar("BETHUSDT", "1", lambda b: print(b.bar.close))

        # Resultado de ordens (confirmado: evento "tradeUpdate")
        def on_trade(event, trade: TradeUpdate):
            print(f"{trade.direction} {trade.status} payout={trade.payout_percent():.0f}%")

        pb.on_event("tradeUpdate", on_trade)

        # Ordem (⚠️ confirme o evento de envio — ver seção abaixo)
        await pb.buy_call("BETHUSDT", amount=2.0, duration=60)

        await pb.listen()

asyncio.run(main())
```

---

## Status de confirmação por componente

| Componente                               | Status        | Observação                          |
| ---------------------------------------- | ------------- | ----------------------------------- |
| WS2 URL (`wsm5`)                         | ✅ Confirmado | Frame real capturado                |
| WS2 `bar_update`                         | ✅ Confirmado | Payload real capturado              |
| WS3 URL (`socket.io`)                    | ✅ Confirmado | Handshake real capturado            |
| WS3 namespaces `/trades` `/otc`          | ✅ Confirmado | Frames reais                        |
| WS3 `subscribe` + account_id             | ✅ Confirmado | Frame real: `["subscribe","28318"]` |
| WS3 `tradeUpdate` (recebido)             | ✅ Confirmado | Frame real capturado                |
| `direction` em maiúsculas (`CALL`/`PUT`) | ✅ Confirmado | Frame real                          |
| `payout` = 0.87 (87%)                    | ✅ Confirmado | Frame real                          |
| `status` = `"ACTIVE"`                    | ✅ Confirmado | Frame real                          |
| Evento de ENVIO de ordem                 | ⚠️ Estimado   | Capturar frame ↑ no DevTools        |

---

## Como confirmar o evento de envio de ordem

O servidor retorna `tradeUpdate` após uma ordem. O evento de **envio**
(cliente → servidor) ainda não foi capturado. Para descobrir:

```
1. Abra DevTools → Network → Socket → socket.io/?EIO=4
2. Aba Messages → marque "↑" para filtrar frames enviados
3. Clique em COMPRA
4. Copie o frame que aparece com seta para cima (↑)
5. Atualize ws_trades.py → place_order() → linha `_send_sio(namespace, "trade", ...)`
   substituindo "trade" pelo evento correto
```

---

## Estrutura

```
pumabroker/
├── pumabroker/
│   ├── __init__.py
│   ├── client.py      → PumaBroker (interface unificada)
│   ├── ws_market.py   → WebSocket wsm5 (candles OHLCV)
│   ├── ws_trades.py   → Socket.IO /trades /otc (ordens)
│   ├── models.py      → Pydantic v2 (frames reais)
│   └── config.py      → URLs e configuração
├── example.py
├── requirements.txt
└── README.md
```
