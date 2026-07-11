# Puma Broker API — Documentação Completa

> Descoberta via engenharia reversa (DevTools) — Junho/Julho 2026  
> Versão: 2.0 | Atualizado em: 05/07/2026

---

## Índice

1. [Arquitetura](#arquitetura)
2. [Autenticação](#autenticação)
3. [REST API](#rest-api)
4. [WebSocket 2 — Candles (wsm5)](#websocket-2--candles-wsm5)
5. [WebSocket 3 — Socket.IO (trades/otc)](#websocket-3--socketio-tradesotc)
6. [Infraestrutura Local (Proxy)](#infraestrutura-local-proxy)
7. [Bugs Conhecidos e Soluções](#bugs-conhecidos-e-soluções)
8. [Inicialização](#inicialização)
9. [Modelos de Dados](#modelos-de-dados)

---

## Arquitetura

```
Navegador (browser)
    │
    ├── REST direto ──────────────────────────────► trade.pumabroker.com
    │     POST /login, POST /trades, GET /history
    │
    ├── via proxy_daemon.py (porta 3001) ─────────► trade.pumabroker.com
    │     GET /api/v1/tradingview/history
    │     (necessário para contornar CORS)
    │
    └── via ws_proxy.py (porta 3002) ────────────► wsm5.pumabroker.com
          WebSocket candles em tempo real
          (necessário pois browser não envia Cookie no handshake WS)

Socket.IO direto ────────────────────────────────► trade.pumabroker.com/socket.io/
    /trades → resultado de ordens (tradeUpdate)
    /otc    → candles OTC/híbridos em tempo real
```

### Por que o proxy é necessário

O browser tem duas limitações de segurança:

1. **CORS** — `trade.pumabroker.com` não libera todas as origens
2. **WebSocket headers** — o browser não permite enviar `Cookie` customizado no handshake HTTP→WS

O Python roda fora do browser e não tem essas restrições, por isso atua como intermediário.

---

## Autenticação

### Login

```
POST https://trade.pumabroker.com/login
Content-Type: application/json
```

**Payload confirmado (16/06/2026):**
```json
{
  "email":    "usuario@email.com",
  "password": "senha"
}
```

**Response confirmada:**
```json
{
  "user": {
    "id":           "28318",
    "email":        "usuario@email.com",
    "name":         "NOME COMPLETO",
    "firstName":    "NOME",
    "lastName":     "SOBRENOME",
    "balance":      0,
    "demoBalance":  9998,
    "bonus":        18.97,
    "isDemo":       true,
    "isVip":        true,
    "vipLevel":     1,
    "verified":     true,
    "country":      "BR",
    "realTrades":   325,
    "rollover":     5182.6,
    "rolloverTotal":275000,
    "depositos":    600,
    "winrate":      0,
    "displayCurrency": "BRL"
  },
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Campos importantes:**
| Campo | Uso |
|-------|-----|
| `token` | JWT Bearer — usado em TODOS os requests seguintes |
| `user.id` | account_id — usado no subscribe do Socket.IO |
| `user.demoBalance` | Saldo da conta demo |
| `user.balance` | Saldo da conta real |

**Como renovar o token:**
O JWT expira em ~24h. Para renovar, basta chamar `POST /login` novamente com as mesmas credenciais.

---

## REST API

Todos os endpoints REST usam:
```
Authorization: Bearer <jwt_token>
Content-Type: application/json
Origin: https://trade.pumabroker.com
```

### Endpoints confirmados

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| POST | `/login` | Autenticação — retorna JWT |
| GET | `/me` | Perfil do usuário |
| GET | `/accounts` | Lista de contas |
| GET | `/balance` | Saldo atual |
| GET | `/settings` | Configurações |
| GET | `/active` | Ativos disponíveis |
| GET | `/currencies` | Moedas disponíveis |
| GET | `/status` | Status da plataforma |
| GET | `/notifications` | Notificações |
| POST | `/trades` | **Abrir ordem** |
| GET | `/api/v1/tradingview/history` | **Histórico de candles** |

---

### Histórico de Candles

**Descoberto em: 05/07/2026**

```
GET https://trade.pumabroker.com/api/v1/tradingview/history
    ?symbol=XRDOGUSDT
    &resolution=1
    &from=1783247524
    &to=1783267324

Authorization: Bearer <jwt_token>
```

**Parâmetros:**
| Parâmetro | Tipo | Descrição | Exemplo |
|-----------|------|-----------|---------|
| `symbol` | string | Nome do ativo | `XRDOGUSDT`, `BETHUSDT`, `AUDUSD` |
| `resolution` | string | Timeframe em minutos | `1`, `5`, `15`, `30`, `60` |
| `from` | int | Unix timestamp início | `1783247524` |
| `to` | int | Unix timestamp fim | `1783267324` |

**Response — formato TradingView (arrays paralelos):**
```json
{
  "s": "ok",
  "t": [1783247520, 1783247580, 1783247640],
  "o": [50.68, 50.70, 50.65],
  "h": [50.72, 50.75, 50.70],
  "l": [50.65, 50.68, 50.60],
  "c": [50.70, 50.65, 50.68],
  "v": [190.0, 210.0, 175.0]
}
```

**Quando retorna vazio:**
```json
{ "s": "no_data" }
```
Causa: período sem dados (mercado fechado, fim de semana, fora do horário do ativo).
Fix: ampliar o range `from` para cobrir mais tempo.

---

### Abrir Ordem

**Descoberto em: 15/06/2026**

```
POST https://trade.pumabroker.com/trades
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Payload completo confirmado (capturado ao clicar COMPRA em AUDUSD M15):**
```json
{
  "userId":     "28318",
  "symbol":     "AUDUSD",
  "direction":  "CALL",
  "amount":     2,
  "duration":   530,
  "entryPrice": 0.70563,
  "mode":       "CANDLE_TIME",
  "payout":     0.85,
  "timeframe":  "M15",
  "verify":     "gAAAAABqMLMX8x8KnN21gQRKFTf...",
  "wallet":     "REAL"
}
```

**Campos:**
| Campo | Tipo | Valores | Notas |
|-------|------|---------|-------|
| `userId` | string | ID da conta | do `user.id` no login |
| `symbol` | string | ex: `AUDUSD` | nome exato do ativo |
| `direction` | string | `CALL` \| `PUT` | **maiúsculas obrigatório** |
| `amount` | number | ex: `2` | valor em BRL/USD |
| `duration` | int | ex: `530` | segundos até expiração da vela |
| `entryPrice` | float | ex: `0.70563` | preço atual do ativo |
| `mode` | string | `CANDLE_TIME` | fixo — sempre este valor |
| `payout` | float | ex: `0.85` | 85% de retorno — varia por ativo |
| `timeframe` | string | `M1`\|`M5`\|`M15`\|`M30`\|`H1` | timeframe da vela |
| `verify` | string | token Fernet | gerado pelo frontend — renovar por sessão |
| `wallet` | string | `REAL` \| `DEMO` | tipo de conta |

**Duração por timeframe:**
| Timeframe | Segundos totais | Duration típico |
|-----------|-----------------|-----------------|
| M1 | 60 | 5–59 |
| M5 | 300 | 5–299 |
| M15 | 900 | 5–899 |
| M30 | 1800 | 5–1799 |
| H1 | 3600 | 5–3599 |

> Duration = segundos restantes até fechar a vela atual. Mínimo 5s.

---

## WebSocket 2 — Candles (wsm5)

**URL:** `wss://wsm5.pumabroker.com/`  
**Protocolo:** WebSocket puro (não Socket.IO)  
**Auth:** Cookie `server_name_session` no handshake  

### ⚠️ Limitação do Browser

O browser **não permite** enviar `Cookie` customizado no handshake WebSocket.  
**Solução:** usar `ws_proxy.py` local na porta 3002.

```
Browser → ws://127.0.0.1:3002?token=<session_token>
    ↓
ws_proxy.py → wss://wsm5.pumabroker.com/ com Cookie: server_name_session=<token>
```

### Frames recebidos

**bar_update** (candle em tempo real):
```json
{
  "type":     "bar_update",
  "symbol":   "EURUSD",
  "interval": "5",
  "bar": {
    "time":   1781556900,
    "open":   1.15884,
    "high":   1.15900,
    "low":    1.15883,
    "close":  1.15896,
    "volume": 190.0
  },
  "last_bar": { "...mesmo formato..." }
}
```

**server_time** (heartbeat):
```json
{ "type": "server_time", "timestamp": 1781557272731 }
```

### Frames enviados

**Heartbeat** (a cada 10s):
```json
{ "method": "server_time" }
```

---

## WebSocket 3 — Socket.IO (trades/otc)

**URL:** `wss://trade.pumabroker.com/socket.io/?EIO=4&transport=websocket`  
**Protocolo:** Socket.IO v4 (Engine.IO v4)  
**Auth:** Cookie `server_name_session` no handshake  

### Handshake completo

```
1. Conecta WSS
2. Recebe: 0{"sid":"AAxrTo2u...","pingInterval":25000,"pingTimeout":20000}
3. Envia:  40/trades,
4. Recebe: 40/trades,{"sid":"OFsHv2_q..."}
5. Envia:  42/trades,["subscribe","28318"]   ← account_id
6. Envia:  40/otc,
7. Recebe: 40/otc,{"sid":"UgJ02aQp..."}
8. Ping/Pong a cada 25s: envia "2", recebe "3"
```

### Evento tradeUpdate (confirmado 15/06/2026)

```
← 42/trades,["tradeUpdate", {
    "id":          "5186325",
    "uid":         "364a4ac0f65720fa8b450f7b0b32cd",
    "userId":      "28318",
    "symbol":      "BETHUSDT",
    "currency":    "BETHUSDT",
    "direction":   "CALL",
    "amount":      2,
    "entryPrice":  "911.31632",
    "exitPrice":   null,
    "profit":      0,
    "payout":      0.87,
    "status":      "ACTIVE",
    "isDemo":      false
}]
```

**Status possíveis:**
| Status | Significado |
|--------|-------------|
| `ACTIVE` | Ordem aberta — aguardando resultado |
| `WIN` | Ganhou — `profit` preenchido |
| `LOSS` | Perdeu — `profit` = 0 |
| `DRAW` | Empate — valor devolvido |

### Candles OTC/Híbridos

```
← 42/otc,["candle", {
    "assetId": 11,
    "symbol":  "BETH/USDT",
    "time":    1781573700000,
    "open":    914.05714,
    "high":    914.25613,
    "low":     913.3959,
    "close":   913.3959,
    "verify":  "gAAAAA..."
}]
```

---

## Infraestrutura Local (Proxy)

### Arquivos

| Arquivo | Porta | Função |
|---------|-------|--------|
| `pumabroker-api/proxy_daemon.py` | 3001 | Proxy REST — history, trades |
| `pumabroker-api/ws_proxy.py` | 3002 | Proxy WebSocket — wsm5 candles |

### Scripts npm (package.json)

```json
{
  "daemon":    "python pumabroker-api/proxy_daemon.py",
  "wsproxy":   "python pumabroker-api/ws_proxy.py --port 3002",
  "services":  "start.bat",
  "start:all": "npm run services && timeout /t 3 /nobreak && npm run dev"
}
```

### Fluxo do ws_proxy.py

```
Browser → ws://127.0.0.1:3002?token=<session_token>
    ↓  extrai token da query string
ws_proxy.py
    ↓  conecta com header Cookie: server_name_session=<token>
wss://wsm5.pumabroker.com/
    ↓  proxy bidirecional de mensagens
Browser recebe bar_update normalmente
```

---

## Bugs Conhecidos e Soluções

### 1. getCandles retorna vazio — symbol com typo

**Erro:**
```
[WARN] getCandles VAZIO: XRPDOGUSDT
[WARN] BLOQUEADO — poucos dados (mínimo: 70), count: 0
```

**Causa:** `XRPDOGUSDT` (com P extra) em vez de `XRDOGUSDT`.  
**Fix:** corrigir o symbol em todos os 13 lugares do código.  
**Descoberto em:** 05/07/2026

---

### 2. WS2 retorna "Not authenticated"

**Erro:**
```
{"method":"subscribe",...} → "error": "Not authenticated"
{"method":"history",...}  → "error": "Not authenticated"
```

**Causa:** browser não envia `Cookie` no handshake WebSocket.  
**Fix:** ativar `useProxy: true` no `PumaWs2Client` e rodar `ws_proxy.py`.

```typescript
// puma-broker-adapter.ts linha 496
const ws2 = new PumaWs2Client({
  token:    ws2Token,
  assets:   activeHybrids,
  interval: "1",
  debug:    true,
  useProxy: true,   // ← FIX
});
```

**Descoberto em:** 05/07/2026

---

### 3. history retorna s:"no_data"

**Causa:** range de tempo muito curto ou mercado fechado no período.  
**Fix:** ampliar o `from` para cobrir 3x o período necessário:

```python
from_ts = int(time.time()) - (count * tf_seconds * 3)
```

---

### 4. JWT expirado — erro 401

**Causa:** token JWT expira em ~24h.  
**Fix:** chamar `POST /login` novamente e atualizar o header `Authorization`.  
**No código Python:** `auth.ensure_token()` renova automaticamente.

---

### 5. start.bat bloqueia o terminal com pause

**Causa:** `pause` no final do script impede encadeamento com `&&`.  
**Fix:** remover o `pause` do `start.bat` para permitir `npm run start:all`.

---

## Inicialização

### Comando único (recomendado)

```bash
npm run start:all
```

Sobe na ordem:
1. `proxy_daemon.py` (porta 3001)
2. `ws_proxy.py` (porta 3002) — aguarda 3s
3. `npm run dev` (frontend) — aguarda 3s

### Manual (debug)

```bash
# Terminal 1
npm run daemon

# Terminal 2
npm run wsproxy

# Terminal 3
npm run dev
```

### Verificação

Após subir, confirmar no console do browser:
```
[WS2] Conectado via proxy ws://127.0.0.1:3002
_fetchHybridHistory: XRDOGUSDT — 200 candles alimentados no buffer
[FlowRobot] SCAN XRDOGUSDT: OK (count: 200)
```

---

## Modelos de Dados

### Candle (TradingView format)

```typescript
interface Candle {
  timestamp: number;  // unix seconds
  open:      number;
  high:      number;
  low:       number;
  close:     number;
  volume:    number;
}
```

### TradeUpdate

```typescript
interface TradeUpdate {
  id:         string;
  uid?:       string;
  userId?:    string;
  symbol:     string;
  currency?:  string;
  direction:  "CALL" | "PUT";
  amount:     number;
  entryPrice?: number;
  exitPrice?:  number | null;
  profit?:    number;
  payout?:    number;        // 0.87 = 87%
  status:     "ACTIVE" | "WIN" | "LOSS" | "DRAW";
  isDemo?:    boolean;
}
```

### OrderRequest

```typescript
interface OrderRequest {
  userId:     string;
  symbol:     string;
  direction:  "CALL" | "PUT";
  amount:     number;
  duration:   number;        // segundos até expiração
  entryPrice: number;
  mode:       "CANDLE_TIME"; // fixo
  payout:     number;        // ex: 0.85
  timeframe:  string;        // "M1"|"M5"|"M15"|"M30"|"H1"
  verify:     string;        // token anti-fraude
  wallet:     "REAL" | "DEMO";
}
```

### BarUpdate (WS2)

```typescript
interface BarUpdate {
  type:     "bar_update";
  symbol:   string;
  interval: string;    // "1", "5", "15", "30", "60"
  bar:      Bar;
  last_bar?: Bar;
}

interface Bar {
  time:   number;  // unix seconds
  open:   number;
  high:   number;
  low:    number;
  close:  number;
  volume: number;
}
```

---

## Cookies e Headers

| Nome | Onde | Valor | Uso |
|------|------|-------|-----|
| `server_name_session` | Cookie | `cd0dc3ba...` | Autenticação WS2 e Socket.IO |
| `Authorization` | Header | `Bearer eyJhbGci...` | Todos os REST requests |

---

*Documentação baseada em capturas reais do DevTools — Junho/Julho 2026*  
*Nunca compartilhe seus tokens JWT ou session cookies publicamente*