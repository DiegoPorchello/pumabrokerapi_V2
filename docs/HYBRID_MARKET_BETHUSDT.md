# Puma Broker V2 — Integração híbrida BETHUSDT (21/10/2026)

> Estado: coleta de candles M1 **confirmada no Windows**; implementação em branch de desenvolvimento. Engenharia reversa de frames observados no Chrome. Não é uma API pública/contrato estável.

## Descoberta principal

A tela da corretora categoriza **BETH/USDT** em **Hybrids**. Captura em Chrome DevTools (Network → WS → Messages) identificou o feed efetivo:

- **Mercado híbrido:** `wss://market.pumabroker.com/ws` (WebSocket puro, HTTP 101).
- **Legado WS2/MT5:** `wss://wsmt5.pumabroker.com/` (não usar como origem de BETHUSDT; comandos/protocolo diferentes).
- **Socket.IO de trades:** `wss://trade.pumabroker.com/socket.io/?EIO=4&transport=websocket` e namespace `/trades`; serve a eventos de ordens, não se demonstrou necessário para os candles BETHUSDT.
- **Socket.IO /otc:** tentativa isolada do `puma_otc_probe.py` foi recusada; não é necessário para a coleta validada de BETHUSDT.

## Protocolo observado no market WS

Ordem observada: cliente envia AUTH, servidor responde `{"type":"auth","status":"ok"}`; cliente envia `SUBSCRIBE_TICKS` e `SUBSCRIBE` com `symbol` e `interval` **singulares**; servidor responde `subscribed`; depois recebe `tick`, `bar_update` e `bar_closed`.

Exemplos com segredos omitidos:

```json
{"method":"AUTH","params":{"key":"<CHAVE_LOCAL_SECRETA>"}}
{"method":"SUBSCRIBE_TICKS"}
{"method":"SUBSCRIBE","params":{"symbol":"BETHUSDT","interval":"1"}}
{"type":"auth","status":"ok"}
{"type":"subscribed","symbol":"BETHUSDT","interval":"1"}
{"type":"tick","symbol":"BETHUSDT","last":727.86,"timestamp":1792624824866}
{"type":"bar_update","symbol":"BETHUSDT","interval":"1","bar":{"time":1792624800,"open":728.05,"high":728.18,"low":727.82,"close":727.82,"volume":59}}
{"type":"bar_closed","symbol":"BETHUSDT","interval":"1","bar":{"time":1792624620,"open":728.97,"high":729.01,"low":728.15,"close":728.17,"volume":115}}
```

Exemplos ilustrativos e reduzidos de eventos/formatos observados; não são necessariamente payloads completos. Campos `verify`, tokens, cookies e IDs de sessão **nunca** devem ir para logs ou commits. A função do `verify` não foi validada.

**Atenção:** `subscribe` minúsculo com arrays, usado antes no feed WS2 legado, **não** é o comando aceito pelo mercado híbrido. O namespace `/trades` também tem um evento `subscribe`, mas ele se refere à conta, não ao feed híbrido.

## Implementação Python

Arquivo: `puma_hybrid_collector.py` (não `puma_readonly_collector.py`). Só lê e grava candles, calcula sinais, **não envia ordens**.

```powershell
cd "C:\Users\oz\Documents\projetos\Git\pumabrokerapi_V2"
git switch feat/probability-paper-bot
git pull origin feat/probability-paper-bot
python -m pip install -r requirements.txt
python -m unittest discover -s tests -p "test_*.py" -v
$env:PUMA_MARKET_WS_KEY = Read-Host "Chave WS de mercado"
python puma_hybrid_collector.py --symbol BETHUSDT --interval 1 --output candles_beth.csv
```

A chave deve ser obtida de forma autorizada e gerenciada **apenas na sessão local**; revogue/rotacione quaisquer chaves ou senhas previamente expostas. Não cole a chave no GitHub nem no chat.

Em **outro PowerShell**:

```powershell
cd "C:\Users\oz\Documents\projetos\Git\pumabrokerapi_V2"
Test-Path .\candles_beth.csv
(Import-Csv .\candles_beth.csv | Measure-Object).Count
Get-Content .\candles_beth.csv -Tail 5
```

### Resultado observado no Windows (21/10/2026)

1. `WS conectado; AUTH enviado`
2. `AUTH OK, aguardando confirmação de assinatura`
3. `ASSINATURA OK BETHUSDT M1`
4. `Test-Path .\candles_beth.csv` → `True`
5. Registro observado: `1792625460,726.48,726.69,726.41,726.54`
6. Contagem evoluiu de **2 para 5 candles** durante a sessão, confirmando gravação contínua naquele período.

Isso valida a recepção e persistência do feed para **aquele teste**, não uptime, correção futura do protocolo, equivalência com preço de liquidação nem rentabilidade.

## Backtest offline

```powershell
python paper_probability_bot.py --csv candles_beth.csv --expiry 1 --payout 0.85 --output resultado.json
```

Apenas cinco candles não bastam: o motor exige pelo menos 24 para sequer começar a buscar sinais, e muito mais para estatística confiável. Os scripts simulam uma entrada na abertura da vela seguinte e vencimento por candles; isso não equivale necessariamente às regras de execução e liquidação do broker.

**Cuidado:** ao reiniciar o coletor, o histórico na memória e o relógio de último candle são zerados, mas o CSV existente continua recebendo append. Faça validação de duplicatas, lacunas, fuso/timestamp e integridade antes do backtest. Não concatene sessões sem auditoria. `bar_closed` e mudança de `bar_update` são duas maneiras de reconhecer um fechamento, com deduplicação temporal durante a sessão.

## Pendências

- Validar reconexão, sessão longa, expiração/rotação de chave e tratamento de heartbeats.
- Validar cobertura de outros timeframes/Hybrids; até agora somente BETHUSDT M1 comprovado.
- Comparar histórico REST `/api/v1/tradingview/history` com candles do feed híbrido, sem presumir disponibilidade do ativo.
- Garantir CSV sem duplicatas entre reinicializações, detectar gaps e testar ingestão fora de ordem.
- Backtest em dados não vistos e forward testing antes de qualquer ordem em demo.
- Jamais habilitar conta real automaticamente.
