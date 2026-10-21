## Coleta WS2 e sinais em tempo real (somente leitura)

O arquivo `puma_readonly_collector.py` consome exclusivamente o proxy local
`ws://127.0.0.1:3002`, alimenta a estratégia com velas **fechadas** e salva
CSV. Ele não carrega senha/JWT, não possui função de ordens e não faz
requisições REST de negociação.

### Executar no Windows
Em um terminal, com Python e dependências de `requirements.txt`:
```powershell
python ws_proxy.py --port 3002
```
Em outro:
```powershell
python puma_readonly_collector.py --symbol BETHUSDT --interval 1 --output candles_live.csv
```
Depois de coletar candles suficientes:
```powershell
python paper_probability_bot.py --csv candles_live.csv --expiry 1 --payout 0.85
```

**Atenção:** o `ws_proxy.py` legado tem material de autenticação
embutido no código. Revogue/rotacione segredos comprometidos e substitua
por configuração privada antes de iniciar; não cole chaves no chat.

A conexão ao broker não foi validada neste ambiente. A leitura só confirma
fechamento quando chega update do próximo candle. Lacunas reiniciam o
histórico da estratégia; CSV de múltiplas sessões precisa de validação extra
para backtest (ordem, intervalo constante e nenhuma lacuna). Os sinais
**não são ordens** nem probabilidades calibradas.
