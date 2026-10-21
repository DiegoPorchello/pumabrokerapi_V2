# Laboratório probabilístico — paper trading (sem ordens)

Este módulo NÃO autentica na Puma, NÃO usa tokens e NÃO executa compra/venda.
É uma implementação isolada para validar a hipótese "tendência + pullback"
antes de qualquer integração com a conta demo.

## Executar

```bash
python -m unittest discover -s tests -p "test_paper_probability_bot.py" -v
python paper_probability_bot.py --csv historico.csv --expiry 1 --payout 0.85 --output resultado.json
```

Formato CSV: `time,open,high,low,close`; timestamps Unix em segundos, candles
em ordem e de mesma duração, **sem lacunas**. Antes de usar histórico real,
confira horário, origem da cotação e qualidade dos dados.

A entrada hipotética ocorre na abertura do candle seguinte ao sinal;
a saída no fechamento do candle de expiração. **Não equivale à execução
na Puma** (preço, horário do vencimento, payout, latência e regras de
empate podem diferir). Não extrapole taxa de acerto histórica para
probabilidade futura. Separe treinamento e validação por ordem temporal;
não otimize hiperparâmetros no conjunto final de teste.

## Segurança
- Nada de martingale/sorosgale ou execução de ordens.
- Mantenha credenciais fora do Git, em variáveis de ambiente locais.
- Se tokens/sessões apareceram no repositório público, revogue-os; apagar o
  arquivo na branch NÃO elimina o histórico do Git.
- Próxima etapa: coletor WS2 read-only com identificação do modo demo,
  timestamp do servidor e auditoria de perdas/reconexão.
