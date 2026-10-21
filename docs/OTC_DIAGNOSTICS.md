# Correção WS2 e diagnóstico do mercado híbrido

A documentação técnica v4.3 (15/07/2026) registra que o WS2 não
aceita `subscribe` ou `history`. O coletor foi atualizado para
**não enviar subscribe** e ouvir as mensagens espontâneas após AUTH.

O catálogo de 77 ativos descrito para WS2 não contém BETHUSDT.
A documentação aponta Socket.IO namespace `/otc` como feed de OTC/híbridos,
mas não documenta o nome/formato dos eventos de candle para BETHUSDT.

## Teste read-only

No PowerShell, na raiz do repositório e após atualizar a branch:
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
python puma_otc_probe.py --seconds 60
```

O probe se conecta ao `/otc`, não executa ordens e registra apenas nomes
dos eventos, tipos de payload e presença do símbolo BETHUSDT.

Se o servidor requer autenticação, configure `PUMA_BEARER_TOKEN` apenas
na sessão local do terminal (não compartilhe token e não registre em Git).
**O mecanismo de autenticação OTC não foi validado** e pode ser diferente.

O probe é diagnóstico, não um cliente de produção. Ele não confirma
preço, corretora, liquidação ou probabilidade de sucesso e não deve
ser usado para negociação automática.
