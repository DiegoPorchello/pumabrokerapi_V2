"""Dashboard local somente leitura para resultados do PumaBot.
Executar: python puma_dashboard.py
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
import json
import csv
import html

ROOT=Path(__file__).resolve().parent
REPORT=ROOT/"backtest_1000_beth.json"
CANDLES=ROOT/"candles_beth.csv"
HOST="127.0.0.1"
PORT=8765

def data():
    report={}
    if REPORT.is_file():
        try:
            report=json.loads(REPORT.read_text(encoding="utf-8"))
        except (OSError,ValueError):
            report={}
    count=0;last=None
    if CANDLES.is_file():
        try:
            with CANDLES.open(newline="",encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    count+=1;last={k:row.get(k) for k in ("time","open","high","low","close")}
        except (OSError,ValueError):
            count=0;last=None
    trades=report.get("trades",[])
    max_losses=0;losses=0
    for trade in trades:
        if trade.get("result")=="LOSS":
            losses+=1; max_losses=max(losses,max_losses)
        else: losses=0
    return {"candles":count,"last_candle":last,"backtest":{"count":report.get("count",0),
            "wins":report.get("wins",0),"losses":report.get("losses",0),
            "win_rate":report.get("win_rate"),"total_pnl":report.get("total_pnl",0),
            "max_loss_streak":max_losses},"trades":trades[-100:]}

PAGE=r"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>PumaBot — Painel Local</title>
<style>
:root{font-family:Inter,Segoe UI,Arial,sans-serif;background:#0b111b;color:#e8edf7}
*{box-sizing:border-box}body{margin:0}header{padding:23px 34px;border-bottom:1px solid #293448;display:flex;align-items:center;justify-content:space-between}h1{margin:0;font-size:22px}h2{font-size:17px;margin:0 0 16px}.sub{color:#91a1b8;font-size:13px;margin-top:5px}
.badge{border:1px solid #355a52;background:#15352f;color:#80e0b7;border-radius:22px;padding:7px 12px;font-size:12px}
main{max-width:1250px;margin:0 auto;padding:30px}.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px}.panel{background:#141e2d;border:1px solid #28364b;border-radius:15px;padding:22px;margin-bottom:18px}.metric{font-size:29px;font-weight:750;margin-top:14px}.green{color:#75ddaa}.red{color:#ff8e91}
.two{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:18px}table{width:100%;border-collapse:collapse;font-size:13px}td,th{padding:12px 10px;border-bottom:1px solid #28364b;text-align:left}th{color:#9cacc4}button,input{font:inherit}button{background:#40c996;color:#07150f;border:0;padding:11px 15px;border-radius:9px;font-weight:700;cursor:pointer}
select{background:#0d1624;border:1px solid #3a4d67;border-radius:8px;color:white;padding:9px}p{line-height:1.6}.muted{color:#91a1b8}
@media(max-width:850px){.stats{grid-template-columns:repeat(2,1fr)}.two{grid-template-columns:1fr}main{padding:16px}header{padding:18px}}
</style></head><body><header><div><h1>◈ PumaBot <span style="color:#45d1a0">Lab</span></h1><div class="sub">Análise do BETH/USDT · Dashboard Python local</div></div><span class="badge">● LEITURA / SEM ORDENS</span></header>
<main><div class="stats">
<div class="panel"><div class="sub">Candles coletados</div><div class="metric" id="candles">—</div></div>
<div class="panel"><div class="sub">Operações simuladas</div><div class="metric" id="orders">—</div></div>
<div class="panel"><div class="sub">Assertividade histórica</div><div class="metric green" id="wr">—</div></div>
<div class="panel"><div class="sub">Maior LOSS seguido</div><div class="metric red" id="streak">—</div></div>
</div><div class="two">
<section class="panel"><h2>Resumo do backtest</h2><p class="muted">Dados do arquivo backtest_1000_beth.json. Sem execução financeira.</p>
<table><tr><th>Indicador</th><th>Resultado</th></tr>
<tr><td>WIN</td><td id="win">—</td></tr><tr><td>LOSS</td><td id="loss">—</td></tr>
<tr><td>P&L unitário (payout do arquivo)</td><td id="pnl">—</td></tr>
<tr><td>Último candle (UTC Unix)</td><td id="last">—</td></tr></table></section>
<section class="panel"><h2>Gerenciamento SorosGale</h2>
<p class="muted">Regras documentadas: Entrada 1 WIN → Entrada 2; Entrada 2 WIN → reset nível 0; qualquer LOSS → próximo nível, Entrada 1.</p>
<p>Stake inicial R$ 2,00 · Payout 86% · Valores da tabela precisam ser revisados antes de automatizar.</p>
<button onclick="simulate()">Simular gerenciamento</button><p id="soros" class="muted">Executa simulação somente no navegador.</p></section>
</div><section class="panel"><h2>Últimas operações do backtest</h2><div style="overflow:auto"><table><thead><tr><th>#</th><th>Direção</th><th>Entrada</th><th>Saída</th><th>Resultado</th></tr></thead><tbody id="rows"></tbody></table></div></section>
<p class="muted">Atualiza dados locais automaticamente a cada 5s. Operações históricas são hipotéticas; vencimento real M1 da Puma ainda não implementado. Nunca divulgue JWT ou senha.</p></main>
<script>
let trades=[];
const money=n=>Number(n??0).toLocaleString('pt-BR',{style:'currency',currency:'BRL'});
async function refresh(){
try{let d=await(await fetch('/api/status',{cache:'no-store'})).json();trades=d.trades||[];
document.querySelector('#candles').textContent=d.candles;
document.querySelector('#orders').textContent=d.backtest.count;
document.querySelector('#wr').textContent=d.backtest.win_rate===null||d.backtest.win_rate===undefined?'N/D':(d.backtest.win_rate*100).toFixed(2)+'%';
document.querySelector('#streak').textContent=d.backtest.max_loss_streak;
document.querySelector('#win').textContent=d.backtest.wins;
document.querySelector('#loss').textContent=d.backtest.losses;
document.querySelector('#pnl').textContent=money(d.backtest.total_pnl);
document.querySelector('#last').textContent=d.last_candle?.time||'N/D';
const tb=document.querySelector('#rows');tb.replaceChildren();
trades.slice(-20).reverse().forEach((t,i)=>{let tr=document.createElement('tr');
[t.signal_time,t.direction,Number(t.entry).toFixed(5),Number(t.exit).toFixed(5),t.result].forEach(value=>{let td=document.createElement('td');td.textContent=value??'';tr.append(td)});tb.append(tr)});
}catch(err){console.error('Leitura local indisponível',err)}
}
function simulate(){let levels=[2,2.60,3.20,3.90,5.85,8.78,13.16,19.75,29.62,44.43,66.64,99.97,149.95,224.93,337.39,506.08],l=0,e=1,pnl=0,resets=0,max=0;
for(let t of trades){if(l>=levels.length){document.querySelector('#soros').textContent='Níveis excedidos; simulação interrompida';return}
max=Math.max(max,l);let stake=e===1?levels[l]:Math.round(levels[l]*1.86*100)/100;
pnl+=t.result==='WIN'?Math.round(stake*.86*100)/100:-stake;
if(t.result==='WIN'&&e===1)e=2;else if(t.result==='WIN'){l=0;e=1;resets++}else{l++;e=1}}
document.querySelector('#soros').textContent='Resultado hipotético: '+money(pnl)+' · maior nível: '+max+' · resets: '+resets+' · sem stop automático.'}
refresh();setInterval(refresh,5000);
</script></body></html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path=urlparse(self.path).path
        if path=="/api/status":
            payload=json.dumps(data(),ensure_ascii=False).encode("utf-8")
            self.send_response(200);self.send_header("Content-Type","application/json; charset=utf-8")
        elif path=="/":
            payload=PAGE.encode("utf-8")
            self.send_response(200);self.send_header("Content-Type","text/html; charset=utf-8")
        else:
            self.send_error(404);return
        self.send_header("Cache-Control","no-store")
        self.send_header("Content-Length",str(len(payload)))
        self.end_headers();self.wfile.write(payload)
    def log_message(self,fmt,*args): pass

if __name__=="__main__":
    print(f"PumaBot Lab: http://{HOST}:{PORT} (somente leitura)")
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
