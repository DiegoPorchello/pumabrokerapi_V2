"""Consulta read-only de histórico TradingView na Puma + backtest dos últimos N candles.
Login interativo local opcional, token somente em memória. Nunca envia ordens.
"""
import argparse
from getpass import getpass
import csv
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
import requests
from paper_probability_bot import Candle, backtest

ENDPOINT="https://trade.pumabroker.com/api/v1/tradingview/history"
LOGIN_ENDPOINT="https://trade.pumabroker.com/api/v1/auth/login"

def login_for_jwt(email, password, session=None):
    """Solicita JWT REST via login. Não registra senha nem persiste tokens."""
    if not email or not password:
        raise ValueError("E-mail ou senha ausente")
    client=session or requests.Session()
    response=client.post(LOGIN_ENDPOINT,json={"email":email,"password":password},timeout=20)
    response.raise_for_status()
    data=response.json()
    if not isinstance(data,dict):
        raise ValueError("Resposta de login inválida")
    token=data.get("accessToken") or data.get("token")
    if not isinstance(token,str) or not token.strip():
        raise ValueError("Login não retornou JWT")
    return token.strip()


def normalize_history(payload):
    if not isinstance(payload,dict) or payload.get("s")!="ok":
        raise ValueError("Histórico indisponível: s != ok")
    keys=("t","o","h","l","c")
    columns=[payload.get(k) for k in keys]
    if any(not isinstance(x,list) for x in columns):
        raise ValueError("OHLC ausente")
    if len(set(map(len,columns)))!=1:
        raise ValueError("Arrays OHLC têm comprimentos divergentes")
    by_time={}
    for t,o,h,l,c in zip(*columns):
        bar=Candle(int(t),float(o),float(h),float(l),float(c))
        bar.validate()
        if bar.time in by_time and by_time[bar.time]!=bar:
            raise ValueError("Candles conflitantes no mesmo timestamp")
        by_time[bar.time]=bar
    return [by_time[k] for k in sorted(by_time)]

def contiguous_tail(candles,interval_seconds,limit):
    if not candles: return []
    tail=[candles[-1]]
    for bar in reversed(candles[:-1]):
        if tail[-1].time-bar.time!=interval_seconds:
            break
        tail.append(bar)
        if len(tail)>=limit: break
    return list(reversed(tail))

def fetch_history(symbol, resolution, count, token, session=None):
    if not token: raise ValueError("JWT ausente; configure PUMA_BEARER_TOKEN localmente")
    if resolution not in ("1","5","15","30","60"):
        raise ValueError("Resolução desconhecida")
    step=int(resolution)*60
    now=int(time.time())
    # Janela maior permite preencher dias com indisponibilidades. Limita memória.
    start=now-count*step*4
    client=session or requests.Session()
    response=client.get(ENDPOINT,params={"symbol":symbol,"resolution":resolution,
                                        "from":start,"to":now},
                        headers={"Authorization":"Bearer "+token,
                                 "Origin":"https://trade.pumabroker.com"},
                        timeout=25)
    response.raise_for_status()
    bars=normalize_history(response.json())
    return contiguous_tail(bars,step,count)

def fetch_history_with_reauth(symbol, resolution, count, token, login_callback=None, session=None):
    """Reautentica uma unica vez em HTTP 401, sem salvar segredos.
    Nao tenta login em HTTP 403, 429 ou erros de rede.
    """
    client=session or requests.Session()
    try:
        return fetch_history(symbol,resolution,count,token,session=client)
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 401 or login_callback is None:
            raise
        new_token=login_callback()
        if not new_token:
            raise ValueError("Reautenticacao nao retornou JWT") from None
        return fetch_history(symbol,resolution,count,new_token,session=client)

def save_csv(bars,path):
    dest=Path(path)
    dest.parent.mkdir(parents=True,exist_ok=True)
    with dest.open("w",newline="",encoding="utf-8") as fh:
        w=csv.writer(fh)
        w.writerow(["time","open","high","low","close"])
        for b in bars: w.writerow([b.time,b.open,b.high,b.low,b.close])

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--symbol",default="BETHUSDT")
    p.add_argument("--resolution",default="1",choices=["1","5","15","30","60"])
    p.add_argument("--count",type=int,default=1000)
    p.add_argument("--payout",type=float,default=.85)
    p.add_argument("--expiry",type=int,default=1)
    p.add_argument("--csv",default="historico_1000_beth.csv")
    p.add_argument("--report",default="backtest_1000_beth.json")
    p.add_argument("--login",action="store_true",help="Fazer login interativo; senha não aparece no terminal")
    a=p.parse_args()
    if a.count<25 or a.count>5000:
        p.error("--count deve estar entre 25 e 5000")
    token=os.getenv("PUMA_BEARER_TOKEN","").strip()
    email=""
    password=None
    if not token and a.login:
        email=os.getenv("PUMA_EMAIL","").strip() or input("E-mail Puma: ").strip()
        password=getpass("Senha Puma (oculta): ")
        try:
            token=login_for_jwt(email,password)
            print("Login REST concluído; JWT disponível apenas em memória.")
        except (ValueError,requests.RequestException):
            print("ERRO: login REST falhou; verifique credenciais e acesso.",file=sys.stderr)
            sys.exit(2)
    if not token:
        print("ERRO: configure PUMA_BEARER_TOKEN ou use --login (sem salvar credenciais).",file=sys.stderr)
        sys.exit(2)
    try:
        relogin=(lambda: login_for_jwt(email,password)) if a.login and password else None
        bars=fetch_history_with_reauth(a.symbol,a.resolution,a.count,token,login_callback=relogin)
    except (ValueError,requests.RequestException) as exc:
        print("ERRO ao consultar histórico: "+type(exc).__name__+
              ". Consulte status REST/autorização ou disponibilidade do ativo.",file=sys.stderr)
        sys.exit(1)
    finally:
        password=None
    if len(bars)<a.count:
        print(f"AVISO: obtidos somente {len(bars)} candles contínuos de {a.count} solicitados; não trataremos isso como 1000.")
    if len(bars)<25:
        print("Histórico insuficiente. Nenhum backtest foi executado.",file=sys.stderr)
        sys.exit(1)
    save_csv(bars,a.csv)
    result=backtest(bars,expiry=a.expiry,payout=a.payout)
    result.update({"symbol":a.symbol,"timeframe_minutes":int(a.resolution),
                   "requested_candles":a.count,"actual_candles":len(bars),
                   "first_candle":datetime.fromtimestamp(bars[0].time,timezone.utc).isoformat(),
                   "last_candle":datetime.fromtimestamp(bars[-1].time,timezone.utc).isoformat(),
                   "source":"puma_rest_tradingview_history","verified_live_trades":False})
    Path(a.report).write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding="utf-8")
    print(json.dumps({k:v for k,v in result.items() if k!="trades"},indent=2,ensure_ascii=False))
    print(f"Arquivos: {a.csv} | {a.report}")

if __name__=="__main__":
    main()
