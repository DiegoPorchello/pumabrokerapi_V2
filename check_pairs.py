#!/usr/bin/env python3
"""
Script para verificar quais pares híbridos estão disponíveis na Puma Broker.
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))
from pumabroker.auth import PumaBrokerAuth
from pumabroker.config import config

async def check_pairs():
    email = os.getenv("PUMA_EMAIL")
    password = os.getenv("PUMA_PASSWORD")
    
    if not email or not password:
        print("Configure PUMA_EMAIL e PUMA_PASSWORD no .env")
        return

    auth = PumaBrokerAuth(email, password)
    
    try:
        print("Fazendo login...")
        session = auth.login()
        
        print(f"Logado: {session.name} (id={session.user_id})")
        print(f"Saldo REAL: R${session.balance:.2f} | DEMO: R${session.demo_balance:.2f}")
        
        print("\nConsultando ativos disponíveis (GET /currencies)...")
        r = auth.http.get(config.ACTIVE_URL, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        ativos = r.json()
        
        print(f"\nResposta da API: {ativos}")
        
        target_pairs = [
            "BETHUSDT",
            "BTSOLUSDT", 
            "MAPEUSDT",
            "XRDOGUSDT",
            "XRPUSDT"
        ]
        
        print("\n--- Verificação dos pares solicitados ---")
        for pair in target_pairs:
            found = False
            if isinstance(ativos, dict):
                for key, value in ativos.items():
                    if pair.upper() in str(key).upper() or pair.upper() in str(value).upper():
                        found = True
                        print(f"✅ {pair} - ENCONTRADO")
                        break
            elif isinstance(ativos, list):
                for item in ativos:
                    item_str = str(item).upper()
                    if pair.upper() in item_str:
                        found = True
                        print(f"✅ {pair} - ENCONTRADO")
                        break
            
            if not found:
                print(f"❌ {pair} - NÃO ENCONTRADO")
        
        print("\n--- Todos os ativos retornados ---")
        if isinstance(ativos, dict):
            for k, v in ativos.items():
                print(f"  {k}: {v}")
        elif isinstance(ativos, list):
            for item in ativos:
                print(f"  {item}")
                
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import asyncio
    asyncio.run(check_pairs())
    