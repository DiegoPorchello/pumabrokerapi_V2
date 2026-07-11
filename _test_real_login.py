"""
Testa login com credenciais reais contra a API v1 do Puma Broker.

Uso:
  $env:PUMA_EMAIL="seu@email.com"; $env:PUMA_PASSWORD="sua_senha"; python _test_real_login.py
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(__file__))

from pumabroker.auth import PumaBrokerAuth

email = os.environ.get("PUMA_EMAIL", "").strip()
password = os.environ.get("PUMA_PASSWORD", "")

if not email or not password:
    print("ERRO: Defina PUMA_EMAIL e PUMA_PASSWORD")
    print('Ex: $env:PUMA_EMAIL="seu@email.com"; $env:PUMA_PASSWORD="sua_senha"; python _test_real_login.py')
    sys.exit(1)

print(f"\nTestando login: {email}")
print(f"Endpoint: POST https://trade.pumabroker.com/api/v1/auth/login")
print()

try:
    auth = PumaBrokerAuth(email, password)
    session = auth.login()
    print(">>> LOGIN OK!")
    print(f"Nome:     {session.name}")
    print(f"ID:       {session.user_id}")
    print(f"Balance:  {session.balance}")
    print(f"Demo:     {session.demo_balance}")
    print(f"Token:    {session.token[:50]}...")
    print(f"Refresh:  {session.refresh_token[:50]}...")
except Exception as e:
    print(f">>> ERRO: {e}")
