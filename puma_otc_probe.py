"""Diagnóstico de eventos OTC/híbridos em modo SOMENTE LEITURA.

Não coloca ordens, não registra payload completo, não exibe tokens.
Uso: python puma_otc_probe.py --seconds 60
Acesso a /otc pode exigir uma sessão válida configurada localmente.
"""
import argparse
import asyncio
import json
import logging
import os
from collections import Counter

LOG = logging.getLogger("puma.otc.probe")
URL = "https://trade.pumabroker.com"
REDACT_KEYS = ("token", "password", "cookie", "secret", "jwt", "verify", "authorization")

def describe_event(name, payload, target="BETHUSDT"):
    """Retorna SOMENTE metadados, sem informações de conta ou credenciais."""
    def walk(obj, depth=0):
        if depth > 3:
            return False
        if isinstance(obj, dict):
            return any((str(k).lower() in ("symbol", "asset", "currency") and
                         str(v).replace("/", "").upper() == target.upper())
                       or (not any(s in str(k).lower() for s in REDACT_KEYS) and walk(v, depth+1))
                       for k,v in obj.items())
        if isinstance(obj, (list, tuple)):
            return any(walk(v, depth+1) for v in obj[:100])
        return False
    return {"event": str(name)[:80], "target_present": walk(payload),
            "payload_kind": type(payload).__name__}

async def run_probe(seconds: int):
    import socketio
    token = os.environ.get("PUMA_BEARER_TOKEN", "").strip()
    cookies = {}
    if not token and os.environ.get("PUMA_EMAIL", "").strip():
        from getpass import getpass
        import requests
        email = os.environ["PUMA_EMAIL"].strip()
        password = getpass("Senha Puma (oculta): ")
        try:
            response = requests.post(
                URL + "/api/v1/auth/login",
                json={"email": email, "password": password},
                timeout=15,
            )
            response.raise_for_status()
            body = response.json()
            token = body.get("accessToken") or body.get("token") or ""
            cookies = requests.utils.dict_from_cookiejar(response.cookies)
            LOG.info("Login HTTP concluido; JWT=%s; cookie=%s", bool(token), bool(cookies))
        except (requests.RequestException, ValueError) as exc:
            LOG.error("Login HTTP falhou: %s", type(exc).__name__)
            return
        finally:
            password = None
    if not token:
        LOG.warning("Sem JWT. Configure PUMA_EMAIL localmente ou PUMA_BEARER_TOKEN.")
        return
    # O probe não escuta /trades, só namespace de dados /otc.
    sio = socketio.AsyncClient(reconnection=False, logger=False, engineio_logger=False)
    counts = Counter()
    @sio.event(namespace="/otc")
    async def connect():
        LOG.info("Namespace /otc conectado (somente leitura)")
    @sio.event(namespace="/otc")
    async def disconnect():
        LOG.info("Namespace /otc desconectado")
    @sio.event(namespace="/otc")
    async def connect_error(data):
        LOG.warning("Namespace /otc recusado; tipo de resposta=%s (conteúdo omitido)", type(data).__name__)
    @sio.on("*", namespace="/otc")
    async def on_event(event, *args):
        counts[str(event)] += 1
        info = describe_event(event, args)
        LOG.info("Evento OTC: %s", info)
    try:
        headers = {"Authorization": "Bearer " + token}
        if cookies:
            headers["Cookie"] = "; ".join(k + "=" + v for k, v in cookies.items())
        await sio.connect(URL, namespaces=["/otc"], socketio_path="socket.io",
                          transports=["websocket"], headers=headers,
                          auth={"token":token}, wait_timeout=10)
        await asyncio.sleep(seconds)
    except socketio.exceptions.ConnectionError:
        LOG.error("Conexão Socket.IO ao /otc recusada ou indisponível. Verifique a sessão e o handshake no navegador; não envie tokens ou cookies.")
    except (OSError, TimeoutError) as exc:
        LOG.error("Falha de rede ao conectar ao /otc: %s", type(exc).__name__)
    finally:
        if sio.connected:
            await sio.disconnect()
        LOG.info("Eventos observados (nomes apenas): %s", dict(counts))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=60)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600:
        parser.error("--seconds deve ficar entre 1 e 3600")
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_probe(args.seconds))

if __name__ == "__main__":
    main()
