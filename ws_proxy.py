"""
ws_proxy.py — WebSocket proxy para WS2 (Puma Broker).

O WS2 da Puma (wsmt5.pumabroker.com) exige uma mensagem AUTH explicita
logo apos a conexao, com uma chave estatica compartilhada.

Fluxo:
  1. Frontend conecta: ws://127.0.0.1:3002
  2. Proxy conecta:    wss://wsmt5.pumabroker.com/
  3. Proxy envia:      {"method":"AUTH","params":{"key":"<KEY>"}}
  4. WS2 responde:     {"type":"auth","status":"ok","broker":"pepperstone",...}
  5. So entao as mensagens sao encaminhadas bidirecionalmente

Uso:
  python ws_proxy.py --port 3002
"""

import asyncio
import json
import logging
import signal
import sys

import websockets
from websockets.server import serve, WebSocketServerProtocol

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ws_proxy")

PUMA_WS2_URL = "wss://wsmt5.pumabroker.com/"

# Chave estatica de autenticacao WS2 (descoberta via DevTools do site real)
# Compartilhada entre todos os clientes white-label MT5/Pepperstone
WS2_AUTH_KEY = "mt5_4mkts_pepperstone_2025_pP9sXrL7kN2"

AUTH_TIMEOUT = 10  # segundos para aguardar resposta do AUTH


async def _proxy_handler(client_ws: WebSocketServerProtocol):
    """Handler principal do proxy WebSocket."""
    client_id = str(id(client_ws))
    logger.info("Cliente conectado (id=%s)", client_id)

    headers = {
        "Origin": "https://trade.pumabroker.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    try:
        logger.info("Conectando ao WS2 sem autenticacao previa...")
        async with websockets.connect(
            PUMA_WS2_URL,
            additional_headers=headers,
            compression="deflate",
            ping_interval=30,
            ping_timeout=10,
            close_timeout=5,
        ) as puma_ws:
            logger.info("Conectado ao Puma WS2 (client=%s)", client_id)

            # ── FASE 1: AUTH (antes de qualquer outra mensagem) ──
            auth_msg = json.dumps({
                "method": "AUTH",
                "params": {"key": WS2_AUTH_KEY},
            })
            logger.info("→ Enviando AUTH...")
            await puma_ws.send(auth_msg)

            # Aguarda resposta do AUTH
            autenticado = False
            try:
                auth_resp = await asyncio.wait_for(puma_ws.recv(), timeout=AUTH_TIMEOUT)
                if isinstance(auth_resp, bytes):
                    auth_resp = auth_resp.decode("utf-8")
                data = json.loads(auth_resp)
                if data.get("type") == "auth" and data.get("status") == "ok":
                    autenticado = True
                    broker = data.get("broker", "?")
                    symbols = data.get("symbols", [])
                    logger.info(
                        "AUTH OK | broker=%s | symbols=%d | raw=%s",
                        broker, len(symbols), auth_resp[:300],
                    )
                    # Repassa a resposta de auth para o cliente
                    await client_ws.send(auth_resp)
                else:
                    logger.error("AUTH FALHOU: %s", auth_resp[:500])
                    await client_ws.close(4002, f"AUTH falhou: {auth_resp[:200]}")
                    return
            except asyncio.TimeoutError:
                logger.error("AUTH TIMEOUT (%ds) — servidor nao respondeu", AUTH_TIMEOUT)
                await client_ws.close(4002, "AUTH timeout")
                return

            # ── FASE 2: Encaminhamento bidirecional ──
            msg_count = 0
            sent_count = 0

            async def puma_to_client():
                nonlocal msg_count
                try:
                    async for msg in puma_ws:
                        if isinstance(msg, bytes):
                            msg = msg.decode("utf-8")
                        msg_count += 1
                        truncated = msg[:1000] + f"... [{len(msg)} chars]" if len(msg) > 1000 else msg
                        logger.info("← PUMA WS2 [%d]: %s", msg_count, truncated)
                        await client_ws.send(msg)
                except websockets.ConnectionClosed:
                    logger.info("Puma WS2 desconectou (client=%s, received=%d)", client_id, msg_count)
                except Exception as e:
                    logger.error("Erro puma_to_client: %s", e)

            async def client_to_puma():
                nonlocal sent_count
                try:
                    async for msg in client_ws:
                        if isinstance(msg, bytes):
                            msg = msg.decode("utf-8")
                        sent_count += 1
                        truncated = msg[:1000] + f"... [{len(msg)} chars]" if len(msg) > 1000 else msg
                        logger.info("→ CLIENT [%d] → PUMA: %s", sent_count, truncated)
                        await puma_ws.send(msg)
                except websockets.ConnectionClosed:
                    pass
                except Exception as e:
                    logger.error("Erro client_to_puma: %s", e)

            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(puma_to_client()),
                    asyncio.create_task(client_to_puma()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

    except Exception as e:
        logger.error("Erro na conexao Puma WS2: %s", e)
    finally:
        logger.info("Cliente desconectado (id=%s)", client_id)


async def main(host: str = "127.0.0.1", port: int = 3002):
    """Inicia o servidor WebSocket proxy."""
    stop = asyncio.Event()

    def _signal_handler():
        logger.info("Sinal recebido — encerrando...")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    async with serve(
        _proxy_handler,
        host,
        port,
        ping_interval=20,
        ping_timeout=20,
    ):
        logger.info("=" * 50)
        logger.info("  Puma WS2 Proxy")
        logger.info("  ws://%s:%d", host, port)
        logger.info("=" * 50)
        logger.info("Autenticacao: mensagem AUTH com chave estatica")
        logger.info("Proxy encaminha: %s", PUMA_WS2_URL)
        logger.info("")
        await stop.wait()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Puma WS2 WebSocket Proxy")
    parser.add_argument("--port", type=int, default=3002, help="Porta (3002)")
    parser.add_argument("--host", default="127.0.0.1", help="Host (127.0.0.1)")
    args = parser.parse_args()

    try:
        asyncio.run(main(host=args.host, port=args.port))
    except KeyboardInterrupt:
        print("\nProxy encerrado.")
