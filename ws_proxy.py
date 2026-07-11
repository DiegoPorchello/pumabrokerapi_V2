"""
ws_proxy.py — WebSocket proxy para WS2 (Puma Broker).

O browser não pode enviar headers customizados (Cookie) em conexões WebSocket.
Este proxy aceita conexões do frontend e as encaminha para wss://wsmt5.pumabroker.com/
com o header Cookie correto.

Uso:
  python ws_proxy.py --port 3002

Fluxo:
  1. Frontend conecta: ws://127.0.0.1:3002?token=<server_name_session>
  2. Proxy conecta:    wss://wsmt5.pumabroker.com/ (com Cookie header)
  3. Mensagens são encaminhadas bidirecionalmente
"""

import asyncio
import json
import logging
import signal
import sys
from urllib.parse import urlparse, parse_qs

import websockets
from websockets.server import serve, WebSocketServerProtocol

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ws_proxy")

PUMA_WS2_URL = "wss://wsmt5.pumabroker.com/"
SESSION_COOKIE = "server_name_session"

# Armazena tokens dos clientes conectados
_client_tokens: dict[str, str] = {}


async def _proxy_handler(client_ws: WebSocketServerProtocol):
    """Handler principal do proxy WebSocket."""
    # Extrai token da query string
    query = parse_qs(client_ws.path.split("?")[1] if "?" in client_ws.path else "")
    token = query.get("token", [None])[0]

    if not token:
        logger.warning("Conexão sem token — rejeitando")
        await client_ws.close(4001, "Token obrigatório")
        return

    client_id = str(id(client_ws))
    _client_tokens[client_id] = token

    token_preview = token[:20] + "..." if len(token) > 20 else token
    logger.info(
        "Cliente conectado (id=%s, token_len=%d, preview=%s)",
        client_id, len(token), token_preview,
    )
    # Aviso se o token parece ser JWT (eyJ) em vez de session hash
    if token.startswith("eyJ"):
        logger.warning(
            "⚠️ Token parece ser JWT (%s...), não server_name_session. "
            "WS2 provavelmente rejeitará 'Not authenticated'.",
            token[:30],
        )

    # Conecta ao Puma WS2 com Cookie header
    headers = {
        "Cookie": f"{SESSION_COOKIE}={token}",
        "Origin": "https://trade.pumabroker.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    try:
        async with websockets.connect(
            PUMA_WS2_URL,
            additional_headers=headers,
            compression="deflate",
            ping_interval=None,
            close_timeout=5,
        ) as puma_ws:
            logger.info("Conectado ao Puma WS2 (client=%s, cookie_preview=%s)", client_id, token_preview)

            # Contador de mensagens para logging
            msg_count = 0
            sent_count = 0

            # Tarefa para encaminhar mensagens: Puma → Cliente
            async def puma_to_client():
                nonlocal msg_count
                try:
                    async for msg in puma_ws:
                        if isinstance(msg, bytes):
                            msg = msg.decode("utf-8")
                        msg_count += 1
                        # Log específico para erros de autenticação
                        if "Not authenticated" in msg or "error" in msg.lower():
                            logger.warning(
                                "← PUMA WS2 [%d] ⚠️ ERRO: %s (cookie: %s)",
                                msg_count, msg[:300], token_preview,
                            )
                        else:
                            truncated = msg[:1000] + f"... [{len(msg)} chars]" if len(msg) > 1000 else msg
                            logger.info(
                                "← PUMA WS2 [%d] (%d chars): %s",
                                msg_count, len(msg), truncated,
                            )
                        await client_ws.send(msg)
                except websockets.ConnectionClosed:
                    logger.info("Puma WS2 desconectou (client=%s, received=%d)", client_id, msg_count)
                except Exception as e:
                    logger.error("Erro puma_to_client: %s", e)

            # Tarefa para encaminhar mensagens: Cliente → Puma
            async def client_to_puma():
                nonlocal sent_count
                try:
                    async for msg in client_ws:
                        if isinstance(msg, bytes):
                            msg = msg.decode("utf-8")
                        sent_count += 1
                        truncated = msg[:1000] + f"... [{len(msg)} chars]" if len(msg) > 1000 else msg
                        logger.info(
                            "→ CLIENT [%d] → PUMA: %s",
                            sent_count, truncated,
                        )
                        await puma_ws.send(msg)
                except websockets.ConnectionClosed:
                    pass
                except Exception as e:
                    logger.error("Erro client_to_puma: %s", e)

            # Executa ambas as direções em paralelo
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(puma_to_client()),
                    asyncio.create_task(client_to_puma()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancela tarefas restantes
            for task in pending:
                task.cancel()

    except Exception as e:
        logger.error("Erro na conexão Puma WS2: %s", e)
    finally:
        _client_tokens.pop(client_id, None)
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
            pass  # Windows não suporta add_signal_handler

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
        logger.info("Frontend conecta: ws://127.0.0.1:%d?token=<session_cookie>", port)
        logger.info("Proxy encaminha:  %s", PUMA_WS2_URL)
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
