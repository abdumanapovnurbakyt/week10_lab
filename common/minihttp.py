"""Минимальный HTTP-сервер на asyncio без зависимостей.

Через него работают статус шлюза, управление симуляторами, прокси обрыва WAN и collector.
Только GET/POST, ответ всегда закрывает соединение.
"""
import asyncio
import json
import urllib.parse
from dataclasses import dataclass

REASONS = {200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Internal Server Error"}


@dataclass
class Request:
    method: str
    path: str
    query: dict
    body: bytes = b""

    def json(self):
        return json.loads(self.body or b"{}")


async def _client(reader, writer, handler):
    try:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        lines = head.decode("latin-1").split("\r\n")
        method, target, _ = lines[0].split(" ", 2)
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        body = b""
        n = int(headers.get("content-length", 0) or 0)
        if n:
            body = await asyncio.wait_for(reader.readexactly(n), 5)
        u = urllib.parse.urlsplit(target)
        query = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        status, payload = await handler(Request(method, u.path, query, body))
    except (asyncio.IncompleteReadError, ConnectionError, asyncio.TimeoutError):
        writer.close()
        return
    except Exception as e:  # noqa: BLE001 - любая ошибка обработчика превращается в 500
        status, payload = 500, {"error": repr(e)}
    if isinstance(payload, (dict, list)):
        data, ctype = json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8"
    elif isinstance(payload, str):
        data, ctype = payload.encode(), "text/plain; charset=utf-8"
    else:
        data, ctype = payload, "application/octet-stream"
    try:
        writer.write(f"HTTP/1.1 {status} {REASONS.get(status, 'OK')}\r\nContent-Type: {ctype}\r\n"
                     f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode() + data)
        await writer.drain()
    except ConnectionError:
        pass
    finally:
        writer.close()


async def serve(host: str, port: int, handler):
    """handler(Request) -> (status, dict|list|str|bytes)."""
    return await asyncio.start_server(lambda r, w: _client(r, w, handler), host, port)
