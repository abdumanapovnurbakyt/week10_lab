"""Прокси «WAN»: TCP-ретранслятор между шлюзом и облачным брокером с управляемым обрывом канала (шаг 5).

Четыре режима (переключаются по HTTP, см. ниже):
  up        - прозрачная ретрансляция;
  blackhole - канал теряет всё: данные в обе стороны читаются и выбрасываются, новые соединения зависают
              (аналог `tc netem loss 100%`: клиент узнаёт об обрыве только по тайм-аутам/keepalive,
              отправленное за время обрыва не доходит никогда и шлюз досылает его сам);
  stall     - канал «застрял»: прокси перестаёт читать, данные копятся в буферах ядра и доходят ПОСЛЕ
              восстановления (затор/буферизация на пути; даёт всплеск дубликатов, см. отчёт, опыт E4b);
  reject    - канал физически разорван: действующие соединения сбрасываются (RST), новые не принимаются
              (аналог `docker network disconnect`).
Прокси считает байты в обе стороны - это «трафик по WAN» (вместе с TLS и заголовками MQTT).

HTTP управления:  GET /health, GET /stats, POST|GET /mode?set=up|blackhole|stall|reject
"""
import asyncio
import os
import time

from common import minihttp

LISTEN_PORT = int(os.environ.get("LINK_LISTEN", "8883"))
UP_HOST = os.environ.get("UPSTREAM_HOST", "broker-wan")
UP_PORT = int(os.environ.get("UPSTREAM_PORT", "8883"))
CTL_PORT = int(os.environ.get("CTL_PORT", "8081"))

MODES = ("up", "blackhole", "stall", "reject")


class Relay:
    def __init__(self, cw, uw):
        self.cw, self.uw, self.dead = cw, uw, False

    def close(self, abort: bool = False) -> None:
        self.dead = True
        for w in (self.cw, self.uw):
            try:
                w.transport.abort() if abort else w.close()
            except Exception:  # noqa: BLE001
                pass


class Link:
    def __init__(self):
        self.mode = "up"
        self.gate = asyncio.Event()      # сброшен в stall: читающие циклы встают
        self.gate.set()
        self.hold = asyncio.Event()      # сброшен в blackhole и stall: новые соединения «зависают»
        self.hold.set()
        self.relays: set = set()
        self.stats = {"bytes_up": 0, "bytes_down": 0, "bytes_dropped": 0, "conns_total": 0, "conns_refused": 0}
        self.history = [(time.time(), "up")]

    def set_mode(self, mode: str) -> None:
        if mode not in MODES:
            raise ValueError(mode)
        if mode == self.mode:
            return
        self.mode = mode
        self.history.append((time.time(), mode))
        self.gate.clear() if mode == "stall" else self.gate.set()
        self.hold.clear() if mode in ("blackhole", "stall") else self.hold.set()
        if mode == "reject":
            for r in list(self.relays):
                r.close(abort=True)

    async def pump(self, src, dst, key: str, relay: Relay) -> None:
        try:
            while True:
                data = await src.read(65536)
                if not data:
                    break
                if self.mode == "blackhole":            # потеря 100 %: читаем и выбрасываем
                    self.stats["bytes_dropped"] += len(data)
                    continue
                await self.gate.wait()                  # в stall стоим здесь и больше не читаем
                if relay.dead:
                    break
                dst.write(data)
                await dst.drain()
                self.stats[key] += len(data)
        except (ConnectionError, OSError):
            pass
        finally:
            relay.close()

    async def handle(self, cr, cw) -> None:
        self.stats["conns_total"] += 1
        if self.mode == "reject":
            self.stats["conns_refused"] += 1
            cw.transport.abort()
            return
        await self.hold.wait()                          # новое соединение «зависает», пока канал в blackhole/stall
        if self.mode == "reject":
            self.stats["conns_refused"] += 1
            cw.transport.abort()
            return
        try:
            ur, uw = await asyncio.open_connection(UP_HOST, UP_PORT)
        except OSError:
            cw.transport.abort()
            return
        relay = Relay(cw, uw)
        self.relays.add(relay)
        try:
            await asyncio.gather(self.pump(cr, uw, "bytes_up", relay), self.pump(ur, cw, "bytes_down", relay))
        finally:
            self.relays.discard(relay)
            relay.close()

    def snapshot(self) -> dict:
        return {"mode": self.mode, "since": self.history[-1][0], "conns_active": len(self.relays),
                **self.stats, "history": [[round(t, 3), m] for t, m in self.history[-50:]], "now": time.time()}


async def main() -> None:
    link = Link()

    async def handler(req):
        if req.path == "/health":
            return 200, {"ok": True}
        if req.path == "/stats":
            return 200, link.snapshot()
        if req.path == "/mode":
            m = req.query.get("set")
            if m not in MODES:
                return 400, {"error": "режим: up | blackhole | stall | reject", "current": link.mode}
            link.set_mode(m)
            return 200, link.snapshot()
        return 404, {"error": "not found"}

    await minihttp.serve("0.0.0.0", CTL_PORT, handler)
    server = await asyncio.start_server(link.handle, "0.0.0.0", LISTEN_PORT)
    print(f"wan-link: :{LISTEN_PORT} -> {UP_HOST}:{UP_PORT}, управление :{CTL_PORT}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
