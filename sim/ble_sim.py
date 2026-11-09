"""Эмулятор BLE-периферии с GATT (вместо реального адаптера и `bleak`).

Реальный BLE в контейнере недоступен, поэтому GATT эмулируется по TCP: строки JSON (по одной в строке).
Операции клиента: scan, connect, read, subscribe, unsubscribe, disconnect.
Уведомления сервера: {"ev": "notify", "addr": ..., "uuid": ..., "value": "<hex>"}.
Значения кодируются в стандартных форматах GATT (little-endian):
  0x2A6E температура - sint16, 0.01 °C;  0x2A6F влажность - uint16, 0.01 %;  0x2A19 заряд - uint8, %.
Управление по HTTP: GET /health, GET /state, POST /drop (разорвать все сессии, как потеря связи).
"""
import asyncio
import json
import os
import struct
import time

from common import minihttp
from sim.signals import BLE_DEVICES, Plant

PORT = int(os.environ.get("BLE_PORT", "1530"))
CTL_PORT = int(os.environ.get("CTL_PORT", "8080"))
NOTIFY_PERIOD = float(os.environ.get("BLE_NOTIFY_S", "2.0"))
DROP_EVERY = float(os.environ.get("BLE_DROP_EVERY_S", "0"))

FORMATS = {"2A6E": ("<h", 0.01), "2A6F": ("<H", 0.01), "2A19": ("<B", 1.0)}
T0 = time.monotonic()
PLANT = Plant()
SESSIONS: set = set()
STATS = {"notifications": 0, "reads": 0, "sessions": 0}


def encode(uuid: str, phys: float) -> str:
    fmt, scale = FORMATS[uuid]
    return struct.pack(fmt, int(round(phys / scale))).hex()


def gatt_value(addr: str, uuid: str) -> str:
    return encode(uuid, PLANT.ble_values(BLE_DEVICES[addr], time.monotonic() - T0)[uuid])


async def session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    STATS["sessions"] += 1
    SESSIONS.add(writer)
    subs: dict = {}

    def send(obj: dict) -> None:
        writer.write((json.dumps(obj, separators=(",", ":")) + "\n").encode())

    async def notifier(addr: str, uuid: str):
        while True:
            await asyncio.sleep(NOTIFY_PERIOD)
            STATS["notifications"] += 1
            send({"ev": "notify", "addr": addr, "uuid": uuid, "value": gatt_value(addr, uuid)})

    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except ValueError:
                send({"ok": False, "error": "bad json"})
                continue
            op, addr, uuid = msg.get("op"), msg.get("addr"), msg.get("uuid")
            if op == "scan":
                send({"ok": True, "devices": [{"addr": a, "name": n} for a, n in BLE_DEVICES.items()]})
            elif addr not in BLE_DEVICES:
                send({"ok": False, "error": "unknown device", "op": op})
            elif op == "connect":
                send({"ok": True, "op": op, "addr": addr, "chars": sorted(FORMATS)})
            elif op == "read" and uuid in FORMATS:
                STATS["reads"] += 1
                send({"ok": True, "op": op, "addr": addr, "uuid": uuid, "value": gatt_value(addr, uuid)})
            elif op == "subscribe" and uuid in FORMATS:
                if (addr, uuid) not in subs:
                    subs[(addr, uuid)] = asyncio.create_task(notifier(addr, uuid))
                send({"ok": True, "op": op, "addr": addr, "uuid": uuid})
            elif op == "unsubscribe" and (addr, uuid) in subs:
                subs.pop((addr, uuid)).cancel()
                send({"ok": True, "op": op})
            elif op == "disconnect":
                send({"ok": True, "op": op})
            else:
                send({"ok": False, "error": "unsupported", "op": op})
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        for t in subs.values():
            t.cancel()
        SESSIONS.discard(writer)
        writer.close()


async def drop_all() -> int:
    n = len(SESSIONS)
    for w in list(SESSIONS):
        w.close()
    return n


async def periodic_drop():
    while DROP_EVERY > 0:
        await asyncio.sleep(DROP_EVERY)
        await drop_all()


async def handler(req):
    if req.path == "/health":
        return 200, {"ok": True}
    if req.path == "/state":
        return 200, {"sim_t": round(time.monotonic() - T0, 2), "sessions": len(SESSIONS), **STATS}
    if req.path == "/drop" and req.method == "POST":
        return 200, {"dropped": await drop_all()}
    return 404, {"error": "not found"}


async def main():
    await minihttp.serve("0.0.0.0", CTL_PORT, handler)
    server = await asyncio.start_server(session, "0.0.0.0", PORT)
    asyncio.create_task(periodic_drop())
    print(f"ble-sim: GATT-эмуляция :{PORT}, устройств {len(BLE_DEVICES)}, управление :{CTL_PORT}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
