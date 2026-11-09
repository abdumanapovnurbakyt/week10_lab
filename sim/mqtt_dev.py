"""Эмулятор ESP32-устройств, публикующих телеметрию в локальный MQTT-брокер юга.

Каждое устройство раз в 2 с шлёт JSON в «сырых» единицах (десятые доли °C, десятые доли %, мВ, дБм)
на топик south/<device>/telemetry (QoS 0) и держит retained-статус south/<device>/status
(online/offline по Last Will). Метки времени ESP32 не отправляет - шлюз ставит время приёма.
Управление по HTTP: GET /health, GET /state, POST /event?name=esp32_lowbat
"""
import asyncio
import json
import os
import time

import aiomqtt

from common import minihttp
from sim.signals import ESP32_DEVICES, EVENTS, Plant

BROKER = os.environ.get("SOUTH_BROKER", "broker-south")
BROKER_PORT = int(os.environ.get("SOUTH_PORT", "1883"))
CTL_PORT = int(os.environ.get("CTL_PORT", "8080"))
PERIOD = float(os.environ.get("MQTT_DEV_PERIOD_S", "2.0"))

T0 = time.monotonic()
PLANT = Plant()
STATS = {"published": 0, "reconnects": 0}


async def device(dev: str, offset: float) -> None:
    """Одно устройство: подключение с повтором, статус, периодическая телеметрия."""
    while True:
        try:
            will = aiomqtt.Will(f"south/{dev}/status", b"offline", qos=1, retain=True)
            async with aiomqtt.Client(BROKER, BROKER_PORT, identifier=f"sim-{dev}", will=will, keepalive=15) as c:
                await c.publish(f"south/{dev}/status", b"online", qos=1, retain=True)
                await asyncio.sleep(offset)
                nxt = time.monotonic()
                while True:
                    payload = PLANT.esp32_payload(dev, time.monotonic() - T0)
                    await c.publish(f"south/{dev}/telemetry", json.dumps(payload, separators=(",", ":")), qos=0)
                    STATS["published"] += 1
                    nxt += PERIOD
                    await asyncio.sleep(max(0.0, nxt - time.monotonic()))
        except (aiomqtt.MqttError, OSError):
            STATS["reconnects"] += 1
            await asyncio.sleep(2)


async def handler(req):
    if req.path == "/health":
        return 200, {"ok": True}
    if req.path == "/state":
        return 200, {"sim_t": round(time.monotonic() - T0, 2), "events": {k: round(time.monotonic() - T0 - v, 1) for k, v in PLANT.events.items()}, **STATS}
    if req.path == "/event" and req.method == "POST":
        name = req.query.get("name", "")
        if name not in EVENTS:
            return 400, {"error": "неизвестное событие", "known": sorted(EVENTS)}
        PLANT.start_event(name, time.monotonic() - T0)
        return 200, {"event": name, "duration_s": EVENTS[name], "wall_ms": int(time.time() * 1000)}
    return 404, {"error": "not found"}


async def main():
    await minihttp.serve("0.0.0.0", CTL_PORT, handler)
    print(f"mqtt-dev: {len(ESP32_DEVICES)} устройств -> {BROKER}:{BROKER_PORT}, период {PERIOD} с", flush=True)
    await asyncio.gather(*(device(d, i * PERIOD / len(ESP32_DEVICES)) for i, d in enumerate(ESP32_DEVICES)))


if __name__ == "__main__":
    asyncio.run(main())
