"""Точка входа шлюза: запускает драйверы, отправку в облако и HTTP-статус/управление.

HTTP (порт cfg.api.port):
  GET  /health, /status, /dropped, /config
  POST /config               - слить JSON-патч в конфигурацию (deadband, backfill, publish, buffer.capacity ...)
  POST /chaos/crash          - аварийно завершить процесс (имитация kill -9/питания; только если GW_CHAOS=1)
"""
import asyncio
import os
import signal
import sys

from common import minihttp

from . import config
from .drivers.ble import BleDriver
from .drivers.modbus import ModbusDriver
from .drivers.mqtt_src import MqttSourceDriver
from .gateway import Gateway


async def amain() -> None:
    cfg = config.load(os.environ.get("GW_CONFIG", "/app/config/gateway.yaml"))
    gw = Gateway(cfg)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    async def handler(req):
        if req.path == "/health":
            return 200, {"ok": True}
        if req.path == "/status":
            return 200, gw.status()
        if req.path == "/dropped":
            seqs = await asyncio.to_thread(gw.outbox.dropped_seqs)
            return 200, {"count": len(seqs), "seqs": seqs}
        if req.path == "/config" and req.method == "GET":
            return 200, cfg.to_dict()
        if req.path == "/config" and req.method == "POST":
            gw.apply_config(req.json())
            return 200, {"ok": True}
        if req.path == "/chaos/crash" and req.method == "POST":
            if os.environ.get("GW_CHAOS", "1") != "1":
                return 400, {"error": "chaos отключён"}
            loop.call_later(0.1, os._exit, 137)
            return 200, {"crashing": True}
        return 404, {"error": "not found"}

    await minihttp.serve("0.0.0.0", int(cfg.api.port), handler)

    src = cfg.sources
    if src.modbus.get("enabled", True):
        gw.drivers.append(ModbusDriver(src.modbus, gw.ingest))
    if src.ble.get("enabled", True):
        gw.drivers.append(BleDriver(src.ble, gw.ingest))
    if src.mqtt.get("enabled", True):
        gw.drivers.append(MqttSourceDriver(src.mqtt, gw.ingest))
    tasks = [asyncio.create_task(gw.uplink.run())] + [asyncio.create_task(d.run()) for d in gw.drivers]
    print(f"gateway {gw.gw_id}: источники {[d.name for d in gw.drivers]}, буфер {cfg.buffer.path} "
          f"(восстановлено записей: {gw.outbox.recovered}), uplink {cfg.uplink.host}:{cfg.uplink.port}", flush=True)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    gw.uplink.stop = True
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    gw.outbox.close()
    print("gateway: остановлен, буфер сохранён", flush=True)


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
