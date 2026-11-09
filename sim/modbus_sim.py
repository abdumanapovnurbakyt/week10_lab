"""Эмулятор Modbus TCP-устройства: сервер pymodbus с 20 holding-регистрами (шаг 1 задания).

Значения регистров вычисляются в момент чтения по модели sim.signals.Plant (сигнал меняется с шагом 0.5 с).
Управление по HTTP:
  GET  /health
  GET  /state               - время симуляции, активные события, число прочитанных пакетов
  POST /event?name=...      - запустить событие (pump1_overheat, pump2_vibration, tank_low, boiler_flame_out)
"""
import asyncio
import logging
import os
import time

from pymodbus.simulator import DataType, SimData, SimDevice
from pymodbus.server import StartAsyncTcpServer

from common import minihttp
from sim.signals import EVENTS, N_REGS, Plant

PORT = int(os.environ.get("MODBUS_PORT", "1502"))
CTL_PORT = int(os.environ.get("CTL_PORT", "8080"))


STATS = {"reads": 0}


async def main():
    logging.getLogger("pymodbus").setLevel(logging.ERROR)
    plant = Plant()
    t0 = time.monotonic()

    def sim_t() -> float:
        return time.monotonic() - t0

    async def on_access(function_code, start_address, address, count, current_registers, set_values):
        """Хук pymodbus: перед ответом на чтение подставляем актуальные значения регистров."""
        if set_values is None:
            STATS["reads"] += 1
            for i, v in enumerate(plant.modbus_registers(sim_t())):
                current_registers[i - start_address] = v
        return None

    device = SimDevice(id=1, simdata=[SimData(address=0, count=100, values=0, datatype=DataType.REGISTERS)],
                       action=on_access)

    async def handler(req):
        if req.path == "/health":
            return 200, {"ok": True}
        if req.path == "/state":
            t = sim_t()
            return 200, {"sim_t": round(t, 2), "events": {k: round(t - v, 1) for k, v in plant.events.items()},
                         "reads": STATS["reads"], "wall_ms": int(time.time() * 1000)}
        if req.path == "/event" and req.method == "POST":
            name = req.query.get("name", "")
            if name not in EVENTS:
                return 400, {"error": "неизвестное событие", "known": sorted(EVENTS)}
            plant.start_event(name, sim_t())
            return 200, {"event": name, "duration_s": EVENTS[name], "wall_ms": int(time.time() * 1000)}
        return 404, {"error": "not found"}

    await minihttp.serve("0.0.0.0", CTL_PORT, handler)
    print(f"modbus-sim: Modbus TCP :{PORT}, {N_REGS} регистров, управление :{CTL_PORT}", flush=True)
    await StartAsyncTcpServer(context=device, address=("0.0.0.0", PORT))


if __name__ == "__main__":
    asyncio.run(main())
