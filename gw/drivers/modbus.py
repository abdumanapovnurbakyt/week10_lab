"""Драйвер Modbus TCP: опрос блока holding-регистров с заданным периодом (шаг 1 задания)."""
import asyncio
import time

from pymodbus.client import AsyncModbusTcpClient

from ..mapping import build_reg_tags, decode_registers
from ..model import Sample


class ModbusDriver:
    name = "modbus"

    def __init__(self, cfg, ingest):
        self.c = cfg
        self.ingest = ingest
        self.tags = build_reg_tags(cfg.tags, cfg.get("word_order", "big"))
        self.link = None                       # True/False - последнее известное состояние связи
        self.m = {"polls": 0, "errors": 0, "poll_ms_last": 0.0, "poll_ms_max": 0.0, "overruns": 0}

    def _link_sample(self, ok: bool, ts: int) -> Sample:
        lt = self.c.link_tag
        return Sample(lt.asset, lt.tag, ok, "", "gateway", ts)

    async def run(self) -> None:
        c = self.c
        client = AsyncModbusTcpClient(c.host, port=int(c.port), timeout=float(c.get("timeout_s", 1.0)), retries=1)
        period = float(c.poll_interval_s)
        loop = asyncio.get_running_loop()
        next_t = loop.time()
        try:
            while True:
                t0 = loop.time()
                ts = int(time.time() * 1000)
                try:
                    if not client.connected:
                        await client.connect()
                    rr = await client.read_holding_registers(int(c.start), count=int(c.count), device_id=int(c.unit))
                    if rr.isError():
                        raise OSError(f"Modbus: {rr}")
                    samples = decode_registers(rr.registers, self.tags, ts, "modbus")
                    samples.append(self._link_sample(True, ts))
                    self.link = True
                    self.m["polls"] += 1
                    await self.ingest(samples)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - любая ошибка опроса = потеря связи с устройством
                    self.m["errors"] += 1
                    if self.link is not False:
                        self.link = False
                        await self.ingest([self._link_sample(False, ts)])
                dt = (loop.time() - t0) * 1000
                self.m["poll_ms_last"] = round(dt, 2)
                self.m["poll_ms_max"] = max(self.m["poll_ms_max"], round(dt, 2))
                next_t += period
                delay = next_t - loop.time()
                if delay < 0:                  # не успели - пропускаем накопившиеся такты, а не отстаём навсегда
                    self.m["overruns"] += 1
                    next_t = loop.time()
                    delay = 0
                await asyncio.sleep(delay)
        finally:
            client.close()
