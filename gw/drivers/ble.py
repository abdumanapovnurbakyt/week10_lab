"""Драйвер BLE/GATT поверх эмулированного транспорта (TCP, строки JSON), см. sim/ble_sim.py.

На реальном железе вместо _session() используется bleak (BleakClient.start_notify / read_gatt_char);
отображение «характеристика -> тег» и декодирование значений (gw.mapping.decode_gatt) остаются теми же.
"""
import asyncio
import json
import time

from ..mapping import convert, decode_gatt
from ..model import Sample


class BleDriver:
    name = "ble"

    def __init__(self, cfg, ingest):
        self.c = cfg
        self.ingest = ingest
        self.devs = {d.addr: d for d in cfg.devices}
        self.link = None
        self.m = {"notifications": 0, "reads": 0, "reconnects": 0, "bad_frames": 0}

    def _sample(self, addr: str, uuid: str, hex_value: str):
        dev = self.devs[addr]
        for ch in dev.chars:
            if ch.uuid == uuid:
                raw = decode_gatt(ch.fmt, hex_value)
                return Sample(dev.asset, ch.tag, convert(raw, float(ch.get("scale", 1.0)), 0.0, ch.get("decimals")),
                              ch.get("unit", ""), "ble", int(time.time() * 1000))
        return None

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.c.host, int(self.c.port))
                backoff = 1.0
                await self._session(reader, writer)
            except asyncio.CancelledError:
                raise
            except (OSError, asyncio.IncompleteReadError, ValueError):
                pass
            if self.link is not False:
                self.link = False
                await self.ingest([Sample(self.c.link_tag.asset, self.c.link_tag.tag, False, "", "gateway",
                                          int(time.time() * 1000))])
            self.m["reconnects"] += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    async def _session(self, reader, writer) -> None:
        def send(obj: dict) -> None:
            writer.write((json.dumps(obj, separators=(",", ":")) + "\n").encode())

        for addr, dev in self.devs.items():
            send({"op": "connect", "addr": addr})
            for ch in dev.chars:
                if ch.get("mode", "notify") == "notify":
                    send({"op": "subscribe", "addr": addr, "uuid": ch.uuid})
        await writer.drain()
        self.link = True
        await self.ingest([Sample(self.c.link_tag.asset, self.c.link_tag.tag, True, "", "gateway", int(time.time() * 1000))])

        async def poller():
            """Периодическое чтение характеристик в режиме read (например, заряд батареи)."""
            reads = [(a, ch) for a, d in self.devs.items() for ch in d.chars if ch.get("mode") == "read"]
            if not reads:
                return
            period = min(float(ch.period_s) for _a, ch in reads)
            while True:
                for addr, ch in reads:
                    send({"op": "read", "addr": addr, "uuid": ch.uuid})
                await writer.drain()
                await asyncio.sleep(period)

        task = asyncio.create_task(poller())
        try:
            while True:
                line = await reader.readline()
                if not line:
                    raise OSError("BLE: соединение закрыто")
                try:
                    msg = json.loads(line)
                    if msg.get("ev") == "notify":
                        self.m["notifications"] += 1
                    elif msg.get("op") == "read" and msg.get("ok"):
                        self.m["reads"] += 1
                    else:
                        continue
                    s = self._sample(msg["addr"], msg["uuid"], msg["value"])
                except (ValueError, KeyError):
                    self.m["bad_frames"] += 1
                    continue
                if s is not None:
                    await self.ingest([s])
        finally:
            task.cancel()
            writer.close()
