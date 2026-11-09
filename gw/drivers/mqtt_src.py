"""Драйвер MQTT-источников (ESP32): подписка на локальный брокер юга, нормализация полей JSON."""
import asyncio
import json
import time

import aiomqtt

from ..mapping import convert
from ..model import Sample


class MqttSourceDriver:
    name = "mqtt"

    def __init__(self, cfg, ingest):
        self.c = cfg
        self.ingest = ingest
        self.m = {"messages": 0, "bad_payloads": 0, "reconnects": 0}

    async def run(self) -> None:
        c = self.c
        while True:
            try:
                async with aiomqtt.Client(c.host, int(c.port), identifier=c.get("client_id", "gw-south"),
                                          keepalive=30) as client:
                    await client.subscribe([(c.telemetry_topic, 0), (c.status_topic, 1)])
                    async for msg in client.messages:
                        await self._handle(str(msg.topic), msg.payload)
            except asyncio.CancelledError:
                raise
            except (aiomqtt.MqttError, OSError):
                self.m["reconnects"] += 1
                await asyncio.sleep(2.0)

    async def _handle(self, topic: str, payload: bytes) -> None:
        parts = topic.split("/")
        if len(parts) != 3:
            return
        _root, dev, kind = parts
        ts = int(time.time() * 1000)
        self.m["messages"] += 1
        if kind == "status":
            await self.ingest([Sample(dev, "link", payload == b"online", "", "mqtt", ts)])
            return
        try:
            data = json.loads(payload)
            samples = []
            for f in self.c.fields:
                if f.field in data:
                    samples.append(Sample(dev, f.tag, convert(data[f.field], float(f.get("scale", 1.0)), 0.0, f.get("decimals")),
                                          f.get("unit", ""), "mqtt", ts))
        except (ValueError, TypeError, AttributeError):
            self.m["bad_payloads"] += 1
            return
        if samples:
            await self.ingest(samples)
