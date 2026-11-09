"""Collector - приёмная сторона вместо ingest/TimescaleDB: принимает данные шлюза и проверяет их по seq.

Подписывается на gw/+/data и gw/+/alarm (QoS 1), запоминает время прихода каждой записи, находит
пропуски и дубликаты seq, а также читает счётчики брокера $SYS (сообщений/байт принято) - по ним
оркестратор строит пиковую нагрузку на брокер при backfill.

HTTP: GET /health, /stats, /verify?gw=&lo=&hi=, /dump?since=<мс>, /alarms?since=<мс>, POST /reset
"""
import asyncio
import bisect
import json
import os
import ssl
import time

import aiomqtt

from common import minihttp

BROKER = os.environ.get("BROKER_HOST", "broker-wan")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "8883"))
CA_FILE = os.environ.get("CA_FILE", "/certs/ca.crt")
TLS = os.environ.get("BROKER_TLS", "1") == "1"
CTL_PORT = int(os.environ.get("CTL_PORT", "8082"))


class Store:
    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.seen: dict = {}                 # gw -> {seq: сколько раз получена}
        self.t_msgs: list = []               # время прихода сообщения (мс) - для bisect
        self.msgs: list = []                 # [t, kind(0 data|1 alarm), n записей, байт]
        self.t_recs: list = []
        self.recs: list = []                 # [t, seq, ts, kind]
        self.alarms: list = []               # [t, seq, ts, id, state]
        self.sys: list = []                  # [t, publish_messages_received, publish_bytes_received]
        self.bad = 0
        self.sys_last = {"msgs": 0, "bytes": 0}

    def on_message(self, topic: str, payload: bytes, t_ms: float) -> None:
        kind = 1 if topic.endswith("/alarm") else 0
        try:
            d = json.loads(payload)
            gw, recs = d["gw"], d["recs"]
            if d.get("v") != 1 or d.get("n") != len(recs):
                raise ValueError("envelope")
        except (ValueError, KeyError, TypeError):
            self.bad += 1
            return
        self.t_msgs.append(t_ms)
        self.msgs.append([t_ms, kind, len(recs), len(payload)])
        seen = self.seen.setdefault(gw, {})
        for r in recs:
            try:
                seq, ts = int(r["seq"]), int(r["ts"])
                r["asset"], r["tag"], r["val"]          # обязательные поля
            except (KeyError, TypeError, ValueError):
                self.bad += 1
                continue
            seen[seq] = seen.get(seq, 0) + 1
            self.t_recs.append(t_ms)
            self.recs.append([t_ms, seq, ts, kind])
            if "alarm" in r:
                self.alarms.append([t_ms, seq, ts, r["alarm"].get("id"), r["alarm"].get("state")])

    def on_sys(self, topic: str, payload: bytes, t_ms: float) -> None:
        try:
            v = int(payload)
        except ValueError:
            return
        if topic.endswith("publish/messages/received"):
            self.sys_last["msgs"] = v
            self.sys.append([t_ms, v, self.sys_last["bytes"]])
        elif topic.endswith("publish/bytes/received"):
            self.sys_last["bytes"] = v

    def verify(self, gw: str, lo: int, hi: int) -> dict:
        seen = self.seen.get(gw, {})
        missing = [s for s in range(lo, hi + 1) if s not in seen]
        dup_extra = sum(c - 1 for s, c in seen.items() if lo <= s <= hi and c > 1)
        return {"gw": gw, "lo": lo, "hi": hi, "expected": max(0, hi - lo + 1),
                "received_unique": (hi - lo + 1) - len(missing) if hi >= lo else 0,
                "missing_count": len(missing), "missing": missing[:500], "duplicates_extra": dup_extra}

    def slice_from(self, since: float):
        i = bisect.bisect_left(self.t_msgs, since)
        j = bisect.bisect_left(self.t_recs, since)
        k = bisect.bisect_left([x[0] for x in self.sys], since)
        return i, j, k


async def consume(store: Store) -> None:
    ctx = ssl.create_default_context(cafile=CA_FILE) if TLS else None
    while True:
        try:
            async with aiomqtt.Client(BROKER, BROKER_PORT, identifier="collector", tls_context=ctx, keepalive=15,
                                      max_queued_incoming_messages=0) as c:
                await c.subscribe([("gw/+/data", 1), ("gw/+/alarm", 1), ("$SYS/broker/publish/messages/received", 0),
                                   ("$SYS/broker/publish/bytes/received", 0)])
                async for m in c.messages:
                    t = time.time() * 1000
                    topic = str(m.topic)
                    if topic.startswith("$SYS"):
                        store.on_sys(topic, m.payload, t)
                    else:
                        store.on_message(topic, m.payload, t)
        except asyncio.CancelledError:
            raise
        except (aiomqtt.MqttError, OSError, ssl.SSLError) as e:
            print(f"collector: переподключение ({e})", flush=True)
            await asyncio.sleep(2)


async def main() -> None:
    store = Store()

    async def handler(req):
        q = req.query
        if req.path == "/health":
            return 200, {"ok": True}
        if req.path == "/stats":
            return 200, {"messages": len(store.msgs), "records": len(store.recs), "bad": store.bad,
                         "gateways": {gw: {"unique": len(s), "max_seq": max(s) if s else 0,
                                           "duplicates_extra": sum(c - 1 for c in s.values() if c > 1)}
                                      for gw, s in store.seen.items()},
                         "last_arrival_ms": store.msgs[-1][0] if store.msgs else None, "now_ms": time.time() * 1000}
        if req.path == "/verify":
            return 200, store.verify(q.get("gw", "gw-01"), int(q["lo"]), int(q["hi"]))
        if req.path == "/dump":
            i, j, k = store.slice_from(float(q.get("since", 0)))
            return 200, {"msgs": store.msgs[i:], "recs": store.recs[j:], "sys": store.sys[k:], "now_ms": time.time() * 1000}
        if req.path == "/alarms":
            since = float(q.get("since", 0))
            return 200, {"alarms": [a for a in store.alarms if a[0] >= since]}
        if req.path == "/reset" and req.method == "POST":
            store.reset()
            return 200, {"ok": True}
        return 404, {"error": "not found"}

    await minihttp.serve("0.0.0.0", CTL_PORT, handler)
    print(f"collector: брокер {BROKER}:{BROKER_PORT} (TLS={TLS}), управление :{CTL_PORT}", flush=True)
    await consume(store)


if __name__ == "__main__":
    asyncio.run(main())
