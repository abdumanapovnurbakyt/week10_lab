"""Ядро шлюза: drivers -> normalize -> alarm rules -> deadband -> buffer(SQLite) -> publisher."""
import asyncio
import os
import resource
import sqlite3
import sys
import time

from .buffer import Outbox
from .deadband import Deadband
from .model import PRIO_ALARM, PRIO_NORMAL, Sample
from .rules import RuleEngine
from .uplink import Uplink


class Gateway:
    def __init__(self, cfg):
        self.cfg = cfg
        self.gw_id = cfg.gateway_id
        self.outbox = Outbox(cfg.buffer.path, int(cfg.buffer.capacity), cfg.buffer.get("synchronous", "NORMAL"))
        self.deadband = Deadband(cfg.deadband)
        self.rules = RuleEngine(cfg.get("rules", []))
        self.uplink = Uplink(cfg, self.outbox, self.gw_id)
        self.drivers: list = []
        self.started = time.time()
        self.m = {"in": {}, "filtered": 0, "buffered": 0, "alarm_records": 0, "evicted": 0, "db_errors": 0}

    # ---------------------------------------------------------------- приём от драйверов
    async def ingest(self, samples: list) -> None:
        items = []
        fast = bool(self.cfg.alarms.get("fast_path", True))
        for s in samples:
            self.m["in"][s.src] = self.m["in"].get(s.src, 0) + 1
            for a in self.rules.evaluate(s):                  # правила - по каждому значению, до deadband
                items.append((a.ts, PRIO_ALARM if fast else PRIO_NORMAL, a.body()))
                self.m["alarm_records"] += 1
            if self.deadband.check(s):
                items.append((s.ts, PRIO_NORMAL, s.body()))
            else:
                self.m["filtered"] += 1
        if not items:
            return
        try:
            _seqs, dropped = await asyncio.to_thread(self.outbox.put_many, items)
        except sqlite3.Error as e:
            self.m["db_errors"] += 1
            print(f"ОШИБКА буфера: {e}", file=sys.stderr, flush=True)
            return
        self.m["buffered"] += len(items)
        self.m["evicted"] += len(dropped)
        self.uplink.notify()

    # ---------------------------------------------------------------- состояние
    def status(self) -> dict:
        ru = resource.getrusage(resource.RUSAGE_SELF)
        try:
            with open("/proc/self/statm") as f:
                rss_kb = int(f.read().split()[1]) * (os.sysconf("SC_PAGE_SIZE") // 1024)
        except OSError:
            rss_kb = ru.ru_maxrss
        b = self.outbox.stats()
        head = self.outbox.head_ts(PRIO_NORMAL)
        total_in = sum(self.m["in"].values())
        return {
            "gateway_id": self.gw_id, "now_ms": int(time.time() * 1000), "uptime_s": round(time.time() - self.started, 1),
            "state": self.uplink.state, "mode": self.uplink.mode, "last_error": self.uplink.last_error,
            "buffer": {**b, "fill_pct": round(100.0 * b["depth"] / max(1, b["capacity"]), 2),
                       "oldest_age_s": round((time.time() * 1000 - head) / 1000.0, 2) if head else 0.0,
                       "recovered_at_start": self.outbox.recovered},
            "records": {"in": total_in, "in_by_source": dict(self.m["in"]), "filtered": self.m["filtered"],
                        "buffered": self.m["buffered"], "alarm_records": self.m["alarm_records"],
                        "evicted": self.m["evicted"], "db_errors": self.m["db_errors"]},
            "uplink": {**self.uplink.m, "resyncs": self.uplink.resyncs[-10:]},
            "drivers": {d.name: {"link": getattr(d, "link", None), **d.m} for d in self.drivers},
            "alarms": {"raised": self.rules.raised, "cleared": self.rules.cleared},
            "process": {"cpu_s": round(ru.ru_utime + ru.ru_stime, 3), "rss_kb": rss_kb},
            "settings": {"backfill_rate": self.cfg.backfill.rate, "backfill_batch": self.cfg.backfill.batch,
                         "batch_max": self.cfg.publish.batch_max, "batch_wait_ms": self.cfg.publish.batch_wait_ms,
                         "deadband_enabled": self.cfg.deadband.get("enabled", True),
                         "fast_path": self.cfg.alarms.get("fast_path", True), "capacity": self.outbox.capacity},
        }

    def apply_config(self, patch: dict) -> None:
        """Горячее изменение параметров (POST /config) - используется оркестратором экспериментов."""
        patch = dict(patch)
        reset_rules = patch.pop("reset_rules", False)
        self.cfg.merge(patch)
        if "deadband" in patch:
            self.deadband.configure(self.cfg.deadband)
        if "buffer" in patch and "capacity" in patch["buffer"]:
            self.outbox.capacity = int(self.cfg.buffer.capacity)
        if reset_rules:
            self.rules.reset()
