"""Офлайн-расчёт эффекта deadband на детерминированной трассе (шаг 3, таблица «deadband -> экономия трафика»).

Трасса - это ровно то, что видит шлюз по настоящему маппингу (config/gateway.yaml): 19 тегов Modbus раз в
секунду, BLE-уведомления раз в 2 с, 3 устройства ESP32 раз в 2 с. Фильтр тот же класс gw.deadband.Deadband,
что работает в шлюзе, поэтому цифры сопоставимы с живым измерением (опыт E2).
"""
import math
import os

from gw import config
from gw.deadband import Deadband
from gw.mapping import build_reg_tags, convert, decode_gatt, decode_registers
from gw.model import Sample, with_seq
from sim import ble_sim
from sim.signals import BLE_DEVICES, ESP32_DEVICES, Plant

CONFIG_PATH = os.environ.get("GW_CONFIG", os.path.join(os.path.dirname(__file__), "..", "config", "gateway.yaml"))
ENVELOPE_BYTES = 43            # {"v":1,"gw":"gw-01","n":10,"recs":[ ]} - накладные расходы одного сообщения
BATCH = 10                     # записей в живом сообщении


def build_trace(cfg, duration_s: int = 3600) -> list:
    """Список Sample в порядке времени за duration_s секунд (ts в мс от 0)."""
    plant = Plant()
    mb = cfg.sources.modbus
    tags = build_reg_tags(mb.tags, mb.get("word_order", "big"))
    out = []
    ble = cfg.sources.ble
    mq = cfg.sources.mqtt
    for sec in range(duration_s):
        t = float(sec)
        ts = sec * 1000
        out += decode_registers(plant.modbus_registers(t), tags, ts, "modbus")
        out.append(Sample(mb.link_tag.asset, mb.link_tag.tag, True, "", "gateway", ts))
        for addr, dev in ((d.addr, d) for d in ble.devices):
            for ch in dev.chars:
                period = 2 if ch.get("mode", "notify") == "notify" else int(ch.get("period_s", 30))
                if sec % period == 0:
                    raw = decode_gatt(ch.fmt, ble_sim.encode(ch.uuid, plant.ble_values(BLE_DEVICES[addr], t)[ch.uuid]))
                    out.append(Sample(dev.asset, ch.tag, convert(raw, float(ch.get("scale", 1.0)), 0.0, ch.get("decimals")),
                                      ch.get("unit", ""), "ble", ts))
        if sec % 2 == 0:
            for dev in ESP32_DEVICES:
                data = plant.esp32_payload(dev, t)
                for f in mq.fields:
                    out.append(Sample(dev, f.tag, convert(data[f.field], float(f.get("scale", 1.0)), 0.0, f.get("decimals")),
                                      f.get("unit", ""), "mqtt", ts))
    return out


def scaled_tuned(cfg, m: float) -> dict:
    """Рабочий профиль deadband из конфигурации, все пороги умножены на m."""
    tags = {}
    for pat, r in cfg.deadband.tags.items():
        tags[pat] = {k: (v * m if k in ("abs", "pct") else v) for k, v in r.items()}
    return {"enabled": True, "max_silence_s": cfg.deadband.max_silence_s, "default": {"abs": 0.0}, "tags": tags}


def profiles(cfg) -> list:
    """(метка, группа, конфигурация deadband) для таблицы эксперимента E1."""
    rows = [("без фильтра", "off", {"enabled": False}),
            ("только изменения", "exception", {"enabled": True, "max_silence_s": cfg.deadband.max_silence_s, "default": {"abs": 0.0}, "tags": {}})]
    for m in (0.25, 0.5, 1, 2, 4):
        rows.append((f"абсолютный ×{m:g}" + (" (рабочий)" if m == 1 else ""), "abs", scaled_tuned(cfg, m)))
    for p in (0.1, 0.5, 1, 2, 5):
        rows.append((f"процентный {p:g} %", "pct", {"enabled": True, "max_silence_s": cfg.deadband.max_silence_s, "default": {"pct": p}, "tags": {}}))
    return rows


def evaluate(trace: list, db_cfg: dict, duration_s: int, per_tag: bool = False) -> dict:
    """Прогоняет трассу через Deadband и считает экономию и ошибку восстановления."""
    db = Deadband(config.Cfg(db_cfg))
    held, rng, kept_n, kept_bytes = {}, {}, 0, 0
    abs_sum = {}
    err_max, err_sum, err_n = {}, {}, {}
    for s in trace:
        v = s.value
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            numeric = False
        else:
            numeric = True
            lo, hi = rng.get(s.key, (v, v))
            rng[s.key] = (min(lo, v), max(hi, v))
            abs_sum[s.key] = abs_sum.get(s.key, 0.0) + abs(v)
        if db.check(s):
            kept_n += 1
            kept_bytes += len(with_seq(kept_n, s.body()))
            held[s.key] = v
        if numeric and s.key in held:
            e = abs(v - held[s.key])
            err_max[s.key] = max(err_max.get(s.key, 0.0), e)
            err_sum[s.key] = err_sum.get(s.key, 0.0) + e
            err_n[s.key] = err_n.get(s.key, 0) + 1
    # ошибка восстановления (удержание последнего отправленного значения) в % от среднего показания тега
    rel_max, rel_mean, worst = [], [], ("", 0.0)
    for k, e in err_max.items():
        level = abs_sum[k] / err_n[k]
        if level > 0:
            rel_max.append(100.0 * e / level)
            if 100.0 * e / level > worst[1]:
                worst = (k, 100.0 * e / level)
            rel_mean.append(100.0 * err_sum[k] / err_n[k] / level)
    n_in = len(trace)
    msgs = math.ceil(kept_n / BATCH)
    res = {"in": n_in, "out": kept_n, "reduction_pct": round(100.0 * (1 - kept_n / n_in), 2) if n_in else 0.0,
           "payload_bytes": kept_bytes + ENVELOPE_BYTES * msgs, "records_per_s": round(kept_n / duration_s, 3),
           "err_max_pct": round(max(rel_max), 3) if rel_max else 0.0, "err_worst_tag": worst[0],
           "err_mean_pct": round(sum(rel_mean) / len(rel_mean), 4) if rel_mean else 0.0}
    if per_tag:
        res["tags"] = {k: {"in": c[0], "out": c[0] - c[1], "reduction_pct": round(100.0 * c[1] / c[0], 1),
                           "err_max": round(err_max.get(k, 0.0), 3),
                           "err_max_pct": round(100.0 * err_max[k] / (abs_sum[k] / err_n[k]), 2)
                           if k in err_max and abs_sum[k] > 0 else 0.0}
                       for k, c in sorted(db.counts.items())}
    return res


def run(duration_s: int = 3600, path: str = CONFIG_PATH) -> dict:
    cfg = config.load(path)
    trace = build_trace(cfg, duration_s)
    rows, base = [], None
    for label, group, dbc in profiles(cfg):
        r = evaluate(trace, dbc, duration_s, per_tag=(label.endswith("(рабочий)")))
        r.update({"label": label, "group": group})
        if base is None:
            base = r
        r["bytes_saved_pct"] = round(100.0 * (1 - r["payload_bytes"] / base["payload_bytes"]), 2)
        rows.append(r)
    return {"duration_s": duration_s, "samples": len(trace), "rows": rows}


if __name__ == "__main__":
    import json
    print(json.dumps(run(600), ensure_ascii=False, indent=1)[:3000])
