"""Оркестратор экспериментов Lab 7. Управляет стендом только по HTTP (docker.sock не нужен).

  docker compose run --rm bench --scale smoke            # ~5 минут, проверка стенда
  docker compose run --rm bench --scale full             # полный прогон по заданию (~1 час 15 минут)
  docker compose run --rm bench --scale full --only e3   # отдельные опыты: e1..e8

Опыты:
  e1  deadband офлайн: таблица «deadband -> экономия трафика» на трассе 1 час (шаг 3)
  e2  deadband вживую: трафик на проводе (байты через прокси WAN) для 4 профилей
  e3  три цикла «обрыв 5 мин -> восстановление» без фильтра (худший случай), потери по seq (шаги 5, 7)
  e3b тот же цикл с рабочим профилем deadband и режимом reject (аналог docker network disconnect)
  e4  скорость backfill: время ресинхронизации и пик нагрузки на брокер при разных лимитах (шаг 6)
  e4b тот же backfill при «застрявшем» канале (stall): всплеск доставки задержанных сообщений
  e5  переполнение буфера: что и в каком порядке теряется, сохранность тревог (шаг 4)
  e6  тревоги: быстрый путь против батчинга (шаг 8)
  e7  аварийный перезапуск шлюза во время обрыва: буфер переживает kill (шаг 4)
  e8  микротест SQLite: режимы synchronous OFF/NORMAL/FULL
"""
import argparse
import csv
import json
import math
import os
import platform
import sys
import tempfile
import threading
import time

from common import httpc
from gw import config
from lab import sweep

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URL = {"gw": os.environ.get("GW_URL", "http://gateway:8080"),
       "link": os.environ.get("LINK_URL", "http://wan-link:8081"),
       "col": os.environ.get("COL_URL", "http://collector:8082"),
       "modbus": os.environ.get("MODBUS_URL", "http://modbus-sim:8080"),
       "ble": os.environ.get("BLE_URL", "http://ble-sim:8080"),
       "mqtt": os.environ.get("MQTT_URL", "http://mqtt-dev:8080")}
STATE = {"work_db": None}


def log(msg: str) -> None:
    print(time.strftime("%H:%M:%S"), msg, flush=True)


# ------------------------------------------------------------------ обращения к стенду
def gw_status() -> dict:
    return httpc.get(URL["gw"] + "/status", 5)


def gw_patch(patch: dict) -> None:
    httpc.post(URL["gw"] + "/config", patch)


def set_link(mode: str) -> dict:
    return httpc.post(f'{URL["link"]}/mode?set={mode}')


def link_stats() -> dict:
    return httpc.get(URL["link"] + "/stats", 5)


def col_dump(since_ms: float) -> dict:
    return httpc.get(URL["col"] + f"/dump?since={int(since_ms)}", 120)


def col_verify(lo: int, hi: int) -> dict:
    return httpc.get(URL["col"] + f"/verify?gw=gw-01&lo={lo}&hi={hi}", 60)


def fire(sim: str, event: str) -> dict:
    return httpc.post(f'{URL[sim]}/event?name={event}')


def pctl(a, p):
    a = sorted(a)
    if not a:
        return None
    k = (len(a) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return a[lo] + (a[hi] - a[lo]) * (k - lo)


def lat_stats(values_ms) -> dict:
    v = list(values_ms)
    return {"n": len(v), "median_ms": round(pctl(v, 50), 1) if v else None, "p95_ms": round(pctl(v, 95), 1) if v else None,
            "max_ms": round(max(v), 1) if v else None}


def peak_window(times, t0, t1, width=1000.0) -> int:
    """Максимум событий в любом скользящем окне width мс внутри [t0, t1]."""
    ts = sorted(t for t in times if t0 <= t <= t1)
    best = j = 0
    for i in range(len(ts)):
        while ts[i] - ts[j] >= width:
            j += 1
        best = max(best, i - j + 1)
    return best


class Sampler(threading.Thread):
    """Раз в секунду снимает состояние шлюза и счётчики прокси WAN - это временной ряд для графиков."""

    def __init__(self, period=1.0):
        super().__init__(daemon=True)
        self.period, self.rows, self._stop_ev = period, [], threading.Event()

    def run(self) -> None:
        while not self._stop_ev.is_set():
            t0 = time.time()
            try:
                s, l = httpc.get(URL["gw"] + "/status", 2), httpc.get(URL["link"] + "/stats", 2)
                b = s["buffer"]
                self.rows.append({"t": round(t0, 2), "state": s["state"], "mode": s["mode"], "depth": b["depth"],
                                  "alarm_pending": b["pending_alarm"], "fill": b["fill_pct"], "age": b["oldest_age_s"],
                                  "acked": b["acked_total"], "dropped": b["dropped_total"], "next_seq": b["next_seq"],
                                  "cpu": s["process"]["cpu_s"], "rss": s["process"]["rss_kb"],
                                  "up": l["bytes_up"], "down": l["bytes_down"], "link": l["mode"]})
            except Exception:  # noqa: BLE001
                self.rows.append({"t": round(t0, 2), "state": "unreachable"})
            self._stop_ev.wait(max(0.0, self.period - (time.time() - t0)))

    def stop(self) -> None:
        self._stop_ev.set()

    def window(self, t0, t1) -> list:
        return [r for r in self.rows if t0 <= r["t"] <= t1]


def wait_for(pred, timeout, poll=0.5, what=""):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = pred()
            if v:
                return v
        except Exception:  # noqa: BLE001
            pass
        time.sleep(poll)
    raise TimeoutError(f"не дождались: {what}")


def settled() -> bool:
    s = gw_status()
    return s["state"] == "online" and s["buffer"]["depth"] <= 40 and s["buffer"]["oldest_age_s"] < 3 and s["mode"] != "backfill"


def reset_defaults() -> None:
    """Возвращает рабочие настройки и канал; ждёт, пока буфер опустеет."""
    set_link("up")
    gw_patch({"deadband": {"_replace": True, **STATE["work_db"]}, "alarms": {"fast_path": True},
              "backfill": {"rate": CFG["limit_msgs_per_s"], "batch": 1}, "buffer": {"capacity": 10000}, "reset_rules": True})
    wait_for(settled, 300, what="шлюз онлайн, буфер пуст")


# ------------------------------------------------------------------ разбор прихода данных
def analyse(dump: dict, t_restore_ms: float, t_end_ms: float, limit: float) -> dict:
    msgs = [m for m in dump["msgs"] if t_restore_ms <= m[0] <= t_end_ms]
    recs = [r for r in dump["recs"] if t_restore_ms <= r[0] <= t_end_ms]
    data_t = [m[0] for m in msgs if m[1] == 0]
    alarm_t = [m[0] for m in msgs if m[1] == 1]
    lags = [(r[0] - r[2], r[0]) for r in recs]
    old = [a for lag, a in lags if lag > 2000]
    sys_rate, sy = [], [x for x in dump["sys"] if t_restore_ms - 1500 <= x[0] <= t_end_ms]
    for a, b in zip(sy, sy[1:]):
        if b[0] > a[0]:
            sys_rate.append((b[1] - a[1]) / ((b[0] - a[0]) / 1000.0))
    return {
        "peak_data_msgs_per_s": peak_window(data_t, t_restore_ms, t_end_ms),
        "peak_all_msgs_per_s": peak_window(data_t + alarm_t, t_restore_ms, t_end_ms),
        "peak_recs_per_s": peak_window([r[0] for r in recs], t_restore_ms, t_end_ms),
        "broker_peak_msgs_per_s": round(max(sys_rate), 1) if sys_rate else None,
        "limit_msgs_per_s": limit,
        "first_msg_kinds": [m[1] for m in sorted(msgs)[:8]],
        "first_alarm_after_restore_s": round((min(alarm_t) - t_restore_ms) / 1000.0, 2) if alarm_t else None,
        "last_old_record_arrival_s": round((max(old) - t_restore_ms) / 1000.0, 2) if old else 0.0,
        "max_record_age_s": round(max(l for l, _ in lags) / 1000.0, 1) if lags else 0.0,
        "records_received": len(recs), "messages_received": len(msgs),
    }


def series(dump: dict, t0_ms: float, t1_ms: float) -> dict:
    """Приход на брокер по секундам: сообщений данных, сообщений-тревог, записей (для графиков)."""
    n = int((t1_ms - t0_ms) / 1000.0) + 1
    m, a, r = [0] * n, [0] * n, [0] * n
    for t, kind, _cnt, _size in dump["msgs"]:
        i = int((t - t0_ms) / 1000.0)
        if 0 <= i < n:
            (a if kind == 1 else m)[i] += 1
    for t, _seq, _ts, _kind in dump["recs"]:
        i = int((t - t0_ms) / 1000.0)
        if 0 <= i < n:
            r[i] += 1
    return {"t0": round(t0_ms / 1000.0, 2), "msgs": m, "alarm_msgs": a, "recs": r}


def wait_verify(lo: int, hi: int, timeout=60, stable_s=8) -> dict:
    """Ждёт, пока collector получит все seq из [lo, hi]. Если число пропусков не меняется stable_s секунд
    (так бывает только при осознанном вытеснении, опыт E5), возвращает итог раньше."""
    end = time.time() + timeout
    last, t_change = None, time.time()
    while True:
        v = col_verify(lo, hi)
        if v["missing_count"] == 0 or time.time() > end:
            return v
        if v["missing_count"] != last:
            last, t_change = v["missing_count"], time.time()
        elif time.time() - t_change >= stable_s:
            return v
        time.sleep(1.0)


def run_cycle(S: dict, sampler: Sampler, label: str, outage_s: float, pre_s: float, post_s: float, mode: str,
              events=(), max_resync_s=420) -> dict:
    """Один цикл: штатная работа -> обрыв на outage_s -> восстановление -> проверка по seq."""
    wait_for(settled, 300, what="штатное состояние перед циклом")
    lo = gw_status()["buffer"]["next_seq"]
    t_start = time.time()
    time.sleep(pre_s)
    t_cut = time.time()
    log(f"  [{label}] обрыв канала ({mode}) на {outage_s:.0f} с")
    set_link(mode)
    pending = sorted(events)
    fired = []
    while time.time() - t_cut < outage_s:
        while pending and time.time() - t_cut >= pending[0][0]:
            frac_s, sim, name = pending.pop(0)
            fire(sim, name)
            fired.append({"t_rel_s": round(time.time() - t_cut, 1), "sim": sim, "event": name})
        time.sleep(0.5)
    s_before = gw_status()
    t_restore = time.time()
    set_link("up")
    log(f"  [{label}] канал восстановлен, в буфере {s_before['buffer']['depth']} записей")
    t_online = t_drained = None
    end = t_restore + max_resync_s
    while time.time() < end:
        try:
            s = gw_status()
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
            continue
        if t_online is None and s["state"] == "online":
            t_online = time.time()
        if t_online and s["buffer"]["depth"] <= 40 and s["buffer"]["oldest_age_s"] < 3 and s["mode"] != "backfill":
            t_drained = time.time()
            break
        time.sleep(0.25)
    time.sleep(post_s)
    s_after = gw_status()
    hi = s_after["buffer"]["next_seq"] - 1
    ver = wait_verify(lo, hi)
    t_end = time.time()
    dump = col_dump(t_start * 1000 - 1000)
    an = analyse(dump, t_restore * 1000, t_end * 1000, S_LIMIT())
    rows = sampler.window(t_start, t_end)
    offline = [r["t"] for r in rows if r["t"] >= t_cut and r.get("state") == "offline"]
    rs = [r for r in s_after["uplink"]["resyncs"] if r["start"] >= t_restore - 1]
    res = {
        "label": label, "mode": mode, "outage_s": outage_s, "seq_lo": lo, "seq_hi": hi, "records_in_cycle": hi - lo + 1,
        "t_start": t_start, "t_cut": t_cut, "t_restore": t_restore, "t_online": t_online, "t_drained": t_drained, "t_end": t_end,
        "backlog_at_restore": s_before["buffer"]["depth"],
        "peak_depth": max((r.get("depth", 0) for r in rows), default=0),
        "peak_fill_pct": max((r.get("fill", 0) for r in rows), default=0),
        "detect_s": round(offline[0] - t_cut, 1) if offline else None,
        "reconnect_s": round(t_online - t_restore, 2) if t_online else None,
        "recovery_s": round(t_drained - t_restore, 2) if t_drained else None,
        "resync_gateway_s": rs[0]["duration_s"] if rs else None, "resync_records": rs[0]["records"] if rs else None,
        "events": fired, "verify": ver, **an, "series": series(dump, t_start * 1000, t_end * 1000),
        "timeline": [{k: r[k] for k in ("t", "state", "mode", "depth", "alarm_pending", "fill", "age", "link") if k in r} for r in rows],
    }
    log(f"  [{label}] seq {lo}..{hi}: получено {ver['received_unique']}/{ver['expected']}, потерь {ver['missing_count']}, "
        f"дубликатов {ver['duplicates_extra']}, пик {an['peak_data_msgs_per_s']} сообщ./с, восстановление {res['recovery_s']} с")
    return res


def S_LIMIT() -> float:
    return float(gw_status()["settings"]["backfill_rate"])


# ------------------------------------------------------------------ опыты
def e1(S, sampler):
    log("E1: deadband офлайн на детерминированной трассе")
    return sweep.run(S["sweep_duration_s"])


def e2(S, sampler):
    log("E2: трафик вживую для профилей deadband")
    work = STATE["work_db"]
    profiles = [("без фильтра", {"enabled": False}),
                ("только изменения", {"enabled": True, "max_silence_s": work["max_silence_s"], "default": {"abs": 0.0}, "tags": {}}),
                ("рабочий профиль", work),
                ("процентный 1 %", {"enabled": True, "max_silence_s": work["max_silence_s"], "default": {"pct": 1.0}, "tags": {}})]
    rows = []

    def snap():
        s, l, c = gw_status(), link_stats(), httpc.get(URL["col"] + "/stats", 5)
        return {"t": time.time(), "cpu": s["process"]["cpu_s"], "rss": s["process"]["rss_kb"], "up": l["bytes_up"],
                "down": l["bytes_down"], "recs": c["records"], "msgs": c["messages"], "in": s["records"]["in"],
                "filtered": s["records"]["filtered"]}

    for label, dbc in profiles:
        gw_patch({"deadband": {"_replace": True, **dbc}})
        time.sleep(S["live_warm_s"])
        a = snap()
        time.sleep(S["live_s"])
        b = snap()
        dt = b["t"] - a["t"]
        r = {"label": label, "seconds": round(dt, 1), "in_per_s": round((b["in"] - a["in"]) / dt, 2),
             "sent_recs_per_s": round((b["recs"] - a["recs"]) / dt, 2), "msgs_per_s": round((b["msgs"] - a["msgs"]) / dt, 2),
             "wire_up_Bps": round((b["up"] - a["up"]) / dt, 1), "wire_down_Bps": round((b["down"] - a["down"]) / dt, 1),
             "cpu_pct": round(100.0 * (b["cpu"] - a["cpu"]) / dt, 2), "rss_kb": b["rss"],
             "wire_B_per_rec": round((b["up"] - a["up"]) / max(1, b["recs"] - a["recs"]), 1)}
        rows.append(r)
        log(f"  {label}: {r['sent_recs_per_s']} зап./с, {r['msgs_per_s']} сообщ./с, {r['wire_up_Bps']} Б/с вверх")
    base = rows[0]["wire_up_Bps"]
    for r in rows:
        r["wire_saved_pct"] = round(100.0 * (1 - r["wire_up_Bps"] / base), 1)
    reset_defaults()
    return {"rows": rows}


def e3(S, sampler):
    log(f"E3: {S['cycles']} цикла обрыва по {S['outage_s']} с, без deadband (худший случай трафика)")
    gw_patch({"deadband": {"enabled": False}})
    wait_for(settled, 120, what="штатное состояние")
    st = gw_status()
    lo = st["buffer"]["next_seq"]
    t0 = time.time()
    cycles = []
    for i in range(S["cycles"]):
        o = S["outage_s"]
        ev = [(0.3 * o, "modbus", "pump1_overheat"), (0.6 * o, "modbus", "tank_low")]
        cycles.append(run_cycle(S, sampler, f"цикл {i + 1}", o, S["pre_s"], S["post_s"], "blackhole", ev))
    hi = gw_status()["buffer"]["next_seq"] - 1
    total = wait_verify(lo, hi)
    cpu = [r for r in sampler.window(t0, time.time()) if "cpu" in r]
    out = {"cycles": cycles, "total": total, "seq_lo": lo, "seq_hi": hi,
           "gateway_cpu_pct": round(100.0 * (cpu[-1]["cpu"] - cpu[0]["cpu"]) / (cpu[-1]["t"] - cpu[0]["t"]), 2) if len(cpu) > 2 else None,
           "gateway_rss_kb_max": max((r["rss"] for r in cpu), default=None)}
    log(f"E3 итог: seq {lo}..{hi}, получено {total['received_unique']}/{total['expected']}, потерь {total['missing_count']}")
    reset_defaults()
    return out


def e3b(S, sampler):
    log("E3b: рабочий deadband, режим reject (разрыв канала как при docker network disconnect)")
    gw_patch({"deadband": {"_replace": True, **STATE["work_db"]}})
    o = S["e3b_outage_s"]
    c = run_cycle(S, sampler, "reject", o, S["pre_s"], S["post_s"], "reject", [(0.4 * o, "modbus", "boiler_flame_out")])
    reset_defaults()
    return {"cycle": c}


def e4(S, sampler):
    L = S["limiter"]
    log(f"E4: лимит backfill, обрыв по {L['outage_s']} с, без deadband")
    gw_patch({"deadband": {"enabled": False}})
    runs = []
    for rate, batch in L["settings"]:
        gw_patch({"backfill": {"rate": rate, "batch": batch}})
        label = f"{rate or 'без лимита'} сообщ./с × {batch}"
        c = run_cycle(S, sampler, label, L["outage_s"], 10, 10, "blackhole", [], max_resync_s=600)
        c["rate"], c["batch"] = rate, batch
        c.pop("timeline", None)
        runs.append(c)
    reset_defaults()
    return {"runs": runs}


def e4b(S, sampler):
    L = S["limiter"]
    log(f"E4b: backfill с лимитом {CFG['limit_msgs_per_s']} сообщ./с при «застрявшем» канале (stall), обрыв {L['outage_s']} с")
    gw_patch({"deadband": {"enabled": False}, "backfill": {"rate": CFG["limit_msgs_per_s"], "batch": 1}})
    c = run_cycle(S, sampler, "stall", L["outage_s"], 10, 10, "stall", [], max_resync_s=600)
    c["rate"], c["batch"] = CFG["limit_msgs_per_s"], 1
    c.pop("timeline", None)
    reset_defaults()
    return {"cycle": c}


def e5(S, sampler):
    O = S["overflow"]
    log(f"E5: переполнение буфера (ёмкость {O['capacity']}), обрыв {O['outage_s']} с, без deadband")
    gw_patch({"deadband": {"enabled": False}, "buffer": {"capacity": O["capacity"]}})
    wait_for(settled, 120, what="штатное состояние")
    ev = [(5, "modbus", "pump1_overheat"), (O["outage_s"] - 70, "modbus", "tank_low")]
    t_a = time.time()
    c = run_cycle(S, sampler, "переполнение", O["outage_s"], 10, 15, "blackhole", ev, max_resync_s=900)
    dropped = httpc.get(URL["gw"] + "/dropped", 30)
    missing = set(c["verify"]["missing"]) if c["verify"]["missing_count"] <= 500 else None
    drop_set = set(dropped["seqs"])
    alarms = httpc.get(URL["col"] + f"/alarms?since={int(t_a * 1000)}", 30)["alarms"]
    ids = sorted({(a[3], a[4]) for a in alarms})
    full_missing = None
    if c["verify"]["missing_count"] > 500:   # collector вернул лишь первые 500 - проверим по всему диапазону через /verify по частям
        full_missing = []
        step = 400   # меньше лимита 500 пропусков в ответе /verify
        for a in range(c["seq_lo"], c["seq_hi"] + 1, step):
            full_missing += col_verify(a, min(a + step - 1, c["seq_hi"]))["missing"]
        missing = set(full_missing)
    out = {"cycle": c, "capacity": O["capacity"], "dropped_count": dropped["count"],
           "missing_equals_dropped": (missing == drop_set) if missing is not None else None,
           "alarms_delivered": ids, "alarm_records": len(alarms)}
    log(f"E5: вытеснено {dropped['count']}, пропущено у получателя {c['verify']['missing_count']}, "
        f"совпадение множеств: {out['missing_equals_dropped']}, тревоги: {ids}")
    reset_defaults()
    return out


def e6(S, sampler):
    A = S["alarms"]
    log("E6: задержка тревог, быстрый путь и путь через батчинг")
    gw_patch({"deadband": {"_replace": True, **STATE["work_db"]}})
    wait_for(settled, 120, what="штатное состояние")
    result = {}
    sims = [("modbus", "pump1_overheat"), ("modbus", "pump2_vibration"), ("modbus", "tank_low"), ("modbus", "boiler_flame_out")]
    for mode in A["modes"]:
        gw_patch({"alarms": {"fast_path": mode == "fast"}, "reset_rules": True})
        time.sleep(3)
        t0 = time.time()
        for r in range(A["rounds"]):
            for sim, name in sims:
                fire(sim, name)
            time.sleep(A["round_s"])
        time.sleep(3)
        dump = col_dump(t0 * 1000)
        alarms = httpc.get(URL["col"] + f"/alarms?since={int(t0 * 1000)}", 30)["alarms"]
        a_seq = {a[1] for a in alarms}
        a_lag = [a[0] - a[2] for a in alarms]
        n_lag = [r[0] - r[2] for r in dump["recs"] if r[1] not in a_seq]
        raise_lag = [a[0] - a[2] for a in alarms if a[4] == "raise"]
        result[mode] = {"alarm": lat_stats(a_lag), "alarm_raise": lat_stats(raise_lag), "normal": lat_stats(n_lag),
                        "alarm_records": len(alarms), "ids": sorted({a[3] for a in alarms})}
        log(f"  {mode}: тревоги {result[mode]['alarm']}, обычные {result[mode]['normal']}")
    gw_patch({"alarms": {"fast_path": True}})
    reset_defaults()
    return result


def e7(S, sampler):
    C = S["crash"]
    log("E7: аварийный перезапуск шлюза во время обрыва канала")
    gw_patch({"deadband": {"enabled": False}})
    wait_for(settled, 120, what="штатное состояние")
    lo = gw_status()["buffer"]["next_seq"]
    time.sleep(C["pre_s"])
    t_cut = time.time()
    set_link("blackhole")
    time.sleep(C["accumulate_s"])
    ev_alarm = fire("modbus", "boiler_flame_out")
    time.sleep(3)
    before = gw_status()
    d_before, up_before = before["buffer"]["depth"], before["uptime_s"]
    log(f"  в буфере {d_before} записей, завершаю шлюз аварийно (os._exit, как kill -9)")
    t_crash = time.time()
    try:
        httpc.post(URL["gw"] + "/chaos/crash")
    except Exception:  # noqa: BLE001
        pass
    time.sleep(1.0)

    def restarted():
        s = gw_status()
        return s if s["uptime_s"] < up_before else None

    after = wait_for(restarted, 90, poll=0.3, what="перезапуск шлюза")
    t_up = time.time()
    recovered = after["buffer"]["recovered_at_start"]
    log(f"  шлюз перезапущен через {t_up - t_crash:.1f} с, восстановлено из SQLite {recovered} записей (было {d_before})")
    time.sleep(C["pre_s"])
    t_restore = time.time()
    set_link("up")
    wait_for(lambda: gw_status()["state"] == "online", 90, what="шлюз онлайн")
    wait_for(settled, 300, what="буфер пуст")
    time.sleep(C["post_s"])
    hi = gw_status()["buffer"]["next_seq"] - 1
    ver = wait_verify(lo, hi)
    alarms = httpc.get(URL["col"] + f"/alarms?since={int(t_cut * 1000)}", 30)["alarms"]
    out = {"depth_before_crash": d_before, "recovered_at_start": recovered, "restart_s": round(t_up - t_crash, 1),
           "seq_lo": lo, "seq_hi": hi, "verify": ver, "alarm_ids": sorted({(a[3], a[4]) for a in alarms}),
           "accumulate_s": C["accumulate_s"], "t_cut": t_cut, "t_crash": t_crash, "t_restore": t_restore}
    log(f"E7: seq {lo}..{hi}: получено {ver['received_unique']}/{ver['expected']}, потерь {ver['missing_count']}, дубликатов {ver['duplicates_extra']}")
    reset_defaults()
    return out


def e8(S, sampler):
    from gw.buffer import Outbox
    log("E8: микротест SQLite (запись пачками по 20 записей и подтверждение пачками по 20)")
    rows = []
    body = '{"ts":1730000000123,"asset":"pump-01","tag":"temperature","val":71.3,"unit":"degC","q":"good","src":"modbus"}'
    for mode in ("OFF", "NORMAL", "FULL"):
        d = tempfile.mkdtemp()
        ob = Outbox(os.path.join(d, "o.db"), capacity=10 ** 9, synchronous=mode)
        n_batches, per = 500, 20
        t0 = time.perf_counter()
        for _ in range(n_batches):
            ob.put_many([(1730000000123, 0, body)] * per)
        t_put = time.perf_counter() - t0
        t0 = time.perf_counter()
        done = 0
        while True:
            rows_ = ob.claim(0, per)
            if not rows_:
                break
            ob.ack([r[0] for r in rows_])
            done += 1
        t_ack = time.perf_counter() - t0
        ob.close()
        rows.append({"synchronous": mode, "put_batches": n_batches, "records": n_batches * per,
                     "put_ms_per_batch": round(1000 * t_put / n_batches, 3), "put_records_per_s": round(n_batches * per / t_put),
                     "ack_ms_per_batch": round(1000 * t_ack / max(1, done), 3), "ack_records_per_s": round(n_batches * per / t_ack)})
        log(f"  synchronous={mode}: вставка {rows[-1]['put_ms_per_batch']} мс/пачка ({rows[-1]['put_records_per_s']} зап./с)")
    return {"rows": rows}


EXPS = {"e1": e1, "e2": e2, "e3": e3, "e3b": e3b, "e4": e4, "e4b": e4b, "e5": e5, "e6": e6, "e7": e7, "e8": e8}
OFFLINE = {"e1", "e8"}


def write_csv(out: str, results: dict) -> None:
    def w(name, header, rows):
        with open(os.path.join(out, name), "w", newline="", encoding="utf-8-sig") as f:
            cw = csv.writer(f, delimiter=";")
            cw.writerow(header)
            cw.writerows(rows)
    if "e1" in results:
        w("deadband.csv", ["профиль", "записей/час", "экономия записей, %", "экономия байт, %", "записей/с", "макс. ошибка, % показания", "средняя ошибка, %"],
          [[r["label"], r["out"], r["reduction_pct"], r["bytes_saved_pct"], r["records_per_s"], r["err_max_pct"], r["err_mean_pct"]] for r in results["e1"]["rows"]])
    cyc = []
    for key in ("e3",):
        if key in results:
            cyc += results[key]["cycles"]
    if "e3b" in results:
        cyc.append(results["e3b"]["cycle"])
    if cyc:
        w("cycles.csv", ["цикл", "режим", "записей накоплено", "пик заполнения, %", "обнаружение, с", "переподключение, с", "восстановление, с",
                         "пик сообщ./с", "пик брокера сообщ./с", "потери", "дубликаты"],
          [[c["label"], c["mode"], c["backlog_at_restore"], c["peak_fill_pct"], c["detect_s"], c["reconnect_s"], c["recovery_s"],
            c["peak_data_msgs_per_s"], c["broker_peak_msgs_per_s"], c["verify"]["missing_count"], c["verify"]["duplicates_extra"]] for c in cyc])
    if "e4" in results:
        w("limiter.csv", ["лимит, сообщ./с", "записей в сообщении", "накоплено", "восстановление, с", "пик сообщ./с", "пик брокера", "потери"],
          [[r["rate"], r["batch"], r["backlog_at_restore"], r["recovery_s"], r["peak_data_msgs_per_s"], r["broker_peak_msgs_per_s"],
            r["verify"]["missing_count"]] for r in results["e4"]["runs"]])


def main() -> int:
    global CFG
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", choices=["smoke", "quick", "full"], default="smoke")
    ap.add_argument("--only", default="", help="через запятую: e1,e2,e3,e3b,e4,e4b,e5,e6,e7,e8")
    ap.add_argument("--out", default=os.path.join(ROOT, "results"))
    args = ap.parse_args()
    CFG = json.load(open(os.path.join(ROOT, "experiments.json"), encoding="utf-8"))
    S = CFG["scales"][args.scale]
    names = [n for n in EXPS if not args.only or n in args.only.split(",")]
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, "results.json")
    results = json.load(open(path, encoding="utf-8")) if os.path.exists(path) and args.only else {}
    t_begin = time.time()

    sampler = Sampler()
    if set(names) - OFFLINE:
        log("жду готовности стенда ...")
        for k, u in URL.items():
            httpc.wait_ready(u + "/health", 120, k)
        gwcfg = httpc.get(URL["gw"] + "/config", 5)
        # рабочий профиль берём из файла конфигурации, а не с шлюза: так он не зависит от прошлых прогонов
        STATE["work_db"] = config.load(sweep.CONFIG_PATH).to_dict()["deadband"]
        wait_for(settled, 180, what="шлюз онлайн")
        sampler.start()
        meta_cfg = {k: gwcfg[k] for k in ("buffer", "uplink", "publish", "backfill", "alarms", "deadband")}
        try:
            import aiomqtt, pymodbus
            vers = {"aiomqtt": getattr(aiomqtt, "__version__", "?"), "pymodbus": pymodbus.__version__}
        except Exception:  # noqa: BLE001
            vers = {}
        results["meta"] = {"scale": args.scale, "started": time.strftime("%Y-%m-%d %H:%M:%S"), "python": platform.python_version(),
                           "platform": platform.platform(), "cpu_count": os.cpu_count(), "docker": os.environ.get("IN_DOCKER") == "1",
                           "limit_msgs_per_s": CFG["limit_msgs_per_s"], "config": meta_cfg, "versions": vers, "scales": S}
    for n in names:
        t0 = time.time()
        try:
            results[n] = EXPS[n](S, sampler)
        except Exception as e:  # noqa: BLE001
            log(f"ОШИБКА в {n}: {type(e).__name__}: {e}")
            results[n] = {"error": f"{type(e).__name__}: {e}"}
            try:
                reset_defaults()
            except Exception:  # noqa: BLE001
                pass
        results[n + "_seconds"] = round(time.time() - t0, 1)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False)
    sampler.stop()
    if "meta" in results:
        results["meta"]["elapsed_s"] = round(time.time() - t_begin, 1)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False)
    write_csv(args.out, results)
    errs = [n for n in names if "error" in results.get(n, {})]
    log(f"Готово за {time.time() - t_begin:.0f} с. Результаты: {path}" + (f"; с ошибками: {errs}" if errs else ""))
    return 1 if errs else 0


CFG = {}

if __name__ == "__main__":
    sys.exit(main())
