"""Автоматические проверки Lab 7.

  docker compose run --rm tests            # модульные + интеграционные (стенд должен быть запущен)
  docker compose run --rm tests --unit     # только модульные (стенд не нужен), ~10 секунд

Модульные: маппинг регистров и типов, deadband, правила тревог, буфер SQLite (WAL, ёмкость, вытеснение,
подтверждение, kill -9), ограничитель скорости, формат сообщений и JSON-схема, детерминизм симулятора.
Интеграционные (~1,5 минуты): сквозная доставка Modbus+BLE+MQTT, обрыв WAN -> накопление без удаления
из буфера -> backfill с лимитом -> потерь 0 по seq, приоритет и скорость доставки тревог.
"""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
import traceback

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from common import httpc  # noqa: E402
from gw import config  # noqa: E402
from gw.buffer import Outbox  # noqa: E402
from gw.deadband import Deadband  # noqa: E402
from gw.limiter import RateLimiter  # noqa: E402
from gw.mapping import RegTag, build_reg_tags, convert, decimals_for, decode_gatt, decode_registers  # noqa: E402
from gw.model import Sample, envelope, with_seq  # noqa: E402
from gw.rules import RuleEngine  # noqa: E402
from sim import ble_sim  # noqa: E402
from sim.signals import LAYOUT, N_REGS, Plant  # noqa: E402

N = {"checks": 0}
CFG_PATH = os.path.join(ROOT, "config", "gateway.yaml")


def ok(cond, msg):
    N["checks"] += 1
    if not cond:
        raise AssertionError(msg)
    print(f"  ок  {msg}")


def approx(a, b, tol):
    return abs(a - b) <= tol


# ============================================================ модульные
def test_conversion():
    ok(decimals_for(0.1) == 1 and decimals_for(0.01) == 2 and decimals_for(1) == 0 and decimals_for(0.001) == 3,
       "число знаков после запятой выбирается по масштабу")
    ok(convert(715, 0.1) == 71.5, "масштаб: 715 * 0.1 = 71.5 (без хвоста 71.50000000000001)")
    ok(convert(7, 1) == 7 and isinstance(convert(7, 1), int), "масштаб 1 сохраняет целое")
    ok(convert(-123, 0.1, 5.0) == -7.3, "смещение: -123 * 0.1 + 5 = -7.3")
    ok(convert(True, 0.1) is True, "логические значения проходят без масштаба")


def test_register_types():
    def tag(**kw):
        return RegTag({"asset": "a", "tag": "t", **kw})
    ok(tag(reg=0, type="int16").raw([0xFFFF]) == -1 and tag(reg=0, type="int16").raw([0x8000]) == -32768, "int16: знаковое значение")
    ok(tag(reg=0, type="uint16").raw([0xFFFF]) == 65535, "uint16: без знака")
    ok(tag(reg=0, type="uint32").raw([0x0001, 0x0002]) == 0x00010002, "uint32: порядок слов big (старшее первым)")
    ok(tag(reg=0, type="uint32", word_order="little").raw([0x0001, 0x0002]) == 0x00020001, "uint32: порядок слов little")
    ok(tag(reg=0, type="int32").raw([0xFFFF, 0xFFFE]) == -2, "int32: знаковое значение")
    ok(approx(tag(reg=0, type="float32").raw([0x41C8, 0x0000]), 25.0, 1e-6), "float32: 0x41C80000 = 25.0")
    ok(tag(reg=0, type="bits", bit=1).raw([0b10]) is True and tag(reg=0, type="bits", bit=0).raw([0b10]) is False, "bits: выбор бита")
    try:
        tag(reg=0, type="int64")
        ok(False, "неизвестный тип должен отвергаться")
    except ValueError:
        ok(True, "неизвестный тип регистра отвергается при загрузке конфигурации")


def test_mapping_against_plant():
    cfg = config.load(CFG_PATH)
    tags = build_reg_tags(cfg.sources.modbus.tags, cfg.sources.modbus.get("word_order", "big"))
    ok(cfg.sources.modbus.count == N_REGS == 20, "устройство Modbus имеет 20 регистров, шлюз читает все 20 одним запросом")
    used = sorted(r for t in tags for r in range(t.reg, t.reg + t.words))
    ok(sorted(set(used)) == list(range(N_REGS)) and used.count(17) == 2, "маппинг покрывает регистры 0..19 (регистр 17 - два бита)")
    plant = Plant()
    plant.start_event("boiler_flame_out", 90.0)
    for t in (0.0, 50.0, 100.5, 777.0, 1800.0):
        regs = plant.modbus_registers(t)
        phys = plant.modbus_physical(t)
        samples = {s.key: s for s in decode_registers(regs, tags, int(t * 1000))}
        worst = 0.0
        for addr, kind, asset, tg, scale in LAYOUT:
            if kind == "bits":
                continue
            v = samples[f"{asset}.{tg}"].value
            worst = max(worst, abs(v - phys[(asset, tg)]) / max(scale, 1e-9))
        ok(worst <= 0.51, f"t={t:g} c: значения после маппинга совпадают с физикой датчика, отклонение {worst:.2f} шага квантования")
    s = {x.key: x for x in decode_registers(plant.modbus_registers(100.0), tags, 0)}
    ok(s["boiler-01.flame"].value is False and s["boiler-01.fault"].value is True, "статусные биты: пламя погасло -> flame=False, fault=True")
    ok(s["pump-01.temperature"].unit == "degC" and s["tank-01.volume"].unit == "m3", "единицы измерения берутся из маппинга")
    ok(all(x.src == "modbus" for x in s.values()), "источник записи = modbus")


def test_gatt_codec():
    for uuid, phys, tol in (("2A6E", 21.34, 0.006), ("2A6E", -5.5, 0.006), ("2A6F", 47.89, 0.006), ("2A19", 87, 0.5)):
        fmt = {"2A6E": "sint16", "2A6F": "uint16", "2A19": "uint8"}[uuid]
        scale = {"2A6E": 0.01, "2A6F": 0.01, "2A19": 1.0}[uuid]
        raw = decode_gatt(fmt, ble_sim.encode(uuid, phys))
        ok(approx(raw * scale, phys, tol), f"GATT {uuid}: {phys} -> hex {ble_sim.encode(uuid, phys)} -> {raw * scale:g}")
    ok(ble_sim.encode("2A6E", 21.34) == "5608", "GATT: 21.34 °C = 2134 = 0x0856, little-endian '5608'")
    try:
        decode_gatt("uint16", "01")
        ok(False, "короткое значение должно отвергаться")
    except ValueError:
        ok(True, "слишком короткое значение характеристики отвергается")


def _s(v, ts=0, asset="a", tag="t"):
    return Sample(asset, tag, v, "u", "modbus", ts)


def test_deadband():
    d = Deadband(config.Cfg({"enabled": True, "max_silence_s": 60, "default": {"abs": 0.5}, "tags": {}}))
    ok(d.check(_s(10.0, 0)), "первое значение тега проходит всегда")
    ok(not d.check(_s(10.3, 1000)) and not d.check(_s(10.49, 2000)), "изменение меньше порога подавляется")
    ok(d.check(_s(10.5, 3000)), "изменение на порог проходит (>=)")
    ok(not d.check(_s(10.9, 4000)) and d.check(_s(11.0, 5000)), "опорное значение - последнее ОТПРАВЛЕННОЕ (10.5), поэтому 11.0 проходит")
    d = Deadband(config.Cfg({"enabled": True, "max_silence_s": 60, "default": {"abs": 0.5}, "tags": {}}))
    drift = [10.0 + 0.1 * i for i in range(21)]
    sent = [d.check(_s(v, i * 1000)) for i, v in enumerate(drift)]
    ok(sum(sent) == 5 and sent[0] and sent[5] and sent[10], "медленный дрейф шагами 0.1 при пороге 0.5 всё же отправляется (5 из 21)")
    d = Deadband(config.Cfg({"enabled": True, "max_silence_s": 60, "default": {"pct": 2.0}, "tags": {}}))
    ok(d.check(_s(200.0)) and not d.check(_s(203.9, 1000)) and d.check(_s(204.0, 2000)), "процентный порог 2 %: 200 -> 203.9 подавлено, 204.0 проходит")
    d = Deadband(config.Cfg({"enabled": True, "max_silence_s": 60, "default": {"abs": 1.0}, "tags": {}}))
    ok(d.check(_s(True)) and not d.check(_s(True, 1000)) and d.check(_s(False, 2000)), "логическое значение: проходит любое изменение")
    d = Deadband(config.Cfg({"enabled": True, "max_silence_s": 30, "default": {"abs": 1.0}, "tags": {}}))
    d.check(_s(5.0, 0))
    ok(not d.check(_s(5.0, 29000)) and d.check(_s(5.0, 30000)), "heartbeat: через max_silence_s проходит даже неизменное значение")
    d = Deadband(config.Cfg({"enabled": False}))
    ok(all(d.check(_s(1.0, i)) for i in range(5)), "при enabled=false проходит всё")
    d = Deadband(config.Cfg({"enabled": True, "default": {"abs": 0.0}, "tags": {"pump-*.temperature": {"abs": 5.0}, "pump-02.temperature": {"abs": 0.1}}}))
    ok(d.rule("pump-01.temperature")["abs"] == 5.0 and d.rule("pump-07.temperature")["abs"] == 5.0, "правило по шаблону fnmatch")
    ok(d.rule("pump-02.temperature")["abs"] == 0.1, "порядок поиска: точное имя важнее шаблона")
    ok(d.rule("tank-01.level")["abs"] == 0.0, "для остальных тегов действует правило default")


def test_rules():
    eng = RuleEngine([{"id": "hot", "asset": "a", "tag": "t", "op": ">", "threshold": 85.0, "hysteresis": 3.0, "severity": "critical"},
                      {"id": "flame", "asset": "b", "tag": "f", "op": "==", "threshold": False}])
    out = []
    for i, v in enumerate((80, 84.9, 85.1, 90, 86, 83, 82.5, 82.0, 70)):
        out += [(a.alarm["id"], a.alarm["state"], v) for a in eng.evaluate(_s(v, i * 1000))]
    ok(out == [("hot", "raise", 85.1), ("hot", "clear", 82.0)], "порог 85, гистерезис 3: raise на 85.1; 83 и 82.5 не снимают тревогу, clear только при <= 82")
    ok(eng.raised == 1 and eng.cleared == 1, "счётчики raise/clear")
    got = [a.alarm["state"] for v in (True, False, False, True) for a in eng.evaluate(Sample("b", "f", v, "", "modbus", 0))]
    ok(got == ["raise", "clear"], "дискретное правило «пламя == False»: raise при гашении, clear при розжиге, без повторов")
    a = eng.evaluate(_s(100.0, 99))[0]
    ok(a.alarm == {"id": "hot", "state": "raise", "sev": "critical", "limit": 85.0} and a.value == 100.0, "запись-тревога несёт id, состояние, важность и порог")


def new_box(cap=100, sync="NORMAL"):
    d = tempfile.mkdtemp(prefix="lab7-ob-")
    return Outbox(os.path.join(d, "o.db"), cap, sync), d


def test_outbox_basic():
    ob, d = new_box()
    mode = ob.db.execute("PRAGMA journal_mode").fetchone()[0]
    ok(mode.lower() == "wal", f"журнал SQLite в режиме WAL (journal_mode={mode})")
    seqs, ev = ob.put_many([(1000 + i, 0, f'{{"i":{i}}}') for i in range(10)])
    ok(seqs == list(range(1, 11)) and ev == [], "seq присваиваются подряд: 1..10")
    seqs2, _ = ob.put_many([(2000, 1, '{"alarm":1}')])
    ok(seqs2 == [11], "следующий seq = 11 (сквозная нумерация между пачками)")
    ok(ob.stats()["depth"] == 11 and ob.pending(0) == 10 and ob.pending(1) == 1, "глубина буфера 11: 10 обычных + 1 тревога")
    first = ob.claim(1, 5)
    ok([r[0] for r in first] == [11], "claim по приоритету: тревога берётся первой")
    rows = ob.claim(0, 4)
    ok([r[0] for r in rows] == [1, 2, 3, 4], "claim отдаёт самые старые записи первыми")
    ok(ob.stats()["depth"] == 11 and ob.stats()["inflight"] == 5, "записи «в полёте» остаются в глубине буфера (ещё не подтверждены)")
    n_disk = ob.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    ok(n_disk == 11, "до подтверждения на диске по-прежнему все 11 записей")
    ob.ack([r[0] for r in rows])
    n_disk = ob.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    ok(n_disk == 7 and ob.stats()["depth"] == 7, "после ack (аналог PUBACK) запись удалена с диска: 11 - 4 = 7")
    ob.release([11])
    ok(ob.pending(1) == 1 and ob.stats()["inflight"] == 0, "release возвращает неподтверждённую запись в очередь, ничего не теряя")
    ok(ob.head_ts(0) == 1004, "head_ts: время самой старой ожидающей записи")
    got = ob.claim(0, 3)
    ok(ob.stats()["inflight"] == 3, "три записи забраны в полёт")
    ok(ob.release_inflight() == 3 and ob.stats()["inflight"] == 0 and ob.pending(0) == 6,
       "release_inflight при закрытии соединения возвращает ВСЕ записи в полёте (защита от отменённых задач отправки)")
    again = ob.claim(0, 6)
    ok([r[0] for r in again] == sorted(r[0] for r in again) and again[0][0] == 5, "после возврата порядок отправки сохраняется (первой снова seq=5)")
    ob.close()


def test_outbox_overflow():
    ob, _ = new_box(cap=10)
    ob.put_many([(i, 0, "{}") for i in range(10)])
    _, ev = ob.put_many([(100 + i, 1, "{}") for i in range(3)])
    ok(ev == [1, 2, 3], "переполнение: вытесняются самые старые ОБЫЧНЫЕ записи (seq 1..3)")
    ok(ob.stats()["depth"] == 10 and ob.dropped_seqs() == [1, 2, 3] and ob.stats()["dropped_total"] == 3, "потеря явная: seq записаны в таблицу dropped, счётчик = 3")
    ok(ob.pending(1) == 3, "тревоги сохранены")
    ob2, _ = new_box(cap=5)
    ob2.put_many([(i, 1, "{}") for i in range(5)])
    _, ev = ob2.put_many([(10 + i, 0, "{}") for i in range(3)])
    ok(ev == [6, 7, 8] and ob2.pending(1) == 5, "поток обычных данных не вытесняет тревоги: отбрасываются новые обычные")
    ob3, _ = new_box(cap=5)
    ob3.put_many([(i, 1, "{}") for i in range(5)])
    _, ev = ob3.put_many([(20 + i, 1, "{}") for i in range(2)])
    ok(ev == [1, 2], "если в буфере одни тревоги, вытесняются самые старые тревоги")


def test_outbox_persistence():
    code = (
        "import os, sys\n"
        f"sys.path.insert(0, {ROOT!r})\n"
        "from gw.buffer import Outbox\n"
        "ob = Outbox(sys.argv[1], 1000, 'NORMAL')\n"
        "ob.put_many([(1000 + i, 0, '{\"x\":%d}' % i) for i in range(50)])\n"
        "rows = ob.claim(0, 20); ob.ack([r[0] for r in rows])\n"
        "ob.claim(0, 5)\n"
        "os._exit(137)\n")
    d = tempfile.mkdtemp(prefix="lab7-kill-")
    path = os.path.join(d, "o.db")
    p = subprocess.run([sys.executable, "-c", code, path], timeout=60)
    ok(p.returncode == 137, "процесс записи убит без закрытия БД (os._exit(137), как kill -9)")
    ob = Outbox(path, 1000)
    ok(ob.recovered == 30, f"после «аварии» восстановлено {ob.recovered} записей: 50 - 20 подтверждённых (5 были в полёте и остались)")
    ok(ob.stats()["next_seq"] == 51, "нумерация продолжается с 51 (seq не переиспользуются)")
    rows = ob.claim(0, 3)
    ok([r[0] for r in rows] == [21, 22, 23], "порядок сохранён: первой уйдёт запись seq=21")
    ob.close()
    ob = Outbox(path, 1000)
    ok(ob.recovered == 30 and ob.stats()["next_seq"] == 51, "повторное открытие после штатного закрытия даёт то же состояние")
    ob.close()


def test_limiter():
    async def run(rate, n):
        lim, t0, ts = RateLimiter(), time.monotonic(), []
        for _ in range(n):
            await lim.acquire(rate)
            ts.append(time.monotonic())
        return ts, ts[-1] - t0
    ts, dt = asyncio.run(run(100, 260))
    best = j = 0
    for i in range(len(ts)):
        while ts[i] - ts[j] >= 1.0:
            j += 1
        best = max(best, i - j + 1)
    ok(best <= 100, f"лимит 100/с: в любом скользящем окне 1 с отправок не больше 100 (максимум {best})")
    ok(dt >= 2.4, f"260 отправок при лимите 100/с заняли {dt:.2f} с (не меньше 2.4 с, т.е. не быстрее лимита)")
    b10 = j = 0
    for i in range(len(ts)):
        while ts[i] - ts[j] >= 0.1:
            j += 1
        b10 = max(b10, i - j + 1)
    ok(b10 <= 11, f"темп равномерный: в любом окне 100 мс не больше {b10} отправок (при пачками было бы до 100)")
    ts, dt = asyncio.run(run(0, 2000))
    ok(dt < 0.5, f"rate=0: без ограничения (2000 вызовов за {dt * 1000:.0f} мс)")


def test_model_and_schema():
    import jsonschema
    schema = json.load(open(os.path.join(ROOT, "schemas", "canonical.schema.json"), encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    v = jsonschema.Draft202012Validator(schema)
    s1 = Sample("pump-01", "temperature", 71.3, "degC", "modbus", 1730000000123)
    s2 = Sample("boiler-01", "flame", False, "", "modbus", 1730000000124, "good",
                {"id": "boiler01_flame_out", "state": "raise", "sev": "critical", "limit": False})
    rows = [(7, s1.ts, s1.body()), (8, s2.ts, s2.body())]
    msg = json.loads(envelope("gw-01", rows))
    ok(msg["v"] == 1 and msg["n"] == 2 and msg["recs"][0]["seq"] == 7 and msg["recs"][1]["alarm"]["state"] == "raise", "конверт: версия, число записей, seq, блок alarm")
    ok(not list(v.iter_errors(msg)), "сообщение валидно по JSON-схеме canonical.schema.json")
    bad = json.loads(envelope("gw-01", rows))
    del bad["recs"][0]["seq"]
    ok(list(v.iter_errors(bad)), "запись без seq схемой отвергается")
    bad = json.loads(envelope("gw-01", rows))
    bad["recs"][0]["q"] = "maybe"
    ok(list(v.iter_errors(bad)), "недопустимое качество q отвергается")
    ok(with_seq(5, s1.body()).startswith('{"seq":5,"ts":1730000000123'), "seq вставляется в начало JSON без повторной сериализации")


def test_signals_deterministic():
    a, b = Plant(), Plant()
    ok(all(a.modbus_registers(t) == b.modbus_registers(t) for t in (0.0, 13.5, 999.0)), "симулятор детерминирован: одинаковое время -> одинаковые регистры")
    ok(len(a.modbus_registers(5.0)) == 20, "20 регистров")
    base = a.modbus_physical(100.0)[("pump-01", "temperature")]
    a.start_event("pump1_overheat", 100.0)
    hot = a.modbus_physical(112.0)[("pump-01", "temperature")]
    ok(base < 70 and hot > 90, f"событие pump1_overheat поднимает температуру {base:.1f} -> {hot:.1f} °C (выше порога аварии 85)")


def test_config():
    os.environ.pop("LAB7_X", None)
    ok(config.parse("a: ${LAB7_X:-5}\nb: {c: ${LAB7_X:-x}}")["a"] == 5, "подстановка ${VAR:-по_умолчанию}")
    os.environ["LAB7_X"] = "7"
    ok(config.parse("a: ${LAB7_X:-5}")["a"] == 7, "переменная окружения перекрывает значение по умолчанию")
    c = config.Cfg({"x": {"a": 1, "b": {"c": 2}}, "t": {"k": 1}})
    c.merge({"x": {"b": {"c": 3}}, "t": {"_replace": True, "z": 9}})
    ok(c.x.a == 1 and c.x.b.c == 3 and c.t == {"z": 9}, "слияние патча: вложенные ключи обновляются, _replace заменяет раздел")
    cfg = config.load(CFG_PATH)
    ok(cfg.buffer.capacity == 10000 and cfg.backfill.rate == 200 and cfg.buffer.synchronous == "NORMAL", "конфигурация по умолчанию: буфер 10 000, backfill 200 сообщ./с")


def test_sweep():
    from lab import sweep
    r = sweep.run(120)
    rows = {x["label"]: x for x in r["rows"]}
    base = rows["без фильтра"]
    work = rows["абсолютный ×1 (рабочий)"]
    ok(base["reduction_pct"] == 0.0 and base["out"] == r["samples"], "без фильтра отправляется всё")
    ok(work["reduction_pct"] > 70, f"рабочий профиль deadband сокращает число записей на {work['reduction_pct']} % (> 70 %)")
    red = [rows[f"абсолютный ×{m}"]["reduction_pct"] if m != 1 else work["reduction_pct"] for m in ("0.25", "0.5", 1, "2", "4")]
    ok(red == sorted(red), f"экономия растёт с порогом: {red}")


UNIT = [test_conversion, test_register_types, test_mapping_against_plant, test_gatt_codec, test_deadband, test_rules,
        test_outbox_basic, test_outbox_overflow, test_outbox_persistence, test_limiter, test_model_and_schema,
        test_signals_deterministic, test_config, test_sweep]


# ============================================================ интеграционные
URL = {"gw": os.environ.get("GW_URL", "http://gateway:8080"),
       "link": os.environ.get("LINK_URL", "http://wan-link:8081"),
       "col": os.environ.get("COL_URL", "http://collector:8082"),
       "modbus": os.environ.get("MODBUS_URL", "http://modbus-sim:8080"),
       "ble": os.environ.get("BLE_URL", "http://ble-sim:8080"),
       "mqtt": os.environ.get("MQTT_URL", "http://mqtt-dev:8080")}
BROKER = (os.environ.get("BROKER_HOST", "broker-wan"), int(os.environ.get("BROKER_PORT", "8883")), os.environ.get("CA_FILE", "/certs/ca.crt"))


def status():
    return httpc.get(URL["gw"] + "/status", 5)


def wait_for(pred, timeout, what):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = pred()
            if v:
                return v
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.5)
    raise AssertionError(f"не дождались: {what}")


def settled():
    s = status()
    return s["state"] == "online" and s["buffer"]["depth"] <= 40 and s["buffer"]["oldest_age_s"] < 3 and s["mode"] != "backfill"


def work_deadband():
    return config.load(CFG_PATH).to_dict()["deadband"]


def restore_defaults():
    httpc.post(URL["link"] + "/mode?set=up")
    httpc.post(URL["gw"] + "/config", {"deadband": {"_replace": True, **work_deadband()}, "alarms": {"fast_path": True},
                                       "backfill": {"rate": 200, "batch": 1}, "buffer": {"capacity": 10000}, "reset_rules": True})
    wait_for(settled, 180, "шлюз онлайн и буфер пуст")


async def sniff(seconds):
    import ssl
    import aiomqtt
    host, port, ca = BROKER
    ctx = ssl.create_default_context(cafile=ca)
    out = []
    async with aiomqtt.Client(host, port, identifier="tests-sniffer", tls_context=ctx, timeout=10) as c:
        await c.subscribe([("gw/+/data", 1), ("gw/+/alarm", 1)])
        end = time.time() + seconds
        while time.time() < end:
            try:
                m = await asyncio.wait_for(c.messages.__anext__(), timeout=max(0.1, end - time.time()))
            except (asyncio.TimeoutError, StopAsyncIteration):
                break
            out.append((str(m.topic), json.loads(m.payload), time.time() * 1000))
    return out


def it_stack_ready():
    for k, u in URL.items():
        httpc.wait_ready(u + "/health", 90, k)
    s = wait_for(lambda: (lambda x: x if x["state"] == "online" else None)(status()), 90, "шлюз онлайн")
    ok(s["state"] == "online", "все сервисы стенда отвечают, шлюз подключён к брокеру по TLS")
    ok(s["settings"]["capacity"] == 10000, "ёмкость буфера 10 000 записей")
    restore_defaults()


def it_end_to_end():
    import jsonschema
    schema = json.load(open(os.path.join(ROOT, "schemas", "canonical.schema.json"), encoding="utf-8"))
    v = jsonschema.Draft202012Validator(schema)
    msgs = asyncio.run(sniff(14))
    ok(len(msgs) >= 5, f"за 14 с на брокер пришло {len(msgs)} сообщений")
    errs = [e for _t, m, _a in msgs for e in v.iter_errors(m)]
    ok(not errs, "все сообщения соответствуют JSON-схеме канонической модели")
    ok(all(m["n"] == len(m["recs"]) for _t, m, _a in msgs), "поле n равно числу записей")
    recs = [r for _t, m, _a in msgs for r in m["recs"]]
    src = {r["src"] for r in recs}
    ok({"modbus", "ble", "mqtt"} <= src, f"в канонической модели есть все три протокола: {sorted(src)}")
    assets = {r["asset"] for r in recs}
    ok({"pump-01", "pump-02", "tank-01", "boiler-01", "ble-01", "ble-02", "esp32-01", "esp32-02", "esp32-03"} <= assets, f"asset_id источников: {sorted(assets)}")
    seqs = [r["seq"] for r in recs]
    ok(len(set(seqs)) == len(seqs), "seq уникальны в потоке без обрывов")
    t = [r for r in recs if (r["asset"], r["tag"]) == ("pump-01", "temperature")]
    ok(t and all(40 < r["val"] < 120 and r["unit"] == "degC" for r in t), "pump-01.temperature: регистр int16 * 0.1 -> °C в разумных пределах")
    e = [r for r in recs if (r["asset"], r["tag"]) == ("esp32-01", "temperature")]
    ok(e and all(10 < r["val"] < 40 and r["unit"] == "degC" for r in e), "esp32-01.temperature: JSON t_c10 * 0.1 -> °C")
    b = [r for r in recs if (r["asset"], r["tag"]) == ("ble-01", "temperature")]
    ok(b and all(10 < r["val"] < 40 for r in b), "ble-01.temperature: GATT 0x2A6E sint16 * 0.01 -> °C")
    fl = [r for r in recs if (r["asset"], r["tag"]) == ("boiler-01", "flame")]
    ok(all(isinstance(r["val"], bool) for r in fl), "boiler-01.flame - логический тип")


def it_config_patch():
    httpc.post(URL["gw"] + "/config", {"backfill": {"rate": 123}})
    ok(status()["settings"]["backfill_rate"] == 123, "горячее изменение параметров через POST /config")
    httpc.post(URL["gw"] + "/config", {"backfill": {"rate": 200}})
    ok(status()["settings"]["backfill_rate"] == 200, "параметры возвращены")


def peak_window(times, t0, t1, width=1000.0):
    ts = sorted(t for t in times if t0 <= t <= t1)
    best = j = 0
    for i in range(len(ts)):
        while ts[i] - ts[j] >= width:
            j += 1
        best = max(best, i - j + 1)
    return best


def it_outage_cycle():
    RATE, OUT = 50, 12
    httpc.post(URL["gw"] + "/config", {"deadband": {"enabled": False}, "backfill": {"rate": RATE, "batch": 1}})
    wait_for(settled, 120, "штатное состояние")
    lo = status()["buffer"]["next_seq"]
    time.sleep(3)
    s0 = status()
    t_cut = time.time()
    httpc.post(URL["link"] + "/mode?set=blackhole")
    time.sleep(2)
    httpc.post(URL["modbus"] + "/event?name=pump2_vibration")
    time.sleep(7 - 2)
    a1 = status()["buffer"]
    time.sleep(OUT - 7 - 1)
    a2 = status()["buffer"]
    time.sleep(1)
    ok(a2["acked_total"] == a1["acked_total"], f"во время обрыва PUBACK нет - из буфера ничего не удаляется (acked_total {a1['acked_total']} -> {a2['acked_total']})")
    ok(a2["depth"] > a1["depth"] + 80 and a2["depth"] > 150, f"записи накапливаются: глубина буфера {a1['depth']} -> {a2['depth']}")
    ok(a2["pending_alarm"] >= 1, f"тревога, возникшая при обрыве, ждёт в буфере с высоким приоритетом ({a2['pending_alarm']} шт.)")
    t_restore = time.time()
    httpc.post(URL["link"] + "/mode?set=up")
    wait_for(lambda: status()["state"] == "online", 60, "шлюз вернулся онлайн")
    wait_for(settled, 180, "буфер пуст")
    time.sleep(4)
    hi = status()["buffer"]["next_seq"] - 1
    end = time.time() + 40
    while True:
        ver = httpc.get(URL["col"] + f"/verify?gw=gw-01&lo={lo}&hi={hi}", 30)
        if ver["missing_count"] == 0 or time.time() > end:
            break
        time.sleep(1)
    ok(ver["expected"] > 300 and ver["missing_count"] == 0, f"потери = 0: seq {lo}..{hi}, получено {ver['received_unique']} из {ver['expected']}")
    d = httpc.get(URL["col"] + f"/dump?since={int(t_cut * 1000 - 1000)}", 60)
    after = sorted(m for m in d["msgs"] if m[0] >= t_restore * 1000)
    data_t = [m[0] for m in after if m[1] == 0]
    peak = peak_window(data_t, t_restore * 1000, (time.time() + 1) * 1000)
    ok(peak <= RATE, f"пик backfill на стороне брокера {peak} сообщ./с не превышает лимит {RATE}")
    ok(peak >= RATE * 0.8, f"лимитер действительно работает на пределе (пик {peak} >= 0.8 x {RATE})")
    kinds = [m[1] for m in after[:4]]
    ok(1 in kinds, f"тревога доставлена среди первых сообщений после восстановления (виды первых сообщений: {kinds})")
    al = httpc.get(URL["col"] + f"/alarms?since={int(t_cut * 1000)}", 30)["alarms"]
    ok(any(a[3] == "pump02_vibration_high" and a[4] == "raise" for a in al), "тревога pump02_vibration_high (raise) дошла до получателя")
    restore_defaults()


def it_alarm_fast_path():
    t0 = time.time()
    httpc.post(URL["modbus"] + "/event?name=pump2_vibration")
    time.sleep(16)
    al = [a for a in httpc.get(URL["col"] + f"/alarms?since={int(t0 * 1000)}", 30)["alarms"] if a[3] == "pump02_vibration_high"]
    raises = [a for a in al if a[4] == "raise"]
    ok(raises, "тревога поднята и доставлена")
    lag = [a[0] - a[2] for a in raises]
    ok(max(lag) < 400, f"аларм доставлен за {max(lag):.0f} мс после снятия значения (быстрый путь, меньше окна батчинга 500 мс)")
    restore_defaults()


INTEGRATION = [it_stack_ready, it_end_to_end, it_config_patch, it_outage_cycle, it_alarm_fast_path]


def main():
    unit_only = "--unit" in sys.argv
    failed = []
    for group, name in ((UNIT, "Модульные проверки"), (INTEGRATION, "Интеграционные проверки (стенд)")):
        if unit_only and group is INTEGRATION:
            break
        print(f"\n== {name} ==")
        for fn in group:
            print(f"\n{fn.__name__}")
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                failed.append(fn.__name__)
                print(f"  ОШИБКА  {type(e).__name__}: {e}")
                if not isinstance(e, AssertionError):
                    traceback.print_exc()
                if group is INTEGRATION:
                    try:
                        restore_defaults()
                    except Exception:  # noqa: BLE001
                        pass
    print(f"\nПроверок: {N['checks']}")
    if failed:
        print("ПРОВАЛЕНЫ: " + ", ".join(failed))
        return 1
    print("ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
