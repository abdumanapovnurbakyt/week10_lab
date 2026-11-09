"""Каноническая модель данных шлюза.

Любой источник (Modbus, BLE, MQTT) приводится к записи Sample: asset_id, тег, значение в инженерных
единицах, единица, качество, источник, время снятия (мс UTC). На провод уходит JSON:

  {"v":1,"gw":"gw-01","n":2,"recs":[{"seq":101,"ts":1730000000123,"asset":"pump-01","tag":"temperature",
                                      "val":71.3,"unit":"degC","q":"good","src":"modbus"}, ...]}

seq присваивается буфером при вставке и монотонно растёт без пропусков (переживает перезапуск), по нему
приёмная сторона находит потери и дубликаты. Схема сообщения: schemas/canonical.schema.json.
"""
import json
from dataclasses import dataclass

PRIO_NORMAL = 0
PRIO_ALARM = 1

_dumps = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False).encode


@dataclass(slots=True)
class Sample:
    asset: str
    tag: str
    value: object          # float | int | bool
    unit: str
    src: str               # modbus | ble | mqtt | gateway
    ts: int                # мс UTC, момент снятия значения шлюзом
    q: str = "good"        # good | bad | stale
    alarm: dict | None = None

    @property
    def key(self) -> str:
        return f"{self.asset}.{self.tag}"

    def body(self) -> str:
        """JSON записи без seq (seq вставляется при отправке, см. with_seq)."""
        d = {"ts": self.ts, "asset": self.asset, "tag": self.tag, "val": self.value,
             "unit": self.unit, "q": self.q, "src": self.src}
        if self.alarm:
            d["alarm"] = self.alarm
        return _dumps(d)


def with_seq(seq: int, body: str) -> str:
    return '{"seq":%d,%s' % (seq, body[1:])


def envelope(gw_id: str, rows) -> bytes:
    """rows: список (seq, ts, body) -> полезная нагрузка MQTT-сообщения."""
    recs = ",".join(with_seq(seq, body) for seq, _ts, body in rows)
    return ('{"v":1,"gw":%s,"n":%d,"recs":[%s]}' % (_dumps(gw_id), len(rows), recs)).encode("utf-8")
