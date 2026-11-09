"""Модель технологической установки: значения датчиков как детерминированная функция времени.

Одна и та же функция питает три симулятора (Modbus, BLE, ESP32/MQTT) и офлайн-расчёт deadband,
поэтому «живой» и офлайн-опыты видят одинаковые сигналы. Шум - целочисленный хеш от (канал, такт),
без random и numpy, так что результаты воспроизводимы.
"""
import math
import struct

TICK = 0.5          # период обновления регистров/сенсоров, с
SEED = 20261108
TAU = 2 * math.pi


def _hash(i: int, k: int) -> float:
    x = (i * 0x9E3779B1 + k * 0x85EBCA6B + SEED) & 0xFFFFFFFF
    x ^= x >> 15
    x = (x * 0x2C1B3C6D) & 0xFFFFFFFF
    x ^= x >> 12
    x = (x * 0x297A2D39) & 0xFFFFFFFF
    x ^= x >> 15
    return x / 4294967296.0


def noise(i: int, k: int) -> float:
    """Приближённо N(0, 1), детерминированно по (канал, такт)."""
    return ((_hash(i, 3 * k) + _hash(i, 3 * k + 1) + _hash(i, 3 * k + 2)) - 1.5) * 2.0


def trap(dt: float, rise: float, hold: float, fall: float) -> float:
    """Трапеция 0..1..0: форма события относительно момента старта."""
    if dt < 0:
        return 0.0
    if dt < rise:
        return dt / rise if rise else 1.0
    if dt < rise + hold:
        return 1.0
    if dt < rise + hold + fall:
        return 1.0 - (dt - rise - hold) / fall
    return 0.0


# Раскладка 20 holding-регистров Modbus-устройства: (адрес, тип, asset, тег, масштаб)
# Совпадает с config/gateway.yaml (там то же самое, но на стороне шлюза: паспорт устройства -> маппинг).
LAYOUT = [
    (0, "int16", "pump-01", "temperature", 0.1),
    (1, "uint16", "pump-01", "pressure", 0.01),
    (2, "uint16", "pump-01", "speed", 1.0),
    (3, "uint16", "pump-01", "vibration", 0.01),
    (4, "uint16", "pump-01", "current", 0.1),
    (5, "int16", "pump-02", "temperature", 0.1),
    (6, "uint16", "pump-02", "pressure", 0.01),
    (7, "uint16", "pump-02", "speed", 1.0),
    (8, "uint16", "pump-02", "vibration", 0.01),
    (9, "uint16", "pump-02", "current", 0.1),
    (10, "uint16", "tank-01", "level", 0.1),
    (11, "int16", "tank-01", "temperature", 0.1),
    (12, "float32", "tank-01", "volume", 1.0),       # 2 регистра: 12-13
    (14, "uint16", "tank-01", "valve", 1.0),
    (15, "uint16", "boiler-01", "temperature", 0.1),
    (16, "uint16", "boiler-01", "pressure", 0.01),
    (17, "bits", "boiler-01", "status", 1.0),        # бит 0 - пламя, бит 1 - авария
    (18, "uint32", "boiler-01", "energy", 0.1),      # 2 регистра: 18-19
]
N_REGS = 20

# События, которые можно запустить по требованию (аларм-сценарии): имя -> длительность, с
EVENTS = {"pump1_overheat": 40.0, "pump2_vibration": 20.0, "tank_low": 40.0,
          "boiler_flame_out": 20.0, "esp32_lowbat": 50.0}

BLE_DEVICES = {"AA:BB:CC:00:00:01": "ble-01", "AA:BB:CC:00:00:02": "ble-02"}
ESP32_DEVICES = ["esp32-01", "esp32-02", "esp32-03"]


class Plant:
    """Состояние установки: набор активных событий и функции «физики»."""

    def __init__(self):
        self.events: dict = {}          # имя -> время старта (с симуляции)

    def start_event(self, name: str, t: float) -> None:
        if name not in EVENTS:
            raise KeyError(name)
        self.events[name] = t

    def _ev(self, name: str, t: float, rise: float, hold: float, fall: float) -> float:
        t0 = self.events.get(name)
        return 0.0 if t0 is None else trap(t - t0, rise, hold, fall)

    def _active(self, name: str, t: float, dur: float) -> bool:
        t0 = self.events.get(name)
        return t0 is not None and 0.0 <= t - t0 < dur

    # ---------------------------------------------------------------- Modbus
    def modbus_physical(self, t: float) -> dict:
        k = int(t / TICK)
        n = lambda i, sd: noise(i, k) * sd  # noqa: E731
        P = {}
        P[("pump-01", "temperature")] = 62.0 + 4.0 * math.sin(TAU * t / 900) + n(0, 0.12) + 30.0 * self._ev("pump1_overheat", t, 10, 12, 15)
        P[("pump-01", "pressure")] = 4.20 + 0.15 * math.sin(TAU * t / 120) + n(1, 0.010)
        P[("pump-01", "speed")] = 1480.0 + 6.0 * math.sin(TAU * t / 300 + 1.0) + n(2, 2.5)
        P[("pump-01", "vibration")] = max(0.0, 2.2 + 0.4 * math.sin(TAU * t / 200) + n(3, 0.25))
        P[("pump-01", "current")] = 12.5 + 0.8 * math.sin(TAU * t / 600 + 0.5) + n(4, 0.15)
        P[("pump-02", "temperature")] = 58.0 + 3.0 * math.sin(TAU * t / 700 + 2.0) + n(5, 0.12)
        P[("pump-02", "pressure")] = 3.90 + 0.12 * math.sin(TAU * t / 150 + 1.0) + n(6, 0.010)
        P[("pump-02", "speed")] = 1470.0 + 5.0 * math.sin(TAU * t / 280) + n(7, 2.5)
        P[("pump-02", "vibration")] = max(0.0, 1.9 + 0.3 * math.sin(TAU * t / 240 + 2.0) + n(8, 0.25)) + 8.0 * self._ev("pump2_vibration", t, 3, 10, 5)
        P[("pump-02", "current")] = 11.8 + 0.7 * math.sin(TAU * t / 500 + 1.0) + n(9, 0.15)
        p = (t % 2400.0) / 2400.0
        level = 40.0 + 30.0 * (1.0 - abs(2.0 * p - 1.0))                    # заполнение/слив бака
        level *= 1.0 - 0.95 * self._ev("tank_low", t, 5, 20, 10)
        level = min(100.0, max(0.0, level + n(10, 0.05)))
        P[("tank-01", "level")] = level
        P[("tank-01", "temperature")] = 18.0 + 1.5 * math.sin(TAU * t / 1800) + n(11, 0.05)
        P[("tank-01", "volume")] = level * 0.25
        P[("tank-01", "valve")] = [0, 50, 100, 50][int(t // 200) % 4]
        P[("boiler-01", "temperature")] = 82.0 + 2.0 * math.sin(TAU * t / 400) + n(12, 0.20)
        P[("boiler-01", "pressure")] = 6.50 + 0.10 * math.sin(TAU * t / 90) + n(13, 0.015)
        flame_out = self._active("boiler_flame_out", t, 15.0)
        P[("boiler-01", "status")] = 0b10 if flame_out else 0b01
        P[("boiler-01", "energy")] = 1000.0 + 0.004 * t
        return P

    def modbus_registers(self, t: float) -> list:
        P = self.modbus_physical(t)
        regs = [0] * N_REGS
        for addr, kind, asset, tag, scale in LAYOUT:
            v = P[(asset, tag)]
            if kind == "int16":
                regs[addr] = int(round(v / scale)) & 0xFFFF
            elif kind == "uint16":
                regs[addr] = min(0xFFFF, max(0, int(round(v / scale))))
            elif kind == "bits":
                regs[addr] = int(v) & 0xFFFF
            elif kind == "uint32":
                raw = int(round(v / scale)) & 0xFFFFFFFF
                regs[addr], regs[addr + 1] = raw >> 16, raw & 0xFFFF
            elif kind == "float32":
                hi, lo = struct.unpack(">HH", struct.pack(">f", v))
                regs[addr], regs[addr + 1] = hi, lo
        return regs

    # ------------------------------------------------------------------- BLE
    def ble_values(self, dev: str, t: float) -> dict:
        """Физические значения BLE-датчика: uuid -> число."""
        i = 40 + (0 if dev.endswith("1") else 10)
        k = int(t / TICK)
        ph = 0.0 if dev.endswith("1") else 1.3
        temp = 21.5 + 1.0 * math.sin(TAU * t / 1200 + ph) + noise(i, k) * 0.03
        hum = 47.0 + 4.0 * math.sin(TAU * t / 900 + ph) + noise(i + 1, k) * 0.25
        batt = max(1.0, 100.0 - t / 360.0)
        return {"2A6E": temp, "2A6F": hum, "2A19": batt}

    # ----------------------------------------------------------------- ESP32
    def esp32_payload(self, dev: str, t: float) -> dict:
        idx = ESP32_DEVICES.index(dev)
        i = 70 + 10 * idx
        k = int(t / TICK)
        temp = 23.0 + idx + 1.5 * math.sin(TAU * t / 1000 + idx) + noise(i, k) * 0.08
        hum = 52.0 - 2 * idx + 5.0 * math.sin(TAU * t / 800 + idx) + noise(i + 1, k) * 0.4
        bat = 3950.0 - 0.02 * t - 700.0 * self._ev("esp32_lowbat", t, 5, 25, 10) * (1 if idx == 2 else 0) + noise(i + 2, k) * 4.0
        rssi = -60 - 3 * idx + noise(i + 3, k) * 2.0
        return {"t_c10": int(round(temp * 10)), "h_p10": int(round(hum * 10)),
                "bat_mv": int(round(bat)), "rssi": int(round(rssi))}
