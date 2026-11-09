"""Маппинг сырых значений устройств в канонические теги (шаг 2 задания).

Для Modbus: регистр(ы) -> тип (int16/uint16/int32/uint32/float32/bits), масштаб, смещение, единица, asset_id.
Для BLE (GATT) и MQTT-устройств - те же операции масштабирования над «сырым» числом.
"""
import math
import struct

from .model import Sample


def decimals_for(scale: float) -> int:
    """Сколько знаков после запятой имеет смысл хранить при данном масштабе."""
    if scale >= 1:
        return 0
    return max(0, int(math.ceil(-math.log10(scale) - 1e-9)))


def convert(raw, scale: float = 1.0, offset: float = 0.0, decimals: int | None = None):
    """raw * scale + offset с округлением до разумной точности."""
    if isinstance(raw, bool):
        return raw
    d = decimals_for(scale) if decimals is None else decimals
    v = round(raw * scale + offset, d)
    return int(v) if d == 0 else v


class RegTag:
    """Описание одного тега Modbus."""

    WORDS = {"int16": 1, "uint16": 1, "bits": 1, "int32": 2, "uint32": 2, "float32": 2}

    def __init__(self, d: dict, word_order: str = "big"):
        self.reg = int(d["reg"])
        self.type = d["type"]
        if self.type not in self.WORDS:
            raise ValueError(f"неизвестный тип регистра: {self.type}")
        self.scale = float(d.get("scale", 1.0))
        self.offset = float(d.get("offset", 0.0))
        self.unit = d.get("unit", "")
        self.asset = d["asset"]
        self.tag = d["tag"]
        self.bit = d.get("bit")
        self.decimals = d.get("decimals")
        self.word_order = d.get("word_order", word_order)

    @property
    def words(self) -> int:
        return self.WORDS[self.type]

    def raw(self, regs):
        r = regs[self.reg]
        if self.type == "uint16":
            return r
        if self.type == "int16":
            return r - 65536 if r >= 32768 else r
        if self.type == "bits":
            return bool((r >> int(self.bit or 0)) & 1)
        a, b = regs[self.reg], regs[self.reg + 1]
        hi, lo = (a, b) if self.word_order == "big" else (b, a)
        u = (hi << 16) | lo
        if self.type == "uint32":
            return u
        if self.type == "int32":
            return u - (1 << 32) if u >= (1 << 31) else u
        return struct.unpack(">f", u.to_bytes(4, "big"))[0]      # float32

    def value(self, regs):
        raw = self.raw(regs)
        if self.type == "bits":
            return raw
        return convert(raw, self.scale, self.offset, self.decimals)


def build_reg_tags(cfg_tags, word_order: str = "big") -> list:
    return [RegTag(d, word_order) for d in cfg_tags]


def decode_registers(regs, tags, ts: int, src: str = "modbus") -> list:
    """Блок регистров -> список канонических записей."""
    out = []
    for t in tags:
        if t.reg + t.words > len(regs):
            raise ValueError(f"тег {t.asset}.{t.tag}: регистр {t.reg} вне прочитанного блока")
        out.append(Sample(t.asset, t.tag, t.value(regs), t.unit, src, ts))
    return out


# --------------------------------------------------------------------------- BLE / GATT
GATT_FORMATS = {"sint16": ("<h", 2), "uint16": ("<H", 2), "uint8": ("<B", 1), "sint8": ("<b", 1), "uint32": ("<I", 4)}


def decode_gatt(fmt: str, hex_value: str):
    """Значение характеристики GATT (hex, little-endian) -> число."""
    code, size = GATT_FORMATS[fmt]
    data = bytes.fromhex(hex_value)
    if len(data) < size:
        raise ValueError("слишком короткое значение характеристики")
    return struct.unpack(code, data[:size])[0]
