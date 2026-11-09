"""Загрузка YAML-конфигурации с подстановкой ${VAR:-по_умолчанию} и доступом через атрибуты."""
import os
import re

import yaml

_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class Cfg(dict):
    """dict с доступом cfg.uplink.host; вложенные словари и словари в списках тоже Cfg."""

    def __init__(self, data=None):
        super().__init__()
        for k, v in (data or {}).items():
            self[k] = _wrap(v)

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k) from None

    def __setattr__(self, k, v):
        self[k] = _wrap(v)

    def to_dict(self) -> dict:
        return {k: _plain(v) for k, v in self.items()}

    def merge(self, patch: dict) -> None:
        """Рекурсивное слияние (для POST /config)."""
        for k, v in patch.items():
            if isinstance(v, dict) and v.get("_replace"):          # "_replace": true - заменить раздел целиком
                self[k] = _wrap({a: b for a, b in v.items() if a != "_replace"})
            elif isinstance(v, dict) and isinstance(self.get(k), Cfg):
                self[k].merge(v)
            else:
                self[k] = _wrap(v)


def _wrap(v):
    if isinstance(v, dict) and not isinstance(v, Cfg):
        return Cfg(v)
    if isinstance(v, list):
        return [_wrap(x) for x in v]
    return v


def _plain(v):
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_plain(x) for x in v]
    return v


def parse(text: str) -> Cfg:
    text = _ENV.sub(lambda m: os.environ.get(m.group(1)) or (m.group(2) or ""), text)
    return Cfg(yaml.safe_load(text))


def load(path: str) -> Cfg:
    with open(path, encoding="utf-8") as f:
        return parse(f.read())
