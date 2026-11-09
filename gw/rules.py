"""Локальные правила тревог на шлюзе (шаг 8).

Правило срабатывает по КАЖДОМУ прочитанному значению (до deadband-фильтра - иначе фильтр мог бы скрыть
превышение порога) и меняет состояние только на границах: raise при превышении, clear при возврате
с гистерезисом. Тревога получает приоритет и публикуется сразу, минуя батчинг и ограничитель backfill.
"""
import operator

from .model import Sample

OPS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le,
       "==": operator.eq, "!=": operator.ne}


class Rule:
    def __init__(self, d: dict):
        self.id = d["id"]
        self.asset = d["asset"]
        self.tag = d["tag"]
        self.op = d["op"]
        if self.op not in OPS:
            raise ValueError(f"оператор правила {self.id}: {self.op}")
        self.threshold = d["threshold"]
        self.hysteresis = float(d.get("hysteresis", 0.0))
        self.severity = d.get("severity", "warning")
        self.for_s = float(d.get("for_s", 0.0))
        self.active = False
        self._since = None

    def _holds(self, v) -> bool:
        return OPS[self.op](v, self.threshold)

    def _cleared(self, v) -> bool:
        if isinstance(v, bool) or self.op in ("==", "!="):
            return not self._holds(v)
        shift = -self.hysteresis if self.op in (">", ">=") else self.hysteresis
        return not OPS[self.op](v, self.threshold + shift)


class RuleEngine:
    def __init__(self, rules_cfg):
        self.rules = {}
        for d in rules_cfg or []:
            r = Rule(d)
            self.rules.setdefault(f"{r.asset}.{r.tag}", []).append(r)
        self.raised = 0
        self.cleared = 0

    def reset(self) -> None:
        for lst in self.rules.values():
            for r in lst:
                r.active, r._since = False, None

    def evaluate(self, s: Sample) -> list:
        """Возвращает новые записи-тревоги (raise/clear) для этого значения."""
        out = []
        for r in self.rules.get(s.key, ()):
            v = s.value
            if not r.active:
                if r._holds(v):
                    r._since = r._since if r._since is not None else s.ts
                    if (s.ts - r._since) / 1000.0 >= r.for_s:
                        r.active = True
                        self.raised += 1
                        out.append(self._alarm(r, s, "raise"))
                else:
                    r._since = None
            elif r._cleared(v):
                r.active, r._since = False, None
                self.cleared += 1
                out.append(self._alarm(r, s, "clear"))
        return out

    @staticmethod
    def _alarm(r: Rule, s: Sample, state: str) -> Sample:
        return Sample(s.asset, s.tag, s.value, s.unit, s.src, s.ts, s.q,
                      {"id": r.id, "state": state, "sev": r.severity, "limit": r.threshold})
