"""Deadband-фильтр (шаг 3): пропускает значение, только если оно изменилось «достаточно».

Опорное значение - последнее ОТПРАВЛЕННОЕ, а не последнее прочитанное: иначе медленный дрейф мелкими
шагами никогда не пересёк бы порог. Правила:
  * первое значение тега проходит всегда;
  * для bool/строк проходит любое изменение;
  * для чисел: |v - last| >= max(abs, pct/100 * |last|) и |v - last| > 0;
  * heartbeat: если за max_silence_s ничего не проходило, проходит текущее значение (признак «жив»).
Правило ищется по точному ключу «asset.tag», затем по шаблонам fnmatch (в порядке записи), затем default.
"""
import fnmatch


class Deadband:
    def __init__(self, cfg):
        self.configure(cfg)

    def configure(self, cfg) -> None:
        self.cfg = cfg
        self._cache = {}
        self.reset()

    def reset(self) -> None:
        self._last = {}                 # key -> (значение, ts отправки, мс)
        self.counts = {}                # key -> [прочитано, пропущено фильтром]

    def rule(self, key: str) -> dict:
        r = self._cache.get(key)
        if r is None:
            tags = self.cfg.get("tags") or {}
            r = tags.get(key)
            if r is None:
                for pat, rule in tags.items():
                    if fnmatch.fnmatchcase(key, pat):
                        r = rule
                        break
            self._cache[key] = r = dict(r if r is not None else (self.cfg.get("default") or {}))
        return r

    def check(self, s) -> bool:
        """True - запись нужно отправить."""
        c = self.counts.setdefault(s.key, [0, 0])
        c[0] += 1
        if not self.cfg.get("enabled", True):
            return True
        last = self._last.get(s.key)
        v = s.value
        if last is None:
            self._last[s.key] = (v, s.ts)
            return True
        lv, lts = last
        r = self.rule(s.key)
        if isinstance(v, bool) or not isinstance(v, (int, float)) or isinstance(lv, bool):
            changed = v != lv
        else:
            delta = abs(v - lv)
            thr = max(float(r.get("abs", 0.0)), float(r.get("pct", 0.0)) / 100.0 * abs(lv))
            changed = delta > 0 and delta >= thr
        silence = float(r.get("max_silence_s", self.cfg.get("max_silence_s", 60.0)))
        if changed or (s.ts - lts) / 1000.0 >= silence:
            self._last[s.key] = (v, s.ts)
            return True
        c[1] += 1
        return False
