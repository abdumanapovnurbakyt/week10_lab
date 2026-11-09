"""Синхронный HTTP-клиент для оркестратора и тестов (urllib, без зависимостей)."""
import json
import time
import urllib.error
import urllib.request


def get(url: str, timeout: float = 5.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def post(url: str, obj=None, timeout: float = 5.0):
    data = json.dumps(obj if obj is not None else {}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def wait_ready(url: str, timeout: float = 90.0, what: str = "") -> bool:
    """Ждёт, пока GET url не вернёт 200."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            get(url, timeout=2)
            return True
        except (urllib.error.URLError, OSError, ValueError):
            time.sleep(0.5)
    raise TimeoutError(f"не дождались {what or url}")
