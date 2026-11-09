# Один образ для всех Python-сервисов стенда (шлюз, эмуляторы, прокси WAN, collector, тесты, bench).
# Что запускать, определяет command/entrypoint в docker-compose.yml.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    IN_DOCKER=1

# iproute2 нужен только для ручного эксперимента `tc netem` (см. README); без него стенд работает полностью
RUN (apt-get update \
     && apt-get install -y --no-install-recommends iproute2 \
     && rm -rf /var/lib/apt/lists/*) \
    || echo "iproute2 не установлен: tc netem недоступен, остальное работает"

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN mkdir -p /data /certs /app/results

CMD ["python", "-m", "gw.main"]
