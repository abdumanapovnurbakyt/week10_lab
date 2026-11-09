"""Отправка в облачный брокер (MQTT/TLS) с подтверждением и ресинхронизацией (шаги 4, 6, 8).

Принцип: источник правды - буфер на диске. Отправитель берёт записи («claim»), публикует с QoS 1 и
удаляет их из буфера ТОЛЬКО после того, как publish() вернулся, т.е. пришёл PUBACK. Любая ошибка
(обрыв, таймаут подтверждения) возвращает записи в очередь - ничего не теряется, возможны лишь повторы.

Режимы отправки:
  * alarm   - тревоги (prio 1): сразу, отдельным сообщением, без батчинга и без ограничителя;
  * live    - свежие данные: собираются в пачку до batch_max записей или batch_wait_ms;
  * backfill- накопленный хвост (самая старая запись старше live_age_ms): по backfill.batch записей
              в сообщении, не чаще backfill.rate сообщений в секунду.
"""
import asyncio
import ssl
import time

import aiomqtt

from .limiter import RateLimiter
from .model import PRIO_ALARM, PRIO_NORMAL, envelope


class Uplink:
    def __init__(self, cfg, outbox, gw_id: str):
        self.cfg = cfg
        self.outbox = outbox
        self.gw_id = gw_id
        self.limiter = RateLimiter()
        self.wake = asyncio.Event()
        self.state = "offline"           # offline | online
        self.mode = "idle"               # idle | live | backfill
        self.last_error = ""
        self.connected_at = None
        self.stop = False
        self.m = {"connects": 0, "connect_failures": 0, "disconnects": 0, "msgs_acked": 0, "recs_acked": 0,
                  "alarm_msgs": 0, "backfill_msgs": 0, "send_errors": 0, "bytes_acked": 0}
        self.resyncs: list = []          # события ресинхронизации (время, длительность, число записей)
        self._resync = None

    def notify(self) -> None:
        self.wake.set()

    # ------------------------------------------------------------------ соединение
    def _tls(self):
        u = self.cfg.uplink
        if not u.get("tls", True):
            return None
        ctx = ssl.create_default_context(cafile=u.ca)
        return ctx

    async def run(self) -> None:
        u = self.cfg.uplink
        backoff = float(u.reconnect_min_s)
        while not self.stop:
            try:
                status_topic = f"gw/{self.gw_id}/status"
                will = aiomqtt.Will(status_topic, b"offline", qos=1, retain=True)
                async with aiomqtt.Client(u.host, int(u.port), identifier=self.gw_id, tls_context=self._tls(),
                                          keepalive=int(u.keepalive_s), timeout=float(u.connect_timeout_s),
                                          clean_session=True, will=will,
                                          max_inflight_messages=int(u.inflight) * 2) as client:
                    await client.publish(status_topic, b"online", qos=1, retain=True, timeout=float(u.ack_timeout_s))
                    self.state, self.connected_at, self.last_error = "online", time.time(), ""
                    self.m["connects"] += 1
                    backoff = float(u.reconnect_min_s)
                    await self._pump(client)
            except asyncio.CancelledError:
                raise
            except (aiomqtt.MqttError, OSError, ssl.SSLError, ConnectionError, asyncio.TimeoutError) as e:
                self.last_error = f"{type(e).__name__}: {e}"
                if self.state == "online":
                    self.m["disconnects"] += 1
                else:
                    self.m["connect_failures"] += 1
            finally:
                self.state, self.mode = "offline", "idle"
                self._end_resync(aborted=True)
            if self.stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, float(u.reconnect_max_s))

    # ------------------------------------------------------------------ отправка
    async def _wait(self, timeout: float, failed: asyncio.Event) -> None:
        """Ждёт новых записей, ошибки отправки или таймаута."""
        self.wake.clear()
        waiters = [asyncio.ensure_future(self.wake.wait()), asyncio.ensure_future(failed.wait())]
        try:
            await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waiters:
                w.cancel()

    async def _pump(self, client) -> None:
        cfg, ob = self.cfg, self.outbox
        sem = asyncio.Semaphore(int(cfg.uplink.inflight))
        failed = asyncio.Event()
        tasks: set = set()
        topic_data = cfg.uplink.topic_data.format(gw=self.gw_id)
        topic_alarm = cfg.uplink.topic_alarm.format(gw=self.gw_id)

        async def send(topic: str, rows: list, kind: str) -> None:
            seqs = [r[0] for r in rows]
            try:
                payload = envelope(self.gw_id, rows)
                await client.publish(topic, payload, qos=1, timeout=float(cfg.uplink.ack_timeout_s))
                await asyncio.to_thread(ob.ack, seqs)          # PUBACK получен - можно удалять
                self.m["msgs_acked"] += 1
                self.m["recs_acked"] += len(rows)
                self.m["bytes_acked"] += len(payload)
                if kind == "alarm":
                    self.m["alarm_msgs"] += 1
                elif kind == "backfill":
                    self.m["backfill_msgs"] += 1
                    if self._resync:
                        self._resync["records"] += len(rows)
            except asyncio.CancelledError:
                ob.release(seqs)
                raise
            except Exception as e:  # noqa: BLE001 - любая ошибка: вернуть записи и переподключиться
                self.m["send_errors"] += 1
                self.last_error = f"{type(e).__name__}: {e}"
                ob.release(seqs)
                failed.set()
            finally:
                sem.release()

        try:
            while not self.stop:
                if failed.is_set():
                    raise ConnectionError(self.last_error or "ошибка отправки")
                await sem.acquire()
                kind = "alarm"
                rows = ob.claim(PRIO_ALARM, int(cfg.publish.alarm_batch)) if ob.pending(PRIO_ALARM) else []
                if not rows:
                    kind, rows = await self._next_data(failed)
                if not rows:
                    sem.release()
                    continue
                topic = topic_alarm if kind == "alarm" else topic_data
                t = asyncio.create_task(send(topic, rows, kind))
                tasks.add(t)
                t.add_done_callback(tasks.discard)
        finally:
            for t in list(tasks):
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            ob.release_inflight()          # задача, отменённая до старта, сама записи не вернёт

    async def _next_data(self, failed: asyncio.Event):
        """Выбирает следующую пачку обычных данных: ('live'|'backfill', rows) или (kind, [])."""
        cfg, ob = self.cfg, self.outbox
        if ob.pending(PRIO_ALARM):
            return "alarm", []
        n = ob.pending(PRIO_NORMAL)
        if n == 0:
            self.mode = "idle"
            self._end_resync()
            await self._wait(1.0, failed)
            return "live", []
        age_ms = time.time() * 1000 - ob.head_ts(PRIO_NORMAL)
        if age_ms >= float(cfg.backfill.live_age_ms):
            self.mode = "backfill"
            self._begin_resync()
            await self.limiter.acquire(float(cfg.backfill.rate))
            return "backfill", ob.claim(PRIO_NORMAL, int(cfg.backfill.batch))
        self.mode = "live"
        self._end_resync()
        if n >= int(cfg.publish.batch_max) or age_ms >= float(cfg.publish.batch_wait_ms):
            # лимит действует и на «живые» пачки: хвост ресинхронизации (последние ~2 с данных) иначе ушёл бы
            # залпом по batch_max записей; при штатной работе (3 сообщения в секунду) лимитер не задерживает
            await self.limiter.acquire(float(cfg.backfill.rate))
            return "live", ob.claim(PRIO_NORMAL, int(cfg.publish.batch_max))
        await self._wait((float(cfg.publish.batch_wait_ms) - age_ms) / 1000.0, failed)
        return "live", []

    # ------------------------------------------------------------- учёт ресинхронизации
    def _begin_resync(self) -> None:
        if self._resync is None:
            self._resync = {"start": time.time(), "backlog_at_start": self.outbox.count(), "records": 0,
                            "connected_at": self.connected_at}

    def _end_resync(self, aborted: bool = False) -> None:
        r = self._resync
        if r is None:
            return
        self._resync = None
        r["end"] = time.time()
        r["duration_s"] = round(r["end"] - r["start"], 3)
        r["aborted"] = aborted
        self.resyncs.append(r)
        del self.resyncs[:-50]
