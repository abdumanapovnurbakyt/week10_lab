"""Persistent-буфер на SQLite (WAL) - основа store-and-forward (шаг 4).

Все записи сначала пишутся на диск (outbox), и только потом отправляются; из буфера запись удаляется
ТОЛЬКО после PUBACK брокера (ack). Поэтому при обрыве связи, падении брокера или kill -9 шлюза записи
не теряются, а лишь могут быть отправлены повторно (at-least-once; дубликаты снимаются по seq).

Устройство:
  * на диске: outbox(seq, ts, prio, body), dropped(seq), meta(last_seq, dropped) - журнал в режиме WAL;
  * в памяти: зеркало «ожидают отправки» (по приоритетам) и «в полёте»: отправителю не нужно ходить
    в SQLite за каждым сообщением, а дисковые операции идут в рабочих потоках (asyncio.to_thread);
  * ёмкость capacity (по ТЗ 10 000 записей). При переполнении вытесняется самое старое среди записей
    наименьшего приоритета (сначала обычные данные, тревоги - в последнюю очередь), вытесненные
    seq пишутся в таблицу dropped и учитываются в счётчике - потеря всегда явная и измеримая.
"""
import bisect
import sqlite3
import threading

PRIO_NORMAL, PRIO_ALARM = 0, 1


class Outbox:
    def __init__(self, path: str, capacity: int = 10000, synchronous: str = "NORMAL"):
        self.path = path
        self.capacity = capacity
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(f"PRAGMA synchronous={synchronous}")
        self.db.execute("PRAGMA wal_autocheckpoint=1000")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS outbox(seq INTEGER PRIMARY KEY, ts INTEGER NOT NULL,
                                              prio INTEGER NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS dropped(seq INTEGER PRIMARY KEY, prio INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v INTEGER NOT NULL);
            INSERT OR IGNORE INTO meta VALUES('last_seq', 0);
            INSERT OR IGNORE INTO meta VALUES('dropped', 0);
        """)
        self._dblock = threading.Lock()      # одна запись на диск за раз
        self._mem = threading.Lock()         # короткие операции с зеркалом в памяти
        self._pend = {PRIO_NORMAL: ([], {}), PRIO_ALARM: ([], {})}   # prio -> (отсортированные seq, {seq: (ts, body)})
        self._infl = {}                      # seq -> (prio, ts, body)
        self.acked_total = 0
        last_seq = self.db.execute("SELECT v FROM meta WHERE k='last_seq'").fetchone()[0]
        self.dropped_total = self.db.execute("SELECT v FROM meta WHERE k='dropped'").fetchone()[0]
        max_seq = 0
        for seq, ts, prio, body in self.db.execute("SELECT seq, ts, prio, body FROM outbox ORDER BY seq"):
            seqs, rows = self._pend[prio]
            seqs.append(seq)
            rows[seq] = (ts, body)
            max_seq = seq
        self.recovered = self.count()
        self._next_seq = max(last_seq, max_seq) + 1

    # ------------------------------------------------------------------ запись
    def put_many(self, items):
        """items: [(ts, prio, body)] -> (список seq, список вытесненных seq). Вызывается из потока."""
        n = len(items)
        if n == 0:
            return [], []
        with self._dblock:
            with self._mem:
                first = self._next_seq
                self._next_seq += n
                seqs_new = list(range(first, first + n))
                over = self.count_unlocked() + n - self.capacity
                victims_old, victims_new = [], []        # [(seq, prio)], [seq]
                if over > 0:
                    # порядок вытеснения: старые обычные, новые обычные, старые тревоги, новые тревоги.
                    # Старые убираем из зеркала сразу, под тем же замком: отправитель их уже не заберёт.
                    need = over
                    for prio in (PRIO_NORMAL, PRIO_ALARM):
                        seqs, rows = self._pend[prio]
                        old = seqs[:need]
                        for sq in old:
                            del rows[sq]
                        del seqs[:len(old)]
                        victims_old += [(sq, prio) for sq in old]
                        need -= len(old)
                        new = [sq for sq, it in zip(seqs_new, items) if it[1] == prio][:max(need, 0)]
                        victims_new += new
                        need -= len(new)
                    # если кандидатов не хватило (всё в полёте), ёмкость временно превышается
            vset = set(victims_new)
            try:
                self.db.execute("BEGIN IMMEDIATE")
                self.db.executemany("INSERT INTO outbox(seq, ts, prio, body) VALUES(?,?,?,?)",
                                    [(sq, it[0], it[1], it[2]) for sq, it in zip(seqs_new, items) if sq not in vset])
                if victims_old:
                    self.db.executemany("DELETE FROM outbox WHERE seq=?", [(sq,) for sq, _p in victims_old])
                drops = victims_old + [(sq, it[1]) for sq, it in zip(seqs_new, items) if sq in vset]
                if drops:
                    self.db.executemany("INSERT OR REPLACE INTO dropped(seq, prio) VALUES(?,?)", drops)
                self.db.execute("UPDATE meta SET v=? WHERE k='last_seq'", (first + n - 1,))
                if drops:
                    self.db.execute("UPDATE meta SET v=v+? WHERE k='dropped'", (len(drops),))
                self.db.execute("COMMIT")
            except Exception:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            with self._mem:
                for sq, it in zip(seqs_new, items):
                    if sq not in vset:
                        seqs, rows = self._pend[it[1]]
                        seqs.append(sq)
                        rows[sq] = (it[0], it[2])
                self.dropped_total += len(drops)
        return seqs_new, [sq for sq, _p in victims_old] + victims_new

    # ---------------------------------------------------------- чтение/подтверждение
    def claim(self, prio: int, n: int):
        """Забирает до n самых старых записей приоритета prio в «полёт»: [(seq, ts, body)]."""
        with self._mem:
            seqs, rows = self._pend[prio]
            take = seqs[:n]
            del seqs[:n]
            out = []
            for s in take:
                ts, body = rows.pop(s)
                self._infl[s] = (prio, ts, body)
                out.append((s, ts, body))
            return out

    def release(self, seq_list) -> None:
        """Отправка не удалась: записи возвращаются в очередь (остаются на диске, ничего не потеряно)."""
        with self._mem:
            for s in seq_list:
                it = self._infl.pop(s, None)
                if it is not None:
                    prio, ts, body = it
                    seqs, rows = self._pend[prio]
                    bisect.insort(seqs, s)
                    rows[s] = (ts, body)

    def release_inflight(self) -> int:
        """Соединение закрыто: все неподтверждённые записи «в полёте» возвращаются в очередь.

        Нужна как страховка к release(): задача отправки, отменённая ДО первого шага, не доходит до своего
        обработчика и не возвращает записи сама - без этого такая запись навсегда осталась бы «в полёте»
        (на диске есть, к отправке не готова). Этот дефект нашёл интеграционный тест (см. отчёт).
        """
        with self._mem:
            seqs = list(self._infl)
        self.release(seqs)
        return len(seqs)

    def ack(self, seq_list) -> None:
        """PUBACK получен: удаляем записи с диска. Вызывается из потока."""
        seq_list = list(seq_list)
        with self._dblock:
            self.db.execute("BEGIN IMMEDIATE")
            for i in range(0, len(seq_list), 500):
                chunk = seq_list[i:i + 500]
                self.db.execute(f"DELETE FROM outbox WHERE seq IN ({','.join('?' * len(chunk))})", chunk)
            self.db.execute("COMMIT")
            with self._mem:
                for s in seq_list:
                    self._infl.pop(s, None)
                self.acked_total += len(seq_list)

    # ------------------------------------------------------------------ сведения
    def count_unlocked(self) -> int:
        return len(self._pend[0][0]) + len(self._pend[1][0]) + len(self._infl)

    def count(self) -> int:
        with self._mem:
            return self.count_unlocked()

    def pending(self, prio: int) -> int:
        with self._mem:
            return len(self._pend[prio][0])

    def head_ts(self, prio: int):
        with self._mem:
            seqs, rows = self._pend[prio]
            return rows[seqs[0]][0] if seqs else None

    def stats(self) -> dict:
        with self._mem:
            n0, n1 = len(self._pend[0][0]), len(self._pend[1][0])
            return {"depth": n0 + n1 + len(self._infl), "pending": n0, "pending_alarm": n1,
                    "inflight": len(self._infl), "capacity": self.capacity, "acked_total": self.acked_total,
                    "dropped_total": self.dropped_total, "next_seq": self._next_seq}

    def dropped_seqs(self, limit: int = 200000):
        with self._dblock:
            return [r[0] for r in self.db.execute("SELECT seq FROM dropped ORDER BY seq LIMIT ?", (limit,))]

    def close(self) -> None:
        with self._dblock:
            try:
                self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                self.db.close()
