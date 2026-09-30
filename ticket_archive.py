"""Offline ticket archive library.

Migrates eligible tickets from an online SQLite store to local JSON
archive files, with crash-safe recovery.

Layout:
- SQLite ``tickets`` table holds online (not yet archived) records.
- ``batches`` / ``batch_selection`` persist frozen batch metadata (ids only).
- ``archive_index`` maps archived ticket id -> formal JSON file (locator only).
- Archive dir holds formal ``*.json`` files (JSON arrays, <= 1000 records).
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable

CHUNK_SIZE = 1000
ONLINE_THRESHOLD = int(os.environ.get("TICKET_ARCHIVE_THRESHOLD", "100000"))
DAY_SECONDS = 24 * 3600
RETENTION_DAYS = 90
RETENTION_SECONDS = RETENTION_DAYS * DAY_SECONDS

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tickets (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    closed_at REAL,
    body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    now_ts REAL NOT NULL,
    state TEXT NOT NULL,
    total INTEGER NOT NULL,
    files TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_selection (
    batch_id TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    PRIMARY KEY (batch_id, ticket_id)
);
CREATE TABLE IF NOT EXISTS archive_index (
    ticket_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    file_name TEXT NOT NULL
);
"""


def _to_ts(value: Any) -> float | None:
    """Normalize a UTC time (epoch number or ISO-8601 string) to epoch seconds."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    raise ValueError(f"unsupported time value: {value!r}")


def _failpoint(name: str) -> None:
    """Test hook: simulate kill -9 when TICKET_ARCHIVE_FAILPOINT matches."""
    if os.environ.get("TICKET_ARCHIVE_FAILPOINT") == name:
        os._exit(1)


class TicketArchive:
    """Online ticket store plus offline JSON archive, backed by SQLite."""

    def __init__(self, db_path: str, archive_dir: str) -> None:
        self.db_path = db_path
        self.archive_dir = archive_dir
        os.makedirs(archive_dir, exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        row = self._conn.execute(
            "SELECT COUNT(*) FROM batches WHERE state != 'done'"
        ).fetchone()
        self._needs_recovery = bool(row[0])

    def close(self) -> None:
        self._conn.close()

    def _check_ready(self) -> None:
        if self._needs_recovery:
            raise RuntimeError("unfinished batches exist; call recover() first")

    # ------------------------------------------------------------------
    # Import / lookup
    # ------------------------------------------------------------------
    def import_tickets(self, records: Iterable[dict[str, Any]]) -> int:
        """Import records into the online store. Returns rows imported."""
        self._check_ready()
        rows = []
        for rec in records:
            rows.append(
                (
                    str(rec["id"]),
                    rec["status"],
                    _to_ts(rec.get("closed_at")),
                    json.dumps(rec, ensure_ascii=False),
                )
            )
        with self._conn:
            self._conn.executemany(
                "INSERT INTO tickets (id, status, closed_at, body) "
                "VALUES (?, ?, ?, ?)",
                rows,
            )
        return len(rows)

    def get_ticket(self, ticket_id: str) -> dict[str, Any]:
        """Return the full original record from online store or archive."""
        self._check_ready()
        row = self._conn.execute(
            "SELECT body FROM tickets WHERE id = ?", (ticket_id,)
        ).fetchone()
        if row is not None:
            return json.loads(row[0])
        row = self._conn.execute(
            "SELECT file_name FROM archive_index WHERE ticket_id = ?",
            (ticket_id,),
        ).fetchone()
        if row is None:
            raise KeyError(ticket_id)
        path = os.path.join(self.archive_dir, row[0])
        with open(path, "r", encoding="utf-8") as fh:
            for rec in json.load(fh):
                if str(rec["id"]) == ticket_id:
                    return rec
        raise KeyError(ticket_id)

    # ------------------------------------------------------------------
    # Archive / recover (filled in later steps)
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Archive / recover
    # ------------------------------------------------------------------
    def archive(self, batch_id: str, now: Any) -> tuple[list[str], int]:
        """Run one archive batch.

        Returns (formal file paths, total archived ticket count).
        """
        self._check_ready()
        now_ts = _to_ts(now)
        row = self._conn.execute(
            "SELECT state, total, files FROM batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        if row is None:
            self._register_batch(batch_id, now_ts)
        elif row[0] == "done":
            files = [self._file_path(n) for n in json.loads(row[2])]
            return files, row[1]
        files, total = self._run_batch(batch_id)
        with self._conn:
            self._conn.execute(
                "UPDATE batches SET state = 'done', files = ? WHERE batch_id = ?",
                (json.dumps(files), batch_id),
            )
        return [self._file_path(n) for n in files], total

    def recover(self) -> list[tuple[str, list[str], int]]:
        """Finish all registered-but-unfinished batches."""
        results = []
        rows = self._conn.execute(
            "SELECT batch_id FROM batches WHERE state != 'done' ORDER BY batch_id"
        ).fetchall()
        for (batch_id,) in rows:
            files, total = self._run_batch(batch_id)
            with self._conn:
                self._conn.execute(
                    "UPDATE batches SET state = 'done', files = ? "
                    "WHERE batch_id = ?",
                    (json.dumps(files), batch_id),
                )
            results.append((batch_id, [self._file_path(n) for n in files], total))
        self._needs_recovery = False
        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _register_batch(self, batch_id: str, now_ts: float) -> None:
        """Freeze the selection for a new batch (ids only) and persist it."""
        count = self._conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]
        ids: list[str] = []
        if count > ONLINE_THRESHOLD:
            cutoff = now_ts - RETENTION_SECONDS
            rows = self._conn.execute(
                "SELECT id FROM tickets WHERE status = 'closed' "
                "AND closed_at < ? ORDER BY id",
                (cutoff,),
            ).fetchall()
            ids = [r[0] for r in rows]
        with self._conn:
            self._conn.execute(
                "INSERT INTO batches (batch_id, now_ts, state, total, files) "
                "VALUES (?, ?, 'registered', ?, '[]')",
                (batch_id, now_ts, len(ids)),
            )
            self._conn.executemany(
                "INSERT INTO batch_selection (batch_id, ticket_id) VALUES (?, ?)",
                [(batch_id, tid) for tid in ids],
            )

    @staticmethod
    def _safe_name(batch_id: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]", "_", batch_id)

    def _file_path(self, name: str) -> str:
        return os.path.join(self.archive_dir, name)

    def _chunk_name(self, batch_id: str, index: int) -> str:
        return f"{self._safe_name(batch_id)}-{index:05d}.json"

    def _run_batch(self, batch_id: str) -> tuple[list[str], int]:
        """Execute the frozen selection chunk by chunk; idempotent."""
        ids = [
            r[0]
            for r in self._conn.execute(
                "SELECT ticket_id FROM batch_selection WHERE batch_id = ? "
                "ORDER BY ticket_id",
                (batch_id,),
            ).fetchall()
        ]
        files: list[str] = []
        for index in range(0, len(ids), CHUNK_SIZE):
            chunk = ids[index : index + CHUNK_SIZE]
            name = self._chunk_name(batch_id, index // CHUNK_SIZE)
            files.append(name)
            path = self._file_path(name)
            if os.path.exists(path):
                # Formal file already published earlier: never rewrite it.
                with open(path, "r", encoding="utf-8") as fh:
                    file_ids = [str(rec["id"]) for rec in json.load(fh)]
            else:
                file_ids = self._publish_chunk(chunk, path)
            self._delete_chunk(batch_id, name, file_ids)
            _failpoint("after_chunk")
        return files, len(ids)

    def _publish_chunk(self, chunk: list[str], path: str) -> list[str]:
        """Write a formal archive file atomically from online records."""
        placeholders = ",".join("?" for _ in chunk)
        rows = self._conn.execute(
            f"SELECT id, body FROM tickets WHERE id IN ({placeholders})",
            chunk,
        ).fetchall()
        bodies = {row[0]: row[1] for row in rows}
        missing = [tid for tid in chunk if tid not in bodies]
        if missing:
            raise RuntimeError(
                f"selected tickets missing from online store: {missing[:3]}..."
            )
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write("[")
            for i, tid in enumerate(chunk):
                if i:
                    fh.write(",")
                fh.write(bodies[tid])
            fh.write("]")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        _failpoint("after_publish")
        return chunk

    def _delete_chunk(
        self, batch_id: str, name: str, file_ids: list[str]
    ) -> None:
        """Record the locator index and remove archived tickets atomically."""
        placeholders = ",".join("?" for _ in file_ids)
        with self._conn:
            self._conn.executemany(
                "INSERT OR REPLACE INTO archive_index "
                "(ticket_id, batch_id, file_name) VALUES (?, ?, ?)",
                [(tid, batch_id, name) for tid in file_ids],
            )
            self._conn.execute(
                f"DELETE FROM tickets WHERE id IN ({placeholders})", file_ids
            )
        _failpoint("after_delete")
