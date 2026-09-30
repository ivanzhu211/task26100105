"""Offline ticket archive library backed by SQLite and local JSON files.

Persistence model
-----------------
- SQLite holds the online ticket table (full bodies) plus batch metadata and
  a locator index (ticket id -> archive file name, no bodies).
- The archive directory holds formal archive files named ``*.json``. A formal
  file is written to a temp file, fsynced, then atomically renamed into place;
  once visible it is complete and never modified again.
- A batch's selection is frozen in SQLite at first registration, so a killed
  process can be resumed by Recover() without re-evaluating record counts or
  wall-clock time.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

MAX_ONLINE_BEFORE_ARCHIVE = 100_000
MAX_TICKETS_PER_FILE = 1_000
RETENTION_DAYS = 90
HOURS_PER_DAY = 24

FAILPOINT_ENV = "TICKET_ARCHIVE_FAILPOINT"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tickets (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    closed_at_norm TEXT,
    body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    now_norm TEXT NOT NULL,
    state TEXT NOT NULL,            -- 'in_progress' | 'done'
    result_json TEXT                -- frozen result, set when done
);
CREATE TABLE IF NOT EXISTS batch_items (
    batch_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    ticket_id TEXT NOT NULL,
    file_index INTEGER NOT NULL,
    PRIMARY KEY (batch_id, seq)
);
CREATE TABLE IF NOT EXISTS archive_index (
    ticket_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL
);
"""

_NORM_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def _failpoint(name: str) -> None:
    """Controllable crash point for tests: kills the process when armed."""
    if os.environ.get(FAILPOINT_ENV) == name:
        os._exit(1)


def _parse_utc(value: Any) -> datetime:
    """Parse a datetime or ISO-8601 string into an aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _norm_closed_at(value: Any) -> str | None:
    """Normalize a closed_at value to a fixed-width, lexicographically
    sortable UTC string. Returns None when the ticket has no closed_at."""
    if value is None:
        return None
    return _parse_utc(value).strftime(_NORM_FORMAT)


class TicketArchive:
    """Online ticket store plus offline JSON archive with crash recovery."""

    def __init__(self, db_path: str | os.PathLike, archive_dir: str | os.PathLike) -> None:
        self._db_path = str(db_path)
        self._archive_dir = Path(archive_dir)
        self._archive_dir.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        # After reopening, Recover() must succeed before any other operation.
        self._needs_recovery = self._conn.execute(
            "SELECT EXISTS(SELECT 1 FROM batches WHERE state != 'done')"
        ).fetchone()[0] == 1

    # -- internal helpers -------------------------------------------------

    def _check_ready(self) -> None:
        if self._needs_recovery:
            raise RuntimeError(
                "incomplete batches exist; call Recover() before any other operation"
            )

    def _formal_path(self, file_name: str) -> Path:
        return self._archive_dir / file_name

    def _publish_file(self, file_name: str, records: list[dict]) -> None:
        """Write a formal archive file atomically; never touch an existing one."""
        formal = self._formal_path(file_name)
        if formal.exists():
            return
        tmp = self._archive_dir / (file_name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(records, fh, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, formal)

    def _file_chunks(self, batch_id: str) -> list[tuple[int, list[str]]]:
        """Frozen selection grouped as (file_index, [ticket_id, ...])."""
        rows = self._conn.execute(
            "SELECT ticket_id, file_index FROM batch_items "
            "WHERE batch_id = ? ORDER BY seq",
            (batch_id,),
        ).fetchall()
        chunks: dict[int, list[str]] = {}
        for ticket_id, file_index in rows:
            chunks.setdefault(file_index, []).append(ticket_id)
        return sorted(chunks.items())

    def _load_online_bodies(self, ticket_ids: list[str]) -> list[dict]:
        """Read full records from the online table, preserving input order."""
        found: dict[str, dict] = {}
        for i in range(0, len(ticket_ids), 500):
            part = ticket_ids[i : i + 500]
            placeholders = ",".join("?" for _ in part)
            for tid, body in self._conn.execute(
                f"SELECT id, body FROM tickets WHERE id IN ({placeholders})", part
            ):
                found[tid] = json.loads(body)
        missing = [t for t in ticket_ids if t not in found]
        if missing:
            raise RuntimeError(
                f"frozen selection refers to tickets missing from online store: "
                f"{missing[:5]}"
            )
        return [found[t] for t in ticket_ids]

    def _retire_chunk(self, file_name: str, ticket_ids: list[str]) -> None:
        """Delete a chunk from the online table and record its locator index.

        Only ever called after the formal file containing these exact records
        is visible, so the overlap window keeps every ticket readable.
        """
        with self._conn:
            for i in range(0, len(ticket_ids), 500):
                part = ticket_ids[i : i + 500]
                placeholders = ",".join("?" for _ in part)
                self._conn.execute(
                    f"DELETE FROM tickets WHERE id IN ({placeholders})", part
                )
                self._conn.executemany(
                    "INSERT OR IGNORE INTO archive_index (ticket_id, file_name) "
                    "VALUES (?, ?)",
                    [(t, file_name) for t in part],
                )

    def _run_batch(self, batch_id: str, in_recover: bool) -> tuple[list[str], int]:
        """Execute (or resume) a frozen batch selection to completion."""
        chunks = self._file_chunks(batch_id)
        files: list[str] = []
        total = 0
        for file_index, ticket_ids in chunks:
            file_name = f"{batch_id}_{file_index:05d}.json"
            formal = self._formal_path(file_name)
            if not formal.exists():
                records = self._load_online_bodies(ticket_ids)
                self._publish_file(file_name, records)
                if not in_recover:
                    _failpoint("archive.after_publish")
            # Formal file is complete and readable; now retire the sources.
            self._retire_chunk(file_name, ticket_ids)
            if not in_recover:
                _failpoint("archive.after_delete")
            files.append(str(formal))
            total += len(ticket_ids)
            if in_recover:
                _failpoint("recover.after_file")
        result = {"files": files, "total": total}
        with self._conn:
            self._conn.execute(
                "UPDATE batches SET state = 'done', result_json = ? "
                "WHERE batch_id = ?",
                (json.dumps(result), batch_id),
            )
        return files, total

    # -- public API ---------------------------------------------------------

    def ImportTickets(self, records: Iterable[dict]) -> None:
        """Write tickets into the online store."""
        self._check_ready()
        rows = [
            (
                str(rec["id"]),
                rec["status"],
                _norm_closed_at(rec.get("closed_at")),
                json.dumps(rec, ensure_ascii=False),
            )
            for rec in records
        ]
        with self._conn:
            self._conn.executemany(
                "INSERT INTO tickets (id, status, closed_at_norm, body) "
                "VALUES (?, ?, ?, ?)",
                rows,
            )

    def Archive(self, batch_id: str, now: Any) -> tuple[list[str], int]:
        """Run one archive batch; returns (formal file paths, ticket total).

        The selection is frozen the first time batch_id is persisted; later
        calls with the same batch_id return the recorded result unchanged.
        """
        self._check_ready()
        now_dt = _parse_utc(now)
        row = self._conn.execute(
            "SELECT state, result_json FROM batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        if row is None:
            self._register_batch(batch_id, now_dt)
        else:
            state, result_json = row
            if state == "done":
                result = json.loads(result_json)
                return result["files"], result["total"]
        return self._run_batch(batch_id, in_recover=False)

    def Recover(self) -> list[tuple[list[str], int]]:
        """Finish every persisted but incomplete batch using its frozen
        selection. Safe to call repeatedly and after a kill at any point."""
        pending = [
            r[0]
            for r in self._conn.execute(
                "SELECT batch_id FROM batches WHERE state != 'done' "
                "ORDER BY rowid"
            )
        ]
        results = []
        for batch_id in pending:
            results.append(self._run_batch(batch_id, in_recover=True))
        self._needs_recovery = False
        return results

    def GetTicket(self, ticket_id: str) -> dict | None:
        """Return the full original record from the online store or an
        archive file, wherever the ticket currently lives."""
        self._check_ready()
        row = self._conn.execute(
            "SELECT body FROM tickets WHERE id = ?", (str(ticket_id),)
        ).fetchone()
        if row is not None:
            return json.loads(row[0])
        row = self._conn.execute(
            "SELECT file_name FROM archive_index WHERE ticket_id = ?",
            (str(ticket_id),),
        ).fetchone()
        if row is None:
            return None
        with open(self._formal_path(row[0]), encoding="utf-8") as fh:
            for record in json.load(fh):
                if str(record.get("id")) == str(ticket_id):
                    return record
        return None

    def close(self) -> None:
        self._conn.close()

    # -- batch registration -------------------------------------------------

    def _register_batch(self, batch_id: str, now_dt: datetime) -> None:
        """Persist the batch and freeze its selection in one transaction."""
        online_count = self._conn.execute(
            "SELECT COUNT(*) FROM tickets"
        ).fetchone()[0]
        selected: list[str] = []
        if online_count > MAX_ONLINE_BEFORE_ARCHIVE:
            cutoff = (now_dt - timedelta(days=RETENTION_DAYS)).strftime(_NORM_FORMAT)
            selected = [
                r[0]
                for r in self._conn.execute(
                    "SELECT id FROM tickets WHERE status = 'closed' "
                    "AND closed_at_norm < ? ORDER BY id",
                    (cutoff,),
                )
            ]
        with self._conn:
            self._conn.execute(
                "INSERT INTO batches (batch_id, now_norm, state, result_json) "
                "VALUES (?, ?, 'in_progress', NULL)",
                (batch_id, now_dt.strftime(_NORM_FORMAT)),
            )
            self._conn.executemany(
                "INSERT INTO batch_items (batch_id, seq, ticket_id, file_index) "
                "VALUES (?, ?, ?, ?)",
                [
                    (batch_id, seq, tid, seq // MAX_TICKETS_PER_FILE)
                    for seq, tid in enumerate(selected)
                ],
            )
