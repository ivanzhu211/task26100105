"""Tests for the offline ticket archive library."""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ticket_archive import TicketArchive

NOW = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
NOW_ISO = NOW.isoformat()
WORKER = str(Path(__file__).resolve().parent / "crash_worker.py")

NUM_FILLER_OPEN = 98_000
NUM_CANDIDATES = 2_500  # spans three formal files: 1000 + 1000 + 500


def make_record(tid, status, closed_at, **extra):
    rec = {"id": tid, "status": status, "closed_at": closed_at}
    rec.update(extra)
    return rec


def build_records():
    """The full expected dataset: id -> original record."""
    expected = {}
    old_closed = (NOW - timedelta(days=100)).isoformat()
    ancient = (NOW - timedelta(days=3650)).isoformat()
    for i in range(NUM_FILLER_OPEN):
        tid = f"open-{i:06d}"
        # open tickets created very long ago must never be archived
        expected[tid] = make_record(tid, "open", None, created_at=ancient, seq=i)
    for i in range(NUM_CANDIDATES):
        tid = f"closed-old-{i:06d}"
        expected[tid] = make_record(
            tid, "closed", old_closed, payload={"n": i, "tags": ["a", "b"]}
        )
    # closed exactly at the 90-day boundary: strictly-earlier rule excludes it
    boundary = (NOW - timedelta(days=90)).isoformat()
    expected["closed-boundary"] = make_record(
        "closed-boundary", "closed", boundary, marker="boundary"
    )
    recent = (NOW - timedelta(days=89, hours=23)).isoformat()
    expected["closed-recent"] = make_record(
        "closed-recent", "closed", recent, marker="recent"
    )
    return expected


def import_in_chunks(store, records, chunk=5_000):
    items = list(records)
    for i in range(0, len(items), chunk):
        store.ImportTickets(items[i : i + chunk])


@pytest.fixture(scope="module")
def base_db(tmp_path_factory):
    """A seeded online store with > 100000 tickets, built once per module."""
    root = tmp_path_factory.mktemp("base")
    db_path = root / "tickets.db"
    archive_dir = root / "archive"
    expected = build_records()
    store = TicketArchive(db_path, archive_dir)
    import_in_chunks(store, expected.values())
    store.close()
    return {"db": str(db_path), "expected": expected}


@pytest.fixture()
def case_dir(tmp_path, base_db):
    """Per-test copy of the seeded database plus a fresh archive dir."""
    db_path = tmp_path / "tickets.db"
    shutil.copy(base_db["db"], db_path)
    archive_dir = tmp_path / "archive"
    return {
        "db": str(db_path),
        "archive": str(archive_dir),
        "expected": base_db["expected"],
    }


def run_worker(mode, db_path, archive_dir, batch_id, failpoint=None):
    env = dict(os.environ, WORKER_NOW=NOW_ISO)
    if failpoint:
        env["TICKET_ARCHIVE_FAILPOINT"] = failpoint
    return subprocess.run(
        [sys.executable, WORKER, mode, db_path, archive_dir, batch_id],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def load_formal_archives(archive_dir):
    """Parse every formal (*.json) archive file; fail on any malformed one."""
    archived = {}
    files = sorted(Path(archive_dir).glob("*.json"))
    for path in files:
        with open(path, encoding="utf-8") as fh:
            records = json.load(fh)  # must be independently parseable
        assert isinstance(records, list)
        assert len(records) <= 1_000
        for rec in records:
            assert rec["id"] not in archived, f"duplicate archive record {rec['id']}"
            archived[rec["id"]] = rec
    return files, archived


def check_invariants(db_path, archive_dir, expected):
    """Every imported ticket lives in exactly one place, content unchanged."""
    conn = sqlite3.connect(db_path)
    online = {tid: json.loads(body) for tid, body in conn.execute("SELECT id, body FROM tickets")}
    index = dict(conn.execute("SELECT ticket_id, file_name FROM archive_index"))
    conn.close()

    files, archived = load_formal_archives(archive_dir)

    overlap = set(online) & set(archived)
    assert not overlap, f"tickets in both stores: {list(overlap)[:5]}"
    union = {**online, **archived}
    assert set(union) == set(expected), "tickets lost or invented"
    for tid, rec in expected.items():
        assert union[tid] == rec, f"content changed for {tid}"
    # locator index: every archived id indexed, no archived body left online
    assert set(index) == set(archived)
    for tid, fname in index.items():
        assert (Path(archive_dir) / fname).exists()
    return online, archived, files


# ---------------------------------------------------------------- selection


def test_selection_and_readback(case_dir):
    store = TicketArchive(case_dir["db"], case_dir["archive"])
    files, total = store.Archive("batch-main", NOW_ISO)
    store.close()

    assert total == NUM_CANDIDATES
    assert len(files) == 3
    sizes = []
    for f in files:
        assert f.endswith(".json")
        with open(f, encoding="utf-8") as fh:
            sizes.append(len(json.load(fh)))
    assert sizes == [1_000, 1_000, 500]

    online, archived, _ = check_invariants(
        case_dir["db"], case_dir["archive"], case_dir["expected"]
    )
    # boundary / recent closed and ancient open tickets stay online
    assert "closed-boundary" in online
    assert "closed-recent" in online
    assert "open-000000" in online
    assert set(archived) == {
        f"closed-old-{i:06d}" for i in range(NUM_CANDIDATES)
    }

    # GetTicket reads both worlds back field by field
    store = TicketArchive(case_dir["db"], case_dir["archive"])
    for tid in ("closed-old-000000", "closed-old-001500", "closed-old-002499"):
        assert store.GetTicket(tid) == case_dir["expected"][tid]
    for tid in ("closed-boundary", "closed-recent", "open-000001"):
        assert store.GetTicket(tid) == case_dir["expected"][tid]
    assert store.GetTicket("missing") is None
    store.close()


# ------------------------------------------------------------- crash cases


def test_crash_after_publish_before_delete(case_dir):
    """Killed after a formal file is visible but its sources are not deleted."""
    proc = run_worker(
        "archive", case_dir["db"], case_dir["archive"], "b1",
        failpoint="archive.after_publish",
    )
    assert proc.returncode != 0

    # intermediate state: first formal file complete, sources still online
    files, archived = load_formal_archives(case_dir["archive"])
    assert len(files) == 1 and len(archived) == 1_000
    conn = sqlite3.connect(case_dir["db"])
    still_online = conn.execute(
        "SELECT COUNT(*) FROM tickets WHERE id LIKE 'closed-old-%'"
    ).fetchone()[0]
    conn.close()
    assert still_online == NUM_CANDIDATES

    # reopening demands Recover() before anything else
    store = TicketArchive(case_dir["db"], case_dir["archive"])
    with pytest.raises(RuntimeError):
        store.GetTicket("closed-old-000000")
    store.close()

    proc = run_worker("recover", case_dir["db"], case_dir["archive"], "b1")
    assert proc.returncode == 0, proc.stderr
    online, archived, files = check_invariants(
        case_dir["db"], case_dir["archive"], case_dir["expected"]
    )
    assert len(files) == 3 and len(archived) == NUM_CANDIDATES


def test_crash_after_delete_before_return(case_dir):
    """Killed after source deletion committed but before Archive returned."""
    proc = run_worker(
        "archive", case_dir["db"], case_dir["archive"], "b1",
        failpoint="archive.after_delete",
    )
    assert proc.returncode != 0

    # intermediate state: first chunk retired, rest still online
    conn = sqlite3.connect(case_dir["db"])
    still_online = conn.execute(
        "SELECT COUNT(*) FROM tickets WHERE id LIKE 'closed-old-%'"
    ).fetchone()[0]
    indexed = conn.execute("SELECT COUNT(*) FROM archive_index").fetchone()[0]
    conn.close()
    assert still_online == NUM_CANDIDATES - 1_000
    assert indexed == 1_000

    proc = run_worker("recover", case_dir["db"], case_dir["archive"], "b1")
    assert proc.returncode == 0, proc.stderr
    online, archived, files = check_invariants(
        case_dir["db"], case_dir["archive"], case_dir["expected"]
    )
    assert len(files) == 3 and len(archived) == NUM_CANDIDATES


def test_crash_during_recover(case_dir):
    """Recovery itself is killed midway, then resumed by another Recover."""
    proc = run_worker(
        "archive", case_dir["db"], case_dir["archive"], "b1",
        failpoint="archive.after_delete",
    )
    assert proc.returncode != 0

    proc = run_worker(
        "recover", case_dir["db"], case_dir["archive"], "b1",
        failpoint="recover.after_file",
    )
    assert proc.returncode != 0  # recovery died partway through

    proc = run_worker("recover", case_dir["db"], case_dir["archive"], "b1")
    assert proc.returncode == 0, proc.stderr
    online, archived, files = check_invariants(
        case_dir["db"], case_dir["archive"], case_dir["expected"]
    )
    assert len(files) == 3 and len(archived) == NUM_CANDIDATES
    # no leftover temp files masquerading as archives
    leftovers = [p.name for p in Path(case_dir["archive"]).iterdir()
                 if not p.name.endswith(".json")]
    for name in leftovers:
        assert name.endswith(".tmp")


# -------------------------------------------------------------- idempotence


def test_completed_batch_ignores_new_imports(case_dir):
    store = TicketArchive(case_dir["db"], case_dir["archive"])
    files1, total1 = store.Archive("b1", NOW_ISO)
    assert total1 == NUM_CANDIDATES

    # new fully-qualified tickets imported after the batch completed
    old = (NOW - timedelta(days=200)).isoformat()
    extra = [make_record(f"late-{i}", "closed", old) for i in range(10)]
    store.ImportTickets(extra)

    files2, total2 = store.Archive("b1", NOW_ISO)
    assert (files2, total2) == (files1, total1)
    # late tickets were not absorbed into the old batch
    for rec in extra:
        assert store.GetTicket(rec["id"]) == rec
    files, archived = load_formal_archives(case_dir["archive"])
    assert len(files) == 3
    assert not any(tid.startswith("late-") for tid in archived)
    store.close()


def test_empty_batch_replay(tmp_path):
    db = tmp_path / "t.db"
    arch = tmp_path / "arch"
    store = TicketArchive(db, arch)
    # below the 100000 threshold: selection is empty but still persisted
    store.ImportTickets(
        [make_record(f"t-{i}", "closed", (NOW - timedelta(days=365)).isoformat())
         for i in range(50)]
    )
    assert store.Archive("empty-batch", NOW_ISO) == ([], 0)

    # new qualified imports must not be absorbed by replaying the old batch id
    store.ImportTickets(
        [make_record(f"new-{i}", "closed", (NOW - timedelta(days=365)).isoformat())
         for i in range(5)]
    )
    assert store.Archive("empty-batch", NOW_ISO) == ([], 0)
    assert list(Path(arch).glob("*.json")) == []
    assert store.GetTicket("new-0")["id"] == "new-0"
    store.close()

    # empty result survives reopen + recover and still replays identically
    store = TicketArchive(db, arch)
    assert store.Recover() == []
    assert store.Archive("empty-batch", NOW_ISO) == ([], 0)
    store.close()
