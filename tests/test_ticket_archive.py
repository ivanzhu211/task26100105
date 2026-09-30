"""Tests for the offline ticket archive library."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys

import pytest

import ticket_archive as ta_mod
from ticket_archive import DAY_SECONDS, TicketArchive

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNER = os.path.join(REPO_ROOT, "runner.py")
NOW = 1_800_000_000.0
OLD_TS = NOW - 91 * DAY_SECONDS
BOUNDARY_TS = NOW - 90 * DAY_SECONDS  # exactly 90 days: NOT eligible


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def make_store(tmp_path, threshold=None, monkeypatch=None):
    if monkeypatch is not None and threshold is not None:
        monkeypatch.setattr(ta_mod, "ONLINE_THRESHOLD", threshold)
    db = str(tmp_path / "tickets.db")
    arch = str(tmp_path / "archive")
    return TicketArchive(db, arch), db, arch


def snapshot(db_path, archive_dir):
    """Read online rows and every formal archive file."""
    conn = sqlite3.connect(db_path)
    online = {r[0]: json.loads(r[1]) for r in conn.execute("SELECT id, body FROM tickets")}
    conn.close()
    archived = {}
    formal = sorted(f for f in os.listdir(archive_dir) if f.endswith(".json"))
    for name in formal:
        with open(os.path.join(archive_dir, name), encoding="utf-8") as fh:
            data = json.load(fh)  # each formal file parses independently
        assert isinstance(data, list) and len(data) <= 1000
        for rec in data:
            rid = str(rec["id"])
            assert rid not in archived, f"duplicate {rid} across formal files"
            archived[rid] = rec
    return online, archived, formal


def assert_exact_partition(db_path, archive_dir, expected_records):
    """Every imported record lives in exactly one place, content unchanged."""
    online, archived, _ = snapshot(db_path, archive_dir)
    overlap = set(online) & set(archived)
    assert not overlap, f"records in both stores: {sorted(overlap)[:5]}"
    merged = {**online, **archived}
    expected = {str(r["id"]): r for r in expected_records}
    assert merged == expected


def run_runner(args, env_extra=None):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, RUNNER, *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO_ROOT,
    )


def small_dataset():
    """8 open + 5 old closed (eligible) + boundary + recent = 15 records."""
    recs = [{"id": f"open-{i}", "status": "open", "closed_at": None, "n": i} for i in range(8)]
    recs += [
        {"id": f"closed-{i}", "status": "closed", "closed_at": OLD_TS, "v": i}
        for i in range(5)
    ]
    recs.append({"id": "boundary", "status": "closed", "closed_at": BOUNDARY_TS})
    recs.append({"id": "recent", "status": "closed", "closed_at": NOW - 1000})
    return recs


# ----------------------------------------------------------------------
# 1. Large-scale selection, 3 formal files, boundary rules, field readback
# ----------------------------------------------------------------------
def test_large_selection_three_files_and_roundtrip(tmp_path):
    store, db, arch = make_store(tmp_path)
    records = [
        {"id": f"open-{i:06d}", "status": "open", "closed_at": None, "seq": i}
        for i in range(100_000)  # very old open tickets stay online
    ]
    records += [
        {"id": f"closed-{i:06d}", "status": "closed", "closed_at": OLD_TS, "seq": i}
        for i in range(2_500)  # eligible -> spans 3 formal files
    ]
    records.append({"id": "boundary", "status": "closed", "closed_at": BOUNDARY_TS})
    records.append({"id": "recent", "status": "closed", "closed_at": NOW - 60})
    records.append(
        {"id": "iso-closed", "status": "closed", "closed_at": "2025-01-01T00:00:00Z"}
    )
    store.import_tickets(records)
    store.close()

    store, _, _ = make_store(tmp_path)
    files, total = store.archive("batch-big", NOW)
    assert total == 2_501  # 2500 old closed + 1 ISO closed
    assert len(files) == 3
    sizes = []
    for path in files:
        assert path.endswith(".json")
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        sizes.append(len(data))
    assert sizes == [1000, 1000, 501]

    # Field-by-field readback of archived records through GetTicket.
    by_id = {str(r["id"]): r for r in records}
    for i in (0, 1234, 2499):
        rid = f"closed-{i:06d}"
        assert store.get_ticket(rid) == by_id[rid]
    assert store.get_ticket("iso-closed") == by_id["iso-closed"]
    # Boundary / recent / open records remain online and readable.
    assert store.get_ticket("boundary") == by_id["boundary"]
    assert store.get_ticket("recent") == by_id["recent"]
    assert store.get_ticket("open-000000") == by_id["open-000000"]
    store.close()

    assert_exact_partition(db, arch, records)


# ----------------------------------------------------------------------
# 2. Crash recovery via real subprocess kills
# ----------------------------------------------------------------------
def _prepare_crashed_batch(tmp_path, failpoint):
    """Import small dataset, run archive in a subprocess, kill at failpoint."""
    recs = small_dataset()
    store, db, arch = make_store(tmp_path)
    store.import_tickets(recs)
    store.close()
    env = {"TICKET_ARCHIVE_THRESHOLD": "10", "TICKET_ARCHIVE_FAILPOINT": failpoint}
    proc = run_runner(
        ["archive", "--db", db, "--dir", arch, "--batch-id", "b1", "--now", str(NOW)],
        env_extra=env,
    )
    assert proc.returncode != 0, proc.stderr
    return recs, db, arch


def _assert_store_locked_then_recover(db, arch):
    """After reopening, only recover() is accepted until it succeeds."""
    store = TicketArchive(db, arch)
    with pytest.raises(RuntimeError):
        store.import_tickets([])
    with pytest.raises(RuntimeError):
        store.get_ticket("open-0")
    with pytest.raises(RuntimeError):
        store.archive("b1", NOW)
    results = store.recover()
    assert [r[0] for r in results] == ["b1"]
    return store


def test_crash_after_publish_before_delete(tmp_path):
    recs, db, arch = _prepare_crashed_batch(tmp_path, "after_publish")
    # Formal file is visible and complete while its sources are still online.
    formal = [f for f in os.listdir(arch) if f.endswith(".json")]
    assert formal == ["b1-00000.json"]
    with open(os.path.join(arch, formal[0]), encoding="utf-8") as fh:
        assert len(json.load(fh)) == 5
    online, _, _ = snapshot(db, arch)
    assert all(f"closed-{i}" in online for i in range(5))

    store = _assert_store_locked_then_recover(db, arch)
    store.close()
    assert_exact_partition(db, arch, recs)


def test_crash_after_delete_before_return(tmp_path):
    recs, db, arch = _prepare_crashed_batch(tmp_path, "after_delete")
    # Delete committed, batch not marked done, call never returned.
    online, archived, formal = snapshot(db, arch)
    assert formal == ["b1-00000.json"]
    assert not (set(online) & set(archived))
    conn = sqlite3.connect(db)
    state = conn.execute("SELECT state FROM batches WHERE batch_id='b1'").fetchone()[0]
    conn.close()
    assert state == "registered"

    store = _assert_store_locked_then_recover(db, arch)
    # Repeat archive of the finished batch returns the persisted result.
    files, total = store.archive("b1", NOW)
    assert total == 5 and [os.path.basename(f) for f in files] == ["b1-00000.json"]
    store.close()
    assert_exact_partition(db, arch, recs)


def test_crash_during_recovery_then_recover_again(tmp_path):
    recs, db, arch = _prepare_crashed_batch(tmp_path, "after_publish")
    # First recovery attempt is itself killed mid-batch.
    env = {"TICKET_ARCHIVE_THRESHOLD": "10", "TICKET_ARCHIVE_FAILPOINT": "after_chunk"}
    proc = run_runner(["recover", "--db", db, "--dir", arch], env_extra=env)
    assert proc.returncode != 0, proc.stderr
    # Still locked: a fresh open refuses normal operations.
    store = TicketArchive(db, arch)
    with pytest.raises(RuntimeError):
        store.get_ticket("open-0")
    store.close()
    # Second recovery completes the frozen selection.
    store = TicketArchive(db, arch)
    results = store.recover()
    assert [r[0] for r in results] == ["b1"]
    _, _, formal = snapshot(db, arch)
    assert formal == ["b1-00000.json"]
    store.close()
    assert_exact_partition(db, arch, recs)


def test_recovery_finishes_frozen_selection_below_threshold(tmp_path):
    """Partial deletion dropping online count below threshold must not
    abandon the rest of the frozen selection."""
    recs = [
        {"id": f"open-{i}", "status": "open", "closed_at": None} for i in range(8)
    ]
    recs += [
        {"id": f"closed-{i:03d}", "status": "closed", "closed_at": OLD_TS, "v": i}
        for i in range(2_500)  # 3 chunks; threshold 10 -> all frozen
    ]
    store, db, arch = make_store(tmp_path)
    store.import_tickets(recs)
    store.close()
    # Kill after the first chunk's delete: online count drops to ~8+2000.
    env = {"TICKET_ARCHIVE_THRESHOLD": "10", "TICKET_ARCHIVE_FAILPOINT": "after_delete"}
    proc = run_runner(
        ["archive", "--db", db, "--dir", arch, "--batch-id", "big", "--now", str(NOW)],
        env_extra=env,
    )
    assert proc.returncode != 0, proc.stderr
    store = TicketArchive(db, arch)
    store.recover()
    _, _, formal = snapshot(db, arch)
    assert formal == ["big-00000.json", "big-00001.json", "big-00002.json"]
    store.close()
    assert_exact_partition(db, arch, recs)


# ----------------------------------------------------------------------
# 3. batch_id reuse never re-archives
# ----------------------------------------------------------------------
def test_finished_batch_ignores_new_matching_tickets(tmp_path, monkeypatch):
    store, db, arch = make_store(tmp_path, threshold=10, monkeypatch=monkeypatch)
    recs = small_dataset()
    store.import_tickets(recs)
    files, total = store.archive("b1", NOW)
    assert total == 5

    # New eligible tickets arrive after the batch finished.
    extra = [
        {"id": f"late-{i}", "status": "closed", "closed_at": OLD_TS, "v": i}
        for i in range(20)
    ]
    store.import_tickets(extra)
    files2, total2 = store.archive("b1", NOW)
    assert (files2, total2) == (files, total)
    for i in range(20):
        assert store.get_ticket(f"late-{i}")["id"] == f"late-{i}"  # still online
    store.close()
    assert_exact_partition(db, arch, recs + extra)


def test_empty_batch_is_persistent_and_replayable(tmp_path, monkeypatch):
    store, db, arch = make_store(tmp_path, threshold=10, monkeypatch=monkeypatch)
    recs = small_dataset()[:5]  # only 5 online -> empty selection
    store.import_tickets(recs)
    files, total = store.archive("empty-batch", NOW)
    assert (files, total) == ([], 0)
    assert not [f for f in os.listdir(arch) if f.endswith(".json")]

    # Later imports that would qualify must not be absorbed by the old batch.
    extra = [
        {"id": f"late-{i}", "status": "closed", "closed_at": OLD_TS}
        for i in range(20)
    ]
    store.import_tickets(extra)
    files2, total2 = store.archive("empty-batch", NOW)
    assert (files2, total2) == ([], 0)
    assert not [f for f in os.listdir(arch) if f.endswith(".json")]
    store.close()

    # Result survives reopening.
    store, _, _ = make_store(tmp_path, threshold=10, monkeypatch=monkeypatch)
    assert store.archive("empty-batch", NOW) == ([], 0)
    store.close()
    assert_exact_partition(db, arch, recs + extra)
