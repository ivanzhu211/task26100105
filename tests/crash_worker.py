"""Subprocess worker for crash-injection tests.

Usage: crash_worker.py <archive|recover> <db_path> <archive_dir> <batch_id>
The failpoint is armed via the TICKET_ARCHIVE_FAILPOINT env var; `now` is
passed via WORKER_NOW so every run uses an identical frozen timestamp.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ticket_archive import TicketArchive


def main() -> None:
    mode, db_path, archive_dir, batch_id = sys.argv[1:5]
    now = os.environ["WORKER_NOW"]
    store = TicketArchive(db_path, archive_dir)
    if mode == "archive":
        store.Recover()  # required no-op when nothing is pending
        files, total = store.Archive(batch_id, now)
        print("RESULT", files, total)
    elif mode == "recover":
        print("RECOVERED", store.Recover())
    else:
        raise SystemExit(f"unknown mode {mode}")
    store.close()


if __name__ == "__main__":
    main()
