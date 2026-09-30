"""Subprocess runner used by fault-injection tests.

Usage:
    python3 runner.py archive --db DB --dir DIR --batch-id B --now TS
    python3 runner.py recover --db DB --dir DIR

Set TICKET_ARCHIVE_FAILPOINT to one of: after_publish, after_delete,
after_chunk -- the process hard-exits (os._exit) at that point,
simulating kill -9.
"""

from __future__ import annotations

import argparse
import json
import sys

from ticket_archive import TicketArchive


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["archive", "recover"])
    parser.add_argument("--db", required=True)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--batch-id")
    parser.add_argument("--now", type=float)
    args = parser.parse_args()

    ta = TicketArchive(args.db, args.dir)
    try:
        if args.command == "archive":
            files, total = ta.archive(args.batch_id, args.now)
            print(json.dumps({"files": files, "total": total}))
        else:
            results = ta.recover()
            print(json.dumps({"recovered": [r[0] for r in results]}))
    finally:
        ta.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
