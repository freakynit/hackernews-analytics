#!/usr/bin/env python3
"""Refresh the dashboard's pre-computed statistics.

Run this whenever you want the snapshot-backed dashboards (trends, best-time
heatmap, domain quality, records, leaderboards, topic momentum, ...) to reflect
the latest data in ClickHouse:

    python3 refresh_stats.py

Useful options:
    python3 refresh_stats.py --status       show snapshot ages and row counts
    python3 refresh_stats.py --list         list available snapshot jobs
    python3 refresh_stats.py --only yearly monthly
    python3 refresh_stats.py --if-stale 12  refresh only if data is 12h+ old

The server reads these files directly, so it does not need a restart. If the
server is running, just reload the page (front-end API cache is ~60s).
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from server import SNAPSHOT_JOBS, refresh_snapshots, snapshot_summary


def _age_hours(epoch) -> float | None:
    if not epoch:
        return None
    return (time.time() - float(epoch)) / 3600


def _fmt_age(epoch) -> str:
    hours = _age_hours(epoch)
    if hours is None:
        return "never"
    if hours < 1:
        return f"{max(0, round(hours * 60))}m ago"
    if hours < 48:
        return f"{hours:.1f}h ago"
    return f"{hours / 24:.1f}d ago"


def cmd_list() -> None:
    print(f"{'snapshot':<26} description")
    print("-" * 90)
    for name, job in SNAPSHOT_JOBS.items():
        print(f"{name:<26} {job['desc']}")


def cmd_status() -> None:
    summary = snapshot_summary()
    print(f"{'snapshot':<26} {'rows':>8}  {'generated':<14} description")
    print("-" * 100)
    epochs = []
    missing = 0
    for name, meta in summary.items():
        if not meta.get("generated_at"):
            missing += 1
            print(f"{name:<26} {'—':>8}  {'missing':<14} {meta['description']}")
            continue
        epochs.append(meta.get("epoch"))
        print(f"{name:<26} {meta.get('rows', 0):>8}  "
              f"{_fmt_age(meta.get('epoch')):<14} {meta['description']}")
    print("-" * 100)
    if epochs:
        oldest = min(epochs)
        print(f"{len(summary) - missing}/{len(summary)} built · "
              f"oldest {_fmt_age(oldest)} · "
              f"newest {_fmt_age(max(epochs))}")
    else:
        print("no snapshots built yet — run: python3 refresh_stats.py")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Refresh HN Analytics pre-computed snapshots.")
    ap.add_argument("--only", nargs="+", metavar="NAME",
                    help="refresh only the named snapshot jobs")
    ap.add_argument("--list", action="store_true",
                    help="list snapshot jobs and exit")
    ap.add_argument("--status", action="store_true",
                    help="show snapshot ages and exit")
    ap.add_argument("--if-stale", type=float, metavar="HOURS",
                    help="refresh only if the oldest snapshot is older than HOURS")
    ap.add_argument("--quiet", action="store_true",
                    help="only print errors and the final summary")
    ap.add_argument("--json", action="store_true",
                    help="print the refresh result as JSON")
    args = ap.parse_args()

    if args.list:
        cmd_list()
        return 0
    if args.status:
        cmd_status()
        return 0

    names = args.only
    if names:
        unknown = [n for n in names if n not in SNAPSHOT_JOBS]
        if unknown:
            print(f"unknown snapshot job(s): {', '.join(unknown)}", file=sys.stderr)
            print("run --list to see available jobs", file=sys.stderr)
            return 2

    if args.if_stale is not None:
        summary = snapshot_summary()
        if all(m.get("epoch") for m in summary.values()):
            oldest = min(m["epoch"] for m in summary.values())
            age = _age_hours(oldest)
            if age is not None and age < args.if_stale:
                print(f"snapshots are fresh ({_fmt_age(oldest)}); "
                      f"nothing to do (--if-stale {args.if_stale:g}h)")
                return 0

    selected = names or list(SNAPSHOT_JOBS)
    if not args.quiet and not args.json:
        print(f"Refreshing {len(selected)} snapshot(s) …")

    started = time.time()

    def progress(name, result):
        if args.quiet and result.get("ok"):
            return
        stream = sys.stderr if args.json else sys.stdout
        if result.get("ok"):
            print(f"  {name:<26} {result.get('rows', 0):>6} rows  "
                  f"{result.get('elapsed_s', 0):>5.2f}s  ok", file=stream)
        else:
            print(f"  {name:<26} FAILED: {result.get('error', 'unknown error')}",
                  file=sys.stderr)

    result = refresh_snapshots(names, progress=progress)

    if result.get("error"):
        print(f"refresh failed: {result['error']}", file=sys.stderr)
        return 1

    jobs = result.get("jobs", {})
    ok = sum(1 for j in jobs.values() if j.get("ok"))
    failed = len(jobs) - ok
    elapsed = result.get("elapsed_s", time.time() - started)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"Done in {elapsed:g}s — {ok}/{len(jobs)} ok"
              + (f" · {failed} failed" if failed else ""))
        if ok:
            print("Snapshots written to data/snapshots/")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
