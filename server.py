"""Hacker News Analytics dashboard server.

- Serves a single-page analytics frontend (index.html).
- Proxies safe read-only queries to the free ClickHouse playground
  (https://play.clickhouse.com, table hackernews_history).
- Pre-computes heavy historical aggregates into data/snapshots/*.json.
  Refresh them any time with:  python3 refresh_stats.py

Stdlib only. Run:  python3 server.py  [--port 8000] [--warm-cache] [--no-warm]
"""
from __future__ import annotations

import argparse
import contextvars
import os
import sqlite3
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import html
import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SNAP_DIR = BASE_DIR / "data" / "snapshots"
SNAP_DIR.mkdir(parents=True, exist_ok=True)

CLICKHOUSE_URL = "https://play.clickhouse.com/"
CLICKHOUSE_USER = "play"
LIVE_TTL_S = 120                 # on-the-fly query cache
HTTP_TIMEOUT = 90

_provenance = contextvars.ContextVar("provenance", default=None)

def record_source(source, refreshed_at, dataset):
    records = _provenance.get()
    if records is not None:
        entry = {"source": source, "refreshed_at": refreshed_at, "dataset": dataset}
        if entry not in records:
            records.append(entry)

def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

_mem: dict[str, tuple[float, object, list]] = {}
_mem_lock = threading.Lock()
_snap_cache: dict[str, tuple[float, dict]] = {}
_snap_cache_lock = threading.Lock()
_snap_job_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=6, thread_name_prefix="hn")


# ---------------------------------------------------------------- ClickHouse
def ch_query(sql: str, timeout: int = HTTP_TIMEOUT) -> dict:
    """Run a read-only query against play.clickhouse.com, return parsed JSON."""
    sql = sql.strip().rstrip(";")
    params = urllib.parse.urlencode(
        {"user": CLICKHOUSE_USER, "default_format": "JSON"}
    )
    req = urllib.request.Request(
        f"{CLICKHOUSE_URL}?{params}",
        data=sql.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain;charset=UTF-8",
                 "User-Agent": "hn-analytics/2.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            result = json.loads(r.read().decode("utf-8"))
            record_source("live query", utc_now(), "ClickHouse archive")
            return result
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:2000]
        raise RuntimeError(f"ClickHouse HTTP {e.code}: {body}") from e


def cached(key: str, ttl: int, fn):
    now = time.time()
    with _mem_lock:
        hit = _mem.get(key)
        if hit and now - hit[0] < ttl:
            for entry in hit[2]:
                record_source(**entry)
            return hit[1]
    parent = _provenance.get()
    records = []
    token = _provenance.set(records)
    try:
        val = fn()
    finally:
        _provenance.reset(token)
        if parent is not None:
            parent.extend(r for r in records if r not in parent)
    with _mem_lock:
        _mem[key] = (time.time(), val, records)
    return val


def par(*fns):
    """Run independent query functions concurrently (network-bound)."""
    futures = [_pool.submit(contextvars.copy_context().run, fn) for fn in fns]
    return [f.result() for f in futures]


# ------------------------------------------------------- snapshot definitions
# These are the pricey, historical aggregates. Each one is refreshed by
# refresh_stats.py and served from disk. Queries are deliberately filtered to
# visible content so leaderboards and charts do not over-count deleted items.
SNAPSHOT_JOBS = {
    "totals": {
        "desc": "Dataset totals, author count, deleted/dead counts",
        "sql": """
            SELECT count() total, countIf(type='story') stories,
                   countIf(type='comment') comments, countIf(type='job') jobs,
                   countIf(type='poll') polls,
                   uniqExactIf(by, by != '') users,
                   countIf(deleted) deleted, countIf(dead) dead,
                   max(time) max_time, max(update_time) max_update
            FROM hackernews_history
        """,
    },
    "deltas": {
        "desc": "24h / 7d volume and previous-period comparisons",
        "sql": """
            SELECT
              countIf(time >= now() - INTERVAL 1 DAY) d1,
              countIf(time >= now() - INTERVAL 2 DAY
                      AND time < now() - INTERVAL 1 DAY) pd1,
              countIf(time >= now() - INTERVAL 7 DAY) d7,
              countIf(time >= now() - INTERVAL 14 DAY
                      AND time < now() - INTERVAL 7 DAY) pd7,
              countIf(type='story' AND time >= now() - INTERVAL 7 DAY) s7,
              countIf(type='story' AND time >= now() - INTERVAL 14 DAY
                      AND time < now() - INTERVAL 7 DAY) ps7,
              countIf(type='comment' AND time >= now() - INTERVAL 7 DAY) c7,
              countIf(type='comment' AND time >= now() - INTERVAL 14 DAY
                      AND time < now() - INTERVAL 7 DAY) pc7
            FROM hackernews_history
        """,
    },
    "yearly": {
        "desc": "Stories and comments by year",
        "sql": """
            SELECT toYear(time) y, sum(type='story') stories,
                   sum(type='comment') comments, count() total
            FROM hackernews_history
            WHERE time >= '2006-01-01'
            GROUP BY y ORDER BY y
        """,
    },
    "monthly": {
        "desc": "Stories and comments by month",
        "sql": """
            SELECT toStartOfMonth(time) m, sum(type='story') stories,
                   sum(type='comment') comments,
                   countIf(type='story' AND NOT deleted AND NOT dead)
                       stories_visible,
                   count() total
            FROM hackernews_history
            WHERE time >= '2006-10-01'
            GROUP BY m ORDER BY m
        """,
    },
    "story_stats": {
        "desc": "Score distribution summary for visible stories",
        "sql": """
            SELECT avg(score) avg_score, quantile(0.5)(score) med_score,
                   quantile(0.9)(score) p90_score,
                   avg(descendants) avg_comments,
                   countIf(descendants = 0) zero_comments, count() c
            FROM hackernews_history
            WHERE type='story' AND NOT deleted AND NOT dead
        """,
    },
    "score_distribution": {
        "desc": "Story score histogram buckets",
        "sql": """
            SELECT multiIf(score < 10, '0-9', score < 25, '10-24',
                           score < 50, '25-49', score < 100, '50-99',
                           score < 250, '100-249', score < 500, '250-499',
                           score < 1000, '500-999', '1000+') b,
                   count() c, min(score) lo
            FROM hackernews_history
            WHERE type='story' AND NOT deleted AND NOT dead AND score >= 0
            GROUP BY b ORDER BY lo
        """,
    },
    "best_time": {
        "desc": "Story count and average score by weekday x hour (UTC)",
        "sql": """
            SELECT toDayOfWeek(time) w, toHour(time) h, count() c,
                   avg(score) avg_score
            FROM hackernews_history FINAL
            WHERE type='story' AND NOT deleted AND NOT dead
              AND time >= '2006-01-01'
            GROUP BY w, h ORDER BY w, h
        """,
    },
    "post_kinds": {
        "desc": "Performance of Ask HN / Show HN / link / text posts",
        "sql": """
            SELECT multiIf(title ILIKE 'Ask HN:%', 'Ask HN',
                           title ILIKE 'Show HN:%', 'Show HN',
                           title ILIKE 'Tell HN:%', 'Tell HN',
                           title ILIKE 'Launch HN:%', 'Launch HN',
                           url = '', 'Text post', 'Link post') kind,
                   count() c, avg(score) avg_score,
                   avg(descendants) avg_comments
            FROM hackernews_history
            WHERE type='story' AND NOT deleted AND NOT dead
            GROUP BY kind ORDER BY c DESC
        """,
    },
    "topic_trends": {
        "desc": "Monthly story-title keyword counts for trend analysis",
        "sql": r"""
            SELECT toStartOfMonth(time) m,
              countIf(match(title, '(?i)\\bai\\b|openai')) ai,
              countIf(match(title, '(?i)\\bllms?\\b')) llm,
              countIf(match(title, '(?i)\\brust\\b')) rust,
              countIf(match(title, '(?i)kubernetes|\\bk8s\\b')) kubernetes,
              countIf(match(title, '(?i)climate')) climate,
              countIf(match(title, '(?i)crypto|bitcoin|ethereum')) crypto,
              countIf(match(title, '(?i)startup')) startup
            FROM hackernews_history
            WHERE type='story' AND NOT deleted AND NOT dead
              AND time >= '2010-01-01'
            GROUP BY m ORDER BY m
        """,
    },
    "top_domains_all": {
        "desc": "Top 100 domains by visible story count",
        "sql": """
            SELECT domain(url) d, count() c, avg(score) avg_score,
                   max(score) max_score, sum(descendants) comments
            FROM hackernews_history
            WHERE type='story' AND url != '' AND NOT deleted AND NOT dead
            GROUP BY d ORDER BY c DESC LIMIT 100
        """,
    },
    "domain_quality": {
        "desc": "Highest-rated domains with at least 300 stories",
        "sql": """
            SELECT domain(url) d, count() c, avg(score) avg_score,
                   max(score) max_score, sum(descendants) comments
            FROM hackernews_history
            WHERE type='story' AND url != '' AND NOT deleted AND NOT dead
            GROUP BY d HAVING c >= 300
            ORDER BY avg_score DESC LIMIT 50
        """,
    },
    "domain_low": {
        "desc": "Lowest-rated high-volume domains (1,000+ stories)",
        "sql": """
            SELECT domain(url) d, count() c, avg(score) avg_score,
                   max(score) max_score
            FROM hackernews_history
            WHERE type='story' AND url != '' AND NOT deleted AND NOT dead
            GROUP BY d HAVING c >= 1000
            ORDER BY avg_score ASC LIMIT 30
        """,
    },
    "top_submitters_all": {
        "desc": "Top 100 story submitters with karma and discussion totals",
        "sql": """
            SELECT by u, count() c, avg(score) avg_score, sum(score) karma,
                   sum(descendants) comments
            FROM hackernews_history
            WHERE type='story' AND by != '' AND NOT deleted AND NOT dead
            GROUP BY u ORDER BY c DESC LIMIT 100
        """,
    },
    "top_karma_all": {
        "desc": "Top 100 story submitters by total score",
        "sql": """
            SELECT by u, count() c, avg(score) avg_score, sum(score) karma,
                   sum(descendants) comments
            FROM hackernews_history
            WHERE type='story' AND by != '' AND NOT deleted AND NOT dead
            GROUP BY u ORDER BY karma DESC LIMIT 100
        """,
    },
    "top_commenters_all": {
        "desc": "Top 100 commenters by visible comment count",
        "sql": """
            SELECT by u, count() c
            FROM hackernews_history
            WHERE type='comment' AND by != '' AND NOT deleted AND NOT dead
            GROUP BY u ORDER BY c DESC LIMIT 100
        """,
    },
    "rising_domains": {
        "desc": "Domain submission growth: last 30d vs prior 30d",
        "sql": """
            SELECT domain(url) d,
                   countIf(time >= now() - INTERVAL 30 DAY) cur,
                   countIf(time >= now() - INTERVAL 60 DAY
                           AND time < now() - INTERVAL 30 DAY) prev
            FROM hackernews_history
            WHERE type='story' AND url != '' AND NOT deleted AND NOT dead
              AND time >= now() - INTERVAL 60 DAY
            GROUP BY d HAVING cur >= 30
            ORDER BY cur / greatest(prev, 1) DESC, cur DESC LIMIT 50
        """,
    },
    "top_stories_score": {
        "desc": "Top 100 all-time stories by score",
        "sql": """
            SELECT id, title, url, score, by, time, descendants,
                   toString(update_time) AS _u
            FROM hackernews_history
            WHERE type='story' AND NOT deleted AND NOT dead AND title != ''
            ORDER BY score DESC LIMIT 300
        """,
        "dedupe": 100,
    },
    "top_stories_discussed": {
        "desc": "Top 100 all-time stories by comment count",
        "sql": """
            SELECT id, title, url, score, by, time, descendants,
                   toString(update_time) AS _u
            FROM hackernews_history
            WHERE type='story' AND NOT deleted AND NOT dead AND title != ''
            ORDER BY descendants DESC LIMIT 300
        """,
        "dedupe": 100,
    },
}


# ------------------------------------------------------------ snapshot I/O
def dedupe(rows: list, limit: int) -> list:
    """Keep only the latest version of each item id, preserving order."""
    best: dict[int, dict] = {}
    for r in rows:
        i = r.get("id")
        if i not in best or str(r.get("_u", "")) > str(best[i].get("_u", "")):
            best[i] = r
    out = list(best.values())
    for r in out:
        r.pop("_u", None)
    return out[:limit]


def _snapshot_path(name: str) -> Path:
    return SNAP_DIR / f"{name}.json"


def snapshot_payload(name: str):
    p = _snapshot_path(name)
    try:
        mtime = p.stat().st_mtime_ns
    except OSError:
        return None
    with _snap_cache_lock:
        hit = _snap_cache.get(name)
        if hit and hit[0] == mtime:
            return hit[1]
    try:
        payload = json.loads(p.read_text())
    except Exception:
        return None
    with _snap_cache_lock:
        _snap_cache[name] = (mtime, payload)
    return payload


def job_rows(name: str) -> list:
    """Snapshot rows when present; otherwise compute live and cache briefly."""
    payload = snapshot_payload(name)
    if payload is not None:
        record_source("snapshot", (payload.get("_meta") or {}).get("generated_at"), name)
        return payload.get("data", [])

    def go():
        job = SNAPSHOT_JOBS[name]
        rows = ch_query(job["sql"])["data"]
        if job.get("dedupe"):
            rows = dedupe(rows, job["dedupe"])
        return rows

    return cached(f"job:{name}", 10 * 60, go)


def job_meta(name: str):
    payload = snapshot_payload(name)
    if payload:
        return payload.get("_meta")
    return None


def snapshot_summary() -> dict:
    out = {}
    for name, job in SNAPSHOT_JOBS.items():
        meta = job_meta(name) or {}
        out[name] = {
            "description": job["desc"],
            "generated_at": meta.get("generated_at"),
            "epoch": meta.get("epoch"),
            "rows": meta.get("rows"),
        }
    return out


def run_snapshot_job(name: str) -> dict:
    job = SNAPSHOT_JOBS[name]
    t0 = time.time()
    rows = ch_query(job["sql"])["data"]
    if job.get("dedupe"):
        rows = dedupe(rows, job["dedupe"])
    meta = {
        "name": name,
        "description": job["desc"],
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "epoch": round(time.time(), 3),
        "elapsed_s": round(time.time() - t0, 2),
        "rows": len(rows),
    }
    payload = {"_meta": meta, "data": rows}
    p = _snapshot_path(name)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False))
    tmp.replace(p)
    return meta


def refresh_snapshots(names=None, progress=None) -> dict:
    """Refresh all (or selected) snapshots. Returns a per-job summary."""
    names = list(names or SNAPSHOT_JOBS.keys())
    if not _snap_job_lock.acquire(blocking=False):
        return {"ok": False, "error": "a snapshot refresh is already running",
                "elapsed_s": 0, "jobs": {}}
    t0 = time.time()
    jobs = {}
    try:
        for name in names:
            if name not in SNAPSHOT_JOBS:
                jobs[name] = {"ok": False, "error": "unknown snapshot job"}
                continue
            try:
                meta = run_snapshot_job(name)
                jobs[name] = {"ok": True, **meta}
            except Exception as e:
                jobs[name] = {"ok": False, "error": str(e)[:400]}
            if progress:
                progress(name, jobs[name])
        return {
            "ok": all(j.get("ok") for j in jobs.values()),
            "elapsed_s": round(time.time() - t0, 1),
            "jobs": jobs,
        }
    finally:
        _snap_job_lock.release()


# ---------------------------------------------------------------- SQL safety
FORBIDDEN = re.compile(
    r"\b(INSERT|ALTER|CREATE|DROP|DELETE|UPDATE|TRUNCATE|GRANT|REVOKE|"
    r"KILL|SHUTDOWN|SYSTEM|ATTACH|DETACH|OPTIMIZE|BACKUP|RESTORE)\b",
    re.IGNORECASE,
)


def guard_sql(sql: str) -> str:
    s = sql.strip().rstrip(";").strip()
    if not s:
        raise ValueError("Empty query")
    if not re.match(r"(?is)^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN)\b", s):
        raise ValueError("Only read-only SELECT/WITH/SHOW/DESCRIBE/EXPLAIN allowed")
    if FORBIDDEN.search(s):
        raise ValueError("Write/DDL statements are blocked")
    if "hackernews" not in s.lower() and not re.match(
        r"(?is)^\s*(SHOW|DESCRIBE|DESC|EXPLAIN)\b", s
    ):
        raise ValueError("Queries must touch the hackernews_history table")
    # Only row-returning statements accept LIMIT; DESCRIBE/EXPLAIN reject it.
    if re.match(r"(?is)^\s*(SELECT|WITH)\b", s):
        if not re.search(r"(?i)\bLIMIT\b", s):
            s += " LIMIT 500"
        m = re.search(r"(?i)\bLIMIT\s+(\d+)", s)
        if m and int(m.group(1)) > 1000:
            s = s[: m.start(1)] + "1000" + s[m.end(1):]
    return s


def _esc(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def _int(value, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(int(value), hi))
    except (TypeError, ValueError):
        return default


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_date(value) -> str | None:
    """Validate a YYYY-MM-DD param, returning the normalized string or None."""
    v = (value or "").strip()
    if not v:
        return None
    if not DATE_RE.match(v):
        raise ValueError("Invalid date: use YYYY-MM-DD")
    try:
        datetime.strptime(v, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Invalid date: use YYYY-MM-DD") from None
    if v < "2006-01-01" or v > "2100-01-01":
        raise ValueError("Date out of range (2006-01-01 to 2100-01-01)")
    return v


def _range_filter(days=0, from_s="", to_s="", *, default_days=30):
    """Build a time-range SQL fragment.

    Returns (where_sql, from_d, to_d, days_eff, cache_key). An empty
    where_sql means all time. Explicit from/to params take precedence
    over the relative `days` window.
    """
    from_d = _parse_date(from_s)
    to_d = _parse_date(to_s)
    if from_d and to_d and from_d > to_d:
        raise ValueError("'from' must be on or before 'to'")
    if from_d or to_d:
        conds = []
        if from_d:
            conds.append(f"time >= toDateTime('{from_d} 00:00:00')")
        if to_d:
            conds.append(f"time < toDateTime('{to_d} 00:00:00') + INTERVAL 1 DAY")
        if from_d and to_d:
            d1 = datetime.strptime(from_d, "%Y-%m-%d")
            d2 = datetime.strptime(to_d, "%Y-%m-%d")
            days_eff = max(1, (d2 - d1).days + 1)
        else:
            days_eff = _int(days, default_days, 1, 3650)
        key = f"{from_d or ''}:{to_d or ''}"
        return " AND ".join(conds), from_d, to_d, days_eff, key
    days_eff = _int(days, default_days, 0, 3650)
    if days_eff == 0:
        return "", None, None, 0, "all"
    return f"time >= now() - INTERVAL {days_eff} DAY", None, None, days_eff, f"{days_eff}d"


def _range_label(from_d, to_d, days_eff) -> str:
    if from_d or to_d:
        return f"{from_d or '…'} to {to_d or '…'}"
    if days_eff == 0:
        return "all time"
    return f"last {days_eff}d"


# ------------------------------------------------------------------- API
def api_totals() -> dict:
    try:
        return cached("totals", 60,
                      lambda: ch_query(SNAPSHOT_JOBS["totals"]["sql"])["data"][0])
    except Exception:
        rows = job_rows("totals")
        if rows:
            return rows[0]
        raise


def api_deltas() -> dict:
    try:
        return cached("deltas", 60,
                      lambda: ch_query(SNAPSHOT_JOBS["deltas"]["sql"])["data"][0])
    except Exception:
        rows = job_rows("deltas")
        if rows:
            return rows[0]
        raise


def api_status() -> dict:
    snaps = snapshot_summary()
    epochs = [s["epoch"] for s in snaps.values() if s.get("epoch")]
    try:
        ch_query("SELECT 1", timeout=15)
        totals = api_totals()
        error = None
    except Exception as e:
        error = str(e)[:300]
        try:
            totals = job_rows("totals")[0]
        except Exception:
            totals = None
    return {
        "ok": error is None,
        "error": error,
        "clickhouse": "up" if error is None else "down",
        "server_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "totals": totals,
        "snapshots": snaps,
        "snapshot_oldest": (
            datetime.fromtimestamp(min(epochs), timezone.utc).isoformat(timespec="seconds")
            if epochs else None
        ),
    }


def api_timeseries(days=365, granularity="auto", from_s="", to_s="") -> dict:
    where, from_d, to_d, days_eff, key = _range_filter(
        days, from_s, to_s, default_days=365)
    # Auto granularity scales with the selected span, not the default.
    span = days_eff if (from_d and to_d) else (days_eff or 3650)
    if granularity not in ("hour", "day", "month"):
        granularity = "hour" if span <= 2 else ("month" if span > 730 else "day")
    expr = {"hour": "toStartOfHour(time)", "day": "toDate(time)",
            "month": "toStartOfMonth(time)"}[granularity]
    filt = f"WHERE {where} " if where else ""

    def go():
        return ch_query(
            f"SELECT {expr} d, sum(type='story') s, "
            "sum(type='comment') c, count() t "
            f"FROM hackernews_history {filt}"
            "GROUP BY d ORDER BY d"
        )["data"]

    rows = cached(f"ts:{key}:{granularity}", LIVE_TTL_S, go)
    return {"days": days_eff, "granularity": granularity, "rows": rows,
            "from": from_d, "to": to_d}


def api_top_stories(sort="score", days=30, limit=25, q="", kind="all",
                     from_s="", to_s="") -> list:
    limit = _int(limit, 25, 1, 100)
    days = _int(days, 30, 0, 3650)
    sort = sort if sort in ("score", "recent", "discussed") else "score"
    kind = kind if kind in ("all", "link", "text", "ask", "show") else "all"
    where_range, from_d, to_d, days_eff, key = _range_filter(
        days, from_s, to_s, default_days=30)
    custom = bool(from_d or to_d)

    # Fast path: requested all-time leaderboard already exists as a snapshot.
    if not custom and not q and days_eff == 0 and kind == "all" \
            and sort in ("score", "discussed"):
        name = "top_stories_score" if sort == "score" else "top_stories_discussed"
        return job_rows(name)[:limit]

    order = {"score": "score DESC", "recent": "time DESC",
             "discussed": "descendants DESC"}[sort]
    where = ["type='story'", "NOT deleted", "NOT dead", "title != ''"]
    if where_range:
        where.append(f"({where_range})")
    if q:
        esc = _esc(q[:200])
        where.append(f"(title ILIKE '%{esc}%' OR url ILIKE '%{esc}%')")
    kind_sql = {
        "ask": "title ILIKE 'Ask HN:%'",
        "show": "title ILIKE 'Show HN:%'",
        "link": "url != ''",
        "text": "url = ''",
    }.get(kind)
    if kind_sql:
        where.append(kind_sql)
    sql = (
        "SELECT id, title, url, score, by, time, descendants, "
        "toString(update_time) AS _u "
        f"FROM hackernews_history WHERE {' AND '.join(where)} "
        f"ORDER BY {order} LIMIT {limit * 5}"
    )

    def go():
        return dedupe(ch_query(sql)["data"], limit)

    return cached(f"top:{sort}:{key}:{limit}:{q}:{kind}", LIVE_TTL_S, go)


def api_top_users(kind="submitters", limit=25, days=0,
                  from_s="", to_s="") -> list:
    limit = _int(limit, 25, 1, 100)
    days = _int(days, 0, 0, 3650)
    kind = kind if kind in ("submitters", "commenters", "karma") else "submitters"
    where_range, from_d, to_d, days_eff, key = _range_filter(
        days, from_s, to_s, default_days=0)
    custom = bool(from_d or to_d)

    if not custom and days_eff == 0:
        name = {"submitters": "top_submitters_all",
                "commenters": "top_commenters_all",
                "karma": "top_karma_all"}[kind]
        return job_rows(name)[:limit]

    filt = f" AND ({where_range})" if where_range else ""
    if kind == "commenters":
        sql = (
            "SELECT by u, count() c FROM hackernews_history "
            "WHERE type='comment' AND by != '' AND NOT deleted AND NOT dead "
            f"{filt} "
            f"GROUP BY u ORDER BY c DESC LIMIT {limit}"
        )
    else:
        order = "c DESC" if kind == "submitters" else "karma DESC"
        sql = (
            "SELECT by u, count() c, avg(score) avg_score, sum(score) karma, "
            "sum(descendants) comments FROM hackernews_history "
            "WHERE type='story' AND by != '' AND NOT deleted AND NOT dead "
            f"{filt} "
            f"GROUP BY u ORDER BY {order} LIMIT {limit}"
        )
    return cached(f"users:{kind}:{limit}:{key}", LIVE_TTL_S,
                  lambda: ch_query(sql)["data"])


def api_top_domains(limit=25, days=365, metric="count", min_posts=1,
                    from_s="", to_s="") -> list:
    limit = _int(limit, 25, 1, 100)
    days = _int(days, 365, 0, 3650)
    min_posts = _int(min_posts, 1, 1, 100000)
    metric = metric if metric in (
        "count", "avg_score", "max_score", "comments") else "count"
    where_range, from_d, to_d, days_eff, key = _range_filter(
        days, from_s, to_s, default_days=365)
    custom = bool(from_d or to_d)

    if not custom and days_eff == 0 and metric == "count" and min_posts <= 1:
        return job_rows("top_domains_all")[:limit]

    filt = f" AND ({where_range})" if where_range else ""
    having = f" HAVING c >= {min_posts}" if min_posts > 1 else ""
    order = {"count": "c", "avg_score": "avg_score",
             "max_score": "max_score", "comments": "comments"}[metric]
    sql = (
        "SELECT domain(url) d, count() c, avg(score) avg_score, "
        "max(score) max_score, sum(descendants) comments "
        "FROM hackernews_history WHERE type='story' AND url != '' "
        f"AND NOT deleted AND NOT dead{filt} "
        f"GROUP BY d{having} ORDER BY {order} DESC, c DESC LIMIT {limit}"
    )
    return cached(f"domains:{limit}:{key}:{metric}:{min_posts}",
                  LIVE_TTL_S, lambda: ch_query(sql)["data"])


def api_heatmap(days=90, from_s="", to_s="") -> list:
    where_range, from_d, to_d, days_eff, key = _range_filter(
        days, from_s, to_s, default_days=90)
    custom = bool(from_d or to_d)
    if not custom and days_eff == 0:
        return job_rows("best_time")
    filt_sql = "WHERE type='story' AND NOT deleted AND NOT dead "
    if where_range:
        filt_sql += f"AND ({where_range}) "

    def _heat_go():
        return ch_query(
            "SELECT toHour(time) h, toDayOfWeek(time) w, count() c, "
            "avg(score) avg_score FROM hackernews_history FINAL "
            f"{filt_sql} GROUP BY h, w ORDER BY w, h"
        )["data"]

    return cached(f"heatmap:{key}", LIVE_TTL_S, _heat_go)


def _live_insights(where_range: str) -> dict:
    """Run snapshot-equivalent aggregates live for a custom time window."""
    filt = f"AND ({where_range}) " if where_range else ""
    filt_all = f"AND ({where_range}) " if where_range else ""

    def q_story_stats():
        return ch_query(
            "SELECT avg(score) avg_score, quantile(0.5)(score) med_score, "
            "quantile(0.9)(score) p90_score, avg(descendants) avg_comments, "
            "countIf(descendants = 0) zero_comments, count() c "
            "FROM hackernews_history "
            f"WHERE type='story' AND NOT deleted AND NOT dead {filt}")["data"]

    def q_score_dist():
        return ch_query(
            "SELECT multiIf(score < 10, '0-9', score < 25, '10-24', "
            "score < 50, '25-49', score < 100, '50-99', "
            "score < 250, '100-249', score < 500, '250-499', "
            "score < 1000, '500-999', '1000+') b, count() c, min(score) lo "
            "FROM hackernews_history "
            "WHERE type='story' AND NOT deleted AND NOT dead "
            f"AND score >= 0 {filt} GROUP BY b ORDER BY lo")["data"]

    def q_best_time():
        return ch_query(
            "SELECT toDayOfWeek(time) w, toHour(time) h, count() c, "
            "avg(score) avg_score FROM hackernews_history "
            f"WHERE type='story' AND NOT deleted AND NOT dead {filt} "
            "GROUP BY w, h ORDER BY w, h")["data"]

    def q_post_kinds():
        return ch_query(
            "SELECT multiIf(title ILIKE 'Ask HN:%', 'Ask HN', "
            "title ILIKE 'Show HN:%', 'Show HN', "
            "title ILIKE 'Tell HN:%', 'Tell HN', "
            "title ILIKE 'Launch HN:%', 'Launch HN', "
            "url = '', 'Text post', 'Link post') kind, "
            "count() c, avg(score) avg_score, avg(descendants) avg_comments "
            "FROM hackernews_history "
            f"WHERE type='story' AND NOT deleted AND NOT dead {filt} "
            "GROUP BY kind ORDER BY c DESC")["data"]

    def q_topics():
        return ch_query(
            r"SELECT toStartOfMonth(time) m, "
            r"countIf(match(title, '(?i)\bai\b|openai')) ai, "
            r"countIf(match(title, '(?i)\bllms?\b')) llm, "
            r"countIf(match(title, '(?i)\brust\b')) rust, "
            r"countIf(match(title, '(?i)kubernetes|\bk8s\b')) kubernetes, "
            r"countIf(match(title, '(?i)climate')) climate, "
            r"countIf(match(title, '(?i)crypto|bitcoin|ethereum')) crypto, "
            r"countIf(match(title, '(?i)startup')) startup "
            "FROM hackernews_history "
            f"WHERE type='story' AND NOT deleted AND NOT dead {filt} "
            "GROUP BY m ORDER BY m")["data"]

    def q_domain_quality():
        return ch_query(
            "SELECT domain(url) d, count() c, avg(score) avg_score, "
            "max(score) max_score, sum(descendants) comments "
            "FROM hackernews_history "
            "WHERE type='story' AND url != '' AND NOT deleted AND NOT dead "
            f"{filt} GROUP BY d HAVING c >= 5 "
            "ORDER BY avg_score DESC LIMIT 50")["data"]

    def q_domain_low():
        return ch_query(
            "SELECT domain(url) d, count() c, avg(score) avg_score, "
            "max(score) max_score FROM hackernews_history "
            "WHERE type='story' AND url != '' AND NOT deleted AND NOT dead "
            f"{filt} GROUP BY d HAVING c >= 20 "
            "ORDER BY avg_score ASC LIMIT 30")["data"]

    def q_yearly():
        return ch_query(
            "SELECT toYear(time) y, sum(type='story') stories, "
            "sum(type='comment') comments, count() total "
            "FROM hackernews_history "
            f"WHERE time >= '2006-01-01' {filt_all} GROUP BY y ORDER BY y"
        )["data"]

    def q_monthly():
        return ch_query(
            "SELECT toStartOfMonth(time) m, sum(type='story') stories, "
            "sum(type='comment') comments, "
            "countIf(type='story' AND NOT deleted AND NOT dead) "
            "stories_visible, count() total "
            "FROM hackernews_history "
            f"WHERE time >= '2006-10-01' {filt_all} GROUP BY m ORDER BY m"
        )["data"]

    def q_top_score():
        return dedupe(ch_query(
            "SELECT id, title, url, score, by, time, descendants, "
            "toString(update_time) AS _u FROM hackernews_history "
            "WHERE type='story' AND NOT deleted AND NOT dead "
            f"AND title != '' {filt} ORDER BY score DESC LIMIT 300"
        )["data"], 100)

    def q_top_discussed():
        return dedupe(ch_query(
            "SELECT id, title, url, score, by, time, descendants, "
            "toString(update_time) AS _u FROM hackernews_history "
            "WHERE type='story' AND NOT deleted AND NOT dead "
            f"AND title != '' {filt} ORDER BY descendants DESC LIMIT 300"
        )["data"], 100)

    (story_stats, score_distribution, best_time, post_kinds, topic_trends,
     domain_quality, domain_low, yearly, monthly, top_score,
     top_discussed) = par(
        q_story_stats, q_score_dist, q_best_time, q_post_kinds, q_topics,
        q_domain_quality, q_domain_low, q_yearly, q_monthly, q_top_score,
        q_top_discussed)
    return {
        "story_stats": story_stats,
        "score_distribution": score_distribution,
        "best_time": best_time,
        "post_kinds": post_kinds,
        "topic_trends": topic_trends,
        "domain_quality": domain_quality,
        "domain_low": domain_low,
        "rising_domains": [],
        "yearly": yearly,
        "monthly": monthly,
        "top_stories_score": top_score,
        "top_stories_discussed": top_discussed,
    }


def api_insights(from_s="", to_s="", days=0) -> dict:
    where_range, from_d, to_d, days_eff, key = _range_filter(
        days, from_s, to_s, default_days=0)
    custom = bool(from_d or to_d or days_eff != 0)
    if not custom:
        names = [
            "story_stats", "score_distribution", "best_time", "post_kinds",
            "topic_trends", "domain_quality", "domain_low", "rising_domains",
            "yearly", "monthly", "top_stories_score", "top_stories_discussed",
        ]
        data = {name: job_rows(name) for name in names}
        meta = {name: job_meta(name) for name in names}
        epochs = [m.get("epoch") for m in meta.values() if m and m.get("epoch")]
        return {
            "generated_at": max(
                (m.get("generated_at") for m in meta.values() if m),
                default=None,
            ),
            "snapshot_oldest": (
                datetime.fromtimestamp(min(epochs), timezone.utc).isoformat(
                    timespec="seconds") if epochs else None
            ),
            "meta": meta,
            "live": False,
            "from": None,
            "to": None,
            **data,
            "records": {
                "top_score": data["top_stories_score"][:5],
                "top_discussed": data["top_stories_discussed"][:5],
            },
        }

    def go():
        data = _live_insights(where_range)
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(
                timespec="seconds"),
            "snapshot_oldest": api_status_snapshot_oldest(),
            "meta": {},
            "live": True,
            "from": from_d,
            "to": to_d,
            "days": days_eff,
            **data,
            "records": {
                "top_score": data["top_stories_score"][:5],
                "top_discussed": data["top_stories_discussed"][:5],
            },
        }

    return cached(f"insights:{key}", LIVE_TTL_S, go)


def api_overview(days=30, from_s="", to_s="") -> dict:
    where_range, from_d, to_d, days_eff, key = _range_filter(
        days, from_s, to_s, default_days=30)
    totals = api_totals()
    deltas, series, stories, domains = par(
        api_deltas,
        lambda: api_timeseries(days_eff, "auto", from_s, to_s),
        lambda: api_top_stories("score", days=days_eff, limit=6,
                               from_s=from_s, to_s=to_s),
        lambda: api_top_domains(limit=8, days=days_eff, metric="count",
                                min_posts=1, from_s=from_s, to_s=to_s),
    )
    if where_range:
        filt = f"AND ({where_range}) "
        stats_rows = ch_query(
            "SELECT avg(score) avg_score, quantile(0.5)(score) med_score, "
            "quantile(0.9)(score) p90_score, avg(descendants) avg_comments, "
            "countIf(descendants = 0) zero_comments, count() c "
            "FROM hackernews_history "
            f"WHERE type='story' AND NOT deleted AND NOT dead {filt}"
        )["data"]
        stats = stats_rows[0] if stats_rows else {}
        heat = api_heatmap(days_eff, from_s, to_s)
    else:
        stats = (job_rows("story_stats") or [{}])[0]
        heat = job_rows("best_time")
    eligible = [r for r in heat if r.get("c", 0) >= 100]
    best_window = max(eligible, key=lambda r: r.get("avg_score", 0),
                      default=None)
    peak_window = max(heat, key=lambda r: r.get("c", 0), default=None)
    rising = job_rows("rising_domains")
    return {
        "days": days_eff,
        "from": from_d,
        "to": to_d,
        "range_label": _range_label(from_d, to_d, days_eff),
        "totals": totals,
        "deltas": deltas,
        "story_stats": stats,
        "series": series,
        "top_stories": stories,
        "top_domains": domains,
        "best_window": best_window,
        "peak_window": peak_window,
        "top_rising_domain": rising[0] if rising else None,
        "snapshot_oldest": api_status_snapshot_oldest(),
    }


def api_status_snapshot_oldest():
    epochs = [m.get("epoch") for m in
              (job_meta(n) or {} for n in SNAPSHOT_JOBS) if m.get("epoch")]
    if not epochs:
        return None
    return datetime.fromtimestamp(min(epochs), timezone.utc).isoformat(
        timespec="seconds")


def api_search(q, itype="all", sort="recent", limit=25, offset=0,
               from_s="", to_s="", author="", domain="", min_score=0,
               min_comments=0, as_of="") -> dict:
    limit = _int(limit, 25, 1, 50)
    offset = _int(offset, 0, 0, 1000000)
    itype = itype if itype in ("all", "story", "comment") else "all"
    min_score = _int(min_score, 0, 0, 1000000)
    min_comments = _int(min_comments, 0, 0, 1000000)
    # Keep newly submitted items out of subsequent pages; existing scores can change.
    cutoff = _int(as_of, int(time.time()), 1, int(time.time()))
    where_range, *_ = _range_filter(0, from_s, to_s, default_days=0)
    where = ["NOT deleted", "NOT dead"]
    if itype != "all":
        where.append(f"type='{itype}'")
    if where_range:
        where.append(f"({where_range})")
    if q.strip():
        term = _esc(q[:200])
        where.append(f"(positionCaseInsensitiveUTF8(title, '{term}') > 0 OR "
                     f"positionCaseInsensitiveUTF8(text, '{term}') > 0 OR "
                     f"positionCaseInsensitiveUTF8(url, '{term}') > 0)")
    if author:
        where.append(f"by = '{_esc(author[:100])}'")
    if domain:
        where.append(f"lower(domain(url)) = '{_esc(domain.lower()[:253])}'")
    if min_score or min_comments or sort == "score":
        if itype == "comment":
            if min_score or min_comments:
                raise ValueError("Score and comment-count filters apply to stories only")
            sort = "recent"
        else:
            where += ["type='story'", f"score >= {min_score}",
                      f"descendants >= {min_comments}"]
    snippet_start = f"greatest(1, toInt64(positionCaseInsensitiveUTF8(text, '{_esc(q[:200])}')) - 120)" if q.strip() else "1"
    order = "score DESC, id DESC" if sort == "score" else "time DESC, id DESC"
    # Collapse versions BEFORE filtering and pagination, including the count.
    source = "hackernews_history FINAL"
    where.append(f"time <= toDateTime({cutoff})")
    total, rows = par(
        lambda: ch_query(f"SELECT count() c FROM {source} WHERE {' AND '.join(where)}")["data"][0]["c"],
        lambda: ch_query(
            "SELECT id, type, by, time, title, url, score, parent, descendants, "
            f"substringUTF8(text, {snippet_start}, 700) snippet "
            f"FROM {source} WHERE {' AND '.join(where)} "
            f"ORDER BY {order} LIMIT {limit} OFFSET {offset}")["data"],
    )
    return {"rows": rows, "total": total, "offset": offset, "limit": limit,
            "as_of": cutoff, "sort": sort, "generated_at": utc_now()}


def api_user(name: str) -> dict:
    username = re.sub(r"[^A-Za-z0-9_\-]", "", name or "")[:50]
    if not username:
        raise ValueError("Invalid username")

    def go():
        stats = ch_query(
            "SELECT type t, count() c, avg(score) s, sum(score) karma, "
            "sum(descendants) comments FROM hackernews_history "
            f"WHERE by = '{username}' GROUP BY t"
        )["data"]
        first = ch_query(
            "SELECT min(time) f, max(time) l, count() c "
            f"FROM hackernews_history WHERE by = '{username}'"
        )["data"][0]
        recent = dedupe(ch_query(
            "SELECT id, type, time, title, url, score, parent, descendants, "
            "substring(text, 1, 500) snippet, toString(update_time) AS _u "
            f"FROM hackernews_history WHERE by = '{username}' "
            "ORDER BY time DESC LIMIT 60"
        )["data"], 20)
        top = dedupe(ch_query(
            "SELECT id, title, url, score, time, descendants, "
            "toString(update_time) AS _u FROM hackernews_history "
            f"WHERE by = '{username}' AND type='story' "
            "AND NOT deleted AND NOT dead "
            "ORDER BY score DESC LIMIT 30"
        )["data"], 5)
        activity = ch_query(
            "SELECT toStartOfMonth(time) m, count() c, "
            "sum(type='story') stories FROM hackernews_history "
            f"WHERE by = '{username}' "
            "AND time >= now() - INTERVAL 24 MONTH "
            "GROUP BY m ORDER BY m"
        )["data"]
        return {
            "user": username,
            "stats": stats,
            "recent": recent,
            "top_stories": top,
            "activity": activity,
            "first_seen": first["f"],
            "last_seen": first["l"],
            "total_items": first["c"],
        }

    return cached(f"user:{username}", LIVE_TTL_S, go)


def api_item(item_id) -> dict:
    try:
        item_id = int(item_id)
    except (TypeError, ValueError) as e:
        raise ValueError("Invalid item id") from e
    item = ch_query(
        "SELECT id, type, by, time, title, url, text, score, parent, "
        "descendants FROM hackernews_history "
        f"WHERE id = {item_id} ORDER BY update_time DESC LIMIT 1"
    )["data"]
    if not item:
        return {"item": None, "comments": []}
    comments = api_replies(item_id)
    related = []
    url = item[0].get("url", "")
    if item[0].get("type") == "story" and url:
        # Ignore scheme, www, fragment and trailing slash; retain query params.
        normalized = "trimRight(replaceRegexpOne(cutFragment(url), '^https?://(www[.])?', ''), '/')"
        value = re.sub(r"^https?://(?:www\.)?", "", url.split("#")[0]).rstrip("/")
        related = ch_query(
            "SELECT id, title, by, time, score, descendants, url "
            "FROM hackernews_history FINAL WHERE type='story' AND NOT deleted "
            f"AND NOT dead AND id != {item_id} AND {normalized} = '{_esc(value)}' "
            "ORDER BY time DESC, id DESC LIMIT 100")["data"]
    return {"item": item[0], **comments, "related": related}


def api_replies(parent, limit=100, offset=0):
    parent = _int(parent, 0, 1, 2147483647)
    limit = _int(limit, 100, 1, 200)
    offset = _int(offset, 0, 0, 1000000)
    rows = ch_query(
        "SELECT id, by, time, text, parent, deleted, dead "
        "FROM hackernews_history FINAL "
        f"WHERE parent = {parent} AND type='comment' "
        f"ORDER BY time, id LIMIT {limit + 1} OFFSET {offset}")["data"]
    return {"comments": rows[:limit], "has_more": len(rows) > limit,
            "next_offset": offset + limit}


def api_compare(params):
    threshold = _int(params.get("threshold"), 100, 1, 1000000)
    results = []
    for side in ("a", "b"):
        kind = params.get(f"{side}_kind", "topic")
        value = params.get(f"{side}_value", "")[:200].strip()
        where_range, *_ = _range_filter(365, params.get(f"{side}_from", ""),
                                       params.get(f"{side}_to", ""))
        where = ["type='story'", "NOT deleted", "NOT dead", f"({where_range})"]
        if kind == "domain":
            where.append(f"lower(domain(url)) = '{_esc(value.lower())}'")
        elif kind == "user":
            where.append(f"by = '{_esc(value)}'")
        elif kind == "topic":
            if not value:
                raise ValueError("Enter a topic for each topic cohort")
            where.append(f"positionCaseInsensitiveUTF8(title, '{_esc(value)}') > 0")
        elif kind != "dates":
            raise ValueError("Unknown comparison type")
        row = ch_query(
            "SELECT count() submissions, quantileExact(0.5)(score) median_score, "
            "avg(descendants) comments_per_story, "
            f"100.0 * countIf(score >= {threshold}) / nullIf(count(), 0) reach_pct "
            f"FROM hackernews_history FINAL WHERE {' AND '.join(where)}")["data"][0]
        results.append({"kind": kind, "value": value, **row})
    return {"cohorts": results, "threshold": threshold, "generated_at": utc_now()}


def api_posting(days=365, from_s="", to_s="", tz="UTC", threshold=100,
                min_age=7, max_age=30):
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError("Unknown IANA time zone") from None
    threshold = _int(threshold, 100, 1, 1000000)
    min_age = _int(min_age, 7, 1, 3650)
    max_age = _int(max_age, 30, 2, 3651)
    if max_age <= min_age:
        raise ValueError("Maximum story age must exceed minimum age")
    where_range, *_ = _range_filter(days, from_s, to_s, default_days=365)
    where = ["type='story'", "NOT deleted", "NOT dead",
             f"time <= now() - INTERVAL {min_age} DAY",
             f"time > now() - INTERVAL {max_age} DAY"]
    if where_range:
        where.append(f"({where_range})")
    rows = ch_query(
        f"SELECT toDayOfWeek(time, 0, '{_esc(tz)}') w, toHour(time, '{_esc(tz)}') h, "
        "count() c, avg(score) avg_score, quantileExact(0.5)(score) median_score, "
        f"100.0 * countIf(score >= {threshold}) / count() reach_pct "
        f"FROM hackernews_history FINAL WHERE {' AND '.join(where)} "
        "GROUP BY w, h ORDER BY w, h")["data"]
    return {"rows": rows, "timezone": tz, "threshold": threshold,
            "min_age": min_age, "max_age": max_age, "generated_at": utc_now()}


MOMENTUM_DB = BASE_DIR / "data" / "momentum.sqlite3"


def api_momentum(item_id=""):
    if not MOMENTUM_DB.exists():
        return {"rows": [], "observations": [], "generated_at": None}
    with sqlite3.connect(f"file:{MOMENTUM_DB}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        if item_id:
            item_id = _int(item_id, 0, 1, 2147483647)
            rows = conn.execute("SELECT * FROM observations WHERE id=? ORDER BY observed_at", (item_id,)).fetchall()
            if rows:
                record_source("periodic observations", datetime.fromtimestamp(rows[-1]["observed_at"], timezone.utc).isoformat(), "HN API collector")
            return {"observations": [dict(r) for r in rows], "rows": []}
        rows = conn.execute("""
            WITH recent AS (
                SELECT *, row_number() OVER (PARTITION BY id ORDER BY observed_at) first_n,
                       row_number() OVER (PARTITION BY id ORDER BY observed_at DESC) last_n
                FROM observations WHERE observed_at >= ?
            )
            SELECT a.id, b.title, b.submitted_at, a.observed_at start_at,
                   b.observed_at end_at, b.score, b.comments,
                   b.score-a.score score_gain, b.comments-a.comments comment_gain,
                   (b.score-a.score)*3600.0/(b.observed_at-a.observed_at) points_per_hour
            FROM recent a JOIN recent b ON a.id=b.id
            WHERE a.first_n=1 AND b.last_n=1 AND b.observed_at-a.observed_at >= 300 AND b.score > a.score
            ORDER BY points_per_hour DESC LIMIT 50
        """, (int(time.time()) - 86400,)).fetchall()
        latest = conn.execute("SELECT max(observed_at) FROM observations").fetchone()[0]
    if latest:
        record_source("periodic observations", datetime.fromtimestamp(latest, timezone.utc).isoformat(), "HN API collector")
    return {"rows": [dict(r) for r in rows], "generated_at": latest}


# ------------------------------------------------------------- HTTP layer
class Handler(SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code: int, obj, ctype="application/json"):
        body = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("X-Data-Provenance", json.dumps(_provenance.get() or []))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _index(self):
        data = (BASE_DIR / "index.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        _provenance.set([])
        u = urllib.parse.urlparse(self.path)
        p, qs = u.path, urllib.parse.parse_qs(u.query)
        g = lambda k, d="": qs.get(k, [d])[0]
        try:
            if p in ("/", "/index.html"):
                self._index()
            elif p == "/healthz":
                self._send(200, {"ok": True})
            elif p == "/api/health":
                try:
                    r = ch_query("SELECT 1 x, now() t", timeout=15)
                    self._send(200, {"ok": True, "clickhouse": "up",
                                     "server_time": r["data"][0]["t"]})
                except Exception as e:
                    self._send(200, {"ok": False, "error": str(e)[:300]})
            elif p == "/api/status":
                self._send(200, api_status())
            elif p == "/api/overview":
                self._send(200, api_overview(g("days", "30"),
                                             g("from", ""), g("to", "")))
            elif p == "/api/timeseries":
                self._send(200, api_timeseries(g("days", "365"),
                                               g("granularity", "auto"),
                                               g("from", ""), g("to", "")))
            elif p == "/api/top-stories":
                self._send(200, api_top_stories(
                    g("sort", "score"), g("days", "30"), g("limit", "25"),
                    g("q", ""), g("kind", "all"),
                    g("from", ""), g("to", "")))
            elif p == "/api/top-users":
                self._send(200, api_top_users(g("kind", "submitters"),
                                              g("limit", "50"),
                                              g("days", "0"),
                                              g("from", ""), g("to", "")))
            elif p == "/api/top-domains":
                self._send(200, api_top_domains(
                    g("limit", "50"), g("days", "365"),
                    g("metric", "count"), g("min_posts", "1"),
                    g("from", ""), g("to", "")))
            elif p == "/api/heatmap":
                self._send(200, api_heatmap(g("days", "90"),
                                            g("from", ""), g("to", "")))
            elif p == "/api/insights":
                self._send(200, api_insights(
                    g("from", ""), g("to", ""), g("days", "0")))
            elif p == "/api/search":
                self._send(200, api_search(
                    g("q", ""), g("type", "all"), g("sort", "recent"),
                    g("limit", "25"), g("offset", "0"),
                    g("from", ""), g("to", ""), g("author"), g("domain"),
                    g("min_score", "0"), g("min_comments", "0"), g("as_of")))
            elif p == "/api/compare":
                self._send(200, api_compare({k: v[0] for k, v in qs.items()}))
            elif p == "/api/posting":
                self._send(200, api_posting(g("days", "365"), g("from"), g("to"),
                    g("tz", "UTC"), g("threshold", "100"), g("min_age", "7"), g("max_age", "30")))
            elif p == "/api/replies":
                self._send(200, api_replies(g("parent"), offset=g("offset", "0")))
            elif p == "/api/momentum":
                self._send(200, api_momentum(g("id")))
            elif p.startswith("/api/user/"):
                self._send(200, api_user(urllib.parse.unquote(
                    p.rsplit("/", 1)[-1])))
            elif p.startswith("/api/item/"):
                self._send(200, api_item(p.rsplit("/", 1)[-1]))
            elif p in ("/api/cache", "/api/snapshots"):
                self._send(200, snapshot_summary())
            else:
                self._send(404, {"error": f"unknown route {html.escape(p)}"})
        except ValueError as e:
            self._send(400, {"error": str(e)[:500]})
        except Exception as e:
            self._send(500, {"error": str(e)[:500]})

    def do_POST(self):
        _provenance.set([])
        u = urllib.parse.urlparse(self.path)
        try:
            n = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(max(n, 0)) or b"{}")
            if u.path == "/api/sql":
                sql = guard_sql(payload.get("sql", ""))
                t0 = time.time()
                r = ch_query(sql)
                r["elapsed_s"] = round(time.time() - t0, 3)
                r["guarded_sql"] = sql
                r["data"] = r["data"][:1000]
                return self._send(200, r)
            self._send(404, {"error": "unknown route"})
        except Exception as e:
            self._send(400, {"error": str(e)[:500]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int,
                    default=int(os.environ.get("PORT", "8000")))
    ap.add_argument("--warm-cache", action="store_true",
                    help="refresh every snapshot before serving")
    ap.add_argument("--no-warm", action="store_true",
                    help="do not build missing snapshots on startup")
    args = ap.parse_args()

    missing = [n for n in SNAPSHOT_JOBS if not _snapshot_path(n).exists()]
    if args.warm_cache:
        print("refreshing all snapshots …")
        res = refresh_snapshots(progress=lambda n, r: print(
            f"  {n:<24} {'ok' if r.get('ok') else 'FAIL'} "
            f"{r.get('rows', '')} rows {r.get('elapsed_s', '')}s"))
        print(f"snapshots: {sum(1 for j in res['jobs'].values() if j.get('ok'))}"
              f"/{len(res['jobs'])} ok in {res['elapsed_s']}s")
    elif missing and not args.no_warm:
        print(f"building {len(missing)} missing snapshot(s) — this happens once …")
        refresh_snapshots(missing, progress=lambda n, r: print(
            f"  {n:<24} {'ok' if r.get('ok') else 'FAIL'} "
            f"{r.get('rows', '')} rows {r.get('elapsed_s', '')}s"))

    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"HN analytics → http://{args.host}:{args.port}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
        _pool.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    main()
