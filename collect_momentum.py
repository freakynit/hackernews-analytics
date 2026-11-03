"""Collect prospective HN story observations. Run periodically or with --once."""
import argparse
import json
import sqlite3
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from server import MOMENTUM_DB

API = 'https://hacker-news.firebaseio.com/v0'


def fetch(path):
    with urllib.request.urlopen(f'{API}/{path}.json', timeout=20) as response:
        return json.load(response)


def collect():
    # Keep following previously seen stories for 48h, even off the front page.
    MOMENTUM_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(MOMENTUM_DB, timeout=30) as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('''CREATE TABLE IF NOT EXISTS observations (
            id INTEGER, observed_at INTEGER, submitted_at INTEGER,
            title TEXT, score INTEGER, comments INTEGER,
            PRIMARY KEY (id, observed_at))''')
        tracked = [r[0] for r in conn.execute(
            'SELECT DISTINCT id FROM observations WHERE submitted_at >= ?',
            (int(time.time()) - 172800,))]
        ids = set(fetch('topstories')[:100] + fetch('newstories')[:100] + tracked)
        def observe(item_id):
            try:
                item = fetch(f'item/{item_id}')
                if item and item.get('type') == 'story' and not item.get('deleted') and not item.get('dead'):
                    return (item_id, int(time.time()), item['time'], item.get('title', ''),
                            item.get('score', 0), item.get('descendants', 0))
            except Exception as exc:
                print(f'Observation {item_id} failed: {exc}', flush=True)
            return None
        with ThreadPoolExecutor(max_workers=8) as pool:
            rows = [r for r in pool.map(observe, ids) if r]
        conn.executemany('INSERT OR IGNORE INTO observations VALUES (?, ?, ?, ?, ?, ?)', rows)
    print(f'Collected {len(rows)} observations at {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--interval', type=int, default=300, help='seconds between collections (minimum 60)')
    args = parser.parse_args()
    while True:
        try:
            collect()
        except Exception as exc:
            print(f'Collection failed: {exc}', flush=True)
            if args.once:
                raise
        if args.once:
            break
        time.sleep(max(60, args.interval))
