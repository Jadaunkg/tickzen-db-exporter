"""
One-off repair of technical_indicators from the stored prices (no Yahoo calls).

Per stock: read daily_price_data and technical_indicators, recompute every indicator with the production function
(database/history_sync.plan_indicator_rewrite -> trading_sessions.stored_indicator_frame ->
feature_engineering.add_technical_indicators, standard windows, full warm-up), and report the rows that would
change. The daily export runs the same function after every sync, so this only speeds up the first repair.

    python scripts/indicator_history_backfill.py NVDA KLAC ALH            # dry run (default), details per stock
    python scripts/indicator_history_backfill.py --all                    # dry run, every active stock, counts
    python scripts/indicator_history_backfill.py NVDA --show 2025-12-01   # also print that day's SMA 50/200
    python scripts/indicator_history_backfill.py --all --apply            # WRITE: upsert changed rows, delete
                                                                          # days with no session (needs approval)

The dry run uses a read-only database session; nothing is written.
"""

import argparse
import os
import sys
import time

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, '.env'))

from database.data_mapper import DataMapper  # noqa: E402
from database.history_sync import (  # noqa: E402
    DELETE_INDICATOR_SQL, STORED_INDICATOR_SQL, _day, _num, plan_indicator_rewrite,
)

BATCH = 1000


def connect(readonly: bool):
    conn = psycopg2.connect(
        host=os.getenv('AZURE_POSTGRES_HOST'), dbname=os.getenv('AZURE_POSTGRES_DB', 'postgres'),
        user=os.getenv('AZURE_POSTGRES_USER'), password=os.getenv('AZURE_POSTGRES_PASSWORD'),
        port=os.getenv('AZURE_POSTGRES_PORT', '5432'), sslmode='require', connect_timeout=20)
    conn.set_session(readonly=readonly, autocommit=True)

    def run(sql, params=None):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or [])
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    return run


def last_cross(rows):
    """Newest date where SMA 50 crossed SMA 200, and the direction."""
    prev, out = None, None
    for r in sorted(rows, key=lambda r: _day(r['date'])):
        a, b = _num(r.get('sma_50')), _num(r.get('sma_200'))
        if a is None or b is None:
            prev = None
            continue
        sign = a > b
        if prev is not None and sign != prev:
            out = (_day(r['date']), 'golden' if sign else 'death')
        prev = sign
    return out


def merged(stored, upserts, deletes):
    by_day = {_day(r['date']): r for r in stored}
    for r in upserts:
        by_day[_day(r['date'])] = r
    for d in deletes:
        by_day.pop(d, None)
    return list(by_day.values())


def upsert_sql(records):
    cols = [c for c in records[0].keys()]
    updates = ', '.join(f'{c} = EXCLUDED.{c}' for c in cols if c not in ('stock_id', 'date'))
    return (f"INSERT INTO technical_indicators ({', '.join(cols)}) VALUES %s "
            f"ON CONFLICT (stock_id, date) DO UPDATE SET {updates}, updated_at = now()"), cols


def apply(conn_run, raw_conn, stock_id, upserts, deletes):
    from psycopg2.extras import execute_values
    if upserts:
        sql, cols = upsert_sql(upserts)
        with raw_conn.cursor() as cur:
            for i in range(0, len(upserts), BATCH):
                execute_values(cur, sql, [tuple(r.get(c) for c in cols) for r in upserts[i:i + BATCH]])
    if deletes:
        conn_run(DELETE_INDICATOR_SQL, [stock_id, deletes])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('tickers', nargs='*')
    ap.add_argument('--all', action='store_true', help='every active stock (counts only)')
    ap.add_argument('--show', help='a date (YYYY-MM-DD) to print stored -> new SMA 50/200 for')
    ap.add_argument('--apply', action='store_true', help='WRITE the changes (default is a dry run)')
    args = ap.parse_args()
    if not args.tickers and not args.all:
        ap.error('give tickers or --all')

    run = connect(readonly=not args.apply)
    raw_conn = None
    if args.apply:
        raw_conn = psycopg2.connect(
            host=os.getenv('AZURE_POSTGRES_HOST'), dbname=os.getenv('AZURE_POSTGRES_DB', 'postgres'),
            user=os.getenv('AZURE_POSTGRES_USER'), password=os.getenv('AZURE_POSTGRES_PASSWORD'),
            port=os.getenv('AZURE_POSTGRES_PORT', '5432'), sslmode='require', connect_timeout=20)
        raw_conn.autocommit = True
    mapper = DataMapper()

    if args.all:
        stocks = run('SELECT id, symbol FROM stocks WHERE is_active ORDER BY symbol')
    else:
        stocks = run('SELECT id, symbol FROM stocks WHERE upper(symbol) = ANY(%s) ORDER BY symbol',
                     [[t.upper() for t in args.tickers]])
    detail = not args.all
    totals = {'stocks': 0, 'stored': 0, 'new': 0, 'changed': 0, 'deleted': 0, 'stocks_changed': 0, 'errors': 0}
    started = time.time()
    print(f"{'apply' if args.apply else 'DRY RUN'}: {len(stocks)} stocks")
    for s in stocks:
        try:
            stored = run(STORED_INDICATOR_SQL, [s['id']])
            upserts, deletes, why = plan_indicator_rewrite(run, mapper, s['id'])
        except Exception as e:  # noqa: BLE001 - report and go on
            totals['errors'] += 1
            print(f"{s['symbol']:8s} ERROR {e}")
            continue
        if upserts is None:
            print(f"{s['symbol']:8s} skipped ({why})")
            continue
        stored_days = {_day(r['date']) for r in stored}
        new = sum(1 for r in upserts if _day(r['date']) not in stored_days)
        changed = len(upserts) - new
        totals['stocks'] += 1
        totals['stored'] += len(stored)
        totals['new'] += new
        totals['changed'] += changed
        totals['deleted'] += len(deletes)
        totals['stocks_changed'] += bool(upserts or deletes)
        if detail:
            after = merged(stored, upserts, deletes)
            print(f"{s['symbol']:8s} stored {len(stored):4d}  changed {changed:4d}  new {new:3d}  "
                  f"removed {len(deletes):3d}  50/200 cross: {last_cross(stored)} -> {last_cross(after)}")
            if args.show:
                old = {_day(r['date']): r for r in stored}.get(args.show, {})
                nxt = {_day(r['date']): r for r in after}.get(args.show, {})
                for col in ('sma_50', 'sma_200', 'rsi_14', 'volatility_30d_annual'):
                    print(f"           {args.show} {col:22s} {_num(old.get(col))} -> {_num(nxt.get(col))}")
        if args.apply and (upserts or deletes):
            apply(run, raw_conn, s['id'], upserts, deletes)
    print(f"\nTOTAL {totals}  ({time.time() - started:.0f}s)")
    if not args.apply:
        print('Dry run: nothing was written.')


if __name__ == '__main__':
    main()
