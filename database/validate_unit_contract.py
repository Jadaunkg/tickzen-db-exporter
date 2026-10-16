#!/usr/bin/env python3
"""Validate the live database against the storage-unit contract.

Read-only. Run after every export (CI / cron) and before trusting a new data batch:

    python database/validate_unit_contract.py            # uses the same AZURE_POSTGRES_* env as the exporter
    python database/validate_unit_contract.py --json     # machine readable
    python database/validate_unit_contract.py --max-failures 0

Checks
  1. SCHEMA     every ``table.column`` in column_units.json exists in the database
  2. BOUNDS     latest row per stock respects the per-unit / per-column hard bounds
  3. INVARIANTS the same quantity stored in two tables / units agrees (e.g. D/E percent points
                == 100 x peer ratio).  This is how a unit swap is detected WITHOUT magnitude guessing.
  4. FRESHNESS  "next earnings" dates that are already in the past; margins that are exactly 0.0
                while revenue and earnings are non-zero (a manufactured zero)
Exit status: 0 = clean (or within --max-failures), 1 = violations.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Sequence

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import unit_contract as uc

Query = Callable[[str, Optional[Sequence[Any]]], List[Dict[str, Any]]]

# How to pick the "current" row of each contracted table.
LATEST_ORDER: Dict[str, str] = {
    # Same rule as the website: the newest WRITTEN usable TTM row. period_date alone is wrong because older
    # exporter versions keyed rows by export date, which outranks every real statement date.
    "fundamental_data": (
        "(period_type = 'TTM') DESC, (COALESCE(revenue_ttm, net_income_ttm, total_cash) IS NOT NULL) DESC, "
        "(retrieved_at IS NOT NULL) DESC, COALESCE(retrieved_at, updated_at) DESC NULLS LAST, period_date DESC"
    ),
    "dividend_data": "updated_at DESC NULLS LAST",
    "ownership_data": "report_date DESC",
    "technical_indicators": "date DESC",
    "market_price_snapshot": "date DESC",
    "risk_data": "date DESC",
    "liquidity_risk_data": "date DESC",
    "altman_zscore_data": "date DESC",
    "regime_risk_data": "date DESC",
    "sentiment_data": "date DESC",
    "analyst_eps_trend": "fetched_at DESC, period",
}
PEER_TABLE = "peer_comparison_data"


def _db_query() -> Query:
    from database.azure_postgres_client import AzurePostgresClient

    client = AzurePostgresClient()

    def run(sql_text: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        return client.execute_raw_sql(sql_text, list(params or []))

    return run


def latest_rows(query: Query, table: str) -> Dict[int, Dict[str, Any]]:
    """stock_id -> current row of ``table`` (for peers: the stock's own ``is_target`` row)."""
    if table == PEER_TABLE:
        rows = query(f"SELECT DISTINCT ON (stock_id) * FROM {table} WHERE is_target ORDER BY stock_id, market_cap DESC NULLS LAST", None)
    else:
        order = LATEST_ORDER.get(table, "id DESC")
        rows = query(f"SELECT DISTINCT ON (stock_id) * FROM {table} ORDER BY stock_id, {order}", None)
    return {int(r["stock_id"]): r for r in rows}


def check_schema(query: Query) -> List[str]:
    problems: List[str] = []
    for table in uc.tables():
        have = {
            r["column_name"]
            for r in query("SELECT column_name FROM information_schema.columns WHERE table_name = %s", [table])
        }
        if not have:
            problems.append(f"table {table} does not exist")
            continue
        for column in uc.columns_of(table):
            if column not in have:
                problems.append(f"{table}.{column} is in the contract but missing from the database")
    return problems


def check_bounds(query: Query, tables: Optional[Sequence[str]] = None) -> Dict[str, List[str]]:
    by_table: Dict[str, List[str]] = defaultdict(list)
    for table in tables or uc.tables():
        for stock_id, row in latest_rows(query, table).items():
            for issue in uc.check_record(table, row):
                by_table[table].append(f"stock {stock_id}: {issue}")
    return dict(by_table)


def _close(a: float, b: float, scale: float, rel: float, abs_tol: float) -> bool:
    expected = b * scale
    return abs(a - expected) <= max(abs_tol, abs(expected) * rel)


def check_invariants(query: Query, universe: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None) -> List[str]:
    cache: Dict[str, Dict[int, Dict[str, Any]]] = dict(universe or {})
    failures: List[str] = []
    for inv in uc.CROSS_TABLE_INVARIANTS:
        (ta, ca), (tb, cb) = inv.a, inv.b
        for t in (ta, tb):
            if t not in cache:
                cache[t] = latest_rows(query, t)
        checked = bad = 0
        examples: List[str] = []
        for stock_id, row_a in cache[ta].items():
            row_b = cache[tb].get(stock_id)
            if not row_b:
                continue
            a, b = uc._as_number(row_a.get(ca)), uc._as_number(row_b.get(cb))
            if a is None or b is None:
                continue
            checked += 1
            if not _close(a, b, inv.scale, inv.rel_tolerance, inv.abs_tolerance):
                bad += 1
                if len(examples) < 3:
                    examples.append(f"stock {stock_id}: {ta}.{ca}={a:g} vs {inv.scale:g} x {tb}.{cb}={b:g}")
        # A unit swap shows up as a *large share* of stocks disagreeing; a few mismatches are refresh-timing noise.
        if checked and bad / checked > 0.20:
            failures.append(f"{inv.name}: {bad}/{checked} stocks disagree (e.g. {'; '.join(examples)})")
    return failures


def check_freshness(query: Query, today: Optional[date] = None, universe: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None) -> Dict[str, List[str]]:
    today = today or date.today()
    out: Dict[str, List[str]] = defaultdict(list)

    analyst = latest_analyst(query)
    for stock_id, row in analyst.items():
        d = row.get("next_earnings_date")
        if d is not None and str(d)[:10] < today.isoformat():
            out["next_earnings_in_past"].append(f"stock {stock_id}: next_earnings_date={str(d)[:10]}")

    fundamentals = (universe or {}).get("fundamental_data") or latest_rows(query, "fundamental_data")
    for stock_id, row in fundamentals.items():
        revenue, net_income, net_margin = (uc._as_number(row.get(k)) for k in ("revenue_ttm", "net_income_ttm", "net_margin"))
        if net_margin == 0.0 and revenue and net_income:
            out["manufactured_zero_net_margin"].append(f"stock {stock_id}: net_margin=0.0 but net_income/revenue={net_income / revenue:.4f}")
    return dict(out)


_CURRENT_TTM = (
    "(SELECT DISTINCT ON (stock_id) * FROM fundamental_data WHERE period_type = 'TTM' "
    f"ORDER BY stock_id, {LATEST_ORDER['fundamental_data'].split(', ', 1)[1]})"
)


def check_statement_consistency(query: Query) -> Dict[str, List[str]]:
    """Derived figures must agree with the statements printed beside them (each rule below once failed in production).

    * quarterly gross margin == gross profit / revenue (it was stored as percent points: the page showed 7457 %)
    * TTM gross / net / EBITDA margin == numerator / revenue (net margin used a different net-income basis)
    * free cash flow <= operating cash flow (Yahoo's levered FCF printed NFLX at 2x its OCF)
    * the current TTM row and today's snapshot are populated (a stub Yahoo response blanked AVGO, JPM and DHR)
    """
    checks = {
        "quarterly_gross_margin_not_gp_over_revenue": (
            "SELECT s.symbol, f.period_date FROM fundamental_data f JOIN stocks s ON s.id = f.stock_id "
            "WHERE f.period_type = 'Q' AND f.revenue_ttm > 0 AND f.gross_profit_ttm IS NOT NULL AND f.gross_margin IS NOT NULL "
            "AND ABS(f.gross_margin - f.gross_profit_ttm / f.revenue_ttm) > 0.002"
        ),
        "ttm_gross_margin_not_gp_over_revenue": (
            f"SELECT s.symbol FROM {_CURRENT_TTM} f JOIN stocks s ON s.id = f.stock_id "
            "WHERE f.revenue_ttm > 0 AND f.gross_profit_ttm IS NOT NULL AND f.gross_margin IS NOT NULL "
            "AND ABS(f.gross_margin - f.gross_profit_ttm / f.revenue_ttm) > 0.02"
        ),
        "ttm_net_margin_not_income_over_revenue": (
            f"SELECT s.symbol FROM {_CURRENT_TTM} f JOIN stocks s ON s.id = f.stock_id "
            "WHERE f.revenue_ttm > 0 AND f.net_income_ttm IS NOT NULL AND f.net_margin IS NOT NULL "
            "AND ABS(f.net_margin - f.net_income_ttm / f.revenue_ttm) > 0.002"
        ),
        "ttm_ebitda_margin_not_ebitda_over_revenue": (
            f"SELECT s.symbol FROM {_CURRENT_TTM} f JOIN stocks s ON s.id = f.stock_id "
            "WHERE f.revenue_ttm > 0 AND f.ebitda_ttm IS NOT NULL AND f.ebitda_margin IS NOT NULL "
            "AND ABS(f.ebitda_margin - f.ebitda_ttm / f.revenue_ttm) > 0.02"
        ),
        "free_cash_flow_exceeds_operating_cash_flow": (
            f"SELECT s.symbol FROM {_CURRENT_TTM} f JOIN stocks s ON s.id = f.stock_id "
            "WHERE f.operating_cash_flow > 0 AND f.free_cash_flow > f.operating_cash_flow * 1.001 + 1000"
        ),
        "current_ttm_row_empty": (
            f"SELECT s.symbol FROM {_CURRENT_TTM} f JOIN stocks s ON s.id = f.stock_id "
            "WHERE COALESCE(f.revenue_ttm, f.net_income_ttm, f.total_cash) IS NULL"
        ),
        "latest_snapshot_without_market_cap": (
            "SELECT s.symbol FROM (SELECT DISTINCT ON (stock_id) * FROM market_price_snapshot ORDER BY stock_id, date DESC) m "
            "JOIN stocks s ON s.id = m.stock_id WHERE m.market_cap IS NULL OR m.market_cap <= 0"
        ),
    }
    out: Dict[str, List[str]] = {}
    for name, sql_text in checks.items():
        out[name] = [" ".join(str(v) for v in row.values()) for row in query(sql_text, None)]
    return out


def check_publishable_consistency(query: Query) -> List[str]:
    """The sitemap lists stocks with ``data_end_date``; the page 404s without a price. They must agree."""
    problems: List[str] = []
    listed_without_price = query(
        "SELECT s.symbol FROM stocks s WHERE s.is_active AND s.data_end_date IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM market_price_snapshot m WHERE m.stock_id = s.id AND m.current_price > 0) "
        "AND NOT EXISTS (SELECT 1 FROM daily_price_data d WHERE d.stock_id = s.id AND d.close_price > 0)",
        None,
    )
    for row in listed_without_price:
        problems.append(f"{row['symbol']}: in the sitemap (data_end_date set) but its page would 404 (no price)")
    priced_not_listed = query(
        "SELECT s.symbol FROM stocks s WHERE s.is_active AND s.data_end_date IS NULL "
        "AND EXISTS (SELECT 1 FROM market_price_snapshot m WHERE m.stock_id = s.id AND m.current_price > 0)",
        None,
    )
    for row in priced_not_listed:
        problems.append(f"{row['symbol']}: has prices (page renders) but data_end_date is NULL so it is missing from the sitemap")
    return problems


def check_forecast_integrity(query: Query, expected_version: Optional[str] = None) -> Dict[str, List[str]]:
    """Forecast rows must be positive and ordered, complete (12 months per stock) and from the current model.

    The v1 model produced exact-zero/negative bands that the exporter dropped, leaving stocks with a truncated
    forecast; these checks make that class of failure visible instead of silent.
    """
    if expected_version is None:
        from Models.forecast_core import PIPELINE_VERSION as expected_version
    out: Dict[str, List[str]] = {"invalid_prices": [], "incomplete": [], "stale_model": []}
    bad = query(
        "SELECT s.symbol, f.period_end, f.low_price, f.base_price, f.high_price FROM forecast_data f "
        "JOIN stocks s ON s.id = f.stock_id WHERE f.low_price <= 0 OR f.base_price <= 0 OR f.high_price <= 0 "
        "OR f.low_price > f.base_price OR f.base_price > f.high_price",
        None,
    )
    for row in bad:
        out["invalid_prices"].append(f"{row['symbol']} {row['period_end']}: low/base/high = {row['low_price']}/{row['base_price']}/{row['high_price']}")
    for row in query(
        "SELECT s.symbol, COUNT(f.id) AS n FROM stocks s JOIN forecast_data f ON f.stock_id = s.id "
        "GROUP BY s.symbol HAVING COUNT(f.id) < 12",
        None,
    ):
        out["incomplete"].append(f"{row['symbol']}: {row['n']} of 12 monthly forecast rows")
    for row in query(
        "SELECT s.symbol, f.model_version FROM forecast_data f JOIN stocks s ON s.id = f.stock_id "
        "WHERE f.model_version IS DISTINCT FROM %s GROUP BY s.symbol, f.model_version",
        [expected_version],
    ):
        out["stale_model"].append(f"{row['symbol']}: forecast from {row['model_version'] or 'unknown model'} (current is {expected_version})")
    return out


def latest_analyst(query: Query) -> Dict[int, Dict[str, Any]]:
    rows = query("SELECT DISTINCT ON (stock_id) * FROM analyst_data ORDER BY stock_id, updated_at DESC NULLS LAST", None)
    return {int(r["stock_id"]): r for r in rows}


def run(query: Query, today: Optional[date] = None) -> Dict[str, Any]:
    schema = check_schema(query)
    bounds = check_bounds(query)
    invariants = check_invariants(query)
    freshness = check_freshness(query, today)
    publishable = check_publishable_consistency(query)
    forecast = check_forecast_integrity(query)
    statements = check_statement_consistency(query)
    failures = (
        len(schema) + sum(len(v) for v in bounds.values()) + len(invariants)
        + sum(len(v) for v in freshness.values()) + len(publishable) + sum(len(v) for v in forecast.values())
        + sum(len(v) for v in statements.values())
    )
    return {"schema": schema, "bounds": bounds, "invariants": invariants, "freshness": freshness,
            "publishable": publishable, "forecast": forecast, "statements": statements, "failures": failures}


def _print_report(report: Dict[str, Any]) -> None:
    def section(title: str, items: List[str]) -> None:
        print(f"\n[{'FAIL' if items else ' OK '}] {title} ({len(items)})")
        for line in items[:10]:
            print("   -", line)
        if len(items) > 10:
            print(f"   ... and {len(items) - 10} more")

    section("SCHEMA - contracted columns exist", report["schema"])
    for table in uc.tables():
        section(f"BOUNDS - {table}", report["bounds"].get(table, []))
    section("INVARIANTS - cross-table unit agreement", report["invariants"])
    for name, items in {**{"next_earnings_in_past": [], "manufactured_zero_net_margin": []}, **report["freshness"]}.items():
        section(f"FRESHNESS - {name}", items)
    section("PUBLISHING - sitemap and page agree on which stocks exist", report["publishable"])
    for name, items in report.get("forecast", {}).items():
        section(f"FORECAST - {name}", items)
    for name, items in report.get("statements", {}).items():
        section(f"STATEMENTS - {name}", items)
    print(f"\nTOTAL violations: {report['failures']}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    parser.add_argument("--max-failures", type=int, default=0, help="exit 0 if total violations <= this (default 0)")
    args = parser.parse_args(argv)

    report = run(_db_query())
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_report(report)
    return 0 if report["failures"] <= args.max_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
