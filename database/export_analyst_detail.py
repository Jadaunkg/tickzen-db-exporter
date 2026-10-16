#!/usr/bin/env python3
"""Export analyst detail (rating breakdown, firm actions, EPS/revenue estimates) to Azure PostgreSQL.

Feeds the analyst-based half of the Next.js ``/stocks/<ticker>/forecast`` tab and the estimate-revision facts of
the Insights earnings pack (``analyst_eps_trend``: consensus EPS now vs 7/30/60/90 days ago, up/down revisions).  ``analyst_data`` only keeps the
consensus price targets, one rating key and the analyst count; this adds what sits behind them.  Free source only:
Yahoo (``yfinance``), every request through the shared Yahoo limiter used by the earnings exporter.

Usage::

    python database/export_analyst_detail.py --migrate                      # create the table (idempotent)
    python database/export_analyst_detail.py --tickers NVDA AAPL            # specific stocks
    python database/export_analyst_detail.py --all --skip-fresh-hours 144   # weekly sweep / first backfill
    python database/export_analyst_detail.py --stalest 60                   # batch: oldest first

The per-stock exporter (``export_to_supabase.py``) calls :func:`refresh_if_stale` after the analyst_data export.

Honesty rules: values are stored exactly as the vendor reports them in explicit units (growth as percent points,
prices in the quote currency); a missing field is NULL, never 0 (Yahoo reports a missing prior target as 0, which
is dropped).  A dataset Yahoo does not have for a stock is NULL, so the page shows nothing rather than an empty chart.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from earnings_reports import earnings_history as eh  # noqa: E402  (shared Yahoo limiter + ProviderThrottled)

logger = logging.getLogger(__name__)

MIGRATION_PATH = Path(__file__).with_name("migrations") / "20261006_analyst_detail.sql"
TREND_MIGRATION_PATH = Path(__file__).with_name("migrations") / "20261016_analyst_eps_trend.sql"
TABLE = "analyst_detail_data"
TREND_TABLE = "analyst_eps_trend"
ACTION_LOOKBACK_DAYS = 200   # firm actions older than this are not stored (the page uses the last 90-180 days)
MAX_ACTIONS = 120            # safety cap per stock; the frontend mirrors it (STORED_ACTIONS_CAP) to tell "truncated" from "complete"
MAX_AGE_HOURS = 144          # weekly cadence, like analyst_data
MAX_CONSECUTIVE_FAILURES = 8

RATING_PERIODS = ("0m", "-1m", "-2m", "-3m")
ESTIMATE_PERIODS = ("0q", "+1q", "0y", "+1y")


# --------------------------------------------------------------------------- normalisers (pure)
def _num(value: Any) -> Optional[float]:
    """Finite float or None (NaN / inf / '' / non-numeric are 'missing', never 0)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _int(value: Any) -> Optional[int]:
    out = _num(value)
    return int(round(out)) if out is not None else None


def _text(value: Any, limit: int = 80) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "nat"}:
        return None
    return s[:limit]


def _price(value: Any) -> Optional[float]:
    """A quoted price: positive or missing (Yahoo sends 0 for 'no prior target')."""
    n = _num(value)
    return round(n, 4) if n is not None and n > 0 else None


def _rows(frame: Any) -> List[Dict[str, Any]]:
    """DataFrame -> list of dicts, keeping the index under '_index'.  Anything else -> []."""
    if frame is None or not hasattr(frame, "to_dict") or getattr(frame, "empty", True):
        return []
    out = []
    for index, record in zip(list(frame.index), frame.to_dict("records")):
        row = dict(record)
        row["_index"] = index
        out.append(row)
    return out


def normalize_rating_trend(frame: Any) -> Optional[List[Dict[str, Any]]]:
    """Yahoo ``recommendations`` -> analysts per rating bucket for this month and the previous three."""
    by_period: Dict[str, Dict[str, Any]] = {}
    for row in _rows(frame):
        period = _text(row.get("period"), 8)
        if period not in RATING_PERIODS:
            continue
        counts = {
            "strong_buy": _int(row.get("strongBuy")),
            "buy": _int(row.get("buy")),
            "hold": _int(row.get("hold")),
            "sell": _int(row.get("sell")),
            "strong_sell": _int(row.get("strongSell")),
        }
        if all(v is None for v in counts.values()):
            continue
        by_period[period] = {"period": period, **{k: (v or 0) for k, v in counts.items()}}
    ordered = [by_period[p] for p in RATING_PERIODS if p in by_period]
    return ordered or None


def _iso_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "date") and callable(value.date):
        try:
            return value.date().isoformat()
        except (ValueError, AttributeError):
            return None
    s = str(value).strip()[:10]
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return None
    return s


def normalize_actions(
    frame: Any,
    limit: int = MAX_ACTIONS,
    *,
    today: Optional[date] = None,
    lookback_days: int = ACTION_LOOKBACK_DAYS,
) -> Optional[List[Dict[str, Any]]]:
    """Yahoo ``upgrades_downgrades`` -> newest-first firm actions with rating and price-target changes.

    Keeps everything from the last ``lookback_days`` days (so "last 90 days" statistics are complete), newest first,
    capped at ``limit``.  Pass ``today=None`` to use the current UTC date.
    """
    today = today or datetime.now(timezone.utc).date()
    cutoff = (today - timedelta(days=lookback_days)).isoformat()
    actions = []
    for row in _rows(frame):
        day = _iso_date(row.get("_index"))
        firm = _text(row.get("Firm"))
        if not day or not firm or day < cutoff:
            continue
        actions.append(
            {
                "date": day,
                "firm": firm,
                "action": _text(row.get("Action"), 16),
                "from_grade": _text(row.get("FromGrade")),
                "to_grade": _text(row.get("ToGrade")),
                "price_target_action": _text(row.get("priceTargetAction"), 24),
                "target_price": _price(row.get("currentPriceTarget")),
                "prior_target_price": _price(row.get("priorPriceTarget")),
            }
        )
    actions.sort(key=lambda a: a["date"], reverse=True)
    return actions[:limit] or None


def normalize_estimates(frame: Any) -> Optional[List[Dict[str, Any]]]:
    """Yahoo ``earnings_estimate`` / ``revenue_estimate`` -> consensus per period.  Growth is a fraction at the
    vendor ('0.9026') and is stored as percent points (90.26)."""
    out = []
    for row in _rows(frame):
        period = _text(row.get("_index"), 8)
        if period not in ESTIMATE_PERIODS:
            continue
        growth = _num(row.get("growth"))
        year_ago = _num(row.get("yearAgoEps") if "yearAgoEps" in row else row.get("yearAgoRevenue"))
        item = {
            "period": period,
            "avg": _num(row.get("avg")),
            "low": _num(row.get("low")),
            "high": _num(row.get("high")),
            "year_ago": year_ago,
            "analysts": _int(row.get("numberOfAnalysts")),
            "growth_pct": round(growth * 100, 2) if growth is not None else None,
            "currency": _text(row.get("currency"), 8),
        }
        if item["avg"] is None:
            continue
        out.append(item)
    out.sort(key=lambda r: ESTIMATE_PERIODS.index(r["period"]))
    return out or None


TREND_FIELDS = {"current": "eps_current", "7daysago": "eps_7d_ago", "30daysago": "eps_30d_ago",
                "60daysago": "eps_60d_ago", "90daysago": "eps_90d_ago"}
REVISION_FIELDS = {"uplast7days": "up_last_7d", "uplast30days": "up_last_30d",
                   "downlast7days": "down_last_7d", "downlast30days": "down_last_30d"}


def normalize_eps_trend(trend: Any, revisions: Any) -> Optional[List[Dict[str, Any]]]:
    """Yahoo ``eps_trend`` + ``eps_revisions`` -> one row per period: the consensus EPS now and 7/30/60/90 days ago
    (usd_per_share) and the analysts who raised / cut it in the last 7 / 30 days (counts).  Yahoo's column casing
    varies ('downLast7days' / 'downLast7Days'), so names are matched case-insensitively."""
    by_period: Dict[str, Dict[str, Any]] = {}

    def merge(frame: Any, fields: Dict[str, str], convert: Callable[[Any], Any]) -> None:
        for row in _rows(frame):
            period = _text(row.get("_index"), 8)
            if period not in ESTIMATE_PERIODS:
                continue
            item = by_period.setdefault(period, {"period": period})
            for key, value in row.items():
                column = fields.get(str(key).lower())
                if column:
                    item[column] = convert(value)
            if not item.get("currency"):
                item["currency"] = _text(row.get("currency"), 8)

    merge(trend, TREND_FIELDS, _num)
    merge(revisions, REVISION_FIELDS, _int)
    columns = list(TREND_FIELDS.values()) + list(REVISION_FIELDS.values())
    out = []
    for period in ESTIMATE_PERIODS:
        item = by_period.get(period)
        if not item or all(item.get(c) is None for c in columns):
            continue
        out.append({"period": period, **{c: item.get(c) for c in columns}, "currency": item.get("currency")})
    return out or None


# --------------------------------------------------------------------------- fetch + build
def fetch_detail(symbol: str, **call_kwargs: Any) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    """The four datasets for one symbol.  A dataset that fails (bad ticker, no coverage) is None; persistent
    throttling propagates as ``ProviderThrottled`` so the caller can stop instead of hammering Yahoo."""
    import yfinance as yf

    handle = yf.Ticker(symbol)

    def fetch(getter: Callable[[Any], Any], normalise: Callable[[Any], Any], label: str):
        try:
            return normalise(eh.yahoo_call(lambda: getter(handle), **call_kwargs))
        except eh.ProviderThrottled:
            raise
        except Exception as exc:  # noqa: BLE001 - one dataset missing must not lose the others
            logger.info("analyst detail %s: %s unavailable (%s)", symbol, label, exc)
            return None

    return {
        "rating_trend": fetch(lambda t: t.recommendations, normalize_rating_trend, "rating trend"),
        "recent_actions": fetch(lambda t: t.upgrades_downgrades, normalize_actions, "recent actions"),
        "eps_estimates": fetch(lambda t: t.earnings_estimate, normalize_estimates, "EPS estimates"),
        "revenue_estimates": fetch(lambda t: t.revenue_estimate, normalize_estimates, "revenue estimates"),
        # Same quoteSummary 'earningsTrend' answer as the two estimates above (cached on the handle): no extra request.
        "eps_trend": fetch(lambda t: (t.eps_trend, t.eps_revisions), lambda pair: normalize_eps_trend(*pair), "EPS trend"),
    }


def build_record(stock_id: int, detail: Dict[str, Any], now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    return {
        "stock_id": stock_id,
        "rating_trend": detail.get("rating_trend"),
        "recent_actions": detail.get("recent_actions"),
        "eps_estimates": detail.get("eps_estimates"),
        "revenue_estimates": detail.get("revenue_estimates"),
        "source": "yfinance",
        "fetched_at": now,
        "updated_at": now,
    }


def build_trend_rows(stock_id: int, trend: Optional[List[Dict[str, Any]]], now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    return [{"stock_id": stock_id, **row, "source": "yfinance", "fetched_at": now} for row in trend or []]


# --------------------------------------------------------------------------- db
def apply_migration(db) -> None:
    """Run the additive, idempotent migrations."""
    for path in (MIGRATION_PATH, TREND_MIGRATION_PATH):
        db.execute_raw_sql(path.read_text(encoding="utf-8"))


def active_stocks(db, tickers: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    rows = db.execute_raw_sql(
        "SELECT id, COALESCE(symbol, ticker) AS symbol FROM public.stocks "
        "WHERE is_active = true AND COALESCE(symbol, ticker) IS NOT NULL ORDER BY COALESCE(symbol, ticker)"
    )
    if tickers:
        wanted = {t.strip().upper() for t in tickers}
        rows = [r for r in rows if str(r["symbol"]).upper() in wanted]
    return [{"id": int(r["id"]), "symbol": str(r["symbol"]).upper()} for r in rows]


def recently_refreshed_ids(db, hours: int) -> set:
    if hours <= 0:
        return set()
    rows = db.execute_raw_sql(
        "SELECT stock_id FROM public.analyst_detail_data WHERE fetched_at > NOW() - (%s * INTERVAL '1 hour')",
        [hours],
    )
    return {int(r["stock_id"]) for r in rows}


def select_stale_stocks(db, limit: int, min_age_hours: int = MAX_AGE_HOURS) -> List[Dict[str, Any]]:
    """Active stocks never fetched first, then the ones fetched longest ago (older than ``min_age_hours``)."""
    rows = db.execute_raw_sql(
        "SELECT s.id, COALESCE(s.symbol, s.ticker) AS symbol FROM public.stocks s "
        "LEFT JOIN public.analyst_detail_data d ON d.stock_id = s.id "
        "WHERE s.is_active = true AND COALESCE(s.symbol, s.ticker) IS NOT NULL "
        "AND (d.fetched_at IS NULL OR d.fetched_at < NOW() - (%s * INTERVAL '1 hour')) "
        "ORDER BY d.fetched_at NULLS FIRST, s.id LIMIT %s",
        [min_age_hours, limit],
    )
    return [{"id": int(r["id"]), "symbol": str(r["symbol"]).upper()} for r in rows]


def refresh_stock(
    db,
    stock: Dict[str, Any],
    *,
    fetch: Callable[[str], Dict[str, Any]] = fetch_detail,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Fetch and upsert one stock's analyst detail.  Returns a small summary."""
    detail = fetch(stock["symbol"])
    record = build_record(stock["id"], detail, now)
    db.table(TABLE).upsert(record, on_conflict="stock_id").execute()
    filled = [k for k in ("rating_trend", "recent_actions", "eps_estimates", "revenue_estimates") if record[k]]
    trend_rows = build_trend_rows(stock["id"], detail.get("eps_trend"), record["fetched_at"])
    if trend_rows:   # no trend from Yahoo: keep the previous rows, their fetched_at shows how old they are
        db.table(TREND_TABLE).upsert(trend_rows, on_conflict="stock_id,period").execute()
        filled.append("eps_trend")
    return {"symbol": stock["symbol"], "datasets": filled}


def refresh_if_stale(db, stock: Dict[str, Any], max_age_hours: int = MAX_AGE_HOURS, **kwargs: Any) -> Optional[Dict[str, Any]]:
    """Per-stock exporter hook: refresh unless fetched within ``max_age_hours``."""
    if stock["id"] in recently_refreshed_ids(db, max_age_hours):
        return None
    return refresh_stock(db, stock, **kwargs)


def refresh_many(
    db,
    stocks: Sequence[Dict[str, Any]],
    *,
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
    pause_seconds: float = 0.0,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Refresh many stocks.  One failing ticker never stops the run; persistent throttling or a run of consecutive
    failures does (circuit breaker), so a provider that is refusing us is not hammered."""
    ok = failed = streak = 0
    failures: List[str] = []
    aborted: Optional[str] = None
    for stock in stocks:
        try:
            summary = refresh_stock(db, stock, **kwargs)
            ok += 1
            streak = 0
            logger.info("analyst detail %s: %s", stock["symbol"], summary["datasets"])
        except eh.ProviderThrottled as exc:
            aborted = f"provider throttled at {stock['symbol']}: {exc}"
            logger.error("analyst detail aborted: %s", aborted)
            break
        except Exception as exc:  # noqa: BLE001
            failed += 1
            streak += 1
            failures.append(stock["symbol"])
            logger.warning("analyst detail %s failed: %s", stock["symbol"], exc)
            if streak >= max_consecutive_failures:
                aborted = f"{streak} consecutive failures (last: {stock['symbol']})"
                logger.error("analyst detail aborted: %s", aborted)
                break
        if pause_seconds:
            time.sleep(pause_seconds)
    out = {"stocks": len(stocks), "ok": ok, "failed": failed, "failures": failures[:50]}
    if aborted:
        out["aborted"] = aborted
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--migrate", action="store_true", help="apply the additive migration first")
    parser.add_argument("--tickers", nargs="*", help="refresh these stocks")
    parser.add_argument("--all", action="store_true", help="refresh every active stock")
    parser.add_argument("--stalest", type=int, help="refresh the N least recently fetched stocks (batch mode)")
    parser.add_argument("--skip-fresh-hours", type=int, default=0, help="with --all: skip stocks fetched within N hours")
    parser.add_argument("--limit", type=int, help="process at most N stocks")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    from database.azure_postgres_client import AzurePostgresClient

    db = AzurePostgresClient()
    try:
        if args.migrate:
            apply_migration(db)
            logger.info("migration applied")

        stocks: List[Dict[str, Any]] = []
        if args.tickers:
            stocks = active_stocks(db, args.tickers)
        elif args.all:
            skip = recently_refreshed_ids(db, args.skip_fresh_hours)
            stocks = [s for s in active_stocks(db) if s["id"] not in skip]
        elif args.stalest:
            stocks = select_stale_stocks(db, args.stalest, args.skip_fresh_hours or MAX_AGE_HOURS)
        if args.limit:
            stocks = stocks[: args.limit]
        if stocks:
            logger.info("refreshing analyst detail for %d stocks", len(stocks))
            print(refresh_many(db, stocks))
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
