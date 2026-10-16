"""Versioned financial-data contract helpers shared by producer code.

This module deliberately contains no provider or database dependencies so the
normalization rules can be tested without network access.  Database columns
whose names end in ``_pct`` are legacy percentage-point fields; the canonical
snapshot contract always uses decimal fractions.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional


SCHEMA_VERSION = "stock-snapshot-v1"
CALCULATION_VERSION = "financial-contract-v1"


def finite_float(value: Any) -> Optional[float]:
    """Return a finite float or ``None``; never manufacture a zero."""
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def percentage_points_to_fraction(value: Any) -> Optional[float]:
    """Convert an explicitly percentage-point value to a decimal fraction."""
    number = finite_float(value)
    if number is None:
        return None
    return number / 100.0


def provider_fraction(value: Any) -> Optional[float]:
    """Validate a provider-owned fraction without magnitude-based guessing."""
    number = finite_float(value)
    if number is None or not 0 <= number <= 1:
        return None
    return number


def latest_statement_period(*frames: Any) -> Optional[str]:
    """Return the newest financial-statement column date as ``YYYY-MM-DD``."""
    candidates = []
    for frame in frames:
        columns = getattr(frame, "columns", None)
        if columns is None:
            continue
        for column in columns:
            try:
                parsed = column.to_pydatetime() if hasattr(column, "to_pydatetime") else column
                parsed = parsed.date() if isinstance(parsed, datetime) else parsed
                if isinstance(parsed, date):
                    candidates.append(parsed)
                else:
                    candidates.append(datetime.fromisoformat(str(parsed)).date())
            except (TypeError, ValueError, OverflowError):
                continue
    return max(candidates).isoformat() if candidates else None


def stable_content_hash(record: Dict[str, Any], ignored: Iterable[str] = ()) -> str:
    """Create a deterministic hash for financial facts, excluding retrieval data."""
    ignored_keys = set(ignored)

    def normalize(value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, dict):
            return {key: normalize(value[key]) for key in sorted(value) if key not in ignored_keys}
        if isinstance(value, (list, tuple)):
            return [normalize(item) for item in value]
        if hasattr(value, "item") and callable(value.item):
            return normalize(value.item())
        return value

    payload = json.dumps(normalize(record), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalized_exchange(code: Any) -> Optional[str]:
    value = str(code or "").strip().upper()
    return {
        "NMS": "NASDAQ",
        "NAS": "NASDAQ",
        "NYQ": "NYSE",
        "NYS": "NYSE",
        "PCX": "NYSE Arca",
        "ASE": "NYSE American",
    }.get(value, value or None)



# ---------------------------------------------------------------------------
# Rules added with the storage-unit contract (see unit_contract.py / column_units.json)
# ---------------------------------------------------------------------------

def reconcile_margin(provider_margin: Any, numerator: Any, revenue: Any) -> Optional[float]:
    """Return a margin *fraction* that is never a manufactured zero.

    Providers (yfinance) return an exact ``0`` for margins they cannot compute. Publishing that as a
    real 0.00 % hides, e.g., a -430 % net margin. Rules:
      * provider value missing            -> computed from the statements, else None
      * provider says exactly 0 but the statements give a non-zero margin -> the computed margin
      * provider says exactly 0 and nothing can verify it -> None (unknown)
      * anything else                     -> the provider value, untouched
    """
    provider = finite_float(provider_margin)
    num = finite_float(numerator)
    rev = finite_float(revenue)
    computed = (num / rev) if (num is not None and rev not in (None, 0)) else None
    if computed is not None and not math.isfinite(computed):
        computed = None
    if provider is None:
        return computed
    if provider == 0.0:
        if computed is not None and computed != 0.0:
            return computed
        if computed is None:
            return None
    return provider


def reconcile_fundamental_margins(record: Dict[str, Any]) -> Dict[str, Any]:
    """Apply :func:`reconcile_margin` to the margin columns of a ``fundamental_data`` record (in place)."""
    revenue = record.get("revenue_ttm")
    # The profit margin Yahoo publishes is built on a different net-income basis than ``netIncomeToCommon``
    # (JCI: 14.3 % beside a $2.18B profit on $25.0B revenue = 8.7 %), so the page printed a margin that
    # contradicted the net income next to it. When both inputs are present the margin is derived from them.
    net_income, rev = finite_float(record.get("net_income_ttm")), finite_float(revenue)
    if net_income is not None and rev not in (None, 0):
        record["net_margin"] = net_income / rev
    else:
        record["net_margin"] = reconcile_margin(record.get("net_margin"), record.get("net_income_ttm"), revenue)
    record["gross_margin"] = reconcile_margin(record.get("gross_margin"), record.get("gross_profit_ttm"), revenue)
    record["ebitda_margin"] = reconcile_margin(record.get("ebitda_margin"), record.get("ebitda_ttm"), revenue)
    # operating income is not part of the TTM record, so a provider 0 can only be verified against revenue existing at all
    if finite_float(record.get("operating_margin")) == 0.0 and finite_float(revenue) in (None, 0):
        record["operating_margin"] = None
    return record


class PartialProviderData(ValueError):
    """Yahoo answered with a stub ``info`` payload (rate limit / outage), not a company profile."""


def is_partial_provider_info(info: Optional[Dict[str, Any]]) -> bool:
    """True when ``info`` has no market cap, share count, enterprise value or revenue at all.

    Such a payload used to be written as-is: the daily snapshot lost its market cap and the TTM
    fundamentals row came out empty (AVGO, JPM and DHR on 2026-10-06), and the empty row then won
    "latest row" on the site. A real company always has at least one of these fields; a pre-revenue
    company still has a market cap.
    """
    info = info or {}
    return not any(finite_float(info.get(key)) for key in ("marketCap", "sharesOutstanding", "enterpriseValue", "totalRevenue"))


SNAPSHOT_SIZE_FIELDS = ("market_cap", "enterprise_value", "shares_outstanding", "float_shares")


def rescale_market_size(
    vendor_market_cap: Any,
    vendor_enterprise_value: Any,
    vendor_price: Any,
    live_price: Any,
    total_debt: Any = None,
    total_cash: Any = None,
) -> "tuple[Optional[float], Optional[float]]":
    """Vendor market cap and EV moved from the price they were quoted at to ``live_price``.

    ``info`` is read from a profile cache that can be weeks old, so its market cap does not move with the
    live price shown beside it. Market cap scales with price. EV is market cap + debt - cash whenever both
    balance-sheet figures are known (the definition the EV multiples and the Net Cash card use, so the tab
    ties out); otherwise the vendor's net debt is kept. Without both prices the vendor figures pass through.
    """
    cap, ev = finite_float(vendor_market_cap), finite_float(vendor_enterprise_value)
    cap = cap if cap and cap > 0 else None
    ev = ev if ev and ev > 0 else None
    debt, cash = finite_float(total_debt), finite_float(total_cash)
    old_price, new_price = finite_float(vendor_price), finite_float(live_price)
    if cap and old_price and new_price and old_price > 0 and new_price > 0:
        cap = cap * new_price / old_price
        if debt is not None and cash is not None:
            return cap, cap + debt - cash
        return cap, (cap + (ev - finite_float(vendor_market_cap)) if ev else None)
    if cap and debt is not None and cash is not None:
        return cap, cap + debt - cash
    return cap, ev


def carry_forward_snapshot_size(
    record: Dict[str, Any],
    previous: Optional[Dict[str, Any]],
    today: Optional[date] = None,
    max_age_days: int = 14,
) -> Dict[str, Any]:
    """Fill empty size fields of today's market snapshot from the last complete one (in place).

    A partial Yahoo response left today's row without market cap / EV / share counts, and the site reads the
    newest row, so those stocks showed no market cap for days. Market cap is price x shares, so the previous
    value is scaled by the price move; EV keeps the previous net debt. Share counts are carried unchanged.
    Rows older than ``max_age_days`` are not trusted. Fields that are already present are never touched.
    """
    if not previous or all(finite_float(record.get(f)) for f in SNAPSHOT_SIZE_FIELDS):
        return record
    prev_date = previous.get("date")
    try:
        prev_day = prev_date if isinstance(prev_date, date) and not isinstance(prev_date, datetime) else datetime.fromisoformat(str(prev_date)[:10]).date()
    except (TypeError, ValueError):
        return record
    if ((today or date.today()) - prev_day).days > max_age_days:
        return record

    price, prev_price = finite_float(record.get("current_price")), finite_float(previous.get("current_price"))
    prev_cap, prev_ev = finite_float(previous.get("market_cap")), finite_float(previous.get("enterprise_value"))
    scale = (price / prev_price) if price and prev_price and price > 0 and prev_price > 0 else 1.0

    if not finite_float(record.get("market_cap")) and prev_cap:
        record["market_cap"] = prev_cap * scale
    cap = finite_float(record.get("market_cap"))
    if not finite_float(record.get("enterprise_value")) and prev_ev is not None and prev_cap and cap:
        record["enterprise_value"] = cap + (prev_ev - prev_cap)
    for field in ("shares_outstanding", "float_shares"):
        if not finite_float(record.get(field)) and finite_float(previous.get(field)):
            record[field] = finite_float(previous.get(field))
    return record


def _statement_value(statement: Any, label: str) -> Optional[float]:
    """Newest-column value of one row of a yfinance statement DataFrame, else None."""
    try:
        if statement is None or getattr(statement, "empty", True) or label not in statement.index:
            return None
        return finite_float(statement.loc[label].iloc[0])
    except (AttributeError, IndexError, KeyError, TypeError, ValueError):
        return None


def ttm_cash_flow_from_statement(statement: Any) -> Dict[str, Optional[float]]:
    """Operating cash flow and FREE cash flow (operating cash flow minus capex) from a trailing-twelve-month statement.

    Yahoo's ``info['freeCashflow']`` is a different ("levered") measure: it printed NFLX at $25.4B beside an
    operating cash flow of $12.0B and AAPL at $107.7B where OCF - capex is $136.7B, while the page labels the
    figure "available after capex". The statement's own lines are consistent with each other, so FCF is derived
    from them: reported FCF is only trusted when capex is unavailable, and never when it exceeds OCF.
    """
    ocf = _statement_value(statement, "Operating Cash Flow")
    capex = _statement_value(statement, "Capital Expenditure")
    reported_fcf = _statement_value(statement, "Free Cash Flow")
    if ocf is not None and capex is not None:
        fcf: Optional[float] = ocf - abs(capex)
    elif ocf is not None and reported_fcf is not None and reported_fcf <= ocf:
        fcf = reported_fcf
    else:
        fcf = reported_fcf if ocf is None else None
    return {"operating_cash_flow": ocf, "free_cash_flow": fcf}


def next_earnings_date(info: Dict[str, Any], today: Optional[date] = None) -> Optional[str]:
    """The earliest earnings date that has NOT happened yet, as ``YYYY-MM-DD`` (US/Eastern), else None.

    Yahoo keeps ``earningsTimestamp`` pointing at the *last* report until it rolls the field forward,
    so storing it blindly published dates months in the past as "next earnings" (163 of 236 stocks).
    Start/End are the announced window; any candidate that is still in the future qualifies.
    """
    try:
        from zoneinfo import ZoneInfo

        eastern = ZoneInfo("America/New_York")
    except Exception:  # pragma: no cover - tzdata missing
        import pytz

        eastern = pytz.timezone("US/Eastern")

    from datetime import timezone

    reference = today or datetime.now(eastern).date()
    candidates = []
    for key in ("earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd"):
        raw = info.get(key)
        if not raw:
            continue
        try:
            moment = datetime.fromtimestamp(float(raw), tz=timezone.utc).astimezone(eastern)
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        candidates.append(moment.date())
    upcoming = [d for d in candidates if d >= reference]
    return min(upcoming).isoformat() if upcoming else None
