"""Storage-unit contract for the TickZen database (producer side).

The unit of a stored number is a property of its ``table.column`` (see ``column_units.json``,
which is shared byte-for-byte with the Next.js ticker-page app), never of its magnitude.
Consumers convert with the registry; this module is the *producer's* half of the deal:

* ``check_record`` / ``guard_records`` - refuse grossly out-of-contract values at the DB-write
  boundary (bounds are intentionally wide: they catch a percentage written into a fraction
  column or a NaN, not an unusual-but-real company).
* ``CROSS_TABLE_INVARIANTS`` - relations that must hold between tables that store the same
  quantity in different units (e.g. ``fundamental_data.debt_to_equity`` percent points ==
  100 x ``peer_comparison_data.debt_to_equity``).  ``validate_unit_contract.py`` checks them
  against the live database.

No magnitude guessing is performed anywhere: when a value is out of bounds it is dropped
(set to NULL) and logged, never "corrected".
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

_REGISTRY_PATH = Path(__file__).with_name("column_units.json")

FRACTION = "fraction"
PERCENT_POINTS = "percent_points"
RATIO = "ratio"
DAYS = "days"
SCORE = "score"
UNIT_INTERVAL = "unit_interval"
USD = "usd"
USD_PER_SHARE = "usd_per_share"
COUNT = "count"
TEXT = "text"
DATE = "date"

NUMERIC_UNITS = {FRACTION, PERCENT_POINTS, RATIO, DAYS, SCORE, UNIT_INTERVAL, USD, USD_PER_SHARE, COUNT}


def _load_units() -> Dict[str, str]:
    with open(_REGISTRY_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)["units"]


UNITS: Dict[str, str] = _load_units()


def unit_of(table: str, column: str) -> Optional[str]:
    """Registered unit of ``table.column`` or None when the column is not part of the contract."""
    return UNITS.get(f"{table}.{column}")


def columns_of(table: str) -> List[str]:
    prefix = f"{table}."
    return [key[len(prefix):] for key in UNITS if key.startswith(prefix)]


def tables() -> List[str]:
    return sorted({key.split(".", 1)[0] for key in UNITS})


# ---------------------------------------------------------------------------
# Hard bounds (inclusive).  Deliberately wide: only gross violations are caught.
# ---------------------------------------------------------------------------

# Bounds by unit.  None = unbounded on that side.
_UNIT_BOUNDS: Dict[str, Tuple[Optional[float], Optional[float]]] = {
    FRACTION: (-1e5, 1e5),            # +-10,000,000 %: real extremes seen are ~2.6e3 (a 257,000 % growth)
    PERCENT_POINTS: (-1e6, 1e6),
    SCORE: (0.0, 100.0),
    UNIT_INTERVAL: (0.0, 1.0),
    DAYS: (-1e5, 1e5),
}

# Column-specific bounds that override the unit default.
_COLUMN_BOUNDS: Dict[str, Tuple[Optional[float], Optional[float]]] = {
    # fractions
    "dividend_data.dividend_yield_pct": (0.0, 0.5),
    "dividend_data.dividend_trailing_yield": (0.0, 0.5),
    "dividend_data.dividend_forward_yield": (0.0, 0.5),
    "dividend_data.avg_dividend_yield_5y": (0.0, 0.5),
    "dividend_data.payout_ratio": (0.0, 1000.0),
    "risk_data.var_95": (-1.0, 1.0),
    "risk_data.var_99": (-1.0, 1.0),
    "risk_data.cvar_95": (-1.0, 1.0),
    "risk_data.cvar_99": (-1.0, 1.0),
    "risk_data.max_drawdown": (-1.0, 0.0),
    "risk_data.volatility_30d_annual": (0.0, 20.0),
    "risk_data.volatility_historical_annual": (0.0, 20.0),
    "risk_data.sharpe_ratio_risk_free_rate_used": (-0.1, 1.0),
    # percent points
    "ownership_data.insider_ownership_pct": (0.0, 100.0),
    "ownership_data.institutional_ownership_pct": (0.0, 250.0),   # >100 happens (double counting, short-covering)
    "ownership_data.short_pct_float": (0.0, 100.0),
    "technical_indicators.volatility_30d_annual": (0.0, 2000.0),
    "peer_comparison_data.dividend_yield": (0.0, 100.0),
    "market_price_snapshot.change_pct": (-100.0, 1e5),
    "market_price_snapshot.change_15d_pct": (-100.0, 1e5),
    "market_price_snapshot.change_52w_pct": (-100.0, 1e6),
    "market_price_snapshot.performance_1y_pct": (-100.0, 1e6),
    "technical_indicators.rsi_14": (0.0, 100.0),
    # earnings: a one-session move cannot lose more than 100 %; wide upper bound for genuine squeezes
    "earnings_history.reaction_1d_pct": (-100.0, 400.0),
    "earnings_history.reaction_3d_pct": (-100.0, 800.0),
    "earnings_history.gap_pct": (-100.0, 400.0),
    "earnings_history.runup_5d_pct": (-100.0, 800.0),
    "earnings_history.runup_20d_pct": (-100.0, 800.0),
    "earnings_history.eps_surprise_pct": (-1e5, 1e5),
    "earnings_calendar.implied_move_pct": (0.0, 100.0),
    "earnings_history.revenue_surprise_pct": (-100.0, 1e5),
    # ratios that cannot be negative
    "fundamental_data.current_ratio": (0.0, None),
    "fundamental_data.quick_ratio": (0.0, None),
    "peer_comparison_data.debt_to_equity": (None, 1e5),
}


def bounds_of(table: str, column: str) -> Tuple[Optional[float], Optional[float]]:
    key = f"{table}.{column}"
    if key in _COLUMN_BOUNDS:
        return _COLUMN_BOUNDS[key]
    return _UNIT_BOUNDS.get(UNITS.get(key, ""), (None, None))


@dataclass(frozen=True)
class Issue:
    table: str
    column: str
    value: Any
    reason: str

    def __str__(self) -> str:  # pragma: no cover - formatting only
        return f"{self.table}.{self.column}={self.value!r}: {self.reason}"


def _as_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def check_value(table: str, column: str, value: Any) -> Optional[Issue]:
    """Return an Issue when ``value`` violates the contract for ``table.column`` (None = fine / not contracted)."""
    unit = unit_of(table, column)
    if unit not in NUMERIC_UNITS or value is None:
        return None
    number = _as_number(value)
    if number is None:
        return Issue(table, column, value, f"not numeric (unit {unit})")
    if not math.isfinite(number):
        return Issue(table, column, value, "NaN/Infinity is not storable")
    low, high = bounds_of(table, column)
    if low is not None and number < low:
        return Issue(table, column, value, f"below {low} for unit {unit}")
    if high is not None and number > high:
        return Issue(table, column, value, f"above {high} for unit {unit}")
    return None


def check_record(table: str, record: Dict[str, Any]) -> List[Issue]:
    issues: List[Issue] = []
    for column, value in record.items():
        issue = check_value(table, column, value)
        if issue is not None:
            issues.append(issue)
    return issues


def guard_mode() -> str:
    """``enforce`` (default): NULL out violations; ``warn``: log only; ``off``: disabled."""
    mode = os.getenv("TICKZEN_UNIT_GUARD", "enforce").strip().lower()
    return mode if mode in {"enforce", "warn", "off"} else "enforce"


def guard_records(table: str, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply the contract to rows about to be written.

    In ``enforce`` mode a violating value is replaced by None (the row is still written - one bad
    metric must not lose the rest of the stock's data) and logged at WARNING with table.column.
    Rows are copied, never mutated in place.
    """
    mode = guard_mode()
    out: List[Dict[str, Any]] = []
    for record in records:
        if mode == "off" or not isinstance(record, dict):
            out.append(record)
            continue
        issues = check_record(table, record)
        if not issues:
            out.append(record)
            continue
        for issue in issues:
            logger.warning("Unit contract violation (%s): %s", mode, issue)
        if mode == "enforce":
            cleaned = dict(record)
            for issue in issues:
                cleaned[issue.column] = None
            out.append(cleaned)
        else:
            out.append(record)
    return out


# ---------------------------------------------------------------------------
# Cross-table invariants: the same quantity stored in two tables / two units.
# ``a`` and ``b`` are (table, column); a == scale * b within ``tolerance`` (relative).
# They are compared per stock on the latest row of each table.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Invariant:
    name: str
    a: Tuple[str, str]
    b: Tuple[str, str]
    scale: float          # expected a / b
    rel_tolerance: float  # allowed relative error (data may be refreshed at different times)
    abs_tolerance: float  # allowed absolute error for tiny values


CROSS_TABLE_INVARIANTS: List[Invariant] = [
    Invariant("debt_to_equity: fundamental (percent points) = 100 x peer target (ratio)",
              ("fundamental_data", "debt_to_equity"), ("peer_comparison_data", "debt_to_equity"), 100.0, 0.05, 0.5),
    Invariant("dividend yield: peer target (percent points) = 100 x dividend_data (fraction)",
              ("peer_comparison_data", "dividend_yield"), ("dividend_data", "dividend_yield_pct"), 100.0, 0.25, 0.15),
    Invariant("30d volatility: technical (percent points) = 100 x risk (fraction)",
              ("technical_indicators", "volatility_30d_annual"), ("risk_data", "volatility_30d_annual"), 100.0, 0.10, 0.5),
    Invariant("roe: peer target = fundamental (both fractions)",
              ("peer_comparison_data", "roe"), ("fundamental_data", "roe"), 1.0, 0.02, 0.005),
    Invariant("net margin: peer target = fundamental (both fractions)",
              ("peer_comparison_data", "net_margin"), ("fundamental_data", "net_margin"), 1.0, 0.02, 0.005),
    Invariant("revenue growth: peer target = fundamental (both fractions)",
              ("peer_comparison_data", "revenue_growth"), ("fundamental_data", "revenue_growth_yoy"), 1.0, 0.02, 0.005),
]
