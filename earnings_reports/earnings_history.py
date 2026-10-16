"""Earnings history, price reaction and options-implied move.

Produces the data behind the earnings pages (Flask ``/stock-analysis/earnings-calendar/*`` and the
Next.js ``/stocks/<ticker>/earnings`` tab).  Everything here is free-source (Yahoo via ``yfinance``)
and computed, never invented:

* ``build_history_records``    EPS estimate / actual / surprise + the measured price reaction
* ``measure_reaction``         close-to-close move over the first session that reflects the report
* ``implied_move_from_chain``  nearest at-the-money straddle that expires after the report

Honesty rules (these are what keep the numbers trustworthy enough to publish):

* A reaction is only stored when the report time (before open / after close) is known.
* If the *other* session shows a far bigger move than the labelled one, the label is suspect and the
  reaction is dropped (NULL) rather than guessed.
* An implied move is only stored from a liquid, tight-spread straddle that expires close to the report,
  quoted live during the regular session.  Outside it Yahoo zeroes every bid, ask and open interest, and
  the last trades can be days old, so an after-hours chain is never used (see ``fetch_implied_move``).
* Units follow ``database/column_units.json``: every ``*_pct`` here is in **percent points**.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from earnings_reports.rate_limit import RateLimiter, get_limiter  # noqa: E402,F401  (RateLimiter re-exported)

logger = logging.getLogger(__name__)

BMO = "BMO"  # before market open
AMC = "AMC"  # after market close
DMH = "DMH"  # during market hours

# Reaction sanity limits.  A single-session move beyond these is a data error (bad split adjustment,
# stale quote), not a market move.
MAX_ABS_REACTION_PCT = 400.0
# The reaction session must start within this many calendar days of the report date (guards price gaps).
MAX_REACTION_LAG_DAYS = 6
# If the un-labelled session moved this many times more (and by at least ALT_MIN_PCT), the report-time
# label is considered unreliable and the reaction is dropped.
ALT_SESSION_RATIO = 2.5
ALT_MIN_PCT = 5.0

# Pre-report run-up windows, in trading sessions ending at the close before the market could react.
RUNUP_SHORT_SESSIONS = 5     # about one week
RUNUP_LONG_SESSIONS = 20     # about one month

# Implied-move gates.
MAX_EXPIRY_AFTER_REACTION_DAYS = 10     # a far-dated straddle is mostly time value, not the earnings jump
MIN_STRADDLE_OPEN_INTEREST = 100        # call OI + put OI at the chosen strike
MAX_RELATIVE_SPREAD = 0.35              # (ask - bid) / mid per leg
MAX_IMPLIED_MOVE_PCT = 60.0


# --------------------------------------------------------------------------- small helpers
def finite(value: Any) -> Optional[float]:
    """float(value) when it is a finite number, else None (None/NaN/inf/str all -> None)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def round_half_up(value: float, places: int = 2) -> float:
    """Round half away from zero on the shortest decimal representation of ``value``.

    Identical to ``roundHalfUp`` in the Next.js app (lib/metrics.ts): both shift the decimal *string*, so a
    stat such as 66.315 prints as 66.32 on every page of the site (Python's ``round`` gives 66.31).
    """
    from decimal import ROUND_HALF_UP, Decimal

    quantum = Decimal(1).scaleb(-places)
    return float(Decimal(repr(float(value))).quantize(quantum, rounding=ROUND_HALF_UP))


def round_pct(value: Optional[float], places: int = 4) -> Optional[float]:
    return None if value is None else round_half_up(value, places)


def surprise_pct(estimate: Optional[float], actual: Optional[float]) -> Optional[float]:
    """(actual - estimate) / |estimate| * 100.  None when it is not meaningful (no/zero estimate)."""
    if estimate is None or actual is None or abs(estimate) < 1e-9:
        return None
    return (actual - estimate) / abs(estimate) * 100.0


def session_of(hour: Optional[int], minute: int = 0, is_midnight_placeholder: bool = False) -> Optional[str]:
    """Report session from a US/Eastern clock time.  Midnight (Yahoo's "time unknown") -> None."""
    if hour is None or is_midnight_placeholder:
        return None
    minutes = hour * 60 + minute
    if minutes == 0:
        return None
    if minutes < 9 * 60 + 30:
        return BMO
    if minutes >= 16 * 60:
        return AMC
    return DMH


def session_from_calendar_hour(value: Optional[str]) -> Optional[str]:
    """Finnhub ``hour`` field (bmo / amc / dmh) -> our session code."""
    if not value:
        return None
    return {"bmo": BMO, "amc": AMC, "dmh": DMH}.get(str(value).strip().lower())


# --------------------------------------------------------------------------- price reaction
Bar = Dict[str, Any]  # {"date": date, "open": float|None, "close": float|None}


def normalize_bars(rows: Iterable[Bar]) -> List[Bar]:
    """Sorted, de-duplicated daily bars with finite closes only."""
    by_date: Dict[date, Bar] = {}
    for row in rows:
        d = _as_date(row.get("date"))
        close = finite(row.get("close"))
        if d is None or close is None or close <= 0:
            continue
        by_date[d] = {"date": d, "open": finite(row.get("open")), "close": close}
    return [by_date[d] for d in sorted(by_date)]


def _index_on_or_after(bars: Sequence[Bar], target: date) -> Optional[int]:
    for i, bar in enumerate(bars):
        if bar["date"] >= target:
            return i
    return None


def _pct(new: Optional[float], base: Optional[float]) -> Optional[float]:
    if new is None or base is None or base <= 0:
        return None
    return (new / base - 1.0) * 100.0


def _reaction_at(bars: Sequence[Bar], report: date, session: str) -> Optional[Dict[str, Any]]:
    """Raw (un-sanity-checked) reaction numbers for ``report`` assuming ``session``."""
    if session == AMC:
        base_i = None
        for i, bar in enumerate(bars):
            if bar["date"] == report:
                base_i = i
                break
        if base_i is None:                       # report date was not a trading day in our data
            return None
        react_i = base_i + 1
    else:  # BMO / DMH: the report-day session already reflects it, baseline is the prior close
        react_i = _index_on_or_after(bars, report)
        if react_i is None or react_i == 0:
            return None
        base_i = react_i - 1
    if react_i >= len(bars):
        return None                              # reaction session has not happened yet

    base, react = bars[base_i], bars[react_i]
    if (react["date"] - report).days > MAX_REACTION_LAG_DAYS:
        return None
    day3_i = react_i + 2
    day3 = bars[day3_i] if day3_i < len(bars) else None
    return {
        "price_before": base["close"],
        "price_after": react["close"],
        "reaction_date": react["date"],
        "reaction_1d_pct": _pct(react["close"], base["close"]),
        "reaction_3d_pct": _pct(day3["close"], base["close"]) if day3 else None,
        "gap_pct": _pct(react.get("open"), base["close"]) if session in (BMO, AMC) else None,
        "runup_5d_pct": _runup(bars, base_i, RUNUP_SHORT_SESSIONS),
        "runup_20d_pct": _runup(bars, base_i, RUNUP_LONG_SESSIONS),
    }


def _runup(bars: Sequence[Bar], base_i: int, sessions: int) -> Optional[float]:
    """Move from the close ``sessions`` trading days before the pre-report close (``bars[base_i]``) up to it.

    None when there is not enough history, or when the bars have a hole (e.g. a delisted gap) so the window
    spans far more calendar days than ``sessions`` trading days could."""
    start_i = base_i - sessions
    if start_i < 0:
        return None
    span_days = (bars[base_i]["date"] - bars[start_i]["date"]).days
    if span_days > sessions * 2 + 6:             # 5 sessions -> 16 days, 20 sessions -> 46 days (holidays included)
        return None
    return _pct(bars[base_i]["close"], bars[start_i]["close"])


def measure_reaction(bars: Sequence[Bar], report: date, session: Optional[str]) -> Optional[Dict[str, Any]]:
    """Measured price reaction to a report, or None when it cannot be trusted.

    ``bars`` must be ``normalize_bars`` output.  Returns the keys of ``earnings_history`` that describe
    the reaction (price_before/after, reaction_date, reaction_1d_pct, reaction_3d_pct, gap_pct) plus the
    run-up into the report (runup_5d_pct, runup_20d_pct: the move over the 5 / 20 sessions before it).
    """
    if session not in (BMO, AMC, DMH) or not bars:
        return None
    primary = _reaction_at(bars, report, session)
    if primary is None or primary["reaction_1d_pct"] is None:
        return None
    move = primary["reaction_1d_pct"]
    if abs(move) > MAX_ABS_REACTION_PCT:
        return None

    # Cross-check the report-time label against the *other* session.
    alt_session = BMO if session == AMC else AMC
    alt = _reaction_at(bars, report, alt_session)
    if alt is not None and alt["reaction_1d_pct"] is not None:
        alt_move = abs(alt["reaction_1d_pct"])
        if alt_move >= ALT_MIN_PCT and alt_move >= ALT_SESSION_RATIO * max(abs(move), 0.01):
            logger.info("Reaction dropped for %s: labelled %s moved %.1f%% but %s moved %.1f%% (label suspect)",
                        report, session, move, alt_session, alt["reaction_1d_pct"])
            return None

    return {
        "price_before": round(primary["price_before"], 4),
        "price_after": round(primary["price_after"], 4),
        "reaction_date": primary["reaction_date"],
        "reaction_1d_pct": round_pct(primary["reaction_1d_pct"]),
        "reaction_3d_pct": round_pct(primary["reaction_3d_pct"]),
        "gap_pct": round_pct(primary["gap_pct"]),
        "runup_5d_pct": round_pct(primary["runup_5d_pct"]),
        "runup_20d_pct": round_pct(primary["runup_20d_pct"]),
    }


# --------------------------------------------------------------------------- history records
def build_history_records(
    stock_id: int,
    events: Iterable[Dict[str, Any]],
    bars: Sequence[Bar],
    *,
    today: Optional[date] = None,
    source: str = "yfinance",
) -> List[Dict[str, Any]]:
    """Rows for ``earnings_history`` from provider events.

    ``events``: dicts with ``date`` (report date), ``session`` (BMO/AMC/DMH/None), ``eps_estimate``,
    ``eps_actual``.  Only *reported* events (finite ``eps_actual``) on or before ``today`` are kept.
    """
    today = today or date.today()
    normalized = normalize_bars(bars)
    records: Dict[date, Dict[str, Any]] = {}
    for event in events:
        report = _as_date(event.get("date"))
        actual = finite(event.get("eps_actual"))
        if report is None or actual is None or report > today:
            continue
        estimate = finite(event.get("eps_estimate"))
        session = event.get("session") if event.get("session") in (BMO, AMC, DMH) else None
        record: Dict[str, Any] = {
            "stock_id": int(stock_id),
            "earnings_date": report,
            "time_of_day": session,
            "eps_estimate": estimate,
            "eps_actual": actual,
            "eps_surprise_pct": round_pct(surprise_pct(estimate, actual)),
            "revenue_estimate": finite(event.get("revenue_estimate")),
            "revenue_actual": finite(event.get("revenue_actual")),
            "revenue_surprise_pct": round_pct(surprise_pct(finite(event.get("revenue_estimate")), finite(event.get("revenue_actual")))),
            "eps_source_conflict": False,
            "price_before": None,
            "price_after": None,
            "reaction_date": None,
            "reaction_1d_pct": None,
            "reaction_3d_pct": None,
            "gap_pct": None,
            "runup_5d_pct": None,
            "runup_20d_pct": None,
            "source": event.get("source") or source,
        }
        reaction = measure_reaction(normalized, report, session)
        if reaction:
            record.update(reaction)
        records[report] = record
    return [records[d] for d in sorted(records, reverse=True)]


def merge_calendar_events(events: Sequence[Dict[str, Any]], calendar_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Fold Finnhub calendar rows (``earnings_calendar``) into Yahoo events.

    * A calendar row within one day of a Yahoo event adds its revenue figures, and its report time when
      Yahoo had none.  Yahoo's EPS stays authoritative (one consistent vendor for estimate and actual).
    * A calendar row with a reported EPS and *no* matching Yahoo event (Yahoo lags a day or two on fresh
      results) becomes a new event with ``source='finnhub'``.
    """
    merged = [dict(e) for e in events]
    for row in calendar_rows:
        report = _as_date(row.get("earnings_date"))
        if report is None:
            continue
        match = next((e for e in merged if e.get("date") and abs((_as_date(e["date"]) - report).days) <= 1), None)
        session = session_from_calendar_hour(row.get("time_of_day"))
        if match is not None:
            if finite(row.get("revenue_actual")) is not None and finite(match.get("revenue_actual")) is None:
                match["revenue_actual"] = finite(row.get("revenue_actual"))
                match["revenue_estimate"] = finite(row.get("revenue_estimate"))
            if match.get("session") is None and session is not None:
                match["session"] = session
            continue
        if finite(row.get("eps_actual")) is None:
            continue
        merged.append({
            "date": report,
            "session": session,
            "eps_estimate": finite(row.get("eps_estimate")),
            "eps_actual": finite(row.get("eps_actual")),
            "revenue_estimate": finite(row.get("revenue_estimate")),
            "revenue_actual": finite(row.get("revenue_actual")),
            "source": "finnhub",
        })
    return merged


# --------------------------------------------------------------------------- implied move
class OptionsMarketClosed(Exception):
    """Yahoo says the regular session is not open: its option quotes are zeroed, so no chain can be trusted."""


def _mid(bid: Optional[float], ask: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """(mid, relative_spread) from a live two-sided quote, else (None, None).

    No last-trade fallback: Yahoo's lastPrice can be days old and is not synchronous with the spot, and a leg
    without a bid has no market to price the straddle from."""
    if bid is not None and ask is not None and bid > 0 and ask >= bid:
        mid = (bid + ask) / 2.0
        return mid, (ask - bid) / mid
    return None, None


def straddle_check(
    spot: Optional[float],
    calls: Sequence[Dict[str, Any]],
    puts: Sequence[Dict[str, Any]],
) -> Tuple[Optional[float], str]:
    """(implied move in percent points, "ok") or (None, reason) naming the gate that rejected the chain.

    ``calls`` / ``puts``: dicts with strike, bid, ask, openInterest.
    """
    spot = finite(spot)
    if spot is None or spot <= 0:
        return None, "no_spot"
    call_by_strike = {finite(c.get("strike")): c for c in calls if finite(c.get("strike")) is not None}
    put_by_strike = {finite(p.get("strike")): p for p in puts if finite(p.get("strike")) is not None}
    common = sorted(set(call_by_strike) & set(put_by_strike), key=lambda k: abs(k - spot))
    if not common:
        return None, "no_common_strike"
    # The two strikes nearest the spot (one strike step apart) price practically the same straddle, so the
    # nearer one is tried first and the other only when the nearer one fails a quote gate (often a half-strike
    # with almost no open interest).  Both must be within 5 % of the spot.
    first_reason = None
    for strike in common[:2]:
        if abs(strike - spot) / spot > 0.05:      # nothing near the money -> not a real straddle
            reason = "no_strike_near_spot"
        else:
            move, reason = _straddle_at(spot, call_by_strike[strike], put_by_strike[strike])
            if move is not None:
                return move, reason
        first_reason = first_reason or reason
    return None, first_reason


def _straddle_at(spot: float, call: Dict[str, Any], put: Dict[str, Any]) -> Tuple[Optional[float], str]:
    legs = []
    for leg in (call, put):
        price, spread = _mid(finite(leg.get("bid")), finite(leg.get("ask")))
        if price is None:
            return None, "no_two_sided_quote"
        if spread > MAX_RELATIVE_SPREAD:
            return None, "wide_spread"
        legs.append(price)

    open_interest = (finite(call.get("openInterest")) or 0.0) + (finite(put.get("openInterest")) or 0.0)
    if open_interest < MIN_STRADDLE_OPEN_INTEREST:
        return None, "thin_open_interest"

    move = (legs[0] + legs[1]) / spot * 100.0
    if not (0.0 < move <= MAX_IMPLIED_MOVE_PCT):
        return None, "implausible_move"
    return round(move, 4), "ok"


def implied_move_from_chain(
    spot: Optional[float],
    calls: Sequence[Dict[str, Any]],
    puts: Sequence[Dict[str, Any]],
) -> Optional[float]:
    """Implied move (percent points) = ATM straddle / spot, or None when the quotes are not trustworthy."""
    return straddle_check(spot, calls, puts)[0]


def pick_expiry(expiries: Sequence[str], report: date, session: Optional[str]) -> Optional[date]:
    """First listed expiry that contains the reaction session, within the allowed distance.

    Unknown report time is treated like after-close (the later possibility), so the straddle always
    covers the reaction whichever session it turns out to be."""
    reaction_day = _reaction_day(report, session)
    for raw in sorted(expiries):
        expiry = _as_date(raw)
        if expiry is None:
            continue
        if expiry >= reaction_day:
            return expiry if expiry_covers(expiry, report, session) else None
    return None


def _reaction_day(report: date, session: Optional[str]) -> date:
    return report if session in (BMO, DMH) else report + timedelta(days=1)


def expiry_covers(expiry: Optional[date], report: date, session: Optional[str]) -> bool:
    """True when a straddle expiring on ``expiry`` contains the reaction session and is not far-dated."""
    if expiry is None:
        return False
    days = (expiry - _reaction_day(report, session)).days
    return 0 <= days <= MAX_EXPIRY_AFTER_REACTION_DAYS


# --------------------------------------------------------------------------- stats (shared read-side logic)
def summarize_history(rows: Sequence[Dict[str, Any]], *, limit: int = 12) -> Dict[str, Any]:
    """Aggregate the last ``limit`` reported quarters (rows newest-first or any order).

    Returns counts and rates that the pages show: beat rate, streak, average / median absolute move,
    and how often the stock fell after a beat.  Rates are ``None`` when there is nothing to base them on.
    """
    ordered = sorted(rows, key=lambda r: _as_date(r.get("earnings_date")) or date.min, reverse=True)[:limit]
    # A quarter whose sources disagree on beat vs miss is shown but never counted as a beat or a miss.
    surprises = [r for r in ordered
                 if finite(r.get("eps_surprise_pct")) is not None and not r.get("eps_source_conflict")]
    beats = [r for r in surprises if finite(r["eps_surprise_pct"]) > 0.0]
    misses = [r for r in surprises if finite(r["eps_surprise_pct"]) < 0.0]
    inline = [r for r in surprises if finite(r["eps_surprise_pct"]) == 0.0]

    streak_kind, streak_len = None, 0
    for r in surprises:                                    # newest first
        s = finite(r["eps_surprise_pct"])
        kind = "beat" if s > 0 else "miss" if s < 0 else "inline"
        if streak_kind is None:
            streak_kind, streak_len = kind, 1
        elif kind == streak_kind:
            streak_len += 1
        else:
            break

    moves = [finite(r.get("reaction_1d_pct")) for r in ordered]
    moves = [m for m in moves if m is not None]
    abs_moves = sorted(abs(m) for m in moves)

    def _median(values: List[float]) -> Optional[float]:
        if not values:
            return None
        mid = len(values) // 2
        return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2.0

    beat_with_move = [r for r in beats if finite(r.get("reaction_1d_pct")) is not None]
    fell_after_beat = [r for r in beat_with_move if finite(r["reaction_1d_pct"]) < 0.0]

    def _rate(n: int, d: int) -> Optional[float]:
        return round_half_up(n / d * 100.0, 1) if d else None

    return {
        "quarters": len(ordered),
        "conflicts": sum(1 for r in ordered if r.get("eps_source_conflict")),
        "with_surprise": len(surprises),
        "beats": len(beats),
        "misses": len(misses),
        "inline": len(inline),
        "beat_rate_pct": _rate(len(beats), len(surprises)),
        "streak_kind": streak_kind,
        "streak_len": streak_len,
        "with_reaction": len(moves),
        "avg_abs_move_pct": round_half_up(sum(abs_moves) / len(abs_moves), 2) if abs_moves else None,
        "median_abs_move_pct": round_half_up(_median(abs_moves), 2) if abs_moves else None,
        "avg_move_pct": round_half_up(sum(moves) / len(moves), 2) if moves else None,
        "up_rate_pct": _rate(sum(1 for m in moves if m > 0), len(moves)),
        "max_up_pct": round_half_up(max(moves), 2) if moves else None,
        "max_down_pct": round_half_up(min(moves), 2) if moves else None,
        "beats_with_reaction": len(beat_with_move),
        "fell_after_beat": len(fell_after_beat),
        "fell_after_beat_rate_pct": _rate(len(fell_after_beat), len(beat_with_move)),
    }


# Implied-vs-history ratio thresholds for the "options pricing" label.
RICH_RATIO = 1.25
CHEAP_RATIO = 0.80
MIN_MOVES_FOR_EDGE = 4


def move_edge(implied_move_pct: Optional[float], avg_abs_move_pct: Optional[float], with_reaction: int) -> Optional[Dict[str, Any]]:
    """Compare what options price in with what the stock has actually done on past reports.

    ``ratio`` = implied / average actual absolute move.  > RICH_RATIO -> options look expensive
    (market expects more than history delivered); < CHEAP_RATIO -> cheap.  None without enough history.
    """
    implied, actual = finite(implied_move_pct), finite(avg_abs_move_pct)
    if implied is None or actual is None or actual <= 0 or with_reaction < MIN_MOVES_FOR_EDGE:
        return None
    ratio = implied / actual
    label = "rich" if ratio >= RICH_RATIO else "cheap" if ratio <= CHEAP_RATIO else "fair"
    return {"ratio": round_half_up(ratio, 2), "label": label, "implied_move_pct": round_half_up(implied, 2), "avg_abs_move_pct": round_half_up(actual, 2)}


# --------------------------------------------------------------------------- Finnhub (free tier, rate limited)
FINNHUB_BASE = "https://finnhub.io/api/v1"
INLINE_BAND_PCT = 0.5                   # |surprise| below this is "in line" for the cross-check


def finnhub_get(path: str, params: Dict[str, Any], *, api_key: Optional[str] = None, session=None,
                limiter: Optional[Any] = None, retries: int = 2) -> Optional[Any]:
    """GET a Finnhub endpoint within the shared rate limit.  Returns parsed JSON or None (never raises)."""
    import os

    import requests

    key = api_key or os.getenv("FINNHUB_API_KEY")
    if not key:
        return None
    http = session or requests
    lim = limiter or get_limiter("finnhub")
    for attempt in range(retries + 1):
        lim.wait()
        try:
            response = http.get(f"{FINNHUB_BASE}{path}", params={**params, "token": key}, timeout=15)
        except requests.RequestException as exc:
            logger.warning("Finnhub %s failed: %s", path, exc)
            return None
        if response.status_code == 429:
            time.sleep(5 * (attempt + 1))
            continue
        if response.status_code != 200:
            logger.warning("Finnhub %s -> HTTP %s", path, response.status_code)
            return None
        try:
            return response.json()
        except ValueError:
            return None
    return None


def fetch_finnhub_surprises(ticker: str, **kwargs) -> List[Dict[str, Any]]:
    """Last four reported quarters (free endpoint): period, estimate, actual, surprisePercent."""
    data = finnhub_get("/stock/earnings", {"symbol": ticker, "limit": 8}, **kwargs)
    return data if isinstance(data, list) else []


def _direction(surprise: Optional[float]) -> int:
    if surprise is None:
        return 0
    return 1 if surprise >= INLINE_BAND_PCT else -1 if surprise <= -INLINE_BAND_PCT else 0


def flag_source_conflicts(records: List[Dict[str, Any]], finnhub_rows: Sequence[Dict[str, Any]]) -> int:
    """Set ``eps_source_conflict`` on records whose beat/miss direction Finnhub contradicts.

    Every quarter Finnhub covers is re-decided from this fresh evidence (a stale flag can clear);
    quarters it does not cover keep whatever flag they already carry.

    Quarters are matched by fiscal period end -> report date (reports land 15-80 days after the period).
    A conflict needs *opposite* directions outside the in-line band; "beat vs in line" is not a conflict.
    Returns the number of flagged records.
    """
    flagged = 0
    for fh_row in finnhub_rows:
        period = _as_date(fh_row.get("period"))
        fh_surprise = finite(fh_row.get("surprisePercent"))
        if period is None or fh_surprise is None:
            continue
        best, best_gap = None, None
        for record in records:
            gap = (record["earnings_date"] - period).days
            if 15 <= gap <= 80 and (best is None or abs(gap - 35) < abs(best_gap - 35)):
                best, best_gap = record, gap
        if best is None:
            continue
        a, b = _direction(finite(best.get("eps_surprise_pct"))), _direction(fh_surprise)
        best["eps_source_conflict"] = a * b < 0
        flagged += 1 if best["eps_source_conflict"] else 0
    return flagged


# --------------------------------------------------------------------------- yfinance IO (throttled)
class ProviderThrottled(Exception):
    """The provider kept answering 'too many requests' after backing off: stop the run, retry later."""


def is_rate_limited(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return "ratelimit" in text or "rate limit" in text or "too many requests" in text or "429" in text


YAHOO_BACKOFFS = (30.0, 90.0, 240.0)


def yahoo_call(fn: Callable[..., Any], *args: Any, limiter: Optional[Any] = None,
               sleep: Callable[[float], None] = time.sleep, backoffs: Sequence[float] = YAHOO_BACKOFFS, **kwargs: Any) -> Any:
    """Run one Yahoo request inside the shared budget; back off and retry when Yahoo says 429.

    Every provider request goes through here (one limiter slot per request).  Non rate-limit errors
    propagate unchanged (a bad ticker is the caller's business); persistent 429 raises ProviderThrottled.
    """
    lim = limiter or get_limiter("yahoo")
    for attempt in range(len(backoffs) + 1):
        lim.wait()
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            if not is_rate_limited(exc):
                raise
            if attempt >= len(backoffs):
                raise ProviderThrottled(str(exc)) from exc
            logger.warning("Yahoo rate limit hit (%s); backing off %.0fs", exc, backoffs[attempt])
            sleep(backoffs[attempt])
    raise ProviderThrottled("unreachable")  # pragma: no cover


def fetch_events(ticker: str, limit: int = 24, **call_kwargs: Any) -> List[Dict[str, Any]]:
    """Reported + upcoming events for ``ticker`` from Yahoo (newest first).  One Yahoo request."""
    import yfinance as yf

    frame = yahoo_call(lambda: yf.Ticker(ticker).get_earnings_dates(limit=limit), **call_kwargs)
    if frame is None or len(frame) == 0:
        return []
    events: List[Dict[str, Any]] = []
    for stamp, row in frame.iterrows():
        try:
            local = stamp.tz_convert("America/New_York") if getattr(stamp, "tzinfo", None) else stamp
        except Exception:  # pragma: no cover - defensive
            local = stamp
        events.append({
            "date": local.date(),
            "session": session_of(local.hour, local.minute),
            "eps_estimate": finite(row.get("EPS Estimate")),
            "eps_actual": finite(row.get("Reported EPS")),
        })
    return events


def fetch_bars(ticker: str, years: int = 6, **call_kwargs: Any) -> List[Bar]:
    """Daily bars (split-adjusted, not dividend-adjusted) for the reaction maths.  One Yahoo request."""
    import yfinance as yf

    frame = yahoo_call(lambda: yf.Ticker(ticker).history(period=f"{years}y", interval="1d", auto_adjust=False), **call_kwargs)
    if frame is None or len(frame) == 0:
        return []
    bars = []
    for stamp, row in frame.iterrows():
        bars.append({"date": stamp.date(), "open": row.get("Open"), "close": row.get("Close")})
    return normalize_bars(bars)


def fetch_implied_move(ticker: str, report: date, session: Optional[str], **call_kwargs: Any) -> Optional[Dict[str, Any]]:
    """Implied move for an upcoming report: {implied_move_pct, expiry} or None.  Two Yahoo requests
    (expiry list + chain); the spot price comes with the chain.

    Raises ``OptionsMarketClosed`` when the chain's ``marketState`` is not ``REGULAR``: outside the session
    Yahoo reports bid = ask = openInterest = 0 on every contract (checked on NFLX, META, SPOT and GE at
    05:30 New York on 2026-10-16), so the caller should stop rather than record a miss for every report."""
    import yfinance as yf

    handle = yf.Ticker(ticker)
    expiries = list(yahoo_call(lambda: handle.options, **call_kwargs) or [])
    expiry = pick_expiry(expiries, report, session)
    if expiry is None:
        logger.info("implied move %s: no expiry within %s days of the reaction", ticker, MAX_EXPIRY_AFTER_REACTION_DAYS)
        return None
    chain = yahoo_call(lambda: handle.option_chain(expiry.isoformat()), **call_kwargs)
    underlying = getattr(chain, "underlying", None) or {}
    state = underlying.get("marketState")
    if state and state != "REGULAR":
        raise OptionsMarketClosed(f"marketState={state}")
    move, reason = straddle_check(underlying.get("regularMarketPrice"),
                                  chain.calls.to_dict("records"), chain.puts.to_dict("records"))
    if move is None:
        logger.info("implied move %s (expiry %s) rejected: %s", ticker, expiry, reason)
        return None
    return {"implied_move_pct": move, "expiry": expiry}
