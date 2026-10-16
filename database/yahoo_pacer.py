"""Process-wide pacing and counting of every HTTP request this worker sends to Yahoo Finance.

yfinance fires its requests in parallel bursts (one stock's collection sent 38 requests in ~9 s), which is what
trips Yahoo's per-IP throttle. ``install()`` patches the HTTP layer used by yfinance (curl_cffi) and by
``requests`` so that every request to a ``*.yahoo.com`` host waits for its slot: requests start at most once
every ``3600 / per_hour`` seconds, evenly spaced, whatever thread sends them. If Yahoo still answers HTTP 429
the pacer pauses for a minute and halves its rate for the rest of the run.

Each worker runs from its own IP, so the budget is per process (per worker), not shared through the database.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

THROTTLE_PAUSE_SECONDS = 60
MAX_SLOWDOWN = 8


class YahooPacer:
    def __init__(self, per_hour: int, clock=time.monotonic, sleep=time.sleep):
        self.base_interval = 3600.0 / max(1, per_hour)
        self.interval = self.base_interval
        self._clock, self._sleep = clock, sleep
        self._next_at = 0.0
        self._lock = threading.Lock()
        self.total = 0
        self.throttled = 0
        self._mark = 0

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            slot = max(now, self._next_at)
            self._next_at = slot + self.interval
            self.total += 1
        if slot > now:
            self._sleep(slot - now)

    def on_throttled(self) -> None:
        with self._lock:
            self.throttled += 1
            self.interval = min(self.interval * 2, self.base_interval * MAX_SLOWDOWN)
            self._next_at = max(self._next_at, self._clock() + THROTTLE_PAUSE_SECONDS)
        logger.warning(f"Yahoo answered HTTP 429: pausing {THROTTLE_PAUSE_SECONDS}s, pace now "
                       f"{3600 / self.interval:.0f} requests/hour")

    def since_mark(self) -> int:
        """Requests sent since the previous call (per-stock counts)."""
        with self._lock:
            count, self._mark = self.total - self._mark, self.total
        return count

    @property
    def per_hour(self) -> float:
        return 3600.0 / self.interval


_pacer: Optional[YahooPacer] = None


def _is_yahoo(url) -> bool:
    return urlparse(str(url)).netloc.endswith("yahoo.com")


def _wrap(cls) -> None:
    if getattr(cls.request, "_tz_paced", False):
        return
    original = cls.request

    def request(self, method, url, *args, **kwargs):
        if _pacer is None or not _is_yahoo(url):
            return original(self, method, url, *args, **kwargs)
        _pacer.wait()
        response = original(self, method, url, *args, **kwargs)
        if getattr(response, "status_code", None) == 429:
            _pacer.on_throttled()
        return response

    request._tz_paced = True
    cls.request = request


def install(per_hour: int) -> YahooPacer:
    """Pace all Yahoo traffic of this process. Call before the first yfinance request; safe to call twice."""
    global _pacer
    _pacer = YahooPacer(per_hour)
    import requests
    _wrap(requests.Session)
    try:
        import curl_cffi.requests
        _wrap(curl_cffi.requests.Session)
    except ImportError:  # yfinance falls back to requests
        pass
    return _pacer
