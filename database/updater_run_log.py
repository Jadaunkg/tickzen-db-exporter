"""Writes the updater's run log (tables in migrations/20261016_updater_runs.sql) for the admin dashboard.

Used by both updaters: the Azure workers (tickzen-db-exporter) and the Mac runner. Logging must never break an
export, so every method swallows database errors (logged as warnings) and a failed ``start_run`` turns the rest
of the run's calls into no-ops.

    log = RunLog(db, worker_name="azure-0", worker_index=0, worker_count=2, region="centralindia")
    log.seen("market open")                       # every wake-up, also when there is nothing to do
    log.start_run(session_cutoff=cutoff)          # once the worker starts exporting
    log.stock("AAPL", "success", started_at, own_part=True, yahoo_requests=14)
    log.finish("completed", yahoo_requests=1234, yahoo_429=0)
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

MIGRATION_PATH = Path(__file__).with_name("migrations") / "20261016_updater_runs.sql"
RETENTION_DAYS = 120
MAX_ERROR_CHARS = 2000


class RunLog:
    def __init__(self, db, worker_name: str, worker_index: Optional[int] = None, worker_count: Optional[int] = None,
                 region: Optional[str] = None, egress_ip: Optional[str] = None, image_tag: Optional[str] = None,
                 pace_per_hour: Optional[int] = None):
        self.db = db
        self.worker_name = worker_name
        self.meta = {
            "worker_index": worker_index, "worker_count": worker_count, "region": region,
            "egress_ip": egress_ip, "image_tag": image_tag, "pace_per_hour": pace_per_hour,
        }
        self.run_id: Optional[int] = None
        self.counts = {"success": 0, "partial": 0, "failed": 0, "helped": 0}
        self._schema_ready = False

    # ------------------------------------------------------------------ helpers
    def _sql(self, query: str, params: Iterable[Any] = ()):
        return self.db.execute_raw_sql(query, list(params))

    def _ensure_schema(self) -> bool:
        if not self._schema_ready:
            try:
                exists = self._sql("SELECT to_regclass('public.updater_run_stocks') IS NOT NULL AS ok")
                if not (exists and exists[0].get("ok")):
                    self._sql(MIGRATION_PATH.read_text(encoding="utf-8"))
                self._schema_ready = True
            except Exception as exc:  # noqa: BLE001
                logger.warning("updater run log unavailable (schema): %s", exc)
        return self._schema_ready

    def _safe(self, what: str, query: str, params: Iterable[Any] = ()):
        if not self._ensure_schema():
            return None
        try:
            return self._sql(query, params)
        except Exception as exc:  # noqa: BLE001
            logger.warning("updater run log: could not record %s: %s", what, exc)
            return None

    # ------------------------------------------------------------------ API
    def seen(self, outcome: str) -> None:
        """Liveness: record that this worker woke up and what it found."""
        m = self.meta
        self._safe(
            "worker heartbeat",
            "INSERT INTO public.updater_workers AS w (worker_name, worker_index, worker_count, region, egress_ip, image_tag, "
            "pace_per_hour, last_seen_at, last_outcome, current_run_id) VALUES (%s, %s, %s, %s, %s, %s, %s, now(), %s, %s) "
            "ON CONFLICT (worker_name) DO UPDATE SET worker_index = EXCLUDED.worker_index, worker_count = EXCLUDED.worker_count, "
            "region = EXCLUDED.region, egress_ip = COALESCE(EXCLUDED.egress_ip, w.egress_ip), image_tag = EXCLUDED.image_tag, "
            "pace_per_hour = EXCLUDED.pace_per_hour, last_seen_at = now(), last_outcome = EXCLUDED.last_outcome, "
            "current_run_id = EXCLUDED.current_run_id",
            [self.worker_name, m["worker_index"], m["worker_count"], m["region"], m["egress_ip"], m["image_tag"],
             m["pace_per_hour"], (outcome or "")[:500], self.run_id],
        )

    def start_run(self, session_cutoff: Optional[datetime] = None) -> Optional[int]:
        m = self.meta
        rows = self._safe(
            "run start",
            "INSERT INTO public.updater_runs (worker_name, worker_index, worker_count, region, egress_ip, image_tag, "
            "session_cutoff, pace_per_hour) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
            [self.worker_name, m["worker_index"], m["worker_count"], m["region"], m["egress_ip"], m["image_tag"],
             session_cutoff, m["pace_per_hour"]],
        )
        self.run_id = rows[0]["id"] if rows else None
        if self.run_id:
            self._safe("retention", "DELETE FROM public.updater_runs WHERE started_at < now() - make_interval(days => %s)",
                       [RETENTION_DAYS])
            self.seen("exporting")
        return self.run_id

    def stock(self, symbol: str, status: str, started_at: datetime, own_part: Optional[bool] = None,
              yahoo_requests: Optional[int] = None, errors: Optional[Iterable[Any]] = None,
              run_yahoo_requests: Optional[int] = None, run_yahoo_429: Optional[int] = None,
              run_pace_per_hour: Optional[int] = None) -> None:
        """Record one stock attempt; also the run's heartbeat and running totals (``run_*`` = whole run so far;
        ``run_pace_per_hour`` is the worker's current Yahoo pace, which drops after a 429)."""
        if not self.run_id:
            return
        key = status if status in ("success", "partial") else "failed"
        self.counts[key] += 1
        if own_part is False:
            self.counts["helped"] += 1
        error_text = "; ".join(str(e) for e in (errors or []))[:MAX_ERROR_CHARS] or None
        self._safe(
            f"result for {symbol}",
            "INSERT INTO public.updater_run_stocks (run_id, symbol, status, own_part, started_at, yahoo_requests, errors) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [self.run_id, symbol, status, own_part, started_at, yahoo_requests, error_text],
        )
        self._safe(
            "run heartbeat",
            "UPDATE public.updater_runs SET heartbeat_at = now(), stocks_success = %s, stocks_partial = %s, "
            "stocks_failed = %s, stocks_helped = %s, yahoo_requests = COALESCE(%s, yahoo_requests), "
            "yahoo_429 = COALESCE(%s, yahoo_429), pace_per_hour = COALESCE(%s, pace_per_hour) WHERE id = %s",
            [self.counts["success"], self.counts["partial"], self.counts["failed"], self.counts["helped"],
             run_yahoo_requests, run_yahoo_429, run_pace_per_hour, self.run_id],
        )
        self._safe("worker heartbeat", "UPDATE public.updater_workers SET last_seen_at = now() WHERE worker_name = %s",
                   [self.worker_name])

    def finish(self, status: str, yahoo_requests: Optional[int] = None, yahoo_429: Optional[int] = None,
               pace_per_hour: Optional[int] = None, note: Optional[str] = None) -> None:
        if not self.run_id:
            return
        self._safe(
            "run end",
            "UPDATE public.updater_runs SET finished_at = now(), heartbeat_at = now(), status = %s, yahoo_requests = %s, "
            "yahoo_429 = %s, pace_per_hour = COALESCE(%s, pace_per_hour), note = %s WHERE id = %s",
            [status, yahoo_requests, yahoo_429, pace_per_hour, (note or None) and note[:1000], self.run_id],
        )
        self.run_id = None
        self.seen(f"finished: {status}")
