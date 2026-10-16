#!/usr/bin/env python3
"""Market-hours job: options-implied earnings moves (Azure Container Apps job ``tickzen-implied-moves``).

Why a separate job: the stock updater runs only while the US market is closed, and after the close Yahoo returns
bid = ask = openInterest = 0 on every option contract, so the implied moves it used to compute were NULL 224 times
out of 232. This job runs a few times per trading day (deploy/azure/deploy_implied_moves_job.sh), only reads option
chains (two Yahoo requests per upcoming report, paced like the workers) and only writes
``earnings_calendar.implied_move_*``. The logic lives in the shared ``database/earnings_local_update.py``
(``run_implied``); this wrapper only adds the exporter's process-wide Yahoo pacer.

    python database/implied_moves_job.py            # exits at once outside 09:45-15:45 New York on trading days
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger("implied_moves_job")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from database.updater_schedule import MARKET_TZ, in_options_session

    now = datetime.now(timezone.utc)
    if not in_options_session(now):
        logger.info("Outside the options session (%s New York) - nothing to do.", now.astimezone(MARKET_TZ).strftime("%a %H:%M"))
        return 0
    per_hour = int(os.getenv("YAHOO_REQUESTS_PER_HOUR", "1200"))
    from database import yahoo_pacer
    yahoo_pacer.install(per_hour)
    logger.info("Yahoo requests paced to %d/hour.", per_hour)

    from database.earnings_local_update import main as earnings_main
    return earnings_main(["--implied-only"])


if __name__ == "__main__":
    raise SystemExit(main())
