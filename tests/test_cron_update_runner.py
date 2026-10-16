"""Hourly-batch behaviour of the cloud runner, with every external dependency faked (no Yahoo, no database).

    python -m pytest -q tests/test_cron_update_runner.py
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import cron_update_runner as runner  # noqa: E402

SYMBOLS = [f"S{i:03d}" for i in range(100)]


class FakePacer:
    def __init__(self, throttle_every=0):
        self.total = 0
        self.throttled = 0
        self.per_hour = 1200
        self._mark = 0
        self.throttle_every = throttle_every

    def request(self):
        self.total += 30
        if self.throttle_every and self.total % (30 * self.throttle_every) == 0:
            self.throttled += 1
            self.per_hour = 600

    def since_mark(self):
        n, self._mark = self.total - self._mark, self.total
        return n


class World:
    """Shared state of the fakes: which stocks are current and which another worker holds."""

    def __init__(self, held_by_other=(), pacer=None):
        self.current = set()
        self.held_by_other = set(held_by_other)
        self.exported = []
        self.earnings_runs = 0
        self.finished = None
        self.pacer = pacer or FakePacer()


@pytest.fixture
def run(monkeypatch):
    def _run(world, argv=(), env=None):
        for key, value in {"WORKER_INDEX": "0", "WORKER_COUNT": "2", **(env or {})}.items():
            monkeypatch.setenv(key, value)

        class Exporter:
            def export_stock_data(self, symbol):
                world.pacer.request()
                world.exported.append(symbol)
                world.current.add(symbol)
                return {"status": "success", "errors": []}

        class Locks:
            def __init__(self, _db):
                pass

            def worker(self, _index):
                return True

            def earnings(self):
                return True

            def claim(self, symbol, _cutoff):
                return symbol not in world.held_by_other and symbol not in world.current

            def release(self, _symbol):
                pass

            def close(self):
                pass

        class Log:
            def __init__(self, *_a, **_k):
                pass

            def seen(self, _outcome):
                pass

            def start_run(self, **_k):
                return 1

            def stock(self, *_a, **_k):
                pass

            def finish(self, status, **_k):
                world.finished = status

        def build_queue(_db, tickers, _cutoff, index, count):
            parts = runner.assign_parts(tickers, count)
            stale = [s for s in tickers if s not in world.current]
            own = [s for s in stale if parts[s] == index]
            others = [s for s in stale if parts[s] != index]
            return own, others, {"total": len(tickers), "own_total": len(own), "current": 0, "new": 0,
                                 "own_stale": len(own), "others_stale": len(others)}

        import database.yahoo_pacer as pacer_module
        import database.updater_locks as locks_module
        import database.updater_run_log as log_module
        import database.export_to_azure_postgres as export_module
        monkeypatch.setattr(runner, "in_export_window", lambda *_a: True)
        monkeypatch.setattr(runner, "internet_and_ip", lambda: (True, "203.0.113.7"))
        monkeypatch.setattr(runner, "get_exporter_and_client", lambda: (Exporter(), object()))
        monkeypatch.setattr(runner, "load_tickers_from_db", lambda _db: list(SYMBOLS))
        monkeypatch.setattr(runner, "build_queue", build_queue)
        monkeypatch.setattr(runner, "run_earnings_step", lambda _locks: setattr(world, "earnings_runs", world.earnings_runs + 1))
        monkeypatch.setattr(pacer_module, "install", lambda _rate: world.pacer)
        monkeypatch.setattr(locks_module, "UpdaterLocks", Locks)
        monkeypatch.setattr(log_module, "RunLog", Log)
        monkeypatch.setattr(export_module, "prepare_bulk_prices", lambda _symbols: None)
        monkeypatch.setattr(sys, "argv", ["cron_update_runner.py", *argv])
        with pytest.raises(SystemExit):
            runner.main()
        return world

    return _run


def test_stops_after_70_stocks_per_run(run):
    world = run(World())
    assert len(world.exported) == 70
    assert world.finished == "batch_done"
    parts = runner.assign_parts(SYMBOLS, 2)
    own = [s for s in world.exported if parts[s] == 0]
    assert world.exported[:len(own)] == own and len(own) == 50   # its own part first, then helping
    assert world.earnings_runs == 1                                # once its own part was done


def test_next_run_finishes_the_rest(run):
    world = World()
    run(world)
    world.exported.clear()
    run(world)
    assert len(world.exported) == 30 and world.finished == "completed"


def test_stocks_held_by_another_worker_are_skipped_not_waited_for(run):
    parts = runner.assign_parts(SYMBOLS, 2)
    held = {s for s in SYMBOLS if parts[s] == 1}           # the other worker is busy with its whole part
    world = run(World(held_by_other=held))
    assert set(world.exported) == {s for s in SYMBOLS if parts[s] == 0}
    assert world.finished == "completed"


def test_three_real_429s_end_the_run(run):
    world = run(World(pacer=FakePacer(throttle_every=5)))
    assert len(world.exported) == 15
    assert world.finished == "rate_limited"


def test_limit_can_be_changed(run):
    world = run(World(), env={"STOCKS_PER_RUN": "10"})
    assert len(world.exported) == 10 and world.finished == "batch_done"
