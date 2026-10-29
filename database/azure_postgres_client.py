from __future__ import annotations

"""
Azure PostgreSQL Client Manager
===============================

Native PostgreSQL connection manager for the Azure migration test path.
This intentionally stays separate from the existing Supabase implementation.
"""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from database.unit_contract import guard_records

try:
    import psycopg2
    import psycopg2.extensions
    from psycopg2 import sql
    from psycopg2.extras import Json, RealDictCursor, execute_values
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

if POSTGRES_AVAILABLE:
    # This network's DNS returns a synthesized IPv6/NAT64 address ahead of the
    # working IPv4 one. psycopg2 does its own C-level (libpq) connection
    # handling, entirely separate from Python's socket/urllib3 stack, so it
    # isn't covered by any requests-level IPv6 workaround - it stalls on the
    # dead IPv6 path for ~30-60s on every single connection before falling
    # back to IPv4. Verified directly: connecting by hostname took 60.55s;
    # the identical connection with the host pre-resolved to IPv4 took 0.47s.
    # Wrapping psycopg2.connect() to resolve hostnames to IPv4 first fixes
    # this for both call styles (host=... kwargs and DSN/URI strings).
    import socket as _socket
    _original_psycopg2_connect = psycopg2.connect

    def _resolve_ipv4(host):
        try:
            return _socket.getaddrinfo(host, None, _socket.AF_INET)[0][4][0]
        except Exception:
            return host

    # The reverse also happens (an IPv6/NAT64-only network with no IPv4 route): once the IPv4 attempt
    # fails at the network level, connect by hostname like plain libpq for the next 10 minutes (so a brief
    # database outage on a healthy IPv4 network doesn't push every later connection onto the slow path).
    import time as _time
    _ipv4_unreachable_until = 0.0
    _NETWORK_ERRORS = ("timeout expired", "unreachable", "no route to host", "could not connect")

    def _ipv4_patched_connect(*args, **kwargs):
        global _ipv4_unreachable_until
        if _time.monotonic() >= _ipv4_unreachable_until:
            try:
                if args and isinstance(args[0], str):
                    parsed = psycopg2.extensions.parse_dsn(args[0])
                    if parsed.get("host"):
                        parsed["host"] = _resolve_ipv4(parsed["host"])
                    new_dsn = " ".join(f"{k}='{v}'" for k, v in parsed.items())
                    return _original_psycopg2_connect(new_dsn, **kwargs)
                elif kwargs.get("host"):
                    # a copy, so the fallback below still gets the original hostname
                    ipv4_kwargs = dict(kwargs, host=_resolve_ipv4(kwargs["host"]))
                    return _original_psycopg2_connect(*args, **ipv4_kwargs)
            except psycopg2.OperationalError as exc:
                if any(s in str(exc).lower() for s in _NETWORK_ERRORS):
                    _ipv4_unreachable_until = _time.monotonic() + 600
            except Exception:
                pass
        return _original_psycopg2_connect(*args, **kwargs)

    psycopg2.connect = _ipv4_patched_connect


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.propagate = False


class AzurePostgresConnectionError(Exception):
    """Raised when Azure PostgreSQL connection fails."""


class AzurePostgresQueryError(Exception):
    """Raised when Azure PostgreSQL query execution fails."""


# One connection per thread per database, shared by every client in the process.
# The web app builds about ten clients (one per service); with a connection each per
# request thread they held ~29 of the server's 40 slots and locked the updaters out.
_SHARED_LOCALS: Dict[tuple, threading.local] = {}
_SHARED_LOCALS_LOCK = threading.Lock()


def _shared_thread_local(key: tuple) -> threading.local:
    with _SHARED_LOCALS_LOCK:
        return _SHARED_LOCALS.setdefault(key, threading.local())


@dataclass
class AzurePostgresResult:
    """Small result object matching the fields used from Supabase responses."""

    data: List[Dict[str, Any]]
    count: Optional[int] = None


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "item") and callable(value.item) and type(value).__module__.startswith("numpy"):
        return value.item()
    return str(value)


def _adapt_value(value: Any) -> Any:
    # Convert numpy types to native Python types
    if value is not None and type(value).__module__.startswith("numpy"):
        if hasattr(value, "item") and callable(value.item):
            value = value.item()
        elif hasattr(value, "tolist") and callable(value.tolist):
            value = value.tolist()

    if isinstance(value, (dict, list)):
        return Json(value, dumps=lambda obj: json.dumps(obj, default=_json_default))
    return value


def _normalize_records(data: Any) -> List[Dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, dict):
        return [data]
    return list(data)


class AzureTableQuery:
    """Supabase-style table query shim backed by psycopg2."""

    def __init__(self, db: "AzurePostgresClient", table: str):
        self.db = db
        self.table = table
        self._operation: Optional[str] = None
        self._data: List[Dict[str, Any]] = []
        self._columns = "*"
        self._filters: List[Tuple[str, str, Any]] = []
        self._order_by: Optional[str] = None
        self._order_desc = False
        self._limit: Optional[int] = None
        self._range: Optional[Tuple[int, int]] = None
        self._on_conflict: Optional[List[str]] = None
        self._count_mode: Optional[str] = None

    def select(self, *columns, count: Optional[str] = None) -> "AzureTableQuery":
        self._operation = "select"
        if not columns:
            self._columns = "*"
        elif len(columns) == 1:
            if isinstance(columns[0], (list, tuple)):
                self._columns = ", ".join(str(c) for c in columns[0])
            else:
                self._columns = str(columns[0])
        else:
            self._columns = ", ".join(str(c) for c in columns)
        self._count_mode = count
        return self

    def insert(self, data: Any) -> "AzureTableQuery":
        self._operation = "insert"
        # Storage-unit contract (database/unit_contract.py): out-of-contract values never reach the DB.
        self._data = guard_records(self.table, _normalize_records(data))
        return self

    def upsert(
        self,
        data: Any,
        on_conflict: Optional[str] = None,
        ignore_duplicates: bool = False,
    ) -> "AzureTableQuery":
        self._operation = "upsert_ignore" if ignore_duplicates else "upsert"
        self._data = guard_records(self.table, _normalize_records(data))
        if on_conflict:
            self._on_conflict = [column.strip() for column in on_conflict.split(",") if column.strip()]
        return self

    def update(self, data: Dict[str, Any]) -> "AzureTableQuery":
        self._operation = "update"
        self._data = [data]
        return self

    def delete(self) -> "AzureTableQuery":
        self._operation = "delete"
        return self

    def eq(self, column: str, value: Any) -> "AzureTableQuery":
        self._filters.append((column, "=", value))
        return self

    def gte(self, column: str, value: Any) -> "AzureTableQuery":
        self._filters.append((column, ">=", value))
        return self

    def lte(self, column: str, value: Any) -> "AzureTableQuery":
        self._filters.append((column, "<=", value))
        return self

    def lt(self, column: str, value: Any) -> "AzureTableQuery":
        self._filters.append((column, "<", value))
        return self

    def in_(self, column: str, values: Sequence[Any]) -> "AzureTableQuery":
        self._filters.append((column, "IN", tuple(values)))
        return self

    def order(self, column: str, desc: bool = False) -> "AzureTableQuery":
        self._order_by = column
        self._order_desc = desc
        return self

    def limit(self, count: int) -> "AzureTableQuery":
        self._limit = count
        return self

    def range(self, start: int, end: int) -> "AzureTableQuery":
        self._range = (start, end)
        return self

    def execute(self) -> AzurePostgresResult:
        if not self._operation:
            raise AzurePostgresQueryError("No table operation selected before execute().")

        return self.db._retry_with_backoff(self._execute_once)

    def _execute_once(self) -> AzurePostgresResult:
        if self._operation == "select":
            return self._execute_select()
        if self._operation == "insert":
            return self._execute_insert()
        if self._operation in ("upsert", "upsert_ignore"):
            return self._execute_upsert(ignore=self._operation == "upsert_ignore")
        if self._operation == "update":
            return self._execute_update()
        if self._operation == "delete":
            return self._execute_delete()

        raise AzurePostgresQueryError(f"Unsupported operation: {self._operation}")

    def _where_sql(self) -> Tuple[sql.Composed, List[Any]]:
        if not self._filters:
            return sql.SQL(""), []

        clauses = []
        params = []
        for column, operator, value in self._filters:
            if operator == "IN":
                clauses.append(sql.SQL("{} = ANY(%s)").format(sql.Identifier(column)))
                params.append(list(value))
            else:
                clauses.append(sql.SQL("{} {} %s").format(sql.Identifier(column), sql.SQL(operator)))
                params.append(value)

        return sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses), params

    def _select_columns_sql(self) -> sql.SQL | sql.Composed:
        columns = (self._columns or "*").strip()
        if columns == "*":
            return sql.SQL("*")
        identifiers = [sql.Identifier(column.strip()) for column in columns.split(",") if column.strip()]
        return sql.SQL(", ").join(identifiers) if identifiers else sql.SQL("*")

    def _execute_select(self) -> AzurePostgresResult:
        where_clause, params = self._where_sql()
        query = sql.SQL("SELECT {} FROM {}{}").format(
            self._select_columns_sql(),
            sql.Identifier(self.table),
            where_clause,
        )

        if self._order_by:
            direction = sql.SQL(" DESC") if self._order_desc else sql.SQL(" ASC")
            query += sql.SQL(" ORDER BY {}{}").format(sql.Identifier(self._order_by), direction)

        if self._range:
            start, end = self._range
            query += sql.SQL(" LIMIT %s OFFSET %s")
            params.extend([end - start + 1, start])
        elif self._limit is not None:
            query += sql.SQL(" LIMIT %s")
            params.append(self._limit)

        connection = self.db.connection
        # End the transaction this read opens, or the connection sits "idle in
        # transaction" until its next use. Leave a caller's open transaction alone.
        own_transaction = connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_IDLE
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            rows = [dict(row) for row in cursor.fetchall()]

        count = None
        if self._count_mode == "exact":
            count_query = sql.SQL("SELECT COUNT(*) AS count FROM {}{}").format(
                sql.Identifier(self.table),
                where_clause,
            )
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(count_query, params[: len(self._filters)])
                count = cursor.fetchone()["count"]

        if own_transaction:
            connection.commit()
        return AzurePostgresResult(data=rows, count=count)

    def _execute_insert(self) -> AzurePostgresResult:
        if not self._data:
            return AzurePostgresResult(data=[])

        return self._insert_or_upsert(on_conflict=None, ignore=False)

    def _execute_upsert(self, ignore: bool = False) -> AzurePostgresResult:
        if not self._data:
            return AzurePostgresResult(data=[])
        if not self._on_conflict:
            raise AzurePostgresQueryError(
                f"Upsert for table '{self.table}' requires on_conflict columns for Azure Postgres."
            )

        return self._insert_or_upsert(on_conflict=self._on_conflict, ignore=ignore)

    def _insert_or_upsert(
        self,
        on_conflict: Optional[List[str]],
        ignore: bool,
    ) -> AzurePostgresResult:
        columns = list(self._data[0].keys())
        values = [[_adapt_value(record.get(column)) for column in columns] for record in self._data]

        base_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(self.table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        )

        if on_conflict:
            conflict_sql = sql.SQL(", ").join(sql.Identifier(column) for column in on_conflict)
            if ignore:
                conflict_clause = sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(conflict_sql)
            else:
                update_columns = [column for column in columns if column not in on_conflict]
                if update_columns:
                    assignments = sql.SQL(", ").join(
                        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
                        for column in update_columns
                    )
                    conflict_clause = sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                        conflict_sql,
                        assignments,
                    )
                else:
                    conflict_clause = sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(conflict_sql)
            base_query += conflict_clause

        base_query += sql.SQL(" RETURNING *")

        with self.db.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            execute_values(cursor, base_query.as_string(cursor), values)
            rows = [dict(row) for row in cursor.fetchall()]
        self.db.connection.commit()
        return AzurePostgresResult(data=rows)

    def _execute_update(self) -> AzurePostgresResult:
        if not self._data:
            return AzurePostgresResult(data=[])

        data = self._data[0]
        where_clause, params = self._where_sql()
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(column)) for column in data.keys()
        )
        query = sql.SQL("UPDATE {} SET {}{} RETURNING *").format(
            sql.Identifier(self.table),
            assignments,
            where_clause,
        )
        query_params = [_adapt_value(value) for value in data.values()] + params

        with self.db.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, query_params)
            rows = [dict(row) for row in cursor.fetchall()]
        self.db.connection.commit()
        return AzurePostgresResult(data=rows)

    def _execute_delete(self) -> AzurePostgresResult:
        where_clause, params = self._where_sql()
        query = sql.SQL("DELETE FROM {}{} RETURNING *").format(
            sql.Identifier(self.table),
            where_clause,
        )

        with self.db.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            rows = [dict(row) for row in cursor.fetchall()]
        self.db.connection.commit()
        return AzurePostgresResult(data=rows)


class AzurePostgresClient:
    """PostgreSQL client for Azure Flexible Server."""

    def __init__(
        self,
        host: str = None,
        database: str = None,
        user: str = None,
        password: str = None,
        port: int = None,
        sslmode: str = None,
        connection_url: str = None,
        max_retries: int = 3,
        timeout: int = 30,
        batch_size: int = 1000,
    ):
        if not POSTGRES_AVAILABLE:
            raise AzurePostgresConnectionError(
                "psycopg2 is not installed. Run: pip install psycopg2-binary"
            )

        self.connection_url = connection_url or os.getenv("AZURE_POSTGRES_URL")
        self.host = host or os.getenv("AZURE_POSTGRES_HOST")
        self.database = database or os.getenv("AZURE_POSTGRES_DB", "postgres")
        self.user = user or os.getenv("AZURE_POSTGRES_USER")
        self.password = password or os.getenv("AZURE_POSTGRES_PASSWORD")
        self.port = int(port or os.getenv("AZURE_POSTGRES_PORT", "5432"))
        self.sslmode = sslmode or os.getenv("AZURE_POSTGRES_SSLMODE", "require")
        self.max_retries = max_retries
        self.timeout = timeout
        self.batch_size = batch_size

        if not self.connection_url and not all([self.host, self.database, self.user, self.password]):
            raise AzurePostgresConnectionError(
                "Azure Postgres credentials required. Set AZURE_POSTGRES_HOST, "
                "AZURE_POSTGRES_DB, AZURE_POSTGRES_USER, AZURE_POSTGRES_PASSWORD, "
                "and optionally AZURE_POSTGRES_PORT/AZURE_POSTGRES_SSLMODE."
            )
        self._local = _shared_thread_local(
            (self.connection_url, self.host, self.port, self.database, self.user, self.sslmode)
        )

        self.query_count = 0
        self.error_count = 0
        self.retry_count = 0
        self._connect()

    @property
    def client(self) -> "AzurePostgresClient":
        return self

    @property
    def connection(self):
        conn = getattr(self._local, "connection", None)
        if conn is None or conn.closed:
            conn = self._connect_new()
            self._local.connection = conn
        return conn

    @connection.setter
    def connection(self, value):
        self._local.connection = value

    def _connect_new(self):
        try:
            if self.connection_url:
                conn = psycopg2.connect(self.connection_url, connect_timeout=self.timeout)
            else:
                conn = psycopg2.connect(
                    host=self.host,
                    dbname=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    sslmode=self.sslmode,
                    connect_timeout=self.timeout,
                )
            logger.info(f"Azure PostgreSQL connection established successfully on thread {threading.current_thread().name}")
            return conn
        except Exception as error:
            logger.error(f"Failed to connect to Azure PostgreSQL on thread {threading.current_thread().name}: {error}")
            raise AzurePostgresConnectionError(f"Connection failed: {error}")

    def _connect(self) -> None:
        _ = self.connection

    def close(self) -> None:
        if hasattr(self, "_local"):
            conn = getattr(self._local, "connection", None)
            if conn:
                conn.close()
                self._local.connection = None

    def table(self, table: str) -> AzureTableQuery:
        return AzureTableQuery(self, table)

    def _retry_with_backoff(self, func, *args, **kwargs):
        last_error = None
        for attempt in range(self.max_retries):
            try:
                self.query_count += 1
                return func(*args, **kwargs)
            except Exception as error:
                last_error = error
                self.error_count += 1
                try:
                    conn = getattr(self._local, "connection", None)
                    if conn and not conn.closed:
                        conn.rollback()
                except Exception as rollback_err:
                    logger.warning(f"Rollback failed during error recovery: {rollback_err}")

                # Non-retryable errors: integrity constraint violations (unique, foreign key, check, not null)
                pgcode = getattr(error, "pgcode", None) or ""
                is_integrity = (
                    (POSTGRES_AVAILABLE and isinstance(error, psycopg2.IntegrityError))
                    or str(pgcode).startswith("23")
                    or "unique constraint" in str(error).lower()
                    or "violates foreign key" in str(error).lower()
                )
                if is_integrity:
                    logger.debug(f"Integrity constraint violation (non-retryable): {error}")
                    raise AzurePostgresQueryError(f"Query failed: {error}") from error

                if attempt < self.max_retries - 1:
                    wait_time = 2**attempt
                    self.retry_count += 1
                    logger.warning(
                        f"Query attempt {attempt + 1}/{self.max_retries} failed. "
                        f"Retrying in {wait_time}s: {error}"
                    )
                    time.sleep(wait_time)

        logger.error(f"Query failed after {self.max_retries} retries: {last_error}")
        raise AzurePostgresQueryError(f"Query failed: {last_error}") from last_error

    def insert(self, table: str, data: Dict[str, Any], return_id: bool = True) -> Dict:
        result = self.table(table).insert(data).execute()
        return result.data[0] if result.data else {}

    def insert_batch(self, table: str, data_list: List[Dict[str, Any]]) -> Tuple[int, int]:
        inserted = 0
        failed = 0
        for i in range(0, len(data_list), self.batch_size):
            batch = data_list[i : i + self.batch_size]
            try:
                result = self.table(table).insert(batch).execute()
                inserted += len(result.data)
            except Exception as error:
                failed += len(batch)
                logger.error(f"Batch insert failed for {len(batch)} records: {error}")
        return inserted, failed

    def upsert(self, table: str, data: Dict[str, Any], conflict_column: str = "id") -> Dict:
        result = self.table(table).upsert(data, on_conflict=conflict_column).execute()
        return result.data[0] if result.data else {}

    def upsert_batch(
        self,
        table: str,
        data_list: List[Dict[str, Any]],
        conflict_columns: str = "id",
    ) -> Tuple[int, int]:
        upserted = 0
        failed = 0
        for i in range(0, len(data_list), self.batch_size):
            batch = data_list[i : i + self.batch_size]
            try:
                result = self.table(table).upsert(batch, on_conflict=conflict_columns).execute()
                upserted += len(result.data)
            except Exception as error:
                failed += len(batch)
                logger.error(f"Batch upsert failed for {len(batch)} records: {error}")
        return upserted, failed

    def select(self, table: str, columns: str = "*", filters: Dict[str, Any] = None) -> List[Dict]:
        query = self.table(table).select(columns)
        if filters:
            for column, value in filters.items():
                if isinstance(value, (list, tuple)):
                    query = query.in_(column, value)
                else:
                    query = query.eq(column, value)
        return query.execute().data

    def select_range(self, table: str, columns: str = "*", start: int = 0, end: int = 999) -> List[Dict]:
        return self.table(table).select(columns).range(start, end).execute().data

    def select_ordered(
        self,
        table: str,
        order_by: str,
        ascending: bool = True,
        limit: int = None,
        columns: str = "*",
        stock_id: int = None,
    ) -> List[Dict]:
        query = self.table(table).select(columns)
        if stock_id is not None:
            query = query.eq("stock_id", stock_id)
        query = query.order(order_by, desc=not ascending)
        if limit:
            query = query.limit(limit)
        return query.execute().data

    def select_latest(
        self,
        table: str,
        stock_id: int,
        order_by: str = "date",
        columns: str = "*",
    ) -> Optional[Dict]:
        records = self.select_ordered(
            table=table,
            columns=columns,
            order_by=order_by,
            ascending=False,
            limit=1,
            stock_id=stock_id,
        )
        return records[0] if records else None

    def update(self, table: str, data: Dict[str, Any], filters: Dict[str, Any]) -> int:
        query = self.table(table).update(data)
        for column, value in filters.items():
            query = query.eq(column, value)
        return len(query.execute().data)

    def delete(self, table: str, filters: Dict[str, Any]) -> int:
        query = self.table(table).delete()
        for column, value in filters.items():
            query = query.eq(column, value)
        return len(query.execute().data)

    def replace_rows_atomic(
        self,
        table: str,
        filters: Dict[str, Any],
        records: List[Dict[str, Any]],
    ) -> int:
        """Replace a bounded row set in one PostgreSQL transaction.

        This is used for versioned forecast and peer datasets.  It refuses an
        empty replacement and an unfiltered delete, preventing a failed refresh
        from erasing the last-known-good public data.
        """
        if not filters:
            raise AzurePostgresQueryError("Atomic replacement requires at least one filter")
        if not records:
            raise AzurePostgresQueryError("Atomic replacement refuses an empty record set")

        columns = list(records[0].keys())
        if not columns or any(list(record.keys()) != columns for record in records):
            raise AzurePostgresQueryError("Atomic replacement records must share identical columns")
        # Raw-SQL path: apply the storage-unit contract here too (it bypasses AzureTableQuery).
        records = guard_records(table, records)

        def _replace() -> int:
            connection = self.connection
            try:
                where_clause = sql.SQL(" AND ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(column))
                    for column in filters
                )
                delete_query = sql.SQL("DELETE FROM {} WHERE {}").format(
                    sql.Identifier(table),
                    where_clause,
                )
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                )
                values = [
                    [_adapt_value(record[column]) for column in columns]
                    for record in records
                ]
                with connection.cursor() as cursor:
                    cursor.execute(delete_query, [_adapt_value(value) for value in filters.values()])
                    execute_values(cursor, insert_query.as_string(cursor), values)
                connection.commit()
                return len(records)
            except Exception:
                connection.rollback()
                raise

        return self._retry_with_backoff(_replace)

    def publish_stock_page_snapshot(self, row: Dict[str, Any]) -> int:
        """Publish a canonical snapshot and retire the prior one atomically."""
        if not row or not row.get("stock_id"):
            raise AzurePostgresQueryError("Snapshot publication requires stock_id")
        columns = list(row.keys())
        values = [_adapt_value(row[column]) for column in columns]

        def _publish() -> int:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        "UPDATE stock_page_snapshot SET is_current = FALSE WHERE stock_id = %s AND is_current",
                        (_adapt_value(row["stock_id"]),),
                    )
                    insert_query = sql.SQL("INSERT INTO stock_page_snapshot ({}) VALUES ({}) ON CONFLICT (stock_id, content_hash) DO UPDATE SET generated_at = EXCLUDED.generated_at, is_current = EXCLUDED.is_current").format(
                        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                    )
                    cursor.execute(insert_query, values)
                self.connection.commit()
                return 1
            except Exception:
                self.connection.rollback()
                raise

        return self._retry_with_backoff(_publish)

    def get_stock_by_symbol(self, symbol: str) -> Optional[Dict]:
        records = self.select("stocks", filters={"symbol": symbol.upper()})
        return records[0] if records else None

    def get_price_range(self, stock_id: int, start_date: str, end_date: str) -> List[Dict]:
        return (
            self.table("daily_price_data")
            .select("*")
            .eq("stock_id", stock_id)
            .gte("date", start_date)
            .lte("date", end_date)
            .order("date")
            .execute()
            .data
        )

    def count_records(self, table: str, filters: Dict[str, Any] = None) -> int:
        query = self.table(table).select("id", count="exact")
        if filters:
            for column, value in filters.items():
                query = query.eq(column, value)
        return query.execute().count or 0

    def execute_raw_sql(self, query: str, params: List[Any] = None) -> List[Dict]:
        def _execute():
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or [])
                if cursor.description is None:
                    self.connection.commit()
                    return []
                rows = [dict(row) for row in cursor.fetchall()]
                self.connection.commit()
                return rows

        return self._retry_with_backoff(_execute)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "queries_executed": self.query_count,
            "errors": self.error_count,
            "retries": self.retry_count,
            "error_rate": self.error_count / max(self.query_count, 1),
            "timestamp": datetime.now().isoformat(),
        }


def get_azure_postgres_client(**kwargs) -> AzurePostgresClient:
    return AzurePostgresClient(**kwargs)


def test_connection(**kwargs) -> bool:
    try:
        client = AzurePostgresClient(**kwargs)
        client.execute_raw_sql("SELECT version()")
        logger.info("Azure PostgreSQL connection test successful")
        client.close()
        return True
    except Exception as error:
        logger.error(f"Azure PostgreSQL connection test failed: {error}")
        return False


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    print("Testing Azure PostgreSQL connection...")
    if test_connection():
        print("Connection successful")
    else:
        print("Connection failed")
