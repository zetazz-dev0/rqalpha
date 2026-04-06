from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Iterator, List, Sequence, Tuple

import libsql_client


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CURRENT_SQLITE_PATH = os.path.join(PROJECT_ROOT, "outputs", "minute_data", "stock_data.db")
DEFAULT_LEGACY_SQLITE_PATH = (
    "/Users/zeta/Projects/python/stock-back-trader/archive/data/db/stock_data.db"
)
TABLE_NAMES = ("stock_daily", "stock_5_min", "stock_1_min_mock")


CREATE_TABLE_SQL = {
    "stock_daily": """
        CREATE TABLE IF NOT EXISTS stock_daily (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, timestamp)
        )
    """,
    "stock_5_min": """
        CREATE TABLE IF NOT EXISTS stock_5_min (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, timestamp)
        )
    """,
    "stock_1_min_mock": """
        CREATE TABLE IF NOT EXISTS stock_1_min_mock (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, timestamp)
        )
    """,
}


UPSERT_SQL = {
    table_name: (
        "INSERT OR REPLACE INTO {table_name} "
        "(symbol, timestamp, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).format(table_name=table_name)
    for table_name in TABLE_NAMES
}


@dataclass(frozen=True)
class SyncTarget:
    table_name: str
    source_label: str
    query: str
    params: Tuple[object, ...] = ()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync source tables from local sqlite DB(s) into a Turso/libSQL database. "
            "Uploads stock_daily, merged stock_5_min (legacy + current), and stock_1_min_mock."
        )
    )
    parser.add_argument("--current-sqlite-path", default=DEFAULT_CURRENT_SQLITE_PATH)
    parser.add_argument("--legacy-sqlite-path", default=DEFAULT_LEGACY_SQLITE_PATH)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--tables",
        default=",".join(TABLE_NAMES),
        help="Comma-separated subset of tables to sync: stock_daily,stock_5_min,stock_1_min_mock",
    )
    parser.add_argument(
        "--skip-legacy-5min",
        action="store_true",
        help="Skip legacy stock_5_min rows and upload current stock_5_min only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect local row counts only; do not connect to Turso or write data.",
    )
    parser.add_argument(
        "--remote-count-check",
        action="store_true",
        help="Query and print remote table counts after each table sync.",
    )
    return parser.parse_args()


def normalize_turso_url(url: str) -> str:
    if url.startswith("libsql://"):
        return "https://" + url[len("libsql://"):]
    return url


def require_turso_client() -> libsql_client.sync.ClientSync:
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    if not url or not token:
        raise RuntimeError(
            "Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN in the current shell environment."
        )
    return libsql_client.create_client_sync(
        normalize_turso_url(url),
        auth_token=token,
    )


def parse_table_names(raw_tables: str) -> List[str]:
    table_names = [table.strip() for table in raw_tables.split(",") if table.strip()]
    invalid = sorted(set(table_names) - set(TABLE_NAMES))
    if invalid:
        raise ValueError("Unsupported table names: {}".format(",".join(invalid)))
    if not table_names:
        raise ValueError("No tables selected for sync.")
    return table_names


def iter_query_rows(
    conn: sqlite3.Connection,
    query: str,
    params: Sequence[object],
    fetch_size: int,
) -> Iterator[List[Tuple[object, ...]]]:
    cursor = conn.execute(query, params)
    while True:
        rows = cursor.fetchmany(fetch_size)
        if not rows:
            return
        yield rows


def count_rows(conn: sqlite3.Connection, query: str, params: Sequence[object]) -> int:
    count_query = "SELECT COUNT(*) FROM ({})".format(query)
    return int(conn.execute(count_query, params).fetchone()[0])


def ensure_remote_tables(client: libsql_client.sync.ClientSync, table_names: Sequence[str]) -> None:
    for table_name in table_names:
        client.execute(CREATE_TABLE_SQL[table_name])


def remote_table_count(client: libsql_client.sync.ClientSync, table_name: str) -> int:
    result = client.execute("SELECT COUNT(*) AS row_count FROM {}".format(table_name))
    return int(result.rows[0]["row_count"])


def sync_batches(
    client: libsql_client.sync.ClientSync,
    conn: sqlite3.Connection,
    target: SyncTarget,
    batch_size: int,
    remote_count_check: bool,
) -> int:
    expected_rows = count_rows(conn, target.query, target.params)
    print(
        "[{}] source={} expected_rows={}".format(
            target.table_name, target.source_label, expected_rows
        )
    )

    synced_rows = 0
    started_at = time.time()
    for batch_index, rows in enumerate(
        iter_query_rows(conn, target.query, target.params, batch_size),
        start=1,
    ):
        statements = [(UPSERT_SQL[target.table_name], list(row)) for row in rows]
        client.batch(statements)

        synced_rows += len(rows)
        elapsed = time.time() - started_at
        print(
            "  batch={} rows={} synced={} elapsed={:.1f}s".format(
                batch_index, len(rows), synced_rows, elapsed
            )
        )

    if remote_count_check:
        print(
            "  remote_count[{}]={}".format(
                target.table_name,
                remote_table_count(client, target.table_name),
            )
        )
    return synced_rows


def build_targets(table_names: Sequence[str], skip_legacy_5min: bool) -> List[SyncTarget]:
    targets: List[SyncTarget] = []
    for table_name in table_names:
        if table_name == "stock_daily":
            targets.append(
                SyncTarget(
                    table_name="stock_daily",
                    source_label="current",
                    query=(
                        "SELECT symbol, timestamp, open, high, low, close, volume "
                        "FROM main.stock_daily ORDER BY symbol, timestamp"
                    ),
                )
            )
        elif table_name == "stock_1_min_mock":
            targets.append(
                SyncTarget(
                    table_name="stock_1_min_mock",
                    source_label="current",
                    query=(
                        "SELECT symbol, timestamp, open, high, low, close, volume "
                        "FROM main.stock_1_min_mock ORDER BY symbol, timestamp"
                    ),
                )
            )
        elif table_name == "stock_5_min":
            if not skip_legacy_5min:
                targets.append(
                    SyncTarget(
                        table_name="stock_5_min",
                        source_label="legacy",
                        query=(
                            "SELECT symbol, timestamp, open, high, low, close, volume "
                            "FROM legacy.stock_5_min ORDER BY symbol, timestamp"
                        ),
                    )
                )
            targets.append(
                SyncTarget(
                    table_name="stock_5_min",
                    source_label="current",
                    query=(
                        "SELECT symbol, timestamp, open, high, low, close, volume "
                        "FROM main.stock_5_min ORDER BY symbol, timestamp"
                    ),
                )
            )
    return targets


def print_local_plan(conn: sqlite3.Connection, targets: Sequence[SyncTarget]) -> None:
    print("local sync plan")
    seen = set()
    for target in targets:
        count = count_rows(conn, target.query, target.params)
        print(
            "  table={} source={} rows={}".format(
                target.table_name, target.source_label, count
            )
        )
        seen.add((target.table_name, target.source_label))

    if any(table_name == "stock_5_min" for table_name, _ in seen):
        union_unique = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT symbol, timestamp FROM main.stock_5_min
                UNION
                SELECT symbol, timestamp FROM legacy.stock_5_min
            )
            """
        ).fetchone()[0]
        print("  stock_5_min unique rows after merge={}".format(int(union_unique)))


def main() -> int:
    args = parse_args()
    table_names = parse_table_names(args.tables)

    if args.batch_size <= 0:
        raise ValueError("--batch-size must be > 0")

    current_sqlite_path = os.path.abspath(args.current_sqlite_path)
    legacy_sqlite_path = os.path.abspath(args.legacy_sqlite_path)
    if not os.path.isfile(current_sqlite_path):
        raise FileNotFoundError("current sqlite db not found: {}".format(current_sqlite_path))
    if (not args.skip_legacy_5min) and ("stock_5_min" in table_names) and (not os.path.isfile(legacy_sqlite_path)):
        raise FileNotFoundError("legacy sqlite db not found: {}".format(legacy_sqlite_path))

    conn = sqlite3.connect(current_sqlite_path)
    conn.execute("PRAGMA query_only = ON")
    try:
        if (not args.skip_legacy_5min) and ("stock_5_min" in table_names):
            conn.execute("ATTACH DATABASE ? AS legacy", (legacy_sqlite_path,))

        targets = build_targets(table_names, args.skip_legacy_5min)
        print_local_plan(conn, targets)

        if args.dry_run:
            print("dry_run = true")
            return 0

        client = require_turso_client()
        try:
            ensure_remote_tables(client, table_names)
            total_synced = 0
            for target in targets:
                total_synced += sync_batches(
                    client=client,
                    conn=conn,
                    target=target,
                    batch_size=args.batch_size,
                    remote_count_check=args.remote_count_check,
                )
            print("sync_complete rows_sent={}".format(total_synced))
        finally:
            client.close()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
