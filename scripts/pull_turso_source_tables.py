from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

import libsql_client


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SQLITE_PATH = os.path.join(PROJECT_ROOT, "outputs", "minute_data", "stock_data.db")
TABLE_NAMES = ("stock_daily", "stock_5_min", "stock_1_min_mock")
DEFAULT_TABLE_NAMES = ("stock_daily", "stock_5_min")
ROW_COLUMNS = ("symbol", "timestamp", "open", "high", "low", "close", "volume")


CREATE_TABLE_SQL = {
    table_name: """
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, timestamp)
        )
    """.format(table_name=table_name)
    for table_name in TABLE_NAMES
}


UPSERT_SQL = {
    table_name: (
        "INSERT OR REPLACE INTO {table_name} "
        "(symbol, timestamp, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).format(table_name=table_name)
    for table_name in TABLE_NAMES
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pull source tables from Turso/libSQL into a local sqlite cache. "
            "Supports stock_daily, stock_5_min, and stock_1_min_mock."
        )
    )
    parser.add_argument("--sqlite-path", default=DEFAULT_SQLITE_PATH)
    parser.add_argument("--tables", default=",".join(DEFAULT_TABLE_NAMES))
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols. Default: all symbols in remote stock_daily within window.")
    parser.add_argument("--from-date", default=None, help="Inclusive start date, YYYY-MM-DD.")
    parser.add_argument("--to-date", default=None, help="Inclusive end date, YYYY-MM-DD.")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument(
        "--replace-window",
        action="store_true",
        help="Delete local rows in the requested window/symbol set before pulling from Turso.",
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
        raise RuntimeError("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN.")
    return libsql_client.create_client_sync(normalize_turso_url(url), auth_token=token)


def parse_table_names(raw_tables: str) -> List[str]:
    table_names = [table.strip() for table in raw_tables.split(",") if table.strip()]
    invalid = sorted(set(table_names) - set(TABLE_NAMES))
    if invalid:
        raise ValueError("Unsupported table names: {}".format(",".join(invalid)))
    if not table_names:
        raise ValueError("No tables selected.")
    return table_names


def parse_symbols(raw_symbols: Optional[str]) -> Optional[List[str]]:
    if not raw_symbols:
        return None
    symbols = [symbol.strip() for symbol in raw_symbols.split(",") if symbol.strip()]
    return symbols or None


def ensure_local_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(CREATE_TABLE_SQL[table_name])
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_time ON {table_name} (symbol, timestamp)".format(
            table_name=table_name
        )
    )
    conn.commit()


def build_filters(
    from_date: Optional[str],
    to_date: Optional[str],
    symbols: Optional[Sequence[str]],
) -> Tuple[List[str], List[object]]:
    clauses: List[str] = []
    params: List[object] = []
    if from_date:
        clauses.append("DATE(timestamp) >= ?")
        params.append(from_date)
    if to_date:
        clauses.append("DATE(timestamp) <= ?")
        params.append(to_date)
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append("symbol IN ({})".format(placeholders))
        params.extend(symbols)
    return clauses, params


def get_remote_symbols(
    client: libsql_client.sync.ClientSync,
    from_date: Optional[str],
    to_date: Optional[str],
) -> List[str]:
    clauses, params = build_filters(from_date=from_date, to_date=to_date, symbols=None)
    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(clauses)
    sql = "SELECT DISTINCT symbol FROM stock_daily{} ORDER BY symbol".format(where_sql)
    result = client.execute(sql, params)
    return [str(row["symbol"]) for row in result.rows]


def count_remote_rows(
    client: libsql_client.sync.ClientSync,
    table_name: str,
    from_date: Optional[str],
    to_date: Optional[str],
    symbols: Optional[Sequence[str]],
) -> int:
    clauses, params = build_filters(from_date=from_date, to_date=to_date, symbols=symbols)
    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(clauses)
    sql = "SELECT COUNT(*) AS row_count FROM {}{}".format(table_name, where_sql)
    result = client.execute(sql, params)
    return int(result.rows[0]["row_count"])


def iter_remote_rows(
    client: libsql_client.sync.ClientSync,
    table_name: str,
    from_date: Optional[str],
    to_date: Optional[str],
    symbols: Optional[Sequence[str]],
    batch_size: int,
) -> Iterator[List[Tuple[object, ...]]]:
    last_symbol: Optional[str] = None
    last_timestamp: Optional[str] = None
    while True:
        clauses, params = build_filters(from_date=from_date, to_date=to_date, symbols=symbols)
        if last_symbol is not None and last_timestamp is not None:
            clauses.append("(symbol > ? OR (symbol = ? AND timestamp > ?))")
            params.extend([last_symbol, last_symbol, last_timestamp])
        where_sql = ""
        if clauses:
            where_sql = " WHERE " + " AND ".join(clauses)
        sql = (
            "SELECT symbol, timestamp, open, high, low, close, volume "
            "FROM {table_name}{where_sql} "
            "ORDER BY symbol, timestamp "
            "LIMIT ?"
        ).format(table_name=table_name, where_sql=where_sql)
        params.append(int(batch_size))
        result = client.execute(sql, params)
        rows = []
        for row in result.rows:
            rows.append(tuple(row[column] for column in ROW_COLUMNS))
        if not rows:
            return
        last_symbol = str(rows[-1][0])
        last_timestamp = str(rows[-1][1])
        yield rows


def delete_local_window(
    conn: sqlite3.Connection,
    table_name: str,
    from_date: Optional[str],
    to_date: Optional[str],
    symbols: Optional[Sequence[str]],
) -> None:
    clauses = ["1 = 1"]
    params: List[object] = []
    if from_date:
        clauses.append("DATE(timestamp) >= ?")
        params.append(from_date)
    if to_date:
        clauses.append("DATE(timestamp) <= ?")
        params.append(to_date)
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append("symbol IN ({})".format(placeholders))
        params.extend(symbols)
    conn.execute(
        "DELETE FROM {table_name} WHERE {where_sql}".format(
            table_name=table_name,
            where_sql=" AND ".join(clauses),
        ),
        params,
    )
    conn.commit()


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: Iterable[Tuple[object, ...]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    conn.executemany(UPSERT_SQL[table_name], rows)
    conn.commit()
    return len(rows)


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be > 0")

    table_names = parse_table_names(args.tables)
    symbols = parse_symbols(args.symbols)
    sqlite_path = os.path.abspath(args.sqlite_path)
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

    client = require_turso_client()
    try:
        if symbols is None:
            symbols = get_remote_symbols(client, from_date=args.from_date, to_date=args.to_date)
            print("resolved symbols from remote stock_daily: {}".format(len(symbols)))

        conn = sqlite3.connect(sqlite_path)
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            total_rows = 0
            for table_name in table_names:
                ensure_local_table(conn, table_name)
                if args.replace_window:
                    delete_local_window(
                        conn=conn,
                        table_name=table_name,
                        from_date=args.from_date,
                        to_date=args.to_date,
                        symbols=symbols,
                    )

                expected_rows = count_remote_rows(
                    client=client,
                    table_name=table_name,
                    from_date=args.from_date,
                    to_date=args.to_date,
                    symbols=symbols,
                )
                print("[{}] expected_rows={}".format(table_name, expected_rows))
                started_at = time.time()
                synced_rows = 0
                for batch_index, rows in enumerate(
                    iter_remote_rows(
                        client=client,
                        table_name=table_name,
                        from_date=args.from_date,
                        to_date=args.to_date,
                        symbols=symbols,
                        batch_size=args.batch_size,
                    ),
                    start=1,
                ):
                    synced_rows += upsert_rows(conn, table_name, rows)
                    elapsed = time.time() - started_at
                    print(
                        "  batch={} rows={} synced={} elapsed={:.1f}s".format(
                            batch_index, len(rows), synced_rows, elapsed
                        )
                    )
                total_rows += synced_rows
                print("[{}] synced_rows={}".format(table_name, synced_rows))
            print("pull_complete rows_written_local={}".format(total_rows))
        finally:
            conn.close()
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
