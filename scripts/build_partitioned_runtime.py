from __future__ import annotations

import argparse
import os
import random
import sqlite3
import sys
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from legacy_minute_data_builder import (  # noqa: E402
    EXPECTED_1M_BARS_PER_DAY,
    ensure_normalized_table,
    ensure_price_table,
    load_minute_rows_for_symbol,
    rebuild_stretch_1min_for_symbol,
    refresh_normalized_daily_data,
    refresh_template_normalized_daily_data,
    validate_identifier,
)


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SQLITE_PATH = os.path.join(PROJECT_ROOT, "outputs", "minute_data", "turso_source_cache.db")
DEFAULT_REGISTRY_TABLE = "runtime_partition_registry"
DEFAULT_RUNTIME_PREFIX = "stock_1_min_runtime_p"
DEFAULT_NORMALIZED_TABLE = "normalized_daily_ohlc"
DEFAULT_FAKE_TABLE = "stock_1_min_fake"
DEFAULT_MOCK_TABLE = "stock_1_min_mock"
DEFAULT_DAILY_TABLE = "stock_daily"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate partitioned runtime minute tables from local stock_daily + stock_1_min_mock. "
            "Missing dates are filled by stretch-generated stock_1_min_fake, then runtime partitions "
            "are materialized with priority mock > fake."
        )
    )
    parser.add_argument("--sqlite-path", default=DEFAULT_SQLITE_PATH)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols. Default: all symbols in local daily table within window.")
    parser.add_argument("--daily-table", default=DEFAULT_DAILY_TABLE)
    parser.add_argument("--mock-table", default=DEFAULT_MOCK_TABLE)
    parser.add_argument("--fake-table", default=DEFAULT_FAKE_TABLE)
    parser.add_argument("--normalized-table", default=DEFAULT_NORMALIZED_TABLE)
    parser.add_argument("--registry-table", default=DEFAULT_REGISTRY_TABLE)
    parser.add_argument("--runtime-prefix", default=DEFAULT_RUNTIME_PREFIX)
    parser.add_argument(
        "--date-partition",
        choices=("year", "month"),
        default="year",
        help="Partition runtime tables by symbol plus year or month.",
    )
    parser.add_argument("--initial-tolerance", type=float, default=0.05)
    parser.add_argument("--max-tolerance", type=float, default=0.5)
    parser.add_argument("--tolerance-step", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def parse_symbols(raw_symbols: Optional[str]) -> Optional[List[str]]:
    if not raw_symbols:
        return None
    symbols = [symbol.strip() for symbol in raw_symbols.split(",") if symbol.strip()]
    return symbols or None


def load_symbols(
    conn: sqlite3.Connection,
    daily_table: str,
    from_date: str,
    to_date: str,
    requested_symbols: Optional[Sequence[str]],
) -> List[str]:
    if requested_symbols:
        return list(requested_symbols)
    rows = conn.execute(
        """
        SELECT DISTINCT symbol
        FROM {daily_table}
        WHERE DATE(timestamp) BETWEEN ? AND ?
        ORDER BY symbol
        """.format(daily_table=daily_table),
        (from_date, to_date),
    ).fetchall()
    return [str(row[0]) for row in rows]


def ensure_registry_table(conn: sqlite3.Connection, registry_table: str) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS {registry_table} (
            table_name TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            partition_kind TEXT NOT NULL,
            partition_value TEXT NOT NULL,
            from_date TEXT NOT NULL,
            to_date TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            trading_day_count INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """.format(registry_table=registry_table)
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_{registry_table}_symbol_part ON {registry_table} (symbol, partition_kind, partition_value)".format(
            registry_table=registry_table
        )
    )
    conn.commit()


def partition_key_for_date(date_str: str, partition_kind: str) -> str:
    if partition_kind == "year":
        return date_str[:4]
    if partition_kind == "month":
        return date_str[:7].replace("-", "")
    raise ValueError("Unsupported partition_kind: {}".format(partition_kind))


def sanitize_symbol(symbol: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in symbol)


def partition_table_name(runtime_prefix: str, partition_kind: str, partition_value: str, symbol: str) -> str:
    if partition_kind == "year":
        suffix = "y{}".format(partition_value)
    else:
        suffix = "m{}".format(partition_value)
    return validate_identifier(
        "{}_{}_s{}".format(runtime_prefix, suffix, sanitize_symbol(symbol)),
        "partition_table",
    )


def iter_partition_specs(
    conn: sqlite3.Connection,
    daily_table: str,
    symbol: str,
    from_date: str,
    to_date: str,
    partition_kind: str,
) -> Iterable[Tuple[str, str, str]]:
    rows = conn.execute(
        """
        SELECT DISTINCT DATE(timestamp) AS trade_date
        FROM {daily_table}
        WHERE symbol = ?
          AND DATE(timestamp) BETWEEN ? AND ?
        ORDER BY trade_date
        """.format(daily_table=daily_table),
        (symbol, from_date, to_date),
    ).fetchall()
    grouped: Dict[str, List[str]] = {}
    for row in rows:
        trade_date = str(row[0])
        grouped.setdefault(partition_key_for_date(trade_date, partition_kind), []).append(trade_date)
    for partition_value, trade_dates in grouped.items():
        yield partition_value, trade_dates[0], trade_dates[-1]


def reset_partition_table(conn: sqlite3.Connection, table_name: str) -> None:
    ensure_price_table(conn, table_name)
    conn.execute("DELETE FROM {}".format(table_name))
    conn.commit()


def write_partition_rows(
    conn: sqlite3.Connection,
    table_name: str,
    rows: Iterable[Tuple[object, ...]],
) -> int:
    rows = list(rows)
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT OR REPLACE INTO {table_name}
        (symbol, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """.format(table_name=table_name),
        rows,
    )
    conn.commit()
    return len(rows)


def update_registry(
    conn: sqlite3.Connection,
    registry_table: str,
    table_name: str,
    symbol: str,
    partition_kind: str,
    partition_value: str,
    from_date: str,
    to_date: str,
    row_count: int,
    trading_day_count: int,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO {registry_table}
        (table_name, symbol, partition_kind, partition_value, from_date, to_date, row_count, trading_day_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.format(registry_table=registry_table),
        (
            table_name,
            symbol,
            partition_kind,
            partition_value,
            from_date,
            to_date,
            int(row_count),
            int(trading_day_count),
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    sqlite_path = os.path.abspath(args.sqlite_path)
    symbols_arg = parse_symbols(args.symbols)

    normalized_table = validate_identifier(args.normalized_table, "normalized_table")
    template_normalized_table = validate_identifier(
        "{}_templates".format(normalized_table),
        "template_normalized_table",
    )
    daily_table = validate_identifier(args.daily_table, "daily_table")
    mock_table = validate_identifier(args.mock_table, "mock_table")
    fake_table = validate_identifier(args.fake_table, "fake_table")
    registry_table = validate_identifier(args.registry_table, "registry_table")
    runtime_prefix = validate_identifier(args.runtime_prefix, "runtime_prefix")

    conn = sqlite3.connect(sqlite_path)
    try:
        ensure_price_table(conn, daily_table)
        ensure_price_table(conn, mock_table)
        ensure_price_table(conn, fake_table)
        ensure_normalized_table(conn, normalized_table)
        ensure_normalized_table(conn, template_normalized_table)
        ensure_registry_table(conn, registry_table)

        symbols = load_symbols(
            conn=conn,
            daily_table=daily_table,
            from_date=args.from_date,
            to_date=args.to_date,
            requested_symbols=symbols_arg,
        )
        print("resolved symbols={}".format(len(symbols)))

        normalized_rows = refresh_normalized_daily_data(
            conn=conn,
            daily_table=daily_table,
            normalized_table=normalized_table,
            from_date=args.from_date,
            to_date=args.to_date,
        )
        print("normalized rows refreshed={}".format(normalized_rows))
        template_rows = refresh_template_normalized_daily_data(
            conn=conn,
            normalized_table=normalized_table,
            source_min_table=mock_table,
            template_normalized_table=template_normalized_table,
            from_date=args.from_date,
            to_date=args.to_date,
        )
        print("template rows refreshed={}".format(template_rows))

        total_fake_rows = 0
        for index, symbol in enumerate(symbols, start=1):
            inserted_1m, day_count, matched_days, generated_days = rebuild_stretch_1min_for_symbol(
                conn=conn,
                symbol=symbol,
                source_min_table=mock_table,
                output_table=fake_table,
                daily_table=daily_table,
                normalized_table=template_normalized_table,
                from_date=args.from_date,
                to_date=args.to_date,
                initial_tolerance=args.initial_tolerance,
                max_tolerance=args.max_tolerance,
                tolerance_step=args.tolerance_step,
                rng=rng,
            )
            total_fake_rows += inserted_1m
            print(
                "[{}/{}] {} fake_rows={} daily_days={} matched_days={} generated_days={}".format(
                    index, len(symbols), symbol, inserted_1m, day_count, matched_days, generated_days
                )
            )

        total_partitions = 0
        total_runtime_rows = 0
        for symbol in symbols:
            for partition_value, part_from_date, part_to_date in iter_partition_specs(
                conn=conn,
                daily_table=daily_table,
                symbol=symbol,
                from_date=args.from_date,
                to_date=args.to_date,
                partition_kind=args.date_partition,
            ):
                table_name = partition_table_name(
                    runtime_prefix=runtime_prefix,
                    partition_kind=args.date_partition,
                    partition_value=partition_value,
                    symbol=symbol,
                )
                reset_partition_table(conn, table_name)
                fake_rows = load_minute_rows_for_symbol(
                    conn=conn,
                    table=fake_table,
                    symbol=symbol,
                    from_date=part_from_date,
                    to_date=part_to_date,
                    expected_bars_per_day=EXPECTED_1M_BARS_PER_DAY,
                )
                mock_rows = load_minute_rows_for_symbol(
                    conn=conn,
                    table=mock_table,
                    symbol=symbol,
                    from_date=part_from_date,
                    to_date=part_to_date,
                    expected_bars_per_day=EXPECTED_1M_BARS_PER_DAY,
                )
                inserted_rows = write_partition_rows(conn, table_name, fake_rows)
                inserted_rows = write_partition_rows(conn, table_name, mock_rows) or inserted_rows
                row_count = int(conn.execute("SELECT COUNT(*) FROM {}".format(table_name)).fetchone()[0])
                trading_day_count = int(
                    conn.execute("SELECT COUNT(DISTINCT DATE(timestamp)) FROM {}".format(table_name)).fetchone()[0]
                )
                update_registry(
                    conn=conn,
                    registry_table=registry_table,
                    table_name=table_name,
                    symbol=symbol,
                    partition_kind=args.date_partition,
                    partition_value=partition_value,
                    from_date=part_from_date,
                    to_date=part_to_date,
                    row_count=row_count,
                    trading_day_count=trading_day_count,
                )
                total_partitions += 1
                total_runtime_rows += row_count
                print(
                    "  partition {} symbol={} rows={} days={}".format(
                        table_name, symbol, row_count, trading_day_count
                    )
                )

        print("fake_rows_total={}".format(total_fake_rows))
        print("runtime_partitions={}".format(total_partitions))
        print("runtime_rows_total={}".format(total_runtime_rows))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
