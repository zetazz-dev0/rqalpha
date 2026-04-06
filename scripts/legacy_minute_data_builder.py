#!/usr/bin/env python3
"""
Build minute bars for legacy-RQAlpha integration with two simulation modes.

Mode 1 (`basic`):
  - fetch 5m bars from akshare (optional)
  - generate 1m mock bars directly from each 5m bar
  - write to `stock_1_min_mock` (or custom table)

Mode 2 (`stretch`):
  - use daily OHLC as target
  - search similar normalized daily pattern from historical daily table
  - stretch matched minute shape to target daily OHLC
  - write to `stock_1_min_fake` (or custom table)
"""

import argparse
import os
import random
import re
import sqlite3
import time as time_module
from datetime import date, datetime
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
DEFAULT_SQLITE_PATH = os.path.join(PROJECT_ROOT, "outputs", "minute_data", "stock_data.db")


DEFAULT_STOCKS_LIST = [
    "000725",
    "601222",
    "600519",
    "600600",
    "600059",
    "600887",
    "000895",
    "600315",
    "601888",
    "600138",
    "002033",
    "000069",
    "600535",
    "000423",
    "600436",
    "000538",
    "600085",
    "600332",
    "600276",
    "600161",
    "300122",
    "300142",
    "600111",
    "600456",
    "601088",
    "601318",
    "600030",
    "600036",
    "600016",
    "600000",
]


VALID_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXPECTED_1M_BARS_PER_DAY = 240
EXPECTED_5M_BARS_PER_DAY = 48


def validate_identifier(name: str, field_name: str) -> str:
    if not isinstance(name, str) or not VALID_SQL_IDENTIFIER.match(name):
        raise ValueError(
            "Invalid {}: {!r}. Only letters, numbers and underscore are allowed.".format(
                field_name, name
            )
        )
    return name


def ensure_price_table(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS {table} (
            symbol TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, timestamp)
        )
        """.format(table=table)
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_{table}_symbol_time ON {table} (symbol, timestamp)".format(
            table=table
        )
    )
    conn.commit()


def ensure_normalized_table(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS {table} (
            symbol TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            close_open_ratio REAL NOT NULL,
            high_open_ratio REAL NOT NULL,
            low_open_ratio REAL NOT NULL,
            PRIMARY KEY (symbol, timestamp)
        )
        """.format(table=table)
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_{table}_symbol_time ON {table} (symbol, timestamp)".format(
            table=table
        )
    )
    conn.execute(
        (
            "CREATE INDEX IF NOT EXISTS idx_{table}_ratios "
            "ON {table} (close_open_ratio, high_open_ratio, low_open_ratio)"
        ).format(
            table=table
        )
    )
    conn.commit()


def ensure_tables(
    conn: sqlite3.Connection,
    five_min_table: str,
    one_min_table: str,
    stretch_source_table: str,
    stretch_output_table: str,
    runtime_table: str,
    daily_table: str,
    normalized_table: str,
    synthetic_table: Optional[str] = None,
) -> None:
    tables = {
        five_min_table,
        one_min_table,
        stretch_source_table,
        stretch_output_table,
        runtime_table,
        daily_table,
    }
    if synthetic_table:
        tables.add(synthetic_table)
    for table in tables:
        ensure_price_table(conn, table)
    ensure_normalized_table(conn, normalized_table)


def symbol_to_ak(symbol: str) -> str:
    return "sh{}".format(symbol) if symbol.startswith("6") else "sz{}".format(symbol)


def fetch_5min_from_akshare(symbol: str) -> pd.DataFrame:
    import akshare as ak

    df = ak.stock_zh_a_minute(
        symbol=symbol_to_ak(symbol),
        period="5",
        adjust="",
    )
    if df.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"])

    df = df.rename(columns={"day": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["symbol"] = symbol
    return df[["symbol", "timestamp", "open", "high", "low", "close", "volume"]]


def fetch_daily_from_akshare(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    import akshare as ak

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if start_date is None:
        # Default to 10-year window when there is no existing data hint.
        start_date = (datetime.now() - pd.Timedelta(days=3650)).strftime("%Y-%m-%d")

    start_str = pd.Timestamp(start_date).strftime("%Y%m%d")
    end_str = pd.Timestamp(end_date).strftime("%Y%m%d")
    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        return pd.DataFrame(columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"])

    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_str,
        end_date=end_str,
        adjust="",
    )
    if df.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"])

    df = df.rename(
        columns={
            "日期": "timestamp",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
        }
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["symbol"] = symbol
    return df[["symbol", "timestamp", "open", "high", "low", "close", "volume"]]


def upsert_rows(conn: sqlite3.Connection, table: str, rows: Iterable[tuple]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT OR REPLACE INTO {table}
        (symbol, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """.format(table=table),
        rows,
    )
    conn.commit()
    return len(rows)


def generate_1min_data_from_5min(df_5min: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    all_times: List[pd.Timestamp] = []
    all_opens: List[float] = []
    all_highs: List[float] = []
    all_lows: List[float] = []
    all_closes: List[float] = []
    all_volumes: List[float] = []

    for i in range(len(df_5min)):
        bar = df_5min.iloc[i]
        base_time = df_5min.index[i]
        minute_times = [base_time - pd.Timedelta(minutes=4) + pd.Timedelta(minutes=m) for m in range(5)]

        open_price = round(float(bar["open"]), 2)
        close_price = round(float(bar["close"]), 2)
        high_price = round(float(bar["high"]), 2)
        low_price = round(float(bar["low"]), 2)
        volume_per_min = float(bar["volume"]) / 5.0

        minute_prices = [open_price]
        middle_minutes = [1, 2, 3]

        if high_price != low_price:
            high_pos = int(rng.choice(middle_minutes))
            middle_minutes.remove(high_pos)
            low_pos = int(rng.choice(middle_minutes))
            middle_minutes.remove(low_pos)

            for minute_i in [1, 2, 3]:
                if minute_i == high_pos:
                    minute_prices.append(high_price)
                elif minute_i == low_pos:
                    minute_prices.append(low_price)
                else:
                    minute_prices.append(round(float(rng.uniform(low_price, high_price)), 2))
        else:
            minute_prices.extend([high_price, high_price, high_price])

        minute_prices.append(close_price)

        for ts, price in zip(minute_times, minute_prices):
            all_times.append(ts)
            all_opens.append(price)
            all_highs.append(price)
            all_lows.append(price)
            all_closes.append(price)
            all_volumes.append(volume_per_min)

    one_min = pd.DataFrame(
        {
            "open": all_opens,
            "high": all_highs,
            "low": all_lows,
            "close": all_closes,
            "volume": all_volumes,
        },
        index=pd.DatetimeIndex(all_times),
    )
    one_min = one_min.reset_index().rename(columns={"index": "timestamp"})
    one_min["timestamp"] = pd.to_datetime(one_min["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return one_min


def load_5min_from_db(conn: sqlite3.Connection, table: str, symbol: str) -> pd.DataFrame:
    query = """
        SELECT timestamp, open, high, low, close, volume
        FROM {table}
        WHERE symbol = ?
        ORDER BY timestamp
    """.format(table=table)
    df = pd.read_sql_query(query, conn, params=(symbol,))
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.set_index("timestamp")


def filter_complete_intraday_days(
    df: pd.DataFrame,
    expected_bars_per_day: int,
) -> pd.DataFrame:
    if df.empty:
        return df
    date_counts = df.groupby(df.index.date).size()
    complete_dates = {d for d, count in date_counts.items() if int(count) == int(expected_bars_per_day)}
    if not complete_dates:
        return df.iloc[0:0]
    mask = [ts.date() in complete_dates for ts in df.index]
    return df.loc[mask]


def get_next_daily_fetch_start(
    conn: sqlite3.Connection,
    daily_table: str,
    symbol: str,
) -> Optional[str]:
    row = conn.execute(
        """
        SELECT MAX(timestamp)
        FROM {table}
        WHERE symbol = ?
        """.format(table=daily_table),
        (symbol,),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    next_day = pd.Timestamp(row[0]) + pd.Timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")


def rebuild_basic_1min_for_symbol(
    conn: sqlite3.Connection,
    symbol: str,
    five_min_table: str,
    one_min_table: str,
    rng: np.random.Generator,
) -> int:
    df_5 = load_5min_from_db(conn, five_min_table, symbol)
    if df_5.empty:
        return 0
    full_df_5 = df_5
    df_5 = filter_complete_intraday_days(df_5, EXPECTED_5M_BARS_PER_DAY)

    df_1 = generate_1min_data_from_5min(df_5, rng)
    df_1["symbol"] = symbol
    rows = list(df_1[["symbol", "timestamp", "open", "high", "low", "close", "volume"]].itertuples(index=False, name=None))

    delete_symbol_rows(
        conn=conn,
        table=one_min_table,
        symbol=symbol,
        from_date=full_df_5.index.min().date().isoformat(),
        to_date=full_df_5.index.max().date().isoformat(),
    )
    return upsert_rows(conn, one_min_table, rows)


def normalize_group(open_price: float, high_price: float, low_price: float, close_price: float) -> List[float]:
    price_range = high_price - low_price
    if price_range == 0:
        price_range = 1e-5
    return [
        (close_price - open_price) / price_range,
        (high_price - open_price) / price_range,
        (open_price - low_price) / price_range,
    ]


def _build_daily_range_clause(from_date: Optional[str], to_date: Optional[str]) -> Tuple[str, List[str]]:
    clauses = []
    params: List[str] = []
    if from_date:
        clauses.append("DATE(timestamp) >= ?")
        params.append(from_date)
    if to_date:
        clauses.append("DATE(timestamp) <= ?")
        params.append(to_date)
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def refresh_normalized_daily_data(
    conn: sqlite3.Connection,
    daily_table: str,
    normalized_table: str,
    from_date: Optional[str],
    to_date: Optional[str],
) -> int:
    where_sql, params = _build_daily_range_clause(from_date, to_date)
    rows = conn.execute(
        """
        SELECT symbol, DATE(timestamp) AS trade_date, open, high, low, close
        FROM {daily_table}
        WHERE open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
        {where_sql}
        ORDER BY symbol, trade_date
        """.format(daily_table=daily_table, where_sql=where_sql),
        params,
    ).fetchall()

    if from_date is None and to_date is None:
        conn.execute("DELETE FROM {table}".format(table=normalized_table))
        conn.commit()

    normalized_rows = []
    for symbol, trade_date, open_price, high_price, low_price, close_price in rows:
        close_open_ratio, high_open_ratio, low_open_ratio = normalize_group(
            float(open_price), float(high_price), float(low_price), float(close_price)
        )
        normalized_rows.append(
            (
                symbol,
                "{} 00:00:00".format(trade_date),
                float(close_open_ratio),
                float(high_open_ratio),
                float(low_open_ratio),
            )
        )

    if not normalized_rows:
        return 0

    conn.executemany(
        """
        INSERT OR REPLACE INTO {table}
        (symbol, timestamp, close_open_ratio, high_open_ratio, low_open_ratio)
        VALUES (?, ?, ?, ?, ?)
        """.format(table=normalized_table),
        normalized_rows,
    )
    conn.commit()
    return len(normalized_rows)


def refresh_template_normalized_daily_data(
    conn: sqlite3.Connection,
    normalized_table: str,
    source_min_table: str,
    template_normalized_table: str,
    from_date: Optional[str],
    to_date: Optional[str],
) -> int:
    ensure_normalized_table(conn, template_normalized_table)

    conn.execute("DROP TABLE IF EXISTS temp.minute_template_dates")
    where_sql, params = _build_minute_range_clause(from_date, to_date)
    conn.execute(
        """
        CREATE TEMP TABLE minute_template_dates AS
        SELECT symbol, DATE(timestamp) AS trade_date
        FROM {source_min_table}
        WHERE 1 = 1
        {where_sql}
        GROUP BY symbol, DATE(timestamp)
        HAVING COUNT(*) = ?
        """.format(source_min_table=source_min_table, where_sql=where_sql),
        params + [EXPECTED_1M_BARS_PER_DAY],
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_temp_minute_template_dates
        ON minute_template_dates (symbol, trade_date)
        """
    )

    if from_date is None and to_date is None:
        conn.execute("DELETE FROM {table}".format(table=template_normalized_table))
    else:
        conn.execute(
            """
            DELETE FROM {table}
            WHERE DATE(timestamp) >= COALESCE(?, DATE(timestamp))
              AND DATE(timestamp) <= COALESCE(?, DATE(timestamp))
            """.format(table=template_normalized_table),
            (from_date, to_date),
        )

    inserted = conn.execute(
        """
        INSERT OR REPLACE INTO {template_table}
        (symbol, timestamp, close_open_ratio, high_open_ratio, low_open_ratio)
        SELECT n.symbol, n.timestamp, n.close_open_ratio, n.high_open_ratio, n.low_open_ratio
        FROM {normalized_table} n
        JOIN minute_template_dates t
          ON t.symbol = n.symbol
         AND t.trade_date = DATE(n.timestamp)
        """.format(
            template_table=template_normalized_table,
            normalized_table=normalized_table,
        )
    ).rowcount
    conn.commit()
    conn.execute("DROP TABLE IF EXISTS temp.minute_template_dates")
    conn.commit()
    return int(inserted if inserted is not None and inserted >= 0 else 0)


def find_random_similar_normalized(
    conn: sqlite3.Connection,
    normalized_table: str,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    symbol: Optional[str],
    trade_date: Optional[str],
    tolerance: float,
    rng: random.Random,
    sample_limit: int = 64,
) -> Optional[Tuple[str, str]]:
    close_open_ratio, high_open_ratio, low_open_ratio = normalize_group(
        float(open_price), float(high_price), float(low_price), float(close_price)
    )
    query = """
        SELECT n.symbol, n.timestamp
        FROM {table} n
        WHERE close_open_ratio BETWEEN ? AND ?
          AND high_open_ratio BETWEEN ? AND ?
          AND low_open_ratio BETWEEN ? AND ?
    """.format(table=normalized_table)
    params: List[object] = [
        close_open_ratio - tolerance,
        close_open_ratio + tolerance,
        high_open_ratio - tolerance,
        high_open_ratio + tolerance,
        low_open_ratio - tolerance,
        low_open_ratio + tolerance,
    ]
    if symbol and trade_date:
        query += " AND (n.symbol != ? OR DATE(n.timestamp) != ?)"
        params.extend([symbol, trade_date])
    query += " LIMIT ?"
    params.append(int(sample_limit))
    rows = conn.execute(query, params).fetchall()
    if not rows:
        return None
    row = rng.choice(rows)
    return row[0], row[1]


def find_random_similar_adaptive(
    conn: sqlite3.Connection,
    normalized_table: str,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    symbol: Optional[str],
    trade_date: Optional[str],
    initial_tolerance: float,
    max_tolerance: float,
    step: float,
    rng: random.Random,
) -> Optional[Tuple[str, str]]:
    current_tol = float(initial_tolerance)
    max_tol = float(max_tolerance)
    step_val = float(step)
    while current_tol <= max_tol + 1e-12:
        result = find_random_similar_normalized(
            conn=conn,
            normalized_table=normalized_table,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            symbol=symbol,
            trade_date=trade_date,
            tolerance=current_tol,
            rng=rng,
        )
        if result is not None:
            return result
        current_tol += step_val
    return None


def sort_indices(high_indices: List[int], low_indices: List[int], last_index: int) -> List[int]:
    index_set = {0}
    index_set.update(high_indices)
    index_set.update(low_indices)
    index_set.add(last_index)
    return sorted(index_set)


def get_random_factor(rng: random.Random, start: float, end: float) -> float:
    if rng.random() < 0.5:
        return float(start)
    return float(rng.uniform(start, end))


def adjust_price_list(
    prices: List[float],
    start_idx: int,
    end_idx: int,
    prev_end_val: float,
    end_target_val: float,
    high_price: float,
    low_price: float,
    rng: random.Random,
) -> Tuple[List[float], float]:
    if start_idx > end_idx:
        raise ValueError("start_idx must be <= end_idx")
    if not prices:
        return prices, 0

    original_end = prices[end_idx]
    if start_idx == end_idx:
        return prices, original_end
    if end_idx - start_idx == 1:
        prices[end_idx] = end_target_val
        return prices, original_end
    if abs(prices[end_idx] - end_target_val) < 1e-4:
        prices[end_idx] = end_target_val
        return prices, original_end

    start_val = prices[start_idx]
    start_diff = start_val - prev_end_val
    end_diff = end_target_val - prices[end_idx]
    prices[end_idx] = end_target_val

    if abs(start_val - end_target_val) < 1e-4:
        for idx in range(start_idx + 1, end_idx):
            prices[idx] += end_diff
            if prices[idx] > high_price:
                prices[idx] = get_random_factor(
                    rng, high_price, high_price - (high_price - low_price) * 0.1
                )
            if prices[idx] < low_price:
                prices[idx] = get_random_factor(
                    rng, low_price, low_price + (high_price - low_price) * 0.1
                )
    else:
        mid_point = (start_idx + end_idx) / 2
        for idx in range(start_idx + 1, end_idx):
            if idx < mid_point:
                factor = start_diff * abs(idx - start_idx - mid_point) / mid_point
                prices[idx] += factor
            elif idx > mid_point:
                factor = end_diff * abs(idx - start_idx - mid_point) / mid_point
                prices[idx] += factor

            if prices[idx] > high_price:
                prices[idx] = get_random_factor(
                    rng, high_price, high_price - (high_price - low_price) * 0.1
                )
            if prices[idx] < low_price:
                prices[idx] = get_random_factor(
                    rng, low_price, low_price + (high_price - low_price) * 0.1
                )
    return prices, original_end


def stretch_pattern(
    conn: sqlite3.Connection,
    original_ohlc: Tuple[float, float, float, float],
    match_symbol: str,
    match_timestamp: str,
    source_table: str,
    daily_table: str,
    rng: random.Random,
) -> Optional[List[Tuple[str, float]]]:
    original_open, original_high, original_low, original_close = original_ohlc

    minute_rows = conn.execute(
        """
        SELECT TIME(timestamp) AS minute_time, close
        FROM {source_table}
        WHERE symbol = ?
          AND DATE(timestamp) = DATE(?)
        ORDER BY timestamp
        """.format(source_table=source_table),
        (match_symbol, match_timestamp),
    ).fetchall()
    if not minute_rows:
        return None
    if len(minute_rows) != EXPECTED_1M_BARS_PER_DAY:
        return None

    daily_row = conn.execute(
        """
        SELECT open
        FROM {daily_table}
        WHERE symbol = ?
          AND DATE(timestamp) = DATE(?)
        LIMIT 1
        """.format(daily_table=daily_table),
        (match_symbol, match_timestamp),
    ).fetchone()
    if daily_row is None:
        return None

    match_open = float(daily_row[0])
    if match_open == 0:
        return None

    base_factor = float(original_open) / match_open
    adjusted_closes = [float(row[1]) * base_factor for row in minute_rows]
    if not adjusted_closes:
        return None

    new_high = max(adjusted_closes)
    new_low = min(adjusted_closes)
    if new_high == new_low:
        return [(row[0], round(price, 2)) for row, price in zip(minute_rows, adjusted_closes)]

    high_indices = [idx for idx, value in enumerate(adjusted_closes) if value == new_high]
    low_indices = [idx for idx, value in enumerate(adjusted_closes) if value == new_low]
    index_list = sort_indices(high_indices, low_indices, len(adjusted_closes) - 1)

    prev_end_val = adjusted_closes[0]
    for idx in range(len(index_list) - 1):
        start_idx, end_idx = index_list[idx], index_list[idx + 1]
        if abs(adjusted_closes[end_idx] - new_high) < 1e-4:
            end_target_val = float(original_high)
        elif abs(adjusted_closes[end_idx] - new_low) < 1e-4:
            end_target_val = float(original_low)
        elif end_idx == len(adjusted_closes) - 1:
            end_target_val = float(original_close)
        else:
            end_target_val = adjusted_closes[0]

        adjusted_closes, prev_end_val = adjust_price_list(
            prices=adjusted_closes,
            start_idx=start_idx,
            end_idx=end_idx,
            prev_end_val=prev_end_val,
            end_target_val=end_target_val,
            high_price=float(original_high),
            low_price=float(original_low),
            rng=rng,
        )

    return [(row[0], round(price, 2)) for row, price in zip(minute_rows, adjusted_closes)]


def delete_symbol_rows(
    conn: sqlite3.Connection,
    table: str,
    symbol: str,
    from_date: Optional[str],
    to_date: Optional[str],
) -> None:
    clauses = ["symbol = ?"]
    params: List[object] = [symbol]
    if from_date:
        clauses.append("DATE(timestamp) >= ?")
        params.append(from_date)
    if to_date:
        clauses.append("DATE(timestamp) <= ?")
        params.append(to_date)
    conn.execute(
        "DELETE FROM {table} WHERE {where_sql}".format(
            table=table, where_sql=" AND ".join(clauses)
        ),
        params,
    )
    conn.commit()


def _build_minute_range_clause(from_date: Optional[str], to_date: Optional[str]) -> Tuple[str, List[str]]:
    clauses = []
    params: List[str] = []
    if from_date:
        clauses.append("DATE(timestamp) >= ?")
        params.append(from_date)
    if to_date:
        clauses.append("DATE(timestamp) <= ?")
        params.append(to_date)
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def load_minute_rows_for_symbol(
    conn: sqlite3.Connection,
    table: str,
    symbol: str,
    from_date: Optional[str],
    to_date: Optional[str],
    expected_bars_per_day: Optional[int] = None,
) -> List[Tuple[str, str, float, float, float, float, float]]:
    if expected_bars_per_day is not None:
        where_sql, params = _build_minute_range_clause(from_date, to_date)
        rows = conn.execute(
            """
            WITH complete_days AS (
                SELECT DATE(timestamp) AS trade_date
                FROM {table}
                WHERE symbol = ?
                {where_sql}
                GROUP BY DATE(timestamp)
                HAVING COUNT(*) = ?
            )
            SELECT p.symbol, p.timestamp, p.open, p.high, p.low, p.close, p.volume
            FROM {table} p
            JOIN complete_days d
              ON d.trade_date = DATE(p.timestamp)
            WHERE p.symbol = ?
            {where_sql}
            ORDER BY p.timestamp
            """.format(table=table, where_sql=where_sql),
            [symbol] + params + [int(expected_bars_per_day), symbol] + params,
        ).fetchall()
    else:
        where_sql, params = _build_minute_range_clause(from_date, to_date)
        rows = conn.execute(
            """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM {table}
            WHERE symbol = ?
            {where_sql}
            ORDER BY timestamp
            """.format(table=table, where_sql=where_sql),
            [symbol] + params,
        ).fetchall()

    normalized_rows: List[Tuple[str, str, float, float, float, float, float]] = []
    for row in rows:
        normalized_rows.append(
            (
                str(row[0]),
                str(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
                float(row[6]),
            )
        )
    return normalized_rows


def merge_runtime_1min_for_symbol(
    conn: sqlite3.Connection,
    symbol: str,
    basic_table: str,
    stretch_table: str,
    runtime_table: str,
    from_date: Optional[str],
    to_date: Optional[str],
    synthetic_table: Optional[str] = None,
) -> Tuple[int, int, int, int]:
    """Merge minute data into runtime table with priority: basic > stretch > synthetic.

    Returns (inserted_basic, inserted_stretch, inserted_synthetic, runtime_count).
    """
    delete_symbol_rows(
        conn=conn,
        table=runtime_table,
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
    )

    # Layer 1 (lowest priority): synthetic
    inserted_synthetic = 0
    if synthetic_table:
        synthetic_rows = load_minute_rows_for_symbol(
            conn=conn, table=synthetic_table, symbol=symbol,
            from_date=from_date, to_date=to_date,
            expected_bars_per_day=EXPECTED_1M_BARS_PER_DAY,
        )
        inserted_synthetic = upsert_rows(conn, runtime_table, synthetic_rows)

    # Layer 2: stretch (overwrites synthetic on overlap)
    stretch_rows = load_minute_rows_for_symbol(
        conn=conn, table=stretch_table, symbol=symbol,
        from_date=from_date, to_date=to_date,
        expected_bars_per_day=EXPECTED_1M_BARS_PER_DAY,
    )
    inserted_stretch = upsert_rows(conn, runtime_table, stretch_rows)

    # Layer 3 (highest priority): basic/mock (overwrites everything on overlap)
    basic_rows = load_minute_rows_for_symbol(
        conn=conn, table=basic_table, symbol=symbol,
        from_date=from_date, to_date=to_date,
        expected_bars_per_day=EXPECTED_1M_BARS_PER_DAY,
    )
    inserted_basic = upsert_rows(conn, runtime_table, basic_rows)

    where_sql, params = _build_minute_range_clause(from_date, to_date)
    runtime_count = conn.execute(
        """
        SELECT COUNT(1)
        FROM {table}
        WHERE symbol = ?
        {where_sql}
        """.format(table=runtime_table, where_sql=where_sql),
        [symbol] + params,
    ).fetchone()[0]

    return inserted_basic, inserted_stretch, inserted_synthetic, int(runtime_count)


def import_legacy_price_data(
    target_conn: sqlite3.Connection,
    legacy_db_path: str,
    symbols: List[str],
    source_table: str,
    target_table: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> int:
    """Read-only import from a legacy DB price table into target_table.

    The legacy DB is opened in read-only mode (uri) so it is never modified.
    Only missing timestamps are imported into the target table.
    """
    legacy_uri = "file:{}?mode=ro".format(os.path.abspath(legacy_db_path))
    legacy_conn = sqlite3.connect(legacy_uri, uri=True)
    try:
        total_imported = 0
        for symbol in symbols:
            where_parts = ["symbol = ?"]
            params: List[object] = [symbol]
            if from_date:
                where_parts.append("DATE(timestamp) >= ?")
                params.append(from_date)
            if to_date:
                where_parts.append("DATE(timestamp) <= ?")
                params.append(to_date)
            rows = legacy_conn.execute(
                "SELECT symbol, timestamp, open, high, low, close, volume "
                "FROM {source_table} WHERE {where_clause} ORDER BY timestamp".format(
                    source_table=source_table,
                    where_clause=" AND ".join(where_parts),
                ),
                params,
            ).fetchall()
            if not rows:
                continue
            target_where_parts = ["symbol = ?"]
            target_params: List[object] = [symbol]
            if from_date:
                target_where_parts.append("DATE(timestamp) >= ?")
                target_params.append(from_date)
            if to_date:
                target_where_parts.append("DATE(timestamp) <= ?")
                target_params.append(to_date)
            existing_ts = set(
                r[0]
                for r in target_conn.execute(
                    "SELECT timestamp FROM {target_table} WHERE {where_clause}".format(
                        target_table=target_table,
                        where_clause=" AND ".join(target_where_parts),
                    ),
                    target_params,
                ).fetchall()
            )
            new_rows = [r for r in rows if r[1] not in existing_ts]
            if new_rows:
                inserted = upsert_rows(target_conn, target_table, new_rows)
                total_imported += inserted
                print(
                    "  [{}] imported {} legacy rows from {} -> {}".format(
                        symbol, inserted, source_table, target_table
                    )
                )
        return total_imported
    finally:
        legacy_conn.close()


def _to_iso_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if len(text) >= 10:
        return text[:10]
    return text


MINUTE_COUNT = 240

# A-share intraday minute timestamps: 09:31~11:30, 13:01~15:00
_MINUTE_TIMES: Optional[List[str]] = None


def get_minute_times() -> List[str]:
    global _MINUTE_TIMES
    if _MINUTE_TIMES is not None:
        return _MINUTE_TIMES
    times: List[str] = []
    for h in range(9, 12):
        start_m = 31 if h == 9 else 0
        end_m = 31 if h == 11 else 60
        for m in range(start_m, end_m):
            times.append("{:02d}:{:02d}:00".format(h, m))
    for h in range(13, 16):
        start_m = 1 if h == 13 else 0
        end_m = 1 if h == 15 else 60
        for m in range(start_m, end_m):
            times.append("{:02d}:{:02d}:00".format(h, m))
    _MINUTE_TIMES = times
    return times


def distribute_volume_u_shape(daily_volume: float, n: int = MINUTE_COUNT) -> List[float]:
    """Distribute daily volume across *n* minute bars with a U-shape curve.

    Higher volume at open/close, lower at midday.  Returns a list of length *n*
    whose sum equals *daily_volume*.
    """
    if daily_volume <= 0 or n <= 0:
        return [0.0] * n
    mid = (n - 1) / 2.0
    raw = [1.0 + 2.0 * ((i - mid) / mid) ** 2 for i in range(n)]
    total = sum(raw)
    return [daily_volume * w / total for w in raw]


def load_target_daily_rows(
    conn: sqlite3.Connection,
    symbol: str,
    daily_table: str,
    from_date: Optional[str],
    to_date: Optional[str],
) -> List[Tuple[str, float, float, float, float, float]]:
    where_sql, params = _build_daily_range_clause(from_date, to_date)
    rows = conn.execute(
        """
        SELECT DATE(timestamp) AS trade_date, open, high, low, close, volume
        FROM {daily_table}
        WHERE symbol = ?
          AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
          {where_sql}
        ORDER BY trade_date
        """.format(daily_table=daily_table, where_sql=where_sql),
        [symbol] + params,
    ).fetchall()

    unique_rows: List[Tuple[str, float, float, float, float, float]] = []
    seen_dates = set()
    for row in rows:
        trade_date = row[0]
        date_text = _to_iso_date(trade_date)
        if date_text in seen_dates:
            continue
        seen_dates.add(date_text)
        vol = float(row[5]) if row[5] is not None else 0.0
        unique_rows.append(
            (
                date_text,
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                vol,
            )
        )
    return unique_rows


def rebuild_stretch_1min_for_symbol(
    conn: sqlite3.Connection,
    symbol: str,
    source_min_table: str,
    output_table: str,
    daily_table: str,
    normalized_table: str,
    from_date: Optional[str],
    to_date: Optional[str],
    initial_tolerance: float,
    max_tolerance: float,
    tolerance_step: float,
    rng: random.Random,
) -> Tuple[int, int, int, int]:
    target_daily_rows = load_target_daily_rows(
        conn=conn,
        symbol=symbol,
        daily_table=daily_table,
        from_date=from_date,
        to_date=to_date,
    )
    if not target_daily_rows:
        return 0, 0, 0, 0

    delete_symbol_rows(
        conn=conn,
        table=output_table,
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
    )

    result_rows = []
    matched_days = 0
    generated_days = 0
    total_days = len(target_daily_rows)
    for idx, (trade_date, open_price, high_price, low_price, close_price, daily_vol) in enumerate(target_daily_rows, start=1):
        match = find_random_similar_adaptive(
            conn=conn,
            normalized_table=normalized_table,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            symbol=symbol,
            trade_date=trade_date,
            initial_tolerance=initial_tolerance,
            max_tolerance=max_tolerance,
            step=tolerance_step,
            rng=rng,
        )
        if match is None:
            if idx % 50 == 0 or idx == total_days:
                print(
                    "  stretch progress: {}/{} matched={} generated={}".format(
                        idx, total_days, matched_days, generated_days
                    )
                )
            continue
        matched_days += 1
        match_symbol, match_timestamp = match

        stretched = stretch_pattern(
            conn=conn,
            original_ohlc=(open_price, high_price, low_price, close_price),
            match_symbol=match_symbol,
            match_timestamp=match_timestamp,
            source_table=source_min_table,
            daily_table=daily_table,
            rng=rng,
        )
        if not stretched:
            if idx % 50 == 0 or idx == total_days:
                print(
                    "  stretch progress: {}/{} matched={} generated={}".format(
                        idx, total_days, matched_days, generated_days
                    )
                )
            continue
        generated_days += 1

        volumes = distribute_volume_u_shape(daily_vol, len(stretched))
        for idx, (minute_time, price) in enumerate(stretched):
            timestamp = "{} {}".format(trade_date, minute_time)
            result_rows.append(
                (symbol, timestamp, price, price, price, price, volumes[idx])
            )
        if idx % 50 == 0 or idx == total_days:
            print(
                "  stretch progress: {}/{} matched={} generated={}".format(
                    idx, total_days, matched_days, generated_days
                )
            )

    inserted = upsert_rows(conn, output_table, result_rows)
    return inserted, len(target_daily_rows), matched_days, generated_days


def synthesize_minutes_from_daily(
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    daily_volume: float,
    rng: random.Random,
) -> List[Tuple[str, float, float, float, float, float]]:
    """Generate 240 synthetic minute bars from a single daily OHLCV bar.

    Returns list of (time_str, open, high, low, close, volume).

    Algorithm:
      1. Decide whether high or low comes first based on candle direction + randomness.
      2. Place high_pos and low_pos along the 240-minute timeline.
      3. Build a price path: open -> first_extreme -> second_extreme -> close
         with linear interpolation + bounded noise.
      4. Derive per-minute OHLC from the path.
      5. Distribute volume with U-shape.
    """
    n = MINUTE_COUNT
    times = get_minute_times()
    if len(times) != n:
        raise ValueError("expected {} minute times, got {}".format(n, len(times)))

    # Degenerate: no range
    price_range = high_price - low_price
    if price_range < 1e-6:
        vols = distribute_volume_u_shape(daily_volume, n)
        return [
            (times[i], open_price, open_price, open_price, open_price, vols[i])
            for i in range(n)
        ]

    # Decide order: high-first or low-first
    if close_price > open_price:
        # Up day: 70% chance low comes first
        high_first = rng.random() < 0.3
    elif close_price < open_price:
        # Down day: 70% chance high comes first
        high_first = rng.random() < 0.7
    else:
        high_first = rng.random() < 0.5

    # Place extreme positions (avoid minute 0 and last minute for more realistic shape)
    morning_end = 120  # index of last morning minute + 1
    if high_first:
        high_pos = rng.randint(1, morning_end - 1)
        low_pos = rng.randint(high_pos + 1, n - 2) if high_pos < n - 2 else n - 2
    else:
        low_pos = rng.randint(1, morning_end - 1)
        high_pos = rng.randint(low_pos + 1, n - 2) if low_pos < n - 2 else n - 2

    # Build waypoints: (index, price)
    waypoints = [(0, open_price)]
    if high_first:
        waypoints.append((high_pos, high_price))
        waypoints.append((low_pos, low_price))
    else:
        waypoints.append((low_pos, low_price))
        waypoints.append((high_pos, high_price))
    waypoints.append((n - 1, close_price))

    # Linear interpolation between waypoints + noise
    path = [0.0] * n
    for seg in range(len(waypoints) - 1):
        i0, p0 = waypoints[seg]
        i1, p1 = waypoints[seg + 1]
        for i in range(i0, i1 + 1):
            if i1 == i0:
                path[i] = p0
            else:
                t = (i - i0) / (i1 - i0)
                path[i] = p0 + t * (p1 - p0)

    # Add noise (bounded to [low, high]), skip waypoint indices
    waypoint_indices = {wp[0] for wp in waypoints}
    noise_scale = price_range * 0.08
    for i in range(n):
        if i not in waypoint_indices:
            noise = rng.gauss(0, noise_scale)
            path[i] = max(low_price, min(high_price, path[i] + noise))

    # Ensure exact OHLC constraints
    path[0] = open_price
    path[n - 1] = close_price
    path[high_pos] = high_price
    path[low_pos] = low_price

    # Round to 2 decimals
    path = [round(p, 2) for p in path]

    # Build per-minute OHLC: use path[i] as close, derive O/H/L from adjacent
    vols = distribute_volume_u_shape(daily_volume, n)
    result: List[Tuple[str, float, float, float, float, float]] = []
    for i in range(n):
        c = path[i]
        o = path[i - 1] if i > 0 else open_price
        h = round(max(o, c) + abs(rng.gauss(0, price_range * 0.005)), 2)
        l = round(min(o, c) - abs(rng.gauss(0, price_range * 0.005)), 2)
        # Clamp to daily range
        h = min(h, high_price)
        l = max(l, low_price)
        # Ensure h >= max(o,c) and l <= min(o,c)
        h = max(h, o, c)
        l = min(l, o, c)
        result.append((times[i], round(o, 2), round(h, 2), round(l, 2), round(c, 2), vols[i]))
    return result


def rebuild_synthetic_1min_for_symbol(
    conn: sqlite3.Connection,
    symbol: str,
    output_table: str,
    daily_table: str,
    from_date: Optional[str],
    to_date: Optional[str],
    rng: random.Random,
    skip_dates: Optional[set] = None,
) -> Tuple[int, int, int]:
    """Generate purely synthetic minute bars for dates not covered by mock/stretch.

    *skip_dates*: set of 'YYYY-MM-DD' date strings that already have minute data
    and should not be synthesized.

    Returns (inserted_rows, total_daily_days, generated_days).
    """
    target_daily_rows = load_target_daily_rows(
        conn=conn, symbol=symbol, daily_table=daily_table,
        from_date=from_date, to_date=to_date,
    )
    if not target_daily_rows:
        return 0, 0, 0

    result_rows = []
    generated_days = 0

    for trade_date, open_price, high_price, low_price, close_price, daily_vol in target_daily_rows:
        if skip_dates and trade_date in skip_dates:
            continue

        minutes = synthesize_minutes_from_daily(
            open_price, high_price, low_price, close_price, daily_vol, rng,
        )
        generated_days += 1

        for minute_time, o, h, l, c, v in minutes:
            timestamp = "{} {}".format(trade_date, minute_time)
            result_rows.append((symbol, timestamp, o, h, l, c, v))

    inserted = upsert_rows(conn, output_table, result_rows)
    return inserted, len(target_daily_rows), generated_days


def parse_symbols(symbols_arg: str) -> List[str]:
    if not symbols_arg:
        return list(DEFAULT_STOCKS_LIST)
    return [s.strip() for s in symbols_arg.split(",") if s.strip()]


def _get_covered_dates(
    conn: sqlite3.Connection,
    symbol: str,
    table: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    expected_bars_per_day: int = EXPECTED_1M_BARS_PER_DAY,
) -> set:
    """Return set of 'YYYY-MM-DD' dates that have minute data for *symbol*."""
    where_sql, params = _build_minute_range_clause(from_date, to_date)
    rows = conn.execute(
        """
        SELECT DATE(timestamp)
        FROM {table}
        WHERE symbol = ? {where}
        GROUP BY DATE(timestamp)
        HAVING COUNT(*) = ?
        """.format(
            table=table, where=where_sql
        ),
        [symbol] + params + [int(expected_bars_per_day)],
    ).fetchall()
    return {str(r[0]) for r in rows}


def main():
    parser = argparse.ArgumentParser(
        description="Build minute bars with basic, stretch, synthetic modes and legacy import for RQAlpha."
    )
    parser.add_argument(
        "--sqlite-path",
        default=DEFAULT_SQLITE_PATH,
        help="Target sqlite db path (default points to current rqalpha repo outputs).",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_STOCKS_LIST),
        help="Comma-separated stock symbols (e.g. 600519,000725).",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip akshare 5m fetch and rebuild basic 1m from existing 5m table.",
    )
    parser.add_argument(
        "--skip-daily-fetch",
        action="store_true",
        help="Skip daily data fetch. Stretch mode requires daily table rows.",
    )
    parser.add_argument(
        "--daily-start-date",
        default=None,
        help="Daily fetch start date (YYYY-MM-DD). If omitted, incremental from table max(timestamp).",
    )
    parser.add_argument(
        "--daily-end-date",
        default=None,
        help="Daily fetch end date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--skip-mock",
        action="store_true",
        help="Skip basic 5m->1m mock rebuild.",
    )
    parser.add_argument(
        "--mock-mode",
        choices=["basic", "stretch", "both", "full"],
        default="basic",
        help=(
            "Minute simulation mode: basic (5m->1m), stretch (similar-pattern), "
            "both (basic+stretch), full (basic+stretch+synthetic). "
            "Runtime merge priority: basic > stretch > synthetic."
        ),
    )
    parser.add_argument(
        "--five-min-table",
        default="stock_5_min",
        help="5m table name.",
    )
    parser.add_argument(
        "--one-min-table",
        default="stock_1_min_mock",
        help="Basic 1m output table name (5m->1m).",
    )
    parser.add_argument(
        "--stretch-source-table",
        default=None,
        help="Source 1m table used by stretch mode. Default: same as --one-min-table.",
    )
    parser.add_argument(
        "--stretch-output-table",
        default="stock_1_min_fake",
        help="Stretch mode output table name.",
    )
    parser.add_argument(
        "--synthetic-table",
        default="stock_1_min_synthetic",
        help="Synthetic 1m output table name (daily->1m).",
    )
    parser.add_argument(
        "--runtime-table",
        default="stock_1_min_runtime",
        help="Merged runtime 1m table. Priority: basic > stretch > synthetic.",
    )
    parser.add_argument(
        "--daily-table",
        default="stock_daily",
        help="Daily OHLC table used as stretch/synthetic target.",
    )
    parser.add_argument(
        "--normalized-table",
        default="normalized_daily_ohlc",
        help="Table storing normalized daily OHLC features.",
    )
    parser.add_argument(
        "--skip-normalized-refresh",
        action="store_true",
        help="Skip rebuilding normalized daily feature table before stretch mode.",
    )
    parser.add_argument(
        "--stretch-from-date",
        default=None,
        help="Stretch/synthetic target start date (YYYY-MM-DD). Default: no lower bound.",
    )
    parser.add_argument(
        "--stretch-to-date",
        default=None,
        help="Stretch/synthetic target end date (YYYY-MM-DD). Default: no upper bound.",
    )
    parser.add_argument(
        "--initial-tolerance",
        type=float,
        default=0.05,
        help="Initial normalized tolerance in stretch mode.",
    )
    parser.add_argument(
        "--max-tolerance",
        type=float,
        default=0.5,
        help="Maximum normalized tolerance in stretch mode.",
    )
    parser.add_argument(
        "--tolerance-step",
        type=float,
        default=0.05,
        help="Tolerance step when adaptive matching expands search.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed used by basic, stretch, and synthetic simulation.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Sleep seconds between akshare symbol fetches.",
    )
    parser.add_argument(
        "--legacy-db",
        default=None,
        help="Path to a legacy sqlite DB. Its 5m/mock tables will be read-only imported to expand the template pool.",
    )
    parser.add_argument(
        "--legacy-five-min-table",
        default="stock_5_min",
        help="Legacy source 5m table name.",
    )
    parser.add_argument(
        "--legacy-mock-table",
        default="stock_1_min_mock",
        help="Legacy source mock 1m table name.",
    )
    args = parser.parse_args()

    symbols = parse_symbols(args.symbols)
    rng = np.random.default_rng(args.seed)
    stretch_rng = random.Random(args.seed)
    synthetic_rng = random.Random(args.seed + 1)

    five_min_table = validate_identifier(args.five_min_table, "five_min_table")
    one_min_table = validate_identifier(args.one_min_table, "one_min_table")
    stretch_source_table = validate_identifier(
        args.stretch_source_table or args.one_min_table, "stretch_source_table"
    )
    stretch_output_table = validate_identifier(args.stretch_output_table, "stretch_output_table")
    synthetic_table = validate_identifier(args.synthetic_table, "synthetic_table")
    runtime_table = validate_identifier(args.runtime_table, "runtime_table")
    daily_table = validate_identifier(args.daily_table, "daily_table")
    normalized_table = validate_identifier(args.normalized_table, "normalized_table")
    template_normalized_table = validate_identifier(
        "{}_templates".format(normalized_table),
        "template_normalized_table",
    )
    legacy_five_min_table = validate_identifier(args.legacy_five_min_table, "legacy_five_min_table")
    legacy_mock_table = validate_identifier(args.legacy_mock_table, "legacy_mock_table")

    basic_enabled = (args.mock_mode in ("basic", "both", "full")) and (not args.skip_mock)
    stretch_enabled = args.mock_mode in ("stretch", "both", "full")
    synthetic_enabled = args.mock_mode == "full"
    merge_enabled = args.mock_mode in ("both", "full")

    sqlite_path = os.path.abspath(args.sqlite_path)
    sqlite_dir = os.path.dirname(sqlite_path)
    if sqlite_dir:
        os.makedirs(sqlite_dir, exist_ok=True)

    with sqlite3.connect(sqlite_path) as conn:
        ensure_tables(
            conn=conn,
            five_min_table=five_min_table,
            one_min_table=one_min_table,
            stretch_source_table=stretch_source_table,
            stretch_output_table=stretch_output_table,
            runtime_table=runtime_table,
            daily_table=daily_table,
            normalized_table=normalized_table,
            synthetic_table=synthetic_table if synthetic_enabled else None,
        )

        total_5min = 0
        total_daily = 0
        total_1min = 0
        total_stretch = 0
        total_synthetic = 0
        total_daily_days = 0
        total_matched_days = 0
        total_generated_days = 0
        total_synthetic_days = 0
        total_runtime_basic_loaded = 0
        total_runtime_stretch_loaded = 0
        total_runtime_synthetic_loaded = 0
        total_runtime_final_rows = 0
        total_legacy_5min_imported = 0
        total_legacy_mock_imported = 0

        # --- Legacy import ---
        if args.legacy_db:
            legacy_path = os.path.abspath(args.legacy_db)
            if not os.path.isfile(legacy_path):
                print("WARNING: legacy db not found: {}".format(legacy_path))
            else:
                print("importing 5m/mock data from legacy db: {} ...".format(legacy_path))
                total_legacy_5min_imported = import_legacy_price_data(
                    target_conn=conn,
                    legacy_db_path=legacy_path,
                    symbols=symbols,
                    source_table=legacy_five_min_table,
                    target_table=five_min_table,
                    from_date=args.stretch_from_date,
                    to_date=args.stretch_to_date,
                )
                total_legacy_mock_imported = import_legacy_price_data(
                    target_conn=conn,
                    legacy_db_path=legacy_path,
                    symbols=symbols,
                    source_table=legacy_mock_table,
                    target_table=one_min_table,
                    from_date=args.stretch_from_date,
                    to_date=args.stretch_to_date,
                )
                print("  total legacy 5m rows imported: {}".format(total_legacy_5min_imported))
                print("  total legacy mock rows imported: {}".format(total_legacy_mock_imported))

        # --- Daily fetch for stretch/synthetic ---
        if (stretch_enabled or synthetic_enabled) and (not args.skip_daily_fetch):
            print("preparing daily table ...")
            for symbol in symbols:
                try:
                    start_date = args.daily_start_date or get_next_daily_fetch_start(
                        conn=conn, daily_table=daily_table, symbol=symbol
                    )
                    df_daily = fetch_daily_from_akshare(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=args.daily_end_date,
                    )
                    rows_daily = list(
                        df_daily[
                            ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
                        ].itertuples(index=False, name=None)
                    )
                    inserted_daily = upsert_rows(conn, daily_table, rows_daily)
                    total_daily += inserted_daily
                    print("  [{}] fetched/upserted daily rows: {}".format(symbol, inserted_daily))
                    if inserted_daily > 0:
                        time_module.sleep(args.sleep_seconds)
                except Exception as exc:
                    print("  [{}] daily fetch failed: {}".format(symbol, exc))

        if stretch_enabled:
            daily_count = conn.execute(
                "SELECT COUNT(1) FROM {table}".format(table=daily_table)
            ).fetchone()[0]
            if daily_count <= 0:
                raise RuntimeError(
                    "daily table '{}' has no rows, stretch mode cannot run".format(daily_table)
                )

            if not args.skip_normalized_refresh:
                normalized_rows = refresh_normalized_daily_data(
                    conn=conn,
                    daily_table=daily_table,
                    normalized_table=normalized_table,
                    from_date=args.stretch_from_date,
                    to_date=args.stretch_to_date,
                )
                print(
                    "normalized table refreshed: {} rows -> {}".format(
                        normalized_rows, normalized_table
                    )
                )
            template_normalized_rows = refresh_template_normalized_daily_data(
                conn=conn,
                normalized_table=normalized_table,
                source_min_table=stretch_source_table,
                template_normalized_table=template_normalized_table,
                from_date=args.stretch_from_date,
                to_date=args.stretch_to_date,
            )
            print(
                "template normalized table refreshed: {} rows -> {}".format(
                    template_normalized_rows, template_normalized_table
                )
            )

        for symbol in symbols:
            print("\n[{}]".format(symbol))

            if not args.skip_fetch:
                try:
                    df_5 = fetch_5min_from_akshare(symbol)
                    rows_5 = list(
                        df_5[["symbol", "timestamp", "open", "high", "low", "close", "volume"]].itertuples(
                            index=False, name=None
                        )
                    )
                    inserted = upsert_rows(conn, five_min_table, rows_5)
                    total_5min += inserted
                    print("  fetched/upserted 5m rows:", inserted)
                    time_module.sleep(args.sleep_seconds)
                except Exception as exc:
                    print("  fetch failed:", exc)
                    if args.mock_mode == "basic":
                        continue

            if basic_enabled:
                try:
                    inserted_1 = rebuild_basic_1min_for_symbol(
                        conn=conn,
                        symbol=symbol,
                        five_min_table=five_min_table,
                        one_min_table=one_min_table,
                        rng=rng,
                    )
                    total_1min += inserted_1
                    print("  rebuilt 1m mock rows:", inserted_1)
                except Exception as exc:
                    print("  rebuild mock failed:", exc)

            if stretch_enabled:
                try:
                    inserted_1m, day_count, matched_days, generated_days = rebuild_stretch_1min_for_symbol(
                        conn=conn,
                        symbol=symbol,
                        source_min_table=stretch_source_table,
                        output_table=stretch_output_table,
                        daily_table=daily_table,
                        normalized_table=template_normalized_table,
                        from_date=args.stretch_from_date,
                        to_date=args.stretch_to_date,
                        initial_tolerance=args.initial_tolerance,
                        max_tolerance=args.max_tolerance,
                        tolerance_step=args.tolerance_step,
                        rng=stretch_rng,
                    )
                    total_stretch += inserted_1m
                    total_daily_days += day_count
                    total_matched_days += matched_days
                    total_generated_days += generated_days
                    print(
                        "  rebuilt 1m stretch rows: {} (daily={}, matched={}, generated={})".format(
                            inserted_1m, day_count, matched_days, generated_days
                        )
                    )
                except Exception as exc:
                    print("  rebuild stretch failed:", exc)

            if synthetic_enabled:
                try:
                    # Collect dates already covered by mock + stretch
                    covered = _get_covered_dates(
                        conn, symbol, one_min_table,
                        args.stretch_from_date, args.stretch_to_date,
                    )
                    covered |= _get_covered_dates(
                        conn, symbol, stretch_output_table,
                        args.stretch_from_date, args.stretch_to_date,
                    )
                    inserted_syn, syn_daily, syn_gen = rebuild_synthetic_1min_for_symbol(
                        conn=conn,
                        symbol=symbol,
                        output_table=synthetic_table,
                        daily_table=daily_table,
                        from_date=args.stretch_from_date,
                        to_date=args.stretch_to_date,
                        rng=synthetic_rng,
                        skip_dates=covered,
                    )
                    total_synthetic += inserted_syn
                    total_synthetic_days += syn_gen
                    print(
                        "  rebuilt 1m synthetic rows: {} (daily={}, generated={}, skipped={})".format(
                            inserted_syn, syn_daily, syn_gen, syn_daily - syn_gen
                        )
                    )
                except Exception as exc:
                    print("  rebuild synthetic failed:", exc)

            if merge_enabled:
                try:
                    rt_basic, rt_stretch, rt_synthetic, rt_final = merge_runtime_1min_for_symbol(
                        conn=conn,
                        symbol=symbol,
                        basic_table=one_min_table,
                        stretch_table=stretch_output_table,
                        runtime_table=runtime_table,
                        from_date=args.stretch_from_date,
                        to_date=args.stretch_to_date,
                        synthetic_table=synthetic_table if synthetic_enabled else None,
                    )
                    total_runtime_basic_loaded += rt_basic
                    total_runtime_stretch_loaded += rt_stretch
                    total_runtime_synthetic_loaded += rt_synthetic
                    total_runtime_final_rows += rt_final
                    print(
                        "  merged runtime (mock>stretch>synthetic): "
                        "mock={}, stretch={}, synthetic={}, final={}".format(
                            rt_basic, rt_stretch, rt_synthetic, rt_final
                        )
                    )
                except Exception as exc:
                    print("  merge runtime failed:", exc)

        print("\nDone.")
        print("  sqlite_path =", sqlite_path)
        if total_legacy_5min_imported:
            print("  total legacy 5m rows imported =", total_legacy_5min_imported)
        if total_legacy_mock_imported:
            print("  total legacy mock rows imported =", total_legacy_mock_imported)
        print("  total 5m rows upserted =", total_5min)
        print("  total daily rows upserted =", total_daily)
        print("  total 1m mock rows rebuilt =", total_1min)
        print("  total 1m stretch rows rebuilt =", total_stretch)
        print(
            "  stretch stats daily/matched/generated = {}/{}/{}".format(
                total_daily_days, total_matched_days, total_generated_days
            )
        )
        if synthetic_enabled:
            print("  total 1m synthetic rows rebuilt =", total_synthetic)
            print("  synthetic days generated =", total_synthetic_days)
        print("  basic table =", one_min_table)
        print("  stretch source table =", stretch_source_table)
        print("  stretch output table =", stretch_output_table)
        if synthetic_enabled:
            print("  synthetic table =", synthetic_table)
        if merge_enabled:
            print(
                "  runtime merge loaded mock/stretch/synthetic = {}/{}/{}".format(
                    total_runtime_basic_loaded, total_runtime_stretch_loaded,
                    total_runtime_synthetic_loaded,
                )
            )
            print(
                "  runtime final rows = {}".format(total_runtime_final_rows)
            )
            print("  runtime table =", runtime_table)


if __name__ == "__main__":
    main()
