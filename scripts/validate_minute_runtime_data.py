from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from typing import Dict, List, Sequence, Set, Tuple


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DEFAULT_SQLITE_PATH = os.path.join(PROJECT_ROOT, "outputs", "minute_data", "stock_data.db")
DEFAULT_DAILY_TABLE = "stock_daily"
DEFAULT_RUNTIME_TABLE = "stock_1_min_runtime"
DEFAULT_EXPECTED_BARS_PER_DAY = 240
DEFAULT_SAMPLE_LIMIT = 10
DEFAULT_MIN_TOTAL_VOLUME = 1.0
DEFAULT_MIN_NONZERO_BARS = 24
DEFAULT_MAX_SINGLE_BAR_VOLUME_SHARE = 0.5


def load_default_universe() -> List[str]:
    from strategies.cursor_rate_layered.params import UNIVERSE

    return list(UNIVERSE)


def parse_symbols(symbols_arg: str | None) -> List[str]:
    if not symbols_arg:
        return load_default_universe()
    return [s.strip() for s in symbols_arg.split(",") if s.strip()]


def normalize_db_symbol(symbol: str) -> str:
    return symbol.split(".")[0]


def fetch_daily_dates(
    conn: sqlite3.Connection,
    table: str,
    symbol: str,
    from_date: str,
    to_date: str,
) -> Set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT DATE(timestamp)
        FROM {table}
        WHERE symbol = ?
          AND DATE(timestamp) BETWEEN ? AND ?
        ORDER BY DATE(timestamp)
        """.format(table=table),
        (symbol, from_date, to_date),
    ).fetchall()
    return {str(row[0]) for row in rows}


def fetch_runtime_day_counts(
    conn: sqlite3.Connection,
    table: str,
    symbol: str,
    from_date: str,
    to_date: str,
) -> Dict[str, int]:
    rows = conn.execute(
        """
        SELECT DATE(timestamp), COUNT(*)
        FROM {table}
        WHERE symbol = ?
          AND DATE(timestamp) BETWEEN ? AND ?
        GROUP BY DATE(timestamp)
        ORDER BY DATE(timestamp)
        """.format(table=table),
        (symbol, from_date, to_date),
    ).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def fetch_runtime_day_volume_metrics(
    conn: sqlite3.Connection,
    table: str,
    symbol: str,
    from_date: str,
    to_date: str,
) -> Dict[str, Dict[str, float]]:
    rows = conn.execute(
        """
        SELECT
            DATE(timestamp),
            COUNT(*),
            COALESCE(SUM(volume), 0),
            COALESCE(SUM(CASE WHEN volume > 0 THEN 1 ELSE 0 END), 0),
            COALESCE(MAX(volume), 0)
        FROM {table}
        WHERE symbol = ?
          AND DATE(timestamp) BETWEEN ? AND ?
        GROUP BY DATE(timestamp)
        ORDER BY DATE(timestamp)
        """.format(table=table),
        (symbol, from_date, to_date),
    ).fetchall()
    metrics = {}
    for trade_date, bars, total_volume, nonzero_bars, max_volume in rows:
        total_volume = float(total_volume or 0.0)
        max_volume = float(max_volume or 0.0)
        metrics[str(trade_date)] = {
            "bars": int(bars),
            "total_volume": total_volume,
            "nonzero_bars": int(nonzero_bars),
            "max_bar_volume": max_volume,
            "max_bar_share": (max_volume / total_volume) if total_volume > 0 else 0.0,
        }
    return metrics


def summarize_symbol(
    conn: sqlite3.Connection,
    daily_table: str,
    runtime_table: str,
    symbol: str,
    from_date: str,
    to_date: str,
    expected_bars_per_day: int,
    min_total_volume: float,
    min_nonzero_bars: int,
    max_single_bar_volume_share: float,
) -> Dict[str, object]:
    db_symbol = normalize_db_symbol(symbol)
    daily_dates = fetch_daily_dates(conn, daily_table, symbol, from_date, to_date)
    if not daily_dates:
        daily_dates = fetch_daily_dates(conn, daily_table, db_symbol, from_date, to_date)
    runtime_counts = fetch_runtime_day_counts(conn, runtime_table, symbol, from_date, to_date)
    if not runtime_counts:
        runtime_counts = fetch_runtime_day_counts(conn, runtime_table, db_symbol, from_date, to_date)
    runtime_volume = fetch_runtime_day_volume_metrics(conn, runtime_table, symbol, from_date, to_date)
    if not runtime_volume:
        runtime_volume = fetch_runtime_day_volume_metrics(conn, runtime_table, db_symbol, from_date, to_date)

    runtime_dates = set(runtime_counts.keys())
    missing_dates = sorted(daily_dates - runtime_dates)
    extra_dates = sorted(runtime_dates - daily_dates)
    partial_days = sorted(
        (trade_date, bars)
        for trade_date, bars in runtime_counts.items()
        if bars != expected_bars_per_day
    )
    volume_anomalies = []
    for trade_date, metrics in sorted(runtime_volume.items()):
        reasons = []
        if metrics["total_volume"] < min_total_volume:
            reasons.append("day_volume<{:.1f}".format(min_total_volume))
        if metrics["nonzero_bars"] < min_nonzero_bars:
            reasons.append("nonzero_bars<{}".format(min_nonzero_bars))
        if metrics["max_bar_share"] > max_single_bar_volume_share:
            reasons.append("max_bar_share>{:.2f}".format(max_single_bar_volume_share))
        if reasons:
            volume_anomalies.append(
                (
                    trade_date,
                    round(metrics["total_volume"], 4),
                    int(metrics["nonzero_bars"]),
                    round(metrics["max_bar_share"], 6),
                    "|".join(reasons),
                )
            )

    return {
        "symbol": symbol,
        "daily_day_count": len(daily_dates),
        "runtime_day_count": len(runtime_dates),
        "missing_dates": missing_dates,
        "extra_dates": extra_dates,
        "partial_days": partial_days,
        "volume_anomalies": volume_anomalies,
    }


def print_sample(prefix: str, values: Sequence[object], sample_limit: int) -> None:
    if not values:
        return
    sample = list(values[:sample_limit])
    suffix = "" if len(values) <= sample_limit else " ..."
    print("{}{}{}".format(prefix, sample, suffix))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate runtime minute data completeness before backtest."
    )
    parser.add_argument("--sqlite-path", default=DEFAULT_SQLITE_PATH)
    parser.add_argument("--symbols", default=",".join(load_default_universe()))
    parser.add_argument("--daily-table", default=DEFAULT_DAILY_TABLE)
    parser.add_argument("--runtime-table", default=DEFAULT_RUNTIME_TABLE)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--expected-bars-per-day", type=int, default=DEFAULT_EXPECTED_BARS_PER_DAY)
    parser.add_argument("--sample-limit", type=int, default=DEFAULT_SAMPLE_LIMIT)
    parser.add_argument("--min-total-volume", type=float, default=DEFAULT_MIN_TOTAL_VOLUME)
    parser.add_argument("--min-nonzero-bars", type=int, default=DEFAULT_MIN_NONZERO_BARS)
    parser.add_argument(
        "--max-single-bar-volume-share",
        type=float,
        default=DEFAULT_MAX_SINGLE_BAR_VOLUME_SHARE,
    )
    args = parser.parse_args()

    symbols = parse_symbols(args.symbols)
    conn = sqlite3.connect(args.sqlite_path)
    try:
        summaries = [
            summarize_symbol(
                conn=conn,
                daily_table=args.daily_table,
                runtime_table=args.runtime_table,
                symbol=symbol,
                from_date=args.from_date,
                to_date=args.to_date,
                expected_bars_per_day=args.expected_bars_per_day,
                min_total_volume=args.min_total_volume,
                min_nonzero_bars=args.min_nonzero_bars,
                max_single_bar_volume_share=args.max_single_bar_volume_share,
            )
            for symbol in symbols
        ]
    finally:
        conn.close()

    total_daily_symbol_days = sum(int(s["daily_day_count"]) for s in summaries)
    total_runtime_symbol_days = sum(int(s["runtime_day_count"]) for s in summaries)
    total_missing_symbol_days = sum(len(s["missing_dates"]) for s in summaries)
    total_extra_symbol_days = sum(len(s["extra_dates"]) for s in summaries)
    total_partial_symbol_days = sum(len(s["partial_days"]) for s in summaries)
    total_volume_anomaly_days = sum(len(s["volume_anomalies"]) for s in summaries)
    perfect_symbols = sum(
        1
        for s in summaries
        if s["daily_day_count"] == s["runtime_day_count"]
        and not s["missing_dates"]
        and not s["extra_dates"]
        and not s["partial_days"]
        and not s["volume_anomalies"]
        and s["daily_day_count"] > 0
    )

    print("minute runtime validation")
    print("  sqlite_path =", args.sqlite_path)
    print("  symbols =", len(symbols))
    print("  window = {} -> {}".format(args.from_date, args.to_date))
    print("  expected_bars_per_day =", args.expected_bars_per_day)
    print("  total_daily_symbol_days =", total_daily_symbol_days)
    print("  total_runtime_symbol_days =", total_runtime_symbol_days)
    print("  total_missing_symbol_days =", total_missing_symbol_days)
    print("  total_extra_symbol_days =", total_extra_symbol_days)
    print("  total_partial_symbol_days =", total_partial_symbol_days)
    print("  total_volume_anomaly_days =", total_volume_anomaly_days)
    print("  perfect_symbols = {}/{}".format(perfect_symbols, len(symbols)))

    failures = []
    for summary in summaries:
        issues = []
        if summary["daily_day_count"] == 0:
            issues.append("no_daily_data")
        if summary["missing_dates"]:
            issues.append("missing_days={}".format(len(summary["missing_dates"])))
        if summary["extra_dates"]:
            issues.append("extra_days={}".format(len(summary["extra_dates"])))
        if summary["partial_days"]:
            issues.append("partial_days={}".format(len(summary["partial_days"])))
        if summary["volume_anomalies"]:
            issues.append("volume_anomalies={}".format(len(summary["volume_anomalies"])))
        if issues:
            failures.append((summary, issues))

    if not failures:
        print("validation_status = PASS")
        return 0

    print("validation_status = FAIL")
    for summary, issues in failures:
        print("")
        print("[{}] {}".format(summary["symbol"], ", ".join(issues)))
        print(
            "  daily_days={}, runtime_days={}".format(
                summary["daily_day_count"], summary["runtime_day_count"]
            )
        )
        print_sample("  missing_dates=", summary["missing_dates"], args.sample_limit)
        print_sample("  extra_dates=", summary["extra_dates"], args.sample_limit)
        print_sample("  partial_days=", summary["partial_days"], args.sample_limit)
        print_sample("  volume_anomalies=", summary["volume_anomalies"], args.sample_limit)

    return 1


if __name__ == "__main__":
    sys.exit(main())
