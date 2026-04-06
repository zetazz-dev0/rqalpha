#!/usr/bin/env python3
"""
Generate an equal-weight watchlist benchmark series for an existing RQAlpha
backtest result pickle.
"""

from __future__ import annotations

import argparse
import os
import pickle
import sqlite3
import sys
from typing import Iterable, List

import numpy as np
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from strategies.cursor_rate_layered.params import UNIVERSE


DEFAULT_SQLITE_PATH = os.path.join(PROJECT_ROOT, "outputs", "minute_data", "stock_data.db")
DEFAULT_RESULT_PICKLE = os.path.join(PROJECT_ROOT, "outputs", "backtest", "cursor_layered_month.pkl")
DEFAULT_OUTPUT_CSV = os.path.join(PROJECT_ROOT, "outputs", "backtest", "cursor_layered_month_equal_weight_benchmark.csv")


def parse_symbols(symbols_arg: str | None) -> List[str]:
    if not symbols_arg:
        return list(UNIVERSE)
    return [s.strip() for s in symbols_arg.split(",") if s.strip()]


def load_strategy_nav(result_pickle_path: str) -> pd.Series:
    with open(result_pickle_path, "rb") as f:
        result = pickle.load(f)
    portfolio = result["portfolio"].copy()
    portfolio.index = pd.to_datetime(portfolio.index)
    return portfolio["unit_net_value"].astype(float)


def build_equal_weight_returns(
    conn: sqlite3.Connection,
    sqlite_table: str,
    symbols: Iterable[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.Series:
    base_symbols = [s.split(".")[0] for s in symbols]
    placeholders = ",".join(["?"] * len(base_symbols))
    sql = """
    SELECT symbol, DATE(timestamp) AS d, close
    FROM {table}
    WHERE symbol IN ({placeholders})
      AND DATE(timestamp) BETWEEN ? AND ?
    ORDER BY symbol, d
    """.format(table=sqlite_table, placeholders=placeholders)
    params = list(base_symbols) + [
        (start_date - pd.Timedelta(days=5)).strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    ]
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        raise RuntimeError("no benchmark daily data loaded from sqlite")
    df["d"] = pd.to_datetime(df["d"])
    pivot = df.pivot(index="d", columns="symbol", values="close").sort_index()
    returns = pivot.pct_change(fill_method=None)
    benchmark_returns = returns.mean(axis=1, skipna=True)
    benchmark_returns = benchmark_returns.loc[start_date:end_date]
    if benchmark_returns.empty:
        raise RuntimeError("no benchmark returns available in target window")
    return benchmark_returns


def compute_summary(strategy_nav: pd.Series, benchmark_returns: pd.Series) -> dict:
    strategy_nav = strategy_nav.sort_index()
    benchmark_returns = benchmark_returns.reindex(strategy_nav.index)
    strategy_returns = strategy_nav.pct_change()
    strategy_returns.iloc[0] = strategy_nav.iloc[0] - 1.0
    benchmark_nav = (1.0 + benchmark_returns).cumprod()

    trading_days_per_year = 252
    periods = len(strategy_nav)

    strategy_total = float(strategy_nav.iloc[-1] - 1.0)
    benchmark_total = float(benchmark_nav.iloc[-1] - 1.0)
    excess_total = float(strategy_nav.iloc[-1] / benchmark_nav.iloc[-1] - 1.0)
    strategy_annual = float((1.0 + strategy_total) ** (trading_days_per_year / periods) - 1.0)
    benchmark_annual = float((1.0 + benchmark_total) ** (trading_days_per_year / periods) - 1.0)

    aligned = pd.concat(
        [strategy_returns.rename("strategy"), benchmark_returns.rename("benchmark")],
        axis=1,
    ).dropna()

    if aligned.empty or aligned["benchmark"].var() == 0:
        beta = float("nan")
        alpha = float("nan")
    else:
        beta = float(aligned["strategy"].cov(aligned["benchmark"]) / aligned["benchmark"].var())
        alpha_daily = float(aligned["strategy"].mean() - beta * aligned["benchmark"].mean())
        alpha = float((1.0 + alpha_daily) ** trading_days_per_year - 1.0) if alpha_daily > -1 else float("nan")

    return {
        "strategy_total_returns": strategy_total,
        "benchmark_total_returns": benchmark_total,
        "excess_total_returns": excess_total,
        "strategy_annualized_returns": strategy_annual,
        "benchmark_annualized_returns": benchmark_annual,
        "beta": beta,
        "alpha_annualized_approx": alpha,
        "days": periods,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate equal-weight benchmark report for an RQAlpha result pickle.")
    parser.add_argument("--result-pickle", default=DEFAULT_RESULT_PICKLE, help="RQAlpha result pickle path")
    parser.add_argument("--sqlite-path", default=DEFAULT_SQLITE_PATH, help="sqlite database path")
    parser.add_argument("--daily-table", default="stock_daily", help="daily data table name")
    parser.add_argument("--symbols", default=",".join(UNIVERSE), help="comma separated order_book_ids")
    parser.add_argument("--output-csv", default=DEFAULT_OUTPUT_CSV, help="output csv path")
    args = parser.parse_args()

    symbols = parse_symbols(args.symbols)
    strategy_nav = load_strategy_nav(args.result_pickle)
    start_date = pd.Timestamp(strategy_nav.index.min())
    end_date = pd.Timestamp(strategy_nav.index.max())

    conn = sqlite3.connect(args.sqlite_path)
    try:
        benchmark_returns = build_equal_weight_returns(
            conn,
            args.daily_table,
            symbols,
            start_date,
            end_date,
        )
    finally:
        conn.close()

    benchmark_returns = benchmark_returns.reindex(strategy_nav.index)
    benchmark_nav = (1.0 + benchmark_returns).cumprod()

    output = pd.DataFrame(
        {
            "strategy_unit_net_value": strategy_nav,
            "strategy_daily_return": strategy_nav.pct_change(),
            "benchmark_daily_return": benchmark_returns,
            "benchmark_unit_net_value": benchmark_nav,
            "geometric_excess": strategy_nav / benchmark_nav - 1.0,
        }
    )
    output.iloc[0, output.columns.get_loc("strategy_daily_return")] = strategy_nav.iloc[0] - 1.0
    os.makedirs(os.path.dirname(args.output_csv), exist_ok=True)
    output.to_csv(args.output_csv, index_label="date")

    summary = compute_summary(strategy_nav, benchmark_returns)
    print("output_csv={}".format(os.path.abspath(args.output_csv)))
    for key, value in summary.items():
        print("{}={}".format(key, value))


if __name__ == "__main__":
    main()
