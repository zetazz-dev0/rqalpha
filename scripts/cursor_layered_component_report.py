from __future__ import annotations

import argparse
import os
import pickle
import sys
import tempfile

import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
DEFAULT_DEBUG_DIR = os.path.join(PROJECT_ROOT, "outputs", "backtest", "cursor_layered_debug")


def load_pickle_summary(result_pickle: str):
    os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "mplconfig"))
    with open(result_pickle, "rb") as infile:
        payload = pickle.load(infile)
    if not isinstance(payload, dict):
        return {}
    return payload.get("summary", {}) or {}


def print_frame(title: str, df: pd.DataFrame, limit: int = 10) -> None:
    print("")
    print(title)
    if df.empty:
        print("  <empty>")
        return
    print(df.head(limit).to_string(index=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize cursor layered strategy component effects.")
    parser.add_argument("--event-csv", required=True)
    parser.add_argument("--day-csv", required=True)
    parser.add_argument("--result-pickle")
    parser.add_argument("--report-dir")
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    event_df = pd.read_csv(args.event_csv)
    day_df = pd.read_csv(args.day_csv)

    output_dir = args.output_dir or os.path.dirname(os.path.abspath(args.event_csv))
    os.makedirs(output_dir, exist_ok=True)

    print("cursor layered component report")
    print("  event_csv =", args.event_csv)
    print("  day_csv =", args.day_csv)
    if args.result_pickle:
        print("  result_pickle =", args.result_pickle)
    if args.report_dir:
        print("  report_dir =", args.report_dir)

    if args.result_pickle:
        summary = load_pickle_summary(args.result_pickle)
        if summary:
            print("")
            print("backtest summary")
            for key in (
                "strategy_name",
                "total_returns",
                "annualized_returns",
                "max_drawdown",
                "sharpe",
                "trades_count",
            ):
                if key in summary:
                    print("  {} = {}".format(key, summary[key]))

    event_summary = (
        event_df.groupby("event_type", dropna=False)
        .agg(
            rows=("event_type", "size"),
            quantity=("quantity", "sum"),
            cash_value=("cash_value", "sum"),
            realized_pnl=("realized_pnl", "sum"),
        )
        .reset_index()
        .sort_values(["realized_pnl", "cash_value"], ascending=[False, False])
    )
    event_summary.to_csv(os.path.join(output_dir, "component_event_summary.csv"), index=False)
    print_frame("event summary", event_summary)

    pnl_events = event_df[event_df["realized_pnl"] != 0].copy()
    symbol_pnl = (
        pnl_events.groupby(["order_book_id", "event_type"], dropna=False)
        .agg(
            rows=("event_type", "size"),
            quantity=("quantity", "sum"),
            realized_pnl=("realized_pnl", "sum"),
        )
        .reset_index()
        .sort_values("realized_pnl", ascending=False)
    )
    symbol_pnl.to_csv(os.path.join(output_dir, "component_symbol_pnl.csv"), index=False)
    print_frame("top realized pnl by symbol/event", symbol_pnl, limit=20)

    daily_summary = (
        day_df.groupby("date", dropna=False)
        .agg(
            gross_buy_cash=("gross_buy_cash", "sum"),
            net_added_cash=("net_added_cash", "sum"),
            t_success_cash=("t_success_cash", "sum"),
            t_success_pnl=("t_success_pnl", "sum"),
            t_failed_cash=("t_failed_cash", "sum"),
            active_quantity=("active_quantity", "sum"),
            active_batch_count=("active_batch_count", "sum"),
        )
        .reset_index()
    )
    daily_summary.to_csv(os.path.join(output_dir, "component_daily_summary.csv"), index=False)
    print_frame("daily component summary", daily_summary, limit=20)

    carryover_df = event_df[event_df["event_type"] == "carryover_batch_created"].copy()
    if not carryover_df.empty:
        carryover_summary = (
            carryover_df.groupby("order_book_id", dropna=False)
            .agg(
                carryover_rows=("event_type", "size"),
                carryover_qty=("quantity", "sum"),
                carryover_cash=("cash_value", "sum"),
            )
            .reset_index()
            .sort_values("carryover_cash", ascending=False)
        )
        carryover_summary.to_csv(os.path.join(output_dir, "component_carryover_summary.csv"), index=False)
        print_frame("carryover summary", carryover_summary, limit=20)

    if args.report_dir:
        positions_path = os.path.join(args.report_dir, "stock_positions.csv")
        if os.path.exists(positions_path):
            pos_df = pd.read_csv(positions_path)
            if not pos_df.empty:
                last_date = str(pos_df["date"].max())
                final_pos = pos_df[pos_df["date"] == last_date].copy()
                final_pos["unrealized_pnl"] = (final_pos["last_price"] - final_pos["avg_price"]) * final_pos["quantity"]
                final_pos["unrealized_return"] = final_pos["unrealized_pnl"] / (final_pos["avg_price"] * final_pos["quantity"])
                final_pos = final_pos.sort_values("unrealized_pnl", ascending=False)
                final_pos.to_csv(os.path.join(output_dir, "component_final_positions.csv"), index=False)
                print("")
                print("final positions")
                print("  last_date =", last_date)
                print("  positions =", len(final_pos))
                print("  total_market_value = {:.2f}".format(float(final_pos["market_value"].sum())))
                weighted_unrealized = 0.0
                mv = float(final_pos["market_value"].sum())
                if mv > 0:
                    weighted_unrealized = float(final_pos["unrealized_pnl"].sum()) / mv
                print("  weighted_unrealized_return = {:.6f}".format(weighted_unrealized))
                print_frame("top final positions", final_pos[[
                    "order_book_id",
                    "quantity",
                    "last_price",
                    "avg_price",
                    "market_value",
                    "unrealized_pnl",
                    "unrealized_return",
                ]], limit=20)

    forward_pnl = float(event_df.loc[event_df["event_type"] == "forward_t_success", "realized_pnl"].sum())
    reverse_pnl = float(event_df.loc[event_df["event_type"] == "reverse_t_success", "realized_pnl"].sum())
    exit_pnl = float(event_df.loc[event_df["event_type"].isin(["exit_sell", "reverse_t_unfilled_exit"]), "realized_pnl"].sum())
    print("")
    print("component totals")
    print("  forward_t_realized_pnl = {:.6f}".format(forward_pnl))
    print("  reverse_t_realized_pnl = {:.6f}".format(reverse_pnl))
    print("  exit_realized_pnl = {:.6f}".format(exit_pnl))
    print("  carryover_cash = {:.6f}".format(float(event_df.loc[event_df["event_type"] == "carryover_batch_created", "cash_value"].sum())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
