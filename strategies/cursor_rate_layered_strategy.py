from __future__ import annotations

import csv
import os
import sqlite3
from typing import Dict, List, Optional

import numpy as np

from rqalpha.apis import *

from strategies.cursor_rate_layered.logic import (
    apply_logical_exit,
    armed_forward_items,
    available_budget_cash,
    build_signal_snapshot,
    cleanup_hedged_pending_items,
    compute_attempt_lots,
    compute_exit_decision,
    consume_sellable_inventory,
    consume_specific_sellable,
    hhmm_from_datetime,
    initialize_intraday_buy_state,
    logical_position_cost,
    merge_failed_pending_buys,
    pending_buy_cost,
    rebuild_sellable_inventory,
    reserve_carry_capacity,
    release_carry_capacity,
    reset_after_buy,
    reversal_triggered,
    restore_logical_quantity,
    reverse_buy_triggered,
    reverse_t_eligible,
    should_freeze,
    should_trigger_buy,
    update_buy_trigger_state,
    update_freeze_after_day,
    update_reverse_pending_state,
    window_name,
)
from strategies.cursor_rate_layered.models import (
    GlobalCapitalState,
    GlobalState,
    OrderIntent,
    PendingReverseTSell,
    PendingTBuy,
    SignalSnapshot,
    SymbolState,
)
from strategies.cursor_rate_layered.params import DEFAULT_SYMBOL_OVERRIDES, UNIVERSE, resolve_symbol_params


PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
DEFAULT_SQLITE_PATH = os.path.join(PROJECT_ROOT, "..", "outputs", "minute_data", "stock_data.db")
DEFAULT_DEBUG_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "..", "outputs", "backtest", "cursor_layered_debug")
EVENT_LOG_COLUMNS = [
    "dt",
    "trade_date",
    "order_book_id",
    "event_type",
    "quantity",
    "price",
    "cash_value",
    "realized_pnl",
    "layer",
    "batch_id",
    "sub_id",
    "pending_id",
    "stage",
    "source",
    "note",
]
DAY_LOG_COLUMNS = [
    "date",
    "order_book_id",
    "lifecycle_state",
    "buy_permission_today",
    "buy_permission_persistent",
    "is_day0_signal",
    "is_day1_signal",
    "gross_buy_cash",
    "net_added_cash",
    "t_success_cash",
    "t_success_pnl",
    "t_failed_cash",
    "active_quantity",
    "pending_forward_count",
    "pending_reverse_count",
    "active_batch_count",
]


def init(context):
    context.watchlist = list(UNIVERSE)
    context.symbol_overrides = dict(DEFAULT_SYMBOL_OVERRIDES)
    context.daily_sqlite_path = os.path.abspath(DEFAULT_SQLITE_PATH)
    context.daily_conn = sqlite3.connect(context.daily_sqlite_path)
    context.state = None
    context.order_intents = {}
    context.sorted_candidates = []
    context.batch_seq = 0
    context.pending_seq = 0
    setup_debug_outputs(context)
    update_universe(context.watchlist)
    subscribe_event(EVENT.TRADE, on_trade)
    logger.info("cursor layered strategy init: {} symbols".format(len(context.watchlist)))


def setup_debug_outputs(context):
    os.makedirs(DEFAULT_DEBUG_OUTPUT_DIR, exist_ok=True)
    start_date = getattr(getattr(context.config, "base", None), "start_date", None)
    end_date = getattr(getattr(context.config, "base", None), "end_date", None)
    start_tag = start_date.strftime("%Y%m%d") if start_date is not None else "unknown"
    end_tag = end_date.strftime("%Y%m%d") if end_date is not None else "unknown"
    run_tag = "{}_{}".format(start_tag, end_tag)
    context.event_log_path = os.path.join(
        DEFAULT_DEBUG_OUTPUT_DIR, "cursor_layered_events_{}.csv".format(run_tag)
    )
    context.day_log_path = os.path.join(
        DEFAULT_DEBUG_OUTPUT_DIR, "cursor_layered_day_summary_{}.csv".format(run_tag)
    )
    initialize_csv_file(context.event_log_path, EVENT_LOG_COLUMNS)
    initialize_csv_file(context.day_log_path, DAY_LOG_COLUMNS)


def initialize_csv_file(path, columns):
    with open(path, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()


def append_csv_row(path, columns, row):
    normalized = {key: row.get(key, "") for key in columns}
    with open(path, "a", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writerow(normalized)


def append_event_row(
    context,
    order_book_id,
    event_type,
    quantity,
    price,
    cash_value=0.0,
    realized_pnl=0.0,
    layer="",
    batch_id="",
    sub_id="",
    pending_id="",
    stage="",
    source="",
    note="",
):
    append_csv_row(
        context.event_log_path,
        EVENT_LOG_COLUMNS,
        {
            "dt": str(context.now),
            "trade_date": str(context.now.date()),
            "order_book_id": order_book_id,
            "event_type": event_type,
            "quantity": int(quantity),
            "price": float(price) if price else 0.0,
            "cash_value": float(cash_value),
            "realized_pnl": float(realized_pnl),
            "layer": layer,
            "batch_id": batch_id,
            "sub_id": sub_id,
            "pending_id": pending_id,
            "stage": stage,
            "source": source,
            "note": note,
        },
    )


def append_day_rows(context):
    for symbol_state in context.state.symbols.values():
        append_csv_row(
            context.day_log_path,
            DAY_LOG_COLUMNS,
            {
                "date": str(context.now.date()),
                "order_book_id": symbol_state.order_book_id,
                "lifecycle_state": symbol_state.lifecycle_state,
                "buy_permission_today": int(bool(symbol_state.buy_permission_today)),
                "buy_permission_persistent": int(bool(symbol_state.buy_permission_persistent)),
                "is_day0_signal": int(bool(symbol_state.signal.is_day0_signal)),
                "is_day1_signal": int(bool(symbol_state.signal.is_day1_signal)),
                "gross_buy_cash": float(symbol_state.today_gross_buy_cash),
                "net_added_cash": float(symbol_state.today_net_added_cash),
                "t_success_cash": float(symbol_state.today_t_success_cash),
                "t_success_pnl": float(symbol_state.today_t_success_pnl),
                "t_failed_cash": float(symbol_state.today_t_failed_cash),
                "active_quantity": int(symbol_state.active_quantity()),
                "pending_forward_count": int(len(symbol_state.pending_t_buys)),
                "pending_reverse_count": int(len(symbol_state.pending_reverse_t_sells)),
                "active_batch_count": int(len(symbol_state.active_batches)),
            },
        )


def before_trading(context):
    ensure_state(context)
    context.state.current_day_index += 1
    context.sorted_candidates = []

    for order_book_id in context.watchlist:
        symbol_state = context.state.symbols[order_book_id]
        params = resolve_symbol_params(order_book_id, context.symbol_overrides)
        reset_daily_symbol_state(symbol_state)
        snapshot = load_signal_snapshot(context, order_book_id, params)
        if snapshot is None:
            snapshot = SignalSnapshot(order_book_id=order_book_id)
        symbol_state.signal = snapshot
        symbol_state.buy_permission_today = symbol_state.buy_permission_persistent
        rebuild_sellable_inventory(symbol_state)

        if snapshot.prev_close > 0:
            daily_carry_capacity_lots = int(context.state.capital.single_batch_cash // (snapshot.prev_close * 100))
        else:
            daily_carry_capacity_lots = 0
        symbol_state.daily_carry_capacity_lots = daily_carry_capacity_lots
        symbol_state.remaining_carry_capacity_lots = daily_carry_capacity_lots
        symbol_state.today_max_attempt_count = min(int(params["max_attempt_count"]), daily_carry_capacity_lots)
        symbol_state.remaining_attempt_slots = symbol_state.today_max_attempt_count
        symbol_state.remaining_am_attempt_slots = min(int(params["am_max_attempt_count"]), symbol_state.today_max_attempt_count)
        symbol_state.remaining_pm_attempt_slots = min(int(params["pm_max_attempt_count"]), symbol_state.today_max_attempt_count)
        initialize_intraday_buy_state(symbol_state, snapshot.prev_close if snapshot.prev_close > 0 else 0.0)

        if symbol_state.has_active_position():
            symbol_state.lifecycle_state = "DAY1_PLUS_ACTIVE" if snapshot.is_day1_signal else "EXIT_ONLY"
        else:
            symbol_state.lifecycle_state = "IDLE"

        if is_buy_candidate(symbol_state):
            context.sorted_candidates.append(order_book_id)

    context.sorted_candidates.sort(
        key=lambda order_book_id: candidate_sort_key(context.state.symbols[order_book_id])
    )
    logger.info(
        "before_trading dt={}, buy_candidates={}, top={}".format(
            context.now,
            len(context.sorted_candidates),
            context.sorted_candidates[:5],
        )
    )


def handle_bar(context, bar_dict):
    ensure_state(context)
    hhmm = hhmm_from_datetime(context.now)

    for order_book_id in context.watchlist:
        symbol_state = context.state.symbols[order_book_id]
        if order_book_id not in bar_dict:
            continue
        bar = bar_dict[order_book_id]
        current_price = safe_price(bar.close)
        minute_low = safe_price(bar.low)
        minute_high = safe_price(bar.high)
        if current_price is None or minute_low is None or minute_high is None:
            continue
        if symbol_state.buy_permission_today and should_freeze(
            current_price=current_price,
            three_day_low_snapshot=symbol_state.signal.three_day_low_snapshot,
            freeze_break_ratio=float(resolve_symbol_params(order_book_id, context.symbol_overrides)["freeze_break_ratio"]),
        ):
            symbol_state.buy_permission_today = False
            symbol_state.buy_permission_persistent = False
            symbol_state.freeze_anchor_low = symbol_state.today_intraday_low or minute_low
            symbol_state.freeze_stable_days = 0

    # Reverse-T buybacks are exit-enhancement actions and should not depend on
    # the ordinary low-cursor buy candidate queue.
    for order_book_id in context.watchlist:
        if order_book_id not in bar_dict:
            continue
        symbol_state = context.state.symbols[order_book_id]
        if not symbol_state.pending_reverse_t_sells:
            continue
        params = resolve_symbol_params(order_book_id, context.symbol_overrides)
        bar = bar_dict[order_book_id]
        current_price = safe_price(bar.close)
        minute_low = safe_price(bar.low)
        minute_high = safe_price(bar.high)
        if current_price is None or minute_low is None or minute_high is None:
            continue
        process_reverse_t_buybacks(context, symbol_state, params, bar, hhmm)

    # Buy side next: ordinary Day0/Day1+ attempts.
    for order_book_id in context.sorted_candidates:
        if order_book_id not in bar_dict:
            continue
        symbol_state = context.state.symbols[order_book_id]
        params = resolve_symbol_params(order_book_id, context.symbol_overrides)
        bar = bar_dict[order_book_id]
        current_price = safe_price(bar.close)
        minute_low = safe_price(bar.low)
        minute_high = safe_price(bar.high)
        if current_price is None or minute_low is None or minute_high is None:
            continue
        process_buy_attempt(context, symbol_state, params, bar, hhmm)

    # Sell side next: exits / reverse-T sells / forward-T sells.
    for order_book_id in context.watchlist:
        if order_book_id not in bar_dict:
            continue
        symbol_state = context.state.symbols[order_book_id]
        params = resolve_symbol_params(order_book_id, context.symbol_overrides)
        bar = bar_dict[order_book_id]
        current_price = safe_price(bar.close)
        minute_low = safe_price(bar.low)
        minute_high = safe_price(bar.high)
        if current_price is None or minute_low is None or minute_high is None:
            continue
        process_exit_and_reverse_t_sells(context, symbol_state, params, bar)
        process_forward_t_sells(context, symbol_state, params, bar, hhmm)
        symbol_state.prev_minute_low = minute_low


def after_trading(context):
    ensure_state(context)
    capital = context.state.capital
    for order_book_id in context.watchlist:
        symbol_state = context.state.symbols[order_book_id]
        params = resolve_symbol_params(order_book_id, context.symbol_overrides)
        batch = merge_failed_pending_buys(
            symbol_state,
            batch_id=next_batch_id(context, order_book_id),
            created_dt=str(context.now),
            created_day_index=context.state.current_day_index,
        )
        if batch is not None:
            symbol_state.active_batches.append(batch)
            append_event_row(
                context,
                order_book_id=order_book_id,
                event_type="carryover_batch_created",
                quantity=int(batch.total_quantity),
                price=float(batch.avg_price),
                cash_value=float(batch.batch_cash),
                batch_id=batch.batch_id,
                source=batch.source,
                note="pending forward buys merged into new batch",
            )
        # unresolved reverse-T keeps the first leg exit result.
        for item in symbol_state.pending_reverse_t_sells:
            realized_pnl = (item.sell_avg_price - item.source_avg_price) * item.quantity
            capital.realized_exit_pnl_total += realized_pnl
            append_event_row(
                context,
                order_book_id=order_book_id,
                event_type="reverse_t_unfilled_exit",
                quantity=int(item.quantity),
                price=float(item.sell_avg_price),
                cash_value=float(item.sell_avg_price * item.quantity),
                realized_pnl=float(realized_pnl),
                layer=item.source_layer,
                batch_id=item.source_batch_id,
                sub_id=item.source_sub_id,
                pending_id=item.pending_id,
                stage="reverse_t_unfilled_exit",
                source="reverse_t",
            )
        symbol_state.pending_reverse_t_sells = []
        update_freeze_after_day(symbol_state, symbol_state.today_intraday_low, params)
        if symbol_state.has_active_position():
            symbol_state.lifecycle_state = "DAY1_PLUS_ACTIVE"
        else:
            symbol_state.lifecycle_state = "IDLE"

    maybe_activate_reserve_tier(context)
    log_day_summary(context)
    append_day_rows(context)


def on_trade(context, event):
    trade = event.trade
    order = event.order
    if order is None:
        return
    intent = context.order_intents.get(order.order_id)
    if intent is None:
        return

    apply_intent_fill(
        context=context,
        intent=intent,
        quantity=int(trade.last_quantity),
        price=float(trade.last_price),
        transaction_cost=float(trade.transaction_cost),
        order_id=order.order_id,
    )


def ensure_state(context):
    if context.state is not None:
        return
    total_cash = float(context.portfolio.total_value)
    input_cash_total = total_cash / 5.0
    reserve_tier_cash = total_cash / 5.0
    capital = GlobalCapitalState(
        total_cash=total_cash,
        input_cash_total=input_cash_total,
        single_batch_cash=input_cash_total / 20.0,
        reserve_tier_cash=reserve_tier_cash,
    )
    symbols = {order_book_id: SymbolState(order_book_id=order_book_id) for order_book_id in context.watchlist}
    for order_book_id, symbol_state in symbols.items():
        symbol_state.signal = SignalSnapshot(order_book_id=order_book_id)
    context.state = GlobalState(capital=capital, symbols=symbols)


def reset_daily_symbol_state(symbol_state):
    symbol_state.today_am_buy_cash = 0.0
    symbol_state.today_pm_buy_cash = 0.0
    symbol_state.today_gross_buy_cash = 0.0
    symbol_state.today_net_added_cash = 0.0
    symbol_state.today_t_success_cash = 0.0
    symbol_state.today_t_success_pnl = 0.0
    symbol_state.today_t_failed_cash = 0.0
    symbol_state.today_intraday_low = None
    symbol_state.prev_minute_low = None


def load_signal_snapshot(context, order_book_id, params):
    lookback = max(
        int(params["overall_cursor_lookback_bars"]),
        int(params["half_year_cursor_lookback_bars"]),
        int(params["freeze_lookback_days"]),
    )
    raw_symbol = order_book_id.split(".")[0]
    trade_date = context.now.date().isoformat()
    rows = context.daily_conn.execute(
        """
        SELECT high, low, close, volume
        FROM stock_daily
        WHERE symbol = ?
          AND DATE(timestamp) < ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (raw_symbol, trade_date, lookback),
    ).fetchall()
    if len(rows) < lookback:
        return None
    rows.reverse()
    highs = np.array([float(row[0]) for row in rows], dtype=float)
    lows = np.array([float(row[1]) for row in rows], dtype=float)
    closes = np.array([float(row[2]) for row in rows], dtype=float)
    volumes = np.array([float(row[3]) for row in rows[-1:]], dtype=float)
    if np.isnan(highs).any() or np.isnan(lows).any() or np.isnan(closes).any():
        return None
    snapshot = build_signal_snapshot(order_book_id, highs, lows, closes, params)
    if snapshot is None:
        return None
    if volumes is not None and len(volumes) > 0 and not np.isnan(volumes).any():
        snapshot.liquidity = float(volumes[-1])
    return snapshot


def candidate_sort_key(symbol_state):
    return (
        symbol_state.signal.half_year_cursor_rate,
        symbol_state.signal.overall_cursor_rate,
        -symbol_state.signal.liquidity,
        symbol_state.order_book_id,
    )


def is_buy_candidate(symbol_state):
    if not symbol_state.buy_permission_today:
        return False
    if symbol_state.signal.prev_close <= 0:
        return False
    if symbol_state.has_active_position():
        return symbol_state.signal.is_day1_signal and symbol_state.lifecycle_state != "EXIT_ONLY"
    return symbol_state.signal.is_day0_signal


def safe_price(value):
    if value is None:
        return None
    value = float(value)
    if np.isnan(value) or value <= 0:
        return None
    return value


def next_batch_id(context, order_book_id):
    context.batch_seq += 1
    return "{}-B{:05d}".format(order_book_id, context.batch_seq)


def next_pending_id(context, order_book_id, prefix):
    context.pending_seq += 1
    return "{}-{}-{:05d}".format(order_book_id, prefix, context.pending_seq)


def available_cash_for_new_buys(context):
    budget_cash = available_budget_cash(context.state.capital, context.state.symbols.values())
    return min(float(context.portfolio.cash), budget_cash)


def process_buy_attempt(context, symbol_state, params, bar, hhmm):
    if not symbol_state.buy_permission_today:
        return
    if symbol_state.lifecycle_state == "EXIT_ONLY":
        return
    has_position = symbol_state.has_active_position()
    if has_position and not symbol_state.signal.is_day1_signal:
        return
    if not has_position and not symbol_state.signal.is_day0_signal:
        return

    window = window_name(hhmm, params)
    if window not in {"am", "pm"}:
        return

    minute_low = safe_price(bar.low)
    minute_high = safe_price(bar.high)
    current_price = safe_price(bar.close)
    if minute_low is None or minute_high is None or current_price is None:
        return

    update_buy_trigger_state(symbol_state, minute_low, minute_high, float(params["forward_rebound_pct"]))
    if not should_trigger_buy(symbol_state, minute_low, float(params["forward_drop_pct"])):
        return

    available_cash = available_cash_for_new_buys(context)
    lots = compute_attempt_lots(
        symbol_state,
        current_price=current_price,
        available_cash=available_cash,
        single_batch_cash=context.state.capital.single_batch_cash,
        window=window,
        params=params,
    )
    if lots <= 0:
        return

    quantity = lots * 100
    trigger_price = float(symbol_state.ref_high) * (1.0 - float(params["forward_drop_pct"]))
    order = order_shares(symbol_state.order_book_id, quantity)
    if order is None:
        return
    logger.info(
        "buy_attempt symbol={}, dt={}, window={}, lots={}, trigger_price={:.3f}, ref_high={:.3f}".format(
            symbol_state.order_book_id,
            context.now,
            window,
            lots,
            trigger_price,
            float(symbol_state.ref_high),
        )
    )

    symbol_state.remaining_attempt_slots = max(symbol_state.remaining_attempt_slots - 1, 0)
    if window == "am":
        symbol_state.remaining_am_attempt_slots = max(symbol_state.remaining_am_attempt_slots - 1, 0)
        symbol_state.today_am_buy_cash += trigger_price * quantity
    else:
        symbol_state.remaining_pm_attempt_slots = max(symbol_state.remaining_pm_attempt_slots - 1, 0)
        symbol_state.today_pm_buy_cash += trigger_price * quantity
    symbol_state.today_gross_buy_cash += trigger_price * quantity
    reserve_carry_capacity(symbol_state, lots)
    reset_after_buy(symbol_state, trigger_price)
    intent = OrderIntent(
        intent_type="buy_attempt",
        order_book_id=symbol_state.order_book_id,
        quantity=quantity,
        lots=lots,
        price_hint=trigger_price,
        metadata={
            "pending_id": next_pending_id(context, symbol_state.order_book_id, "FTB"),
            "reserved_lots": lots,
            "reserved_cash": trigger_price * quantity,
            "window": window,
            "batch_source": "day1_plus" if has_position else "day0",
        },
    )
    register_order_intent(context, order, intent)


def process_forward_t_sells(context, symbol_state, params, bar, hhmm):
    minute_low = safe_price(bar.low)
    minute_high = safe_price(bar.high)
    current_price = safe_price(bar.close)
    if minute_low is None or minute_high is None or current_price is None:
        return

    reversal = reversal_triggered(symbol_state, minute_low)
    armed_items = armed_forward_items(symbol_state)
    if reversal and armed_items:
        submit_forward_sell_order(context, symbol_state, params, armed_items, current_price)

    tail_window = window_name(hhmm, params) == "tail"
    if tail_window:
        tail_items = [item for item in armed_items if current_price >= item.sell_trigger_price]
        if tail_items:
            submit_forward_sell_order(context, symbol_state, params, tail_items, current_price)

    # New arm state becomes effective for future minutes.
    for item in symbol_state.pending_t_buys:
        if item.unhedged_quantity > 0 and not item.sell_submitted and minute_high >= item.sell_trigger_price:
            item.sell_armed = True


def submit_forward_sell_order(context, symbol_state, params, pending_items, current_price):
    sellable_quantity = sum(item.available_quantity for item in symbol_state.sellable_inventory)
    if sellable_quantity <= 0:
        return
    mappings = []
    total_quantity = 0
    remaining = sellable_quantity
    for item in pending_items:
        qty = min(item.unhedged_quantity, remaining)
        if qty <= 0:
            continue
        mappings.append({"pending_id": item.pending_id, "quantity": qty})
        total_quantity += qty
        remaining -= qty
        item.sell_submitted = True
        if remaining <= 0:
            break
    if total_quantity <= 0:
        return
    source_allocations = consume_sellable_inventory(symbol_state, total_quantity)
    order = order_shares(symbol_state.order_book_id, -total_quantity)
    if order is None:
        for item in pending_items:
            item.sell_submitted = False
        return
    intent = OrderIntent(
        intent_type="forward_sell",
        order_book_id=symbol_state.order_book_id,
        quantity=total_quantity,
        lots=total_quantity // 100,
        price_hint=current_price,
        metadata={
            "pending_mappings": mappings,
            "source_allocations": source_allocations,
        },
    )
    register_order_intent(context, order, intent)


def process_exit_and_reverse_t_sells(context, symbol_state, params, bar):
    if not symbol_state.sellable_inventory:
        return
    current_price = safe_price(bar.close)
    if current_price is None:
        return
    for batch in list(symbol_state.active_batches):
        for sub in list(batch.sub_positions):
            if sub.quantity <= 0:
                continue
            available = available_quantity_for_sub(symbol_state, batch.batch_id, sub.sub_id)
            if available <= 0:
                continue
            eligible_reverse, decision = reverse_t_eligible(
                sub,
                current_price=current_price,
                current_day_index=context.state.current_day_index,
                half_year_cursor_rate=symbol_state.signal.half_year_cursor_rate,
                params=params,
            )
            if eligible_reverse:
                submit_reverse_sell_order(
                    context,
                    symbol_state,
                    batch.batch_id,
                    sub.sub_id,
                    min(sub.quantity, available),
                    current_price,
                    decision,
                )
                continue
            decision = compute_exit_decision(sub, current_price, context.state.current_day_index, params)
            if not decision.should_exit:
                continue
            submit_exit_sell_order(
                context,
                symbol_state,
                batch.batch_id,
                sub.sub_id,
                min(sub.quantity, available),
                current_price,
                decision,
            )


def available_quantity_for_sub(symbol_state, batch_id, sub_id):
    for item in symbol_state.sellable_inventory:
        if item.batch_id == batch_id and item.sub_id == sub_id:
            return item.available_quantity
    return 0


def submit_exit_sell_order(context, symbol_state, batch_id, sub_id, quantity, current_price, decision):
    if quantity < 100:
        return
    reserved = consume_specific_sellable(symbol_state, batch_id, sub_id, quantity)
    if reserved <= 0:
        return
    order = order_shares(symbol_state.order_book_id, -reserved)
    if order is None:
        return
    intent = OrderIntent(
        intent_type="exit_sell",
        order_book_id=symbol_state.order_book_id,
        quantity=reserved,
        lots=reserved // 100,
        price_hint=current_price,
        metadata={
            "batch_id": batch_id,
            "sub_id": sub_id,
            "layer": find_layer(symbol_state, batch_id, sub_id),
            "stage": decision.stage,
            "expected_exit_price": decision.expected_exit_price,
        },
    )
    register_order_intent(context, order, intent)


def submit_reverse_sell_order(context, symbol_state, batch_id, sub_id, quantity, current_price, decision):
    if quantity < 100:
        return
    reserved = consume_specific_sellable(symbol_state, batch_id, sub_id, quantity)
    if reserved <= 0:
        return
    order = order_shares(symbol_state.order_book_id, -reserved)
    if order is None:
        return
    intent = OrderIntent(
        intent_type="reverse_sell",
        order_book_id=symbol_state.order_book_id,
        quantity=reserved,
        lots=reserved // 100,
        price_hint=current_price,
        metadata={
            "batch_id": batch_id,
            "sub_id": sub_id,
            "source_layer": find_layer(symbol_state, batch_id, sub_id),
            "stage": decision.stage,
            "expected_exit_price": decision.expected_exit_price,
        },
    )
    register_order_intent(context, order, intent)


def process_reverse_t_buybacks(context, symbol_state, params, bar, hhmm):
    window = window_name(hhmm, params)
    if window not in {"am", "pm"}:
        return
    minute_low = safe_price(bar.low)
    minute_high = safe_price(bar.high)
    current_price = safe_price(bar.close)
    if minute_low is None or minute_high is None or current_price is None:
        return
    for item in list(symbol_state.pending_reverse_t_sells):
        update_reverse_pending_state(item, minute_low, minute_high, params)
        if not reverse_buy_triggered(item):
            continue
        order = order_shares(symbol_state.order_book_id, item.quantity)
        if order is None:
            continue
        item.buyback_submitted = True
        intent = OrderIntent(
            intent_type="reverse_buy",
            order_book_id=symbol_state.order_book_id,
            quantity=item.quantity,
            lots=item.quantity // 100,
            price_hint=current_price,
            metadata={"pending_id": item.pending_id},
        )
        register_order_intent(context, order, intent)


def register_order_intent(context, order, intent):
    if order is None:
        return
    if int(getattr(order, "filled_quantity", 0) or 0) > 0:
        apply_intent_fill(
            context=context,
            intent=intent,
            quantity=int(order.filled_quantity),
            price=float(order.avg_price),
            transaction_cost=float(order.transaction_cost),
            order_id=order.order_id,
        )
        return
    context.order_intents[order.order_id] = intent


def apply_intent_fill(context, intent, quantity, price, transaction_cost, order_id):
    if quantity <= 0:
        return
    order_book_id = intent.order_book_id
    symbol_state = context.state.symbols[order_book_id]
    capital = context.state.capital

    if intent.intent_type == "buy_attempt":
        handle_buy_attempt_trade(context, symbol_state, capital, intent, quantity, price)
    elif intent.intent_type == "forward_sell":
        handle_forward_sell_trade(context, symbol_state, capital, intent, quantity, price, transaction_cost)
    elif intent.intent_type == "exit_sell":
        handle_exit_sell_trade(context, symbol_state, capital, intent, quantity, price, transaction_cost)
    elif intent.intent_type == "reverse_sell":
        handle_reverse_sell_trade(context, symbol_state, intent, quantity, price)
    elif intent.intent_type == "reverse_buy":
        handle_reverse_buy_trade(context, symbol_state, capital, intent, quantity, price, transaction_cost)

    intent.quantity -= quantity
    intent.lots = max(intent.quantity // 100, 0)
    if intent.quantity > 0:
        context.order_intents[order_id] = intent
    else:
        context.order_intents.pop(order_id, None)


def handle_buy_attempt_trade(context, symbol_state, capital, intent, quantity, price):
    params = resolve_symbol_params(symbol_state.order_book_id, context.symbol_overrides)
    reserved_cash = float(intent.metadata.get("reserved_cash", intent.price_hint * quantity))
    actual_cash = price * quantity
    if intent.metadata.get("window") == "am":
        symbol_state.today_am_buy_cash += actual_cash - reserved_cash
    else:
        symbol_state.today_pm_buy_cash += actual_cash - reserved_cash
    symbol_state.today_gross_buy_cash += actual_cash - reserved_cash

    reserved_lots = int(intent.metadata.get("reserved_lots", intent.lots))
    actual_lots = quantity // 100
    if reserved_lots > actual_lots:
        release_carry_capacity(symbol_state, reserved_lots - actual_lots)

    pending = PendingTBuy(
        pending_id=intent.metadata["pending_id"],
        source="buy_attempt",
        batch_source=intent.metadata["batch_source"],
        buy_day_index=context.state.current_day_index,
        buy_dt=str(context.now),
        quantity=quantity,
        avg_price=price,
        sell_trigger_price=price * (1.0 + float(params["forward_t_target_pct"])),
        carry_reserved_lots=actual_lots,
    )
    symbol_state.pending_t_buys.append(pending)
    reset_after_buy(symbol_state, price)
    append_event_row(
        context,
        order_book_id=symbol_state.order_book_id,
        event_type="buy_attempt_fill",
        quantity=int(quantity),
        price=float(price),
        cash_value=float(price * quantity),
        pending_id=pending.pending_id,
        source=pending.batch_source,
    )


def handle_forward_sell_trade(context, symbol_state, capital, intent, quantity, price, transaction_cost):
    remaining = quantity
    total_order_qty = max(quantity, 1)
    source_layers = []
    for mapping in intent.metadata["pending_mappings"]:
        if remaining <= 0:
            break
        pending = find_pending_t_buy(symbol_state, mapping["pending_id"])
        if pending is None:
            continue
        matched = min(mapping["quantity"], remaining, pending.unhedged_quantity)
        if matched <= 0:
            continue
        pending.hedged_quantity += matched
        pending.sell_submitted = False
        pending.sell_armed = False
        release_carry_capacity(symbol_state, matched // 100)
        pnl = (price - pending.avg_price) * matched - transaction_cost * (matched / total_order_qty)
        capital.realized_t_pnl_total += pnl
        symbol_state.today_t_success_pnl += pnl
        symbol_state.today_t_success_cash += price * matched
        source_layers.extend([layer for _, _, layer, _ in intent.metadata.get("source_allocations", [])])
        append_event_row(
            context,
            order_book_id=symbol_state.order_book_id,
            event_type="forward_t_success",
            quantity=int(matched),
            price=float(price),
            cash_value=float(price * matched),
            realized_pnl=float(pnl),
            pending_id=pending.pending_id,
            source=pending.batch_source,
            note="buy_avg={:.4f}".format(float(pending.avg_price)),
        )
        remaining -= matched
    cleanup_hedged_pending_items(symbol_state)


def handle_exit_sell_trade(context, symbol_state, capital, intent, quantity, price, transaction_cost):
    removed_qty, avg_price = apply_logical_exit(
        symbol_state,
        batch_id=intent.metadata["batch_id"],
        sub_id=intent.metadata["sub_id"],
        quantity=quantity,
    )
    if removed_qty > 0:
        realized_pnl = (price - avg_price) * removed_qty - transaction_cost
        capital.realized_exit_pnl_total += realized_pnl
        append_event_row(
            context,
            order_book_id=symbol_state.order_book_id,
            event_type="exit_sell",
            quantity=int(removed_qty),
            price=float(price),
            cash_value=float(price * removed_qty),
            realized_pnl=float(realized_pnl),
            layer=intent.metadata.get("layer", ""),
            batch_id=intent.metadata["batch_id"],
            sub_id=intent.metadata["sub_id"],
            stage=intent.metadata.get("stage", ""),
            source="exit",
            note="expected_exit_price={:.4f}".format(float(intent.metadata.get("expected_exit_price", 0.0))),
        )


def handle_reverse_sell_trade(context, symbol_state, intent, quantity, price):
    removed_qty, avg_price = apply_logical_exit(
        symbol_state,
        batch_id=intent.metadata["batch_id"],
        sub_id=intent.metadata["sub_id"],
        quantity=quantity,
    )
    if removed_qty <= 0:
        return
    params = resolve_symbol_params(symbol_state.order_book_id, context.symbol_overrides)
    item = PendingReverseTSell(
        pending_id=next_pending_id(context, symbol_state.order_book_id, "RTS"),
        source_batch_id=intent.metadata["batch_id"],
        source_sub_id=intent.metadata["sub_id"],
        source_layer=intent.metadata.get("source_layer", "short"),
        source_avg_price=avg_price,
        quantity=removed_qty,
        sell_avg_price=price,
        sell_dt=str(context.now),
        sell_day_index=context.state.current_day_index,
        rebuy_trigger_price=price * (1.0 - float(params["reverse_t_target_pct"])),
        local_low=price,
    )
    symbol_state.pending_reverse_t_sells.append(item)
    append_event_row(
        context,
        order_book_id=symbol_state.order_book_id,
        event_type="reverse_t_sell_open",
        quantity=int(removed_qty),
        price=float(price),
        cash_value=float(price * removed_qty),
        layer=item.source_layer,
        batch_id=item.source_batch_id,
        sub_id=item.source_sub_id,
        pending_id=item.pending_id,
        stage=intent.metadata.get("stage", ""),
        source="reverse_t",
        note="source_avg={:.4f}".format(float(avg_price)),
    )


def handle_reverse_buy_trade(context, symbol_state, capital, intent, quantity, price, transaction_cost):
    item = find_pending_reverse_sell(symbol_state, intent.metadata["pending_id"])
    if item is None:
        return
    restore_logical_quantity(symbol_state, item.source_batch_id, item.source_sub_id, quantity)
    pnl = (item.sell_avg_price - price) * quantity - transaction_cost
    capital.realized_t_pnl_total += pnl
    symbol_state.today_t_success_pnl += pnl
    symbol_state.today_t_success_cash += item.sell_avg_price * quantity
    symbol_state.pending_reverse_t_sells = [current for current in symbol_state.pending_reverse_t_sells if current.pending_id != item.pending_id]
    append_event_row(
        context,
        order_book_id=symbol_state.order_book_id,
        event_type="reverse_t_success",
        quantity=int(quantity),
        price=float(price),
        cash_value=float(price * quantity),
        realized_pnl=float(pnl),
        layer=item.source_layer,
        batch_id=item.source_batch_id,
        sub_id=item.source_sub_id,
        pending_id=item.pending_id,
        stage="reverse_t_success",
        source="reverse_t",
        note="sell_avg={:.4f}".format(float(item.sell_avg_price)),
    )


def find_pending_t_buy(symbol_state, pending_id) -> Optional[PendingTBuy]:
    for item in symbol_state.pending_t_buys:
        if item.pending_id == pending_id:
            return item
    return None


def find_pending_reverse_sell(symbol_state, pending_id) -> Optional[PendingReverseTSell]:
    for item in symbol_state.pending_reverse_t_sells:
        if item.pending_id == pending_id:
            return item
    return None


def find_layer(symbol_state, batch_id, sub_id) -> str:
    for batch in symbol_state.active_batches:
        if batch.batch_id != batch_id:
            continue
        for sub in batch.sub_positions:
            if sub.sub_id == sub_id:
                return sub.layer
    return "short"


def maybe_activate_reserve_tier(context):
    capital = context.state.capital
    available_cash = available_budget_cash(capital, context.state.symbols.values())
    has_positions = any(symbol_state.has_active_position() for symbol_state in context.state.symbols.values())
    if (
        available_cash < 0.5 * capital.single_batch_cash
        and has_positions
        and capital.active_reserve_tier_count < 4
    ):
        capital.active_reserve_tier_count += 1
        logger.info("activate reserve tier: {}".format(capital.active_reserve_tier_count))
        append_event_row(
            context,
            order_book_id="GLOBAL",
            event_type="reserve_tier_activated",
            quantity=0,
            price=0.0,
            cash_value=float(capital.reserve_tier_cash),
            source="capital",
            note="tier={}".format(capital.active_reserve_tier_count),
        )


def log_day_summary(context):
    capital = context.state.capital
    total_batches = sum(len(symbol_state.active_batches) for symbol_state in context.state.symbols.values())
    total_pending = sum(len(symbol_state.pending_t_buys) for symbol_state in context.state.symbols.values())
    logger.info(
        "day summary dt={}, reserve_tiers={}, unlocked={:.2f}, budget_available={:.2f}, batches={}, pending={}"
        .format(
            context.now,
            capital.active_reserve_tier_count,
            capital.unlocked_cash_limit(),
            available_budget_cash(capital, context.state.symbols.values()),
            total_batches,
            total_pending,
        )
    )
