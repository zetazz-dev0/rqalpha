from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import floor
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .models import (
    BatchState,
    PendingReverseTSell,
    PendingTBuy,
    SellableLot,
    SignalSnapshot,
    SubPositionState,
    SymbolState,
)


LAYER_PRIORITY = {"short": 0, "mid": 1, "long": 2}


@dataclass
class ExitDecision:
    should_exit: bool
    stage: str
    expected_exit_price: float


def hhmm_from_datetime(dt) -> int:
    return dt.hour * 100 + dt.minute


def compute_cursor_rate(window_high: float, window_low: float, current_price: float) -> float:
    if window_high == window_low:
        return 100.0 if current_price >= window_high else 0.0
    return (current_price - window_low) / (window_high - window_low) * 100.0


def build_signal_snapshot(order_book_id: str, highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], params: Dict[str, object]) -> Optional[SignalSnapshot]:
    overall_lookback = int(params["overall_cursor_lookback_bars"])
    half_year_lookback = int(params["half_year_cursor_lookback_bars"])
    required = max(overall_lookback, half_year_lookback, int(params["freeze_lookback_days"]))
    if len(closes) < required or len(highs) < required or len(lows) < required:
        return None

    prev_close = float(closes[-1])
    overall_window_high = max(float(v) for v in highs[-overall_lookback:])
    overall_window_low = min(float(v) for v in lows[-overall_lookback:])
    half_window_high = max(float(v) for v in highs[-half_year_lookback:])
    half_window_low = min(float(v) for v in lows[-half_year_lookback:])
    freeze_lookback = int(params["freeze_lookback_days"])
    three_day_low_snapshot = min(float(v) for v in lows[-freeze_lookback:])
    overall_cursor_rate = compute_cursor_rate(overall_window_high, overall_window_low, prev_close)
    half_year_cursor_rate = compute_cursor_rate(half_window_high, half_window_low, prev_close)

    return SignalSnapshot(
        order_book_id=order_book_id,
        prev_close=prev_close,
        liquidity=0.0,
        overall_cursor_rate=overall_cursor_rate,
        half_year_cursor_rate=half_year_cursor_rate,
        three_day_low_snapshot=three_day_low_snapshot,
        is_day0_signal=(
            overall_cursor_rate < float(params["overall_cursor_threshold"])
            and half_year_cursor_rate < float(params["day0_half_year_threshold"])
        ),
        is_day1_signal=(
            overall_cursor_rate < float(params["overall_cursor_threshold"])
            and half_year_cursor_rate < float(params["day1_half_year_threshold"])
        ),
    )


def window_name(hhmm: int, params: Dict[str, object]) -> Optional[str]:
    if int(params["am_window_start"]) <= hhmm <= int(params["am_window_end"]):
        return "am"
    if int(params["pm_window_start"]) <= hhmm <= int(params["pm_window_end"]):
        return "pm"
    if int(params["tail_window_start"]) <= hhmm <= int(params["tail_window_end"]):
        return "tail"
    return None


def compute_window_cash_limit(single_batch_cash: float, window: str, params: Dict[str, object]) -> float:
    if window == "am":
        return single_batch_cash * float(params["am_buy_fraction"])
    if window == "pm":
        return single_batch_cash * float(params["pm_buy_fraction"])
    return 0.0


def initialize_intraday_buy_state(symbol_state: SymbolState, prev_close: float) -> None:
    symbol_state.local_low = prev_close
    symbol_state.ref_high = prev_close
    symbol_state.rebound_confirmed = False
    symbol_state.prev_minute_low = None
    symbol_state.today_intraday_low = None


def update_buy_trigger_state(symbol_state: SymbolState, minute_low: float, minute_high: float, rebound_pct: float) -> None:
    if symbol_state.local_low is None:
        symbol_state.local_low = minute_low
    if symbol_state.ref_high is None:
        symbol_state.ref_high = minute_high

    if symbol_state.today_intraday_low is None:
        symbol_state.today_intraday_low = minute_low
    else:
        symbol_state.today_intraday_low = min(symbol_state.today_intraday_low, minute_low)

    if not symbol_state.rebound_confirmed:
        symbol_state.local_low = min(symbol_state.local_low, minute_low)
        if minute_high >= symbol_state.local_low * (1.0 + rebound_pct):
            symbol_state.rebound_confirmed = True
            symbol_state.ref_high = minute_high
        return

    if minute_low < symbol_state.local_low:
        symbol_state.local_low = minute_low
        symbol_state.rebound_confirmed = False
        symbol_state.ref_high = minute_low
        return

    symbol_state.ref_high = max(symbol_state.ref_high or minute_high, minute_high)


def should_trigger_buy(symbol_state: SymbolState, minute_low: float, drop_pct: float) -> bool:
    if not symbol_state.rebound_confirmed or symbol_state.ref_high is None:
        return False
    return minute_low <= symbol_state.ref_high * (1.0 - drop_pct)


def reset_after_buy(symbol_state: SymbolState, fill_price: float) -> None:
    symbol_state.local_low = fill_price
    symbol_state.ref_high = fill_price
    symbol_state.rebound_confirmed = False


def split_lots_by_weights(total_lots: int) -> Dict[str, int]:
    if total_lots <= 0:
        return {"short": 0, "mid": 0, "long": 0}
    weights = [("short", 0.5), ("mid", 0.25), ("long", 0.25)]
    raw = [(layer, total_lots * weight) for layer, weight in weights]
    base = {layer: int(value) for layer, value in raw}
    used = sum(base.values())
    remainders = sorted(
        ((layer, value - int(value), LAYER_PRIORITY[layer]) for layer, value in raw),
        key=lambda item: (-item[1], item[2]),
    )
    for layer, _, _ in remainders[: max(total_lots - used, 0)]:
        base[layer] += 1
    if all(value == 0 for value in base.values()):
        base["short"] = total_lots
    return base


def build_batch(order_book_id: str, batch_id: str, source: str, created_dt: str, created_day_index: int, quantity: int, avg_price: float) -> BatchState:
    total_lots = quantity // 100
    lot_split = split_lots_by_weights(total_lots)
    assigned_lots = 0
    sub_positions = []
    weights = {"short": 0.5, "mid": 0.25, "long": 0.25}
    for idx, layer in enumerate(("short", "mid", "long")):
        lots = lot_split[layer]
        shares = lots * 100
        assigned_lots += lots
        sub_positions.append(
            SubPositionState(
                sub_id="{}:{}".format(batch_id, layer),
                batch_id=batch_id,
                layer=layer,
                weight=weights[layer],
                quantity=shares,
                avg_price=avg_price,
                created_day_index=created_day_index,
                created_date=created_dt[:10],
            )
        )
    return BatchState(
        batch_id=batch_id,
        order_book_id=order_book_id,
        source=source,
        created_dt=created_dt,
        created_day_index=created_day_index,
        batch_cash=quantity * avg_price,
        avg_price=avg_price,
        total_quantity=quantity,
        sub_positions=sub_positions,
    )


def rebuild_sellable_inventory(symbol_state: SymbolState) -> None:
    inventory = []
    for batch in symbol_state.active_batches:
        for sub in batch.sub_positions:
            if sub.quantity <= 0 or sub.status == "closed":
                continue
            inventory.append(
                SellableLot(
                    batch_id=batch.batch_id,
                    sub_id=sub.sub_id,
                    layer=sub.layer,
                    available_quantity=sub.quantity,
                    created_day_index=sub.created_day_index,
                )
            )
    inventory.sort(key=lambda item: (LAYER_PRIORITY[item.layer], item.created_day_index, item.batch_id, item.sub_id))
    symbol_state.sellable_inventory = inventory
    symbol_state.opening_hedge_capacity_lots = sum(item.available_quantity for item in inventory) // 100
    symbol_state.remaining_hedge_capacity_lots = symbol_state.opening_hedge_capacity_lots


def consume_sellable_inventory(symbol_state: SymbolState, quantity: int) -> List[Tuple[str, str, str, int]]:
    remaining = quantity
    allocations = []
    for item in symbol_state.sellable_inventory:
        if remaining <= 0:
            break
        take = min(item.available_quantity, remaining)
        if take <= 0:
            continue
        item.available_quantity -= take
        remaining -= take
        allocations.append((item.batch_id, item.sub_id, item.layer, take))
    symbol_state.sellable_inventory = [item for item in symbol_state.sellable_inventory if item.available_quantity > 0]
    symbol_state.remaining_hedge_capacity_lots = sum(item.available_quantity for item in symbol_state.sellable_inventory) // 100
    return allocations


def consume_specific_sellable(symbol_state: SymbolState, batch_id: str, sub_id: str, quantity: int) -> int:
    remaining = quantity
    for item in symbol_state.sellable_inventory:
        if item.batch_id != batch_id or item.sub_id != sub_id:
            continue
        take = min(item.available_quantity, remaining)
        item.available_quantity -= take
        remaining -= take
        if remaining <= 0:
            break
    symbol_state.sellable_inventory = [item for item in symbol_state.sellable_inventory if item.available_quantity > 0]
    symbol_state.remaining_hedge_capacity_lots = sum(item.available_quantity for item in symbol_state.sellable_inventory) // 100
    return quantity - remaining


def apply_logical_exit(symbol_state: SymbolState, batch_id: str, sub_id: str, quantity: int) -> Tuple[int, float]:
    remaining = quantity
    avg_price = 0.0
    for batch in symbol_state.active_batches:
        if batch.batch_id != batch_id:
            continue
        for sub in batch.sub_positions:
            if sub.sub_id != sub_id:
                continue
            take = min(sub.quantity, remaining)
            if take <= 0:
                continue
            avg_price = sub.avg_price
            sub.quantity -= take
            remaining -= take
            if sub.quantity == 0:
                sub.status = "closed"
            break
        break
    prune_empty_batches(symbol_state)
    return quantity - remaining, avg_price


def restore_logical_quantity(symbol_state: SymbolState, batch_id: str, sub_id: str, quantity: int) -> None:
    for batch in symbol_state.active_batches:
        if batch.batch_id != batch_id:
            continue
        for sub in batch.sub_positions:
            if sub.sub_id == sub_id:
                sub.quantity += quantity
                if sub.quantity > 0:
                    sub.status = "active"
                return


def prune_empty_batches(symbol_state: SymbolState) -> None:
    pruned = []
    for batch in symbol_state.active_batches:
        batch.sub_positions = [sub for sub in batch.sub_positions if sub.quantity > 0]
        batch.total_quantity = sum(sub.quantity for sub in batch.sub_positions)
        if batch.total_quantity > 0:
            pruned.append(batch)
    symbol_state.active_batches = pruned


def compute_attempt_lots(symbol_state: SymbolState, current_price: float, available_cash: float, single_batch_cash: float, window: str, params: Dict[str, object]) -> int:
    if symbol_state.remaining_attempt_slots <= 0 or symbol_state.remaining_carry_capacity_lots <= 0:
        return 0
    if window == "am" and symbol_state.remaining_am_attempt_slots <= 0:
        return 0
    if window == "pm" and symbol_state.remaining_pm_attempt_slots <= 0:
        return 0

    dynamic_lots = max(1, floor(symbol_state.remaining_carry_capacity_lots / max(symbol_state.remaining_attempt_slots, 1)))
    affordable_lots = int(available_cash // (current_price * 100))
    if window in ("am", "pm"):
        remaining_window_cash = compute_window_cash_limit(single_batch_cash, window, params) - (
            symbol_state.today_am_buy_cash if window == "am" else symbol_state.today_pm_buy_cash
        )
        affordable_lots = min(affordable_lots, int(max(remaining_window_cash, 0.0) // (current_price * 100)))
    return max(min(dynamic_lots, affordable_lots, symbol_state.remaining_carry_capacity_lots), 0)


def reserve_carry_capacity(symbol_state: SymbolState, lots: int) -> None:
    symbol_state.remaining_carry_capacity_lots = max(symbol_state.remaining_carry_capacity_lots - lots, 0)


def release_carry_capacity(symbol_state: SymbolState, lots: int) -> None:
    symbol_state.remaining_carry_capacity_lots += lots
    symbol_state.remaining_carry_capacity_lots = min(symbol_state.remaining_carry_capacity_lots, symbol_state.daily_carry_capacity_lots)


def forward_sell_candidates(symbol_state: SymbolState, minute_high: float) -> None:
    for item in symbol_state.pending_t_buys:
        if item.unhedged_quantity <= 0:
            continue
        if item.sell_submitted:
            continue
        if minute_high >= item.sell_trigger_price:
            item.sell_armed = True


def reversal_triggered(symbol_state: SymbolState, minute_low: float) -> bool:
    if symbol_state.prev_minute_low is None:
        return False
    return minute_low < symbol_state.prev_minute_low


def armed_forward_items(symbol_state: SymbolState) -> List[PendingTBuy]:
    items = [
        item for item in symbol_state.pending_t_buys
        if item.sell_armed and not item.sell_submitted and item.unhedged_quantity > 0
    ]
    items.sort(key=lambda item: (item.sell_trigger_price, item.buy_dt))
    return items


def cleanup_hedged_pending_items(symbol_state: SymbolState) -> None:
    symbol_state.pending_t_buys = [item for item in symbol_state.pending_t_buys if item.unhedged_quantity > 0]


def merge_failed_pending_buys(symbol_state: SymbolState, batch_id: str, created_dt: str, created_day_index: int) -> Optional[BatchState]:
    leftovers = [item for item in symbol_state.pending_t_buys if item.unhedged_quantity > 0]
    if not leftovers:
        return None
    total_quantity = sum(item.unhedged_quantity for item in leftovers)
    if total_quantity < 100:
        symbol_state.pending_t_buys = []
        return None
    total_cost = sum(item.unhedged_quantity * item.avg_price for item in leftovers)
    avg_price = total_cost / total_quantity
    batch_source = leftovers[0].batch_source if len({item.batch_source for item in leftovers}) == 1 else "t_failed"
    batch = build_batch(
        order_book_id=symbol_state.order_book_id,
        batch_id=batch_id,
        source=batch_source,
        created_dt=created_dt,
        created_day_index=created_day_index,
        quantity=total_quantity,
        avg_price=avg_price,
    )
    symbol_state.pending_t_buys = []
    symbol_state.today_net_added_cash += total_quantity * avg_price
    symbol_state.today_t_failed_cash += total_quantity * avg_price
    return batch


def pending_buy_cost(symbol_state: SymbolState) -> float:
    return sum(item.unhedged_quantity * item.avg_price for item in symbol_state.pending_t_buys)


def logical_position_cost(symbol_state: SymbolState) -> float:
    return sum(sub.quantity * sub.avg_price for batch in symbol_state.active_batches for sub in batch.sub_positions)


def available_budget_cash(capital, symbols: Iterable[SymbolState]) -> float:
    active_cost = sum(logical_position_cost(symbol) + pending_buy_cost(symbol) for symbol in symbols)
    return max(capital.unlocked_cash_limit() - active_cost, 0.0)


def should_freeze(current_price: float, three_day_low_snapshot: float, freeze_break_ratio: float) -> bool:
    if three_day_low_snapshot <= 0:
        return False
    return current_price < three_day_low_snapshot * freeze_break_ratio


def update_freeze_after_day(symbol_state: SymbolState, day_low: Optional[float], params: Dict[str, object]) -> None:
    if symbol_state.buy_permission_today:
        return
    if day_low is None:
        return
    if symbol_state.freeze_anchor_low is None:
        symbol_state.freeze_anchor_low = day_low
        symbol_state.freeze_stable_days = 0
        return
    if day_low < symbol_state.freeze_anchor_low:
        symbol_state.freeze_anchor_low = day_low
        symbol_state.freeze_stable_days = 0
        return
    symbol_state.freeze_stable_days += 1
    if symbol_state.freeze_stable_days >= int(params["reopen_stable_days"]):
        symbol_state.buy_permission_persistent = True
        symbol_state.freeze_anchor_low = None
        symbol_state.freeze_stable_days = 0


def compute_exit_decision(sub: SubPositionState, current_price: float, current_day_index: int, params: Dict[str, object]) -> ExitDecision:
    holding_days = current_day_index - sub.created_day_index + 1
    avg_price = sub.avg_price
    if sub.layer == "short":
        if holding_days <= int(params["short_target_days"]):
            return ExitDecision(current_price >= avg_price * (1.0 + float(params["short_target_pct"])), "take_profit", avg_price * (1.0 + float(params["short_target_pct"])))
        if holding_days <= int(params["short_breakeven_end_days"]):
            return ExitDecision(current_price >= avg_price, "breakeven_exit", avg_price)
        return ExitDecision(current_price <= avg_price * (1.0 + float(params["short_final_stop_pct"])), "controlled_loss_exit", avg_price * (1.0 + float(params["short_final_stop_pct"])))

    if sub.layer == "mid":
        for end_days, target in params["mid_schedule"]:
            if holding_days <= int(end_days):
                if target == "breakeven_plus_fee":
                    return ExitDecision(current_price >= avg_price, "breakeven_exit", avg_price)
                target_price = avg_price * (1.0 + float(target))
                return ExitDecision(current_price >= target_price, "take_profit", target_price)
        stop_price = avg_price * (1.0 + float(params["mid_final_stop_pct"]))
        return ExitDecision(current_price <= stop_price, "controlled_loss_exit", stop_price)

    # long
    if not sub.long_take_profit_1_done:
        target_1 = avg_price * (1.0 + float(params["long_take_profit_1_pct"]))
        return ExitDecision(current_price >= target_1, "take_profit", target_1)
    target_2 = avg_price * (1.0 + float(params["long_take_profit_2_pct"]))
    if holding_days <= int(params["long_fixed_target_days"]):
        return ExitDecision(current_price >= target_2, "take_profit", target_2)
    if holding_days >= int(params["long_final_stop_days"]):
        stop_price = avg_price * (1.0 + float(params["long_final_stop_pct"]))
        return ExitDecision(current_price <= stop_price, "controlled_loss_exit", stop_price)
    if holding_days >= int(params["long_breakeven_start_days"]):
        return ExitDecision(current_price >= avg_price, "breakeven_exit", avg_price)
    decay_steps = max((holding_days - int(params["long_fixed_target_days"])) // int(params["long_decay_interval_days"]), 0)
    decayed_target = float(params["long_take_profit_2_pct"]) - decay_steps * float(params["long_decay_step_pct"])
    decayed_target = max(decayed_target, float(params["long_target_floor_pct"]))
    target_price = avg_price * (1.0 + decayed_target)
    return ExitDecision(current_price >= target_price, "take_profit", target_price)


def reverse_t_eligible(sub: SubPositionState, current_price: float, current_day_index: int, half_year_cursor_rate: float, params: Dict[str, object]) -> Tuple[bool, ExitDecision]:
    decision = compute_exit_decision(sub, current_price, current_day_index, params)
    eligible = (
        decision.should_exit
        and decision.stage in {"breakeven_exit", "controlled_loss_exit"}
        and half_year_cursor_rate > 70.0
        and current_price >= decision.expected_exit_price
    )
    return eligible, decision


def update_reverse_pending_state(item: PendingReverseTSell, minute_low: float, minute_high: float, params: Dict[str, object]) -> None:
    if not item.target_drop_reached:
        item.local_low = min(item.local_low, minute_low)
        if minute_low <= item.rebuy_trigger_price:
            item.target_drop_reached = True
            item.ref_low = minute_low
            item.local_low = minute_low
        return

    item.local_low = min(item.local_low, minute_low)
    if minute_high >= item.local_low * (1.0 + float(params["reverse_rebound_pct"])):
        item.rebound_confirmed = True


def reverse_buy_triggered(item: PendingReverseTSell) -> bool:
    return item.target_drop_reached and item.rebound_confirmed and not item.buyback_submitted
