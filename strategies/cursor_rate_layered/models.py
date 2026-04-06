from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class SignalSnapshot:
    order_book_id: str
    prev_close: float = 0.0
    liquidity: float = 0.0
    overall_cursor_rate: float = 100.0
    half_year_cursor_rate: float = 100.0
    three_day_low_snapshot: float = 0.0
    is_day0_signal: bool = False
    is_day1_signal: bool = False


@dataclass
class SellableLot:
    batch_id: str
    sub_id: str
    layer: str
    available_quantity: int
    created_day_index: int


@dataclass
class PendingTBuy:
    pending_id: str
    source: str
    batch_source: str
    buy_day_index: int
    buy_dt: str
    quantity: int
    avg_price: float
    sell_trigger_price: float
    hedged_quantity: int = 0
    sell_armed: bool = False
    sell_submitted: bool = False
    carry_reserved_lots: int = 0

    @property
    def unhedged_quantity(self) -> int:
        return max(self.quantity - self.hedged_quantity, 0)


@dataclass
class PendingReverseTSell:
    pending_id: str
    source_batch_id: str
    source_sub_id: str
    source_layer: str
    source_avg_price: float
    quantity: int
    sell_avg_price: float
    sell_dt: str
    sell_day_index: int
    rebuy_trigger_price: float
    local_low: float
    target_drop_reached: bool = False
    rebound_confirmed: bool = False
    buyback_submitted: bool = False
    ref_low: float = 0.0


@dataclass
class SubPositionState:
    sub_id: str
    batch_id: str
    layer: str
    weight: float
    quantity: int
    avg_price: float
    created_day_index: int
    created_date: str
    status: str = "active"
    realized_pnl: float = 0.0
    long_take_profit_1_done: bool = False


@dataclass
class BatchState:
    batch_id: str
    order_book_id: str
    source: str
    created_dt: str
    created_day_index: int
    batch_cash: float
    avg_price: float
    total_quantity: int
    sub_positions: List[SubPositionState] = field(default_factory=list)


@dataclass
class SymbolState:
    order_book_id: str
    lifecycle_state: str = "IDLE"
    buy_permission_persistent: bool = True
    buy_permission_today: bool = True
    signal: SignalSnapshot = field(default_factory=lambda: SignalSnapshot(order_book_id=""))
    freeze_anchor_low: Optional[float] = None
    freeze_stable_days: int = 0
    daily_carry_capacity_lots: int = 0
    remaining_carry_capacity_lots: int = 0
    opening_hedge_capacity_lots: int = 0
    remaining_hedge_capacity_lots: int = 0
    today_max_attempt_count: int = 0
    remaining_attempt_slots: int = 0
    remaining_am_attempt_slots: int = 0
    remaining_pm_attempt_slots: int = 0
    today_am_buy_cash: float = 0.0
    today_pm_buy_cash: float = 0.0
    today_gross_buy_cash: float = 0.0
    today_net_added_cash: float = 0.0
    today_t_success_cash: float = 0.0
    today_t_success_pnl: float = 0.0
    today_t_failed_cash: float = 0.0
    local_low: Optional[float] = None
    rebound_confirmed: bool = False
    ref_high: Optional[float] = None
    prev_minute_low: Optional[float] = None
    today_intraday_low: Optional[float] = None
    pending_t_buys: List[PendingTBuy] = field(default_factory=list)
    pending_reverse_t_sells: List[PendingReverseTSell] = field(default_factory=list)
    active_batches: List[BatchState] = field(default_factory=list)
    sellable_inventory: List[SellableLot] = field(default_factory=list)

    def has_active_position(self) -> bool:
        return any(sub.quantity > 0 for batch in self.active_batches for sub in batch.sub_positions)

    def active_quantity(self) -> int:
        return sum(sub.quantity for batch in self.active_batches for sub in batch.sub_positions)

    def broker_expected_quantity(self) -> int:
        return self.active_quantity() + sum(item.unhedged_quantity for item in self.pending_t_buys)


@dataclass
class GlobalCapitalState:
    total_cash: float
    input_cash_total: float
    single_batch_cash: float
    reserve_tier_cash: float
    active_reserve_tier_count: int = 0
    realized_t_pnl_total: float = 0.0
    realized_exit_pnl_total: float = 0.0

    def unlocked_cash_limit(self) -> float:
        return (
            self.input_cash_total
            + self.active_reserve_tier_count * self.reserve_tier_cash
            + self.realized_t_pnl_total
            + self.realized_exit_pnl_total
        )


@dataclass
class GlobalState:
    capital: GlobalCapitalState
    symbols: Dict[str, SymbolState]
    current_day_index: int = 0


@dataclass
class OrderIntent:
    intent_type: str
    order_book_id: str
    quantity: int
    lots: int
    price_hint: float
    metadata: dict = field(default_factory=dict)
