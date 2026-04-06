import os
import sys


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from strategies.cursor_rate_layered.logic import (
    build_batch,
    consume_sellable_inventory,
    merge_failed_pending_buys,
    reverse_t_eligible,
    should_trigger_buy,
    update_buy_trigger_state,
    update_freeze_after_day,
    rebuild_sellable_inventory,
)
from strategies.cursor_rate_layered.models import PendingTBuy, SignalSnapshot, SymbolState, SubPositionState
from strategies.cursor_rate_layered.params import resolve_symbol_params


def test_buy_trigger_requires_rebound_then_pullback():
    state = SymbolState(order_book_id="000001.XSHE")
    state.signal = SignalSnapshot(order_book_id="000001.XSHE", prev_close=10.0)
    state.local_low = 10.0
    state.ref_high = 10.0
    state.rebound_confirmed = False

    update_buy_trigger_state(state, minute_low=9.8, minute_high=9.8, rebound_pct=0.003)
    assert not state.rebound_confirmed
    assert not should_trigger_buy(state, minute_low=9.8, drop_pct=0.006)

    update_buy_trigger_state(state, minute_low=9.8, minute_high=9.84, rebound_pct=0.003)
    assert state.rebound_confirmed
    assert state.ref_high == 9.84
    assert not should_trigger_buy(state, minute_low=9.80, drop_pct=0.006)
    assert should_trigger_buy(state, minute_low=9.77, drop_pct=0.006)


def test_new_low_invalidates_rebound_confirmation():
    state = SymbolState(order_book_id="000001.XSHE")
    state.local_low = 9.8
    state.ref_high = 9.84
    state.rebound_confirmed = True

    update_buy_trigger_state(state, minute_low=9.75, minute_high=9.82, rebound_pct=0.003)
    assert not state.rebound_confirmed
    assert state.local_low == 9.75
    assert state.ref_high == 9.75


def test_merge_failed_pending_buys_into_single_batch():
    state = SymbolState(order_book_id="000001.XSHE")
    state.pending_t_buys = [
        PendingTBuy(
            pending_id="a",
            source="buy_attempt",
            batch_source="day1_plus",
            buy_day_index=1,
            buy_dt="2026-03-03 10:00:00",
            quantity=100,
            avg_price=10.0,
            sell_trigger_price=10.2,
        ),
        PendingTBuy(
            pending_id="b",
            source="buy_attempt",
            batch_source="day1_plus",
            buy_day_index=1,
            buy_dt="2026-03-03 10:30:00",
            quantity=200,
            avg_price=8.0,
            sell_trigger_price=8.16,
        ),
    ]

    batch = merge_failed_pending_buys(
        state,
        batch_id="000001.XSHE-B00001",
        created_dt="2026-03-03 15:00:00",
        created_day_index=1,
    )

    assert batch is not None
    assert batch.total_quantity == 300
    assert round(batch.avg_price, 6) == round((100 * 10.0 + 200 * 8.0) / 300, 6)
    assert [sub.quantity for sub in batch.sub_positions] == [100, 100, 100]
    assert state.pending_t_buys == []
    assert state.today_net_added_cash > 0


def test_sellable_inventory_consumption_uses_layer_priority():
    state = SymbolState(order_book_id="000001.XSHE")
    batch = build_batch(
        order_book_id="000001.XSHE",
        batch_id="b1",
        source="day0",
        created_dt="2026-03-03 15:00:00",
        created_day_index=1,
        quantity=400,
        avg_price=10.0,
    )
    state.active_batches = [batch]
    rebuild_sellable_inventory(state)

    allocations = consume_sellable_inventory(state, 200)

    assert allocations == [
        ("b1", "b1:short", "short", 200),
    ]
    assert sum(item.available_quantity for item in state.sellable_inventory) == 200


def test_update_freeze_after_day_restores_after_four_stable_days():
    params = resolve_symbol_params("000001.XSHE")
    state = SymbolState(order_book_id="000001.XSHE")
    state.buy_permission_today = False
    state.buy_permission_persistent = False
    state.freeze_anchor_low = 9.5

    for idx in range(3):
        update_freeze_after_day(state, day_low=9.6, params=params)
        assert not state.buy_permission_persistent
        assert state.freeze_stable_days == idx + 1

    update_freeze_after_day(state, day_low=9.7, params=params)
    assert state.buy_permission_persistent
    assert state.freeze_anchor_low is None
    assert state.freeze_stable_days == 0


def test_reverse_t_eligible_at_controlled_loss_boundary():
    params = resolve_symbol_params("000001.XSHE")
    sub = SubPositionState(
        sub_id="b1:short",
        batch_id="b1",
        layer="short",
        weight=0.5,
        quantity=100,
        avg_price=10.0,
        created_day_index=1,
        created_date="2026-03-01",
    )

    eligible, decision = reverse_t_eligible(
        sub=sub,
        current_price=9.5,
        current_day_index=30,
        half_year_cursor_rate=75.0,
        params=params,
    )

    assert decision.stage == "controlled_loss_exit"
    assert round(decision.expected_exit_price, 2) == 9.5
    assert eligible is True
