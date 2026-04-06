from __future__ import annotations

from copy import deepcopy


UNIVERSE = [
    "000725.XSHE",
    "600600.XSHG",
    "600059.XSHG",
    "600887.XSHG",
    "000895.XSHE",
    "600315.XSHG",
    "601888.XSHG",
    "600138.XSHG",
    "002033.XSHE",
    "000069.XSHE",
    "600535.XSHG",
    "000423.XSHE",
    "600436.XSHG",
    "000538.XSHE",
    "600085.XSHG",
    "600332.XSHG",
    "600276.XSHG",
    "600161.XSHG",
    "300122.XSHE",
    "300142.XSHE",
    "600111.XSHG",
    "600456.XSHG",
    "601088.XSHG",
    "601318.XSHG",
    "600030.XSHG",
    "600036.XSHG",
    "600016.XSHG",
    "600000.XSHG",
]


GLOBAL_DEFAULT_PARAMS = {
    "overall_cursor_threshold": 50.0,
    "day0_half_year_threshold": 15.0,
    "day1_half_year_threshold": 30.0,
    "overall_cursor_lookback_bars": 1250,
    "half_year_cursor_lookback_bars": 122,
    "forward_t_target_pct": 0.02,
    "forward_drop_pct": 0.006,
    "forward_rebound_pct": 0.003,
    "reverse_t_target_pct": 0.015,
    "reverse_rebound_pct": 0.003,
    "max_attempt_count": 10,
    "am_max_attempt_count": 4,
    "pm_max_attempt_count": 6,
    "freeze_break_ratio": 0.95,
    "freeze_lookback_days": 3,
    "reopen_stable_days": 4,
    "am_buy_fraction": 0.5,
    "pm_buy_fraction": 0.5,
    "am_window_start": 935,
    "am_window_end": 1128,
    "pm_window_start": 1300,
    "pm_window_end": 1448,
    "tail_window_start": 1450,
    "tail_window_end": 1500,
    "short_target_pct": 0.10,
    "short_target_days": 10,
    "short_breakeven_end_days": 20,
    "short_final_stop_pct": -0.05,
    "mid_schedule": [
        (20, 0.30),
        (40, 0.20),
        (250, 0.10),
        (500, "breakeven_plus_fee"),
    ],
    "mid_final_stop_pct": -0.10,
    "long_take_profit_1_pct": 0.50,
    "long_take_profit_2_pct": 1.00,
    "long_fixed_target_days": 750,
    "long_decay_interval_days": 20,
    "long_decay_step_pct": 0.05,
    "long_target_floor_pct": 0.30,
    "long_breakeven_start_days": 1250,
    "long_final_stop_days": 1500,
    "long_final_stop_pct": -0.10,
}


DEFAULT_SYMBOL_OVERRIDES = {}


def resolve_symbol_params(order_book_id, symbol_overrides=None):
    params = deepcopy(GLOBAL_DEFAULT_PARAMS)
    if symbol_overrides:
        params.update(symbol_overrides.get(order_book_id, {}))
    return params
