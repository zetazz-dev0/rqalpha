from rqalpha.apis import *

import numpy as np


RAW_CODES = [
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


def to_order_book_id(code):
    if code.startswith("6"):
        return "{}.XSHG".format(code)
    return "{}.XSHE".format(code)


WATCHLIST = [to_order_book_id(code) for code in RAW_CODES]


def init(context):
    context.watchlist = WATCHLIST
    context.lookback = 60
    context.top_n = 5
    context.rebalance_interval = 20
    context.day_count = 0

    update_universe(context.watchlist)
    logger.info("watchlist size: {}".format(len(context.watchlist)))


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    context.day_count += 1
    if context.day_count % context.rebalance_interval != 0:
        return

    scored = []
    for order_book_id in context.watchlist:
        closes = history_bars(order_book_id, context.lookback + 1, "1d", "close")
        if closes is None or len(closes) < context.lookback + 1:
            continue
        if np.isnan(closes).any() or closes[0] <= 0:
            continue

        momentum = closes[-1] / closes[0] - 1.0
        scored.append((order_book_id, momentum))

    if not scored:
        logger.warning("rebalance skipped: no enough history data")
        return

    scored.sort(key=lambda item: item[1], reverse=True)
    selected = [item[0] for item in scored[: context.top_n]]
    target_weight = 1.0 / len(selected)

    for order_book_id in context.watchlist:
        if order_book_id in selected:
            order_target_percent(order_book_id, target_weight)
            continue

        # Avoid placing redundant 0-share sell orders on empty positions.
        if get_position(order_book_id).quantity > 0:
            order_target_percent(order_book_id, 0)

    logger.info("rebalance holdings: {}".format(selected))


def after_trading(context):
    pass
