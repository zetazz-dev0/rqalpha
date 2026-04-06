from rqalpha.apis import *


def init(context):
    context.s1 = "600519.XSHG"
    update_universe(context.s1)
    context.has_ordered = False


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    closes = history_bars(context.s1, 5, "1m", "close", include_now=True)
    if closes is None or len(closes) == 0:
        return

    if not context.has_ordered:
        logger.info(
            "minute probe dt={}, close={}, last5={}".format(
                context.now, bar_dict[context.s1].close, [round(float(x), 2) for x in closes]
            )
        )
        order_shares(context.s1, 100)
        context.has_ordered = True


def after_trading(context):
    pass
