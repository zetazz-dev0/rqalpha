from rqalpha.apis import *


def init(context):
    context.s1 = "600519.XSHG"
    context.fired = False
    update_universe(context.s1)


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    if context.fired:
        return
    order_percent(context.s1, 1.0)
    context.fired = True
    logger.info("buy and hold: {}".format(context.s1))


def after_trading(context):
    pass
