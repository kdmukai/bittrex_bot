import argparse
import boto3
import configparser
import datetime
import json
import time

from decimal import Decimal

from bittrex.bittrex import Bittrex


def get_timestamp():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


parser = argparse.ArgumentParser(description='bittrex_bot.py -- simple DCA buying/selling bot')

# Required positional arguments
parser.add_argument('order_side', type=str,
                    help="BUY or SELL")
parser.add_argument('amount', type=float,
                    help="The quantity to buy or sell")
parser.add_argument('market_currency',
                    help="""The crypto to buy or sell (e.g. XLM)""")
parser.add_argument('base_currency',
                    help="""The base pairing (e.g. BTC) that will be spent or received""")

"""
    e.g. BUY 30 XLM BTC = Buy 30 XLM by spending (market price) BTC
    e.g. SELL 1000 HYDRO BTC = Sell 1000 HYDRO and receive (market price) BTC
"""

# Optional switches
parser.add_argument('-c', '--settings',
                    default="settings.conf",
                    dest="settings_config",
                    help="Override default settings config file location")
parser.add_argument('-warn_after',
                    default=3600,
                    action="store",
                    type=int,
                    dest="warn_after",
                    help="Seconds to wait before sending an alert that an order isn't done")


if __name__ == "__main__":
    args = parser.parse_args()

    market_currency = args.market_currency
    base_currency = args.base_currency
    amount = args.amount
    order_side = args.order_side.lower()
    market_name = "%s-%s" % (base_currency, market_currency)     # Bittrex lists base currency first (e.g. 'BTC-HYDRO')
    warn_after = args.warn_after

    if order_side not in ['buy', 'sell']:
        raise Exception("Invalid order_side: %s (must be BUY or SELL)" % order_side)

    # Read settings
    arg_config = configparser.ConfigParser()
    arg_config.read(args.settings_config)

    bittrex_key = arg_config.get('API_KEYS', 'BITTREX_KEY')
    bittrex_secret = arg_config.get('API_KEYS', 'BITTREX_SECRET')

    sns_topic = arg_config.get('AWS', 'SNS_TOPIC')
    aws_access_key_id = arg_config.get('AWS', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = arg_config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    # Prep boto SNS client for email notifications
    sns = boto3.client(
        "sns",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="us-east-1"     # N. Virginia
    )

    # out_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    my_bittrex = Bittrex(bittrex_key, bittrex_secret)  # defaulting to v1.1
    markets = my_bittrex.get_markets()['result']

    min_trade_size = None
    for market in markets:
        if market['MarketCurrency'] == market_currency and market['BaseCurrency'] == base_currency:
            min_trade_size = market['MinTradeSize']
            break

    if not min_trade_size:
        raise Exception("Market %s-%s not found" % (market_currency, base_currency))

    if amount < min_trade_size:
        raise Exception("'amount' is below the min_trade_size of %f" % min_trade_size)

    print("min_trade_size: %f" % min_trade_size)


    balances = my_bittrex.get_balances()['result']
    balances_report = "\nBalances:\n"
    for balance in balances:
        if balance["Currency"] in [market_currency, base_currency]:
            balances_report += "\t%s: %g\n" % (balance["Currency"], balance["Balance"])

    """
        Check the order book to calculate a market price
        {
          "success": true,
          "message": "''",
          "result": [
            {
              "buy": [
                {
                  "quantity": 12.37,
                  "rate": 32.55412402
                }
              ],
              "sell": [
                {
                  "quantity": 12.37,
                  "rate": 32.55412402
                }
              ]
            }
          ]
        }
    """
    order_book = my_bittrex.get_orderbook(market_name)['result']
    buy_rate = Decimal(order_book["buy"][0]["Rate"])
    sell_rate = Decimal(order_book["sell"][0]["Rate"])
    market_rate = Decimal(round((buy_rate + sell_rate) / Decimal('2.0'), 8))

    print("   buy_rate: %0.8f" % buy_rate)
    print("  sell_rate: %0.8f" % sell_rate)
    print("market_rate: %0.8f (before)" % market_rate)

    # Note: The minimum BTC trade value for orders is 50,000 Satoshis (0.0005)

    if order_side == 'sell':
        if sell_rate - buy_rate <= Decimal('0.00000001'):
            # There's no room to beat the existing sellers. Just take the buyer's price.
            print("Setting market_rate equal to %0.8f" % buy_rate)
            market_rate = buy_rate

        if market_rate * Decimal(amount) < Decimal('0.0005'):
            raise Exception("Sell order is too small (%0.6f BTC). Must be at least 0.0005 BTC" % (market_rate * Decimal(amount)))

        # {'success': True, 'message': '', 'result': {'uuid': '4d8c9832-0918-4d4c-a9c7-bc36124c5cb6'}}
        resp = my_bittrex.sell_limit(market_name, amount, market_rate)

        print(resp)

        if not resp.get('success'):
            raise Exception("Error placing order: %s" % resp)

        order_uuid = resp['result']['uuid']

        subject = "Sold %g %s @ %0.8f %s" % (amount, market_currency, market_rate, base_currency)

    else:
        raise Exception("Have not yet implemented the buy side!")

    '''
        Wait to see if the limit order was fulfilled.
    '''
    order_resp = my_bittrex.get_order(order_uuid)
    wait_time = 60
    total_wait_time = 0

    while (not order_resp['result']['Closed']):
        if total_wait_time > warn_after:
            sns.publish(
                TopicArn=sns_topic,
                Subject="%s %g %s OPEN/UNFILLED @ %0.8f %s" % (order_side, amount, market_currency, market_rate, base_currency),
                Message=str(order_resp)
            )
            exit()

        print("%s: Order %s still pending. Sleeping for %d (total %d)" % (
            get_timestamp(),
            order_uuid,
            wait_time,
            total_wait_time))
        time.sleep(wait_time)
        total_wait_time += wait_time
        order_resp = my_bittrex.get_order(order_uuid)

    report = "order_side: %s\n" % order_side
    report += "amount: %g %s\n" % (amount, market_currency)
    report += "buy_rate: %0.8f %s\n" % (buy_rate, base_currency)
    report += "sell_rate: %0.8f %s\n" % (sell_rate, base_currency)
    report += "market_rate: %0.8f %s\n" % (market_rate, base_currency)
    report += "TOTAL PROCEEDS: %0.8f %s\n" % (market_rate * Decimal(amount) * Decimal('.9975'), base_currency)

    print(report + balances_report)

    sns.publish(
        TopicArn=sns_topic,
        Subject=subject,
        Message=report + balances_report
    )

