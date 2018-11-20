import asyncio
import aiohttp
import requests
import json
import time
import yaml
import logging
import io

logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S',
                    level=logging.INFO)

# Get all the symbols, with information like buy and sell prices, fee,
# volume...
SYMBOLS_URL = 'https://kitchen.kucoin.com/v1/market/open/symbols'

# For trade precision
COINS_INFO_URL = 'https://kitchen.kucoin.com/v1/market/open/coins'

# Get open buy and sell orders in a specific market.
# If the market is NEO-ETH it means: Buy/sell NEO for ETH
OPEN_ORDERS_URL = 'https://kitchen.kucoin.com/v1/{}/open/orders?limit=1'

# 0.1% per operation
TRADING_FEE = 0.001

BUY_TEMPLATE = 'Buy  {} @ {} {} (vol: {} {})'
SELL_TEMPLATE = 'Sell {} @ {} {} (vol: {} {})'

def pprint_oporunities(ops):
    ret = io.StringIO('')
    for i in ops:
        ratio_reversed = (i[2][0] == 'BUY')
        cmp_val = i[1][2] * i[2][2] if not ratio_reversed \
                                        else i[1][2] / i[2][2]
        # Buy price
        percentage = cmp_val / i[0][2]
        ret.write('Oportunity: {} -> {} -> {} -> {} ({:.4f}%)\n'.format(
            i[0][1], i[1][1], i[2][1], i[0][1], percentage
            ))
        for o in i:
            if o[0] == 'BUY':
                ret.write(BUY_TEMPLATE.format(*o[1:]) + '\n')
            elif o[0] == 'SELL':
                ret.write(SELL_TEMPLATE.format(*o[1:]) + '\n')
            else:
                raise ValueError()
        ret.write('\n------------\n')
    return ret.getvalue()


class ArbitrageBot():
    def __init__(self, config_file='config.yaml'):
        self.loop = asyncio.new_event_loop()
        self._symbols = {}

        self.config_file = config_file
        self.load_config()

        ts = time.perf_counter()
        # Get the trade precision of all coins, sync http get may be slow but
        # it's just run once
        info = json.loads(self._get_url_sync(COINS_INFO_URL))
        if not info['success']:
            # TODO
            raise IOError()

        self._trade_precision = {}
        for coin in info['data']:
            coin_sym = coin['coin']
            trade_precision = coin['tradePrecision']
            self._trade_precision[coin_sym] = trade_precision
        t = time.perf_counter() - ts
        logging.info('Time downloading precision page: {:.2f}s'.format(t))

    def __del__(self):
        self.loop.close()

    def load_config(self):
        with open(self.config_file, 'r') as f:
            self.config = yaml.load(f)

    def min_percentage_to_trade(self):
        return self.config['bot']['min_percentage']

    def min_vol_to_trade(self, coin):
        vol_config = self.config['bot']['min_volume']
        if coin in vol_config.keys():
            return vol_config[coin]
        else:
            return None

    def active_coins(self):
        return self.config['bot']['active_coins']

    def get_csv_file(self):
        return self.config['bot']['csv_file']

    async def _get_url_async(self, url, session):
        async with session.get(url, timeout=10) as response:
            assert response.status == 200
            return await response.json()

    async def _get_urls_async(self, urls, session):
        pages = []
        tasks = [self.loop.create_task(self._get_url_async(url, session))
                 for url in urls]
        for t in tasks:
            pages.append(await t)
        return pages

    def _get_url_sync(self, url):
        return requests.get(url).text

    def _arbitrage_still_exists(self, data, ratio_reversed):
        '''
            data:
            [{'SELL': [[68.089535, 8.371073, 569.98246803]],
            'BUY': [[68.0, 0.186491, 12.681388]]}, {'SELL': [[0.00789916,
            0.674902, 0.00533116]], 'BUY': [[0.00787229, 1.0, 0.00787229]]},
            {'SELL': [[8684.996557, 0.06735591, 584.98584644]], 'BUY':
            [[8625.612438, 0.3, 2587.6837314]]}]
        '''
        # Data must be a list of 3 elements representing the 3 markets.

        buy = data[0]['SELL'][0][0]
        buy_other = data[1]['SELL'][0][0]
        sell = data[0]['BUY'][0][0]
        sell_other = data[1]['BUY'][0][0]
        ratio = [data[2]['SELL'][0][0], data[2]['BUY'][0][0]]

        cmp_val = sell_other * ratio[1] if not ratio_reversed \
                                        else sell_other / ratio[0]
        return buy, sell, buy_other, sell_other, cmp_val, (buy < cmp_val)

    async def _get_arbitrage_oportunities(self):
        arbitrage_oportunities = []
        session = aiohttp.ClientSession(loop=self.loop)
        for coin_pair, v in self._symbols.items():
            if coin_pair not in self.active_coins():
                # We don't want to trade with this coin according to the
                # configuration
                continue
            for coin, (buy, sell) in v.items():
                # Iterate through all the markets

                for other_coin_pair, v_other in self._symbols.items():
                    if coin not in v_other.keys():
                        # The other market we're looking at does not have the
                        # coin that we want
                        continue

                    # Buy and sell values in the other market
                    (buy_other, sell_other) = \
                        self._symbols[other_coin_pair][coin]

                    # Get the relationship between the pair coins of both
                    # markets
                    try:
                        # Here the price will be in coin_pair units
                        ratio = self._symbols[coin_pair][other_coin_pair]
                        ratio_reversed = False
                    except KeyError:
                        # The price will be in other_coin_pair units
                        try:
                            ratio = self._symbols[other_coin_pair][coin_pair]
                            ratio_reversed = True
                        except KeyError:
                            # The market does not exist
                            continue

                    # XXX: Check if it's really accurate
                    # If the coin pair is the other market coin, divide per
                    # it's buy price, if not, multiply by it's sell price.
                    cmp_val = sell_other * ratio[1] if not ratio_reversed \
                                                    else sell_other / ratio[0]

                    # Check for triangular arbitrage
                    if buy < cmp_val:
                        # TODO: Buy/sell operations
                        # Buy coin_pair for coin, sell coin for other_coin_pair
                        # sell other_coin_pair for coin_pair

                        market1 = coin + '-' + coin_pair
                        market2 = coin + '-' + other_coin_pair

                        if ratio_reversed:
                            market3 = coin_pair + '-' + other_coin_pair
                        else:
                            market3 = other_coin_pair + '-' + coin_pair

                        ts = time.perf_counter()
                        # Download the markets data asyncronously, it's
                        # usually x10 faster than syncronously :D
                        # Usually between 0.5 and 1.5s
                        data = await self._get_urls_async([
                                              OPEN_ORDERS_URL.format(market1),
                                              OPEN_ORDERS_URL.format(market2),
                                              OPEN_ORDERS_URL.format(market3)
                                              ], session)
                        t = time.perf_counter() - ts
                        logging.info('Time downloading data async: {:.2f}s'.format(t))

                        for d in data:
                            if not d['success']:
                                # TODO: Logging
                                logging.info('Continue because the data for the markets'
                                        ' {} -> {} -> {} can\'t returned an invalid'
                                        ' response'.format(coin_pair,
                                                           coin,
                                                           other_coin_pair))
                                continue

                        # Now data is a dict with two keys, SELL and BUY.
                        # The values are a list of lists, with the format:
                        # [price_coin_pair, volume_coin, volume_coin_pair]
                        data = [d['data'] for d in data]

                        buy, sell, buy_other, sell_other, cmp_val, ok = \
                                            self._arbitrage_still_exists(
                                                                data,
                                                                ratio_reversed)
                        # Check if arbitrage still exists
                        if not ok:
                            logging.info('Continue because arbitrage oportunity'
                                          ' doesn\'t exist anymore for markets'
                                          ' {} -> {} -> {}'.format(coin_pair,
                                                                   coin,
                                                                   other_coin_pair))
                            continue

                        # We have to look at the contrary operation to fill
                        # the order.
                        market1_volume = data[0]['SELL'][0][1]
                        market2_volume = data[1]['BUY'][0][1]
                        if ratio_reversed:
                            market3_volume = data[2]['SELL'][0][1]
                        else:
                            market3_volume = data[2]['BUY'][0][1]

                        percentage = cmp_val / buy

                        if percentage < self.min_percentage_to_trade():
                            # TODO: Delete this
                            logging.info('Continue because of percentage'
                                          ' ({:.4f}%) in markets {} -> {} -> {}'.format(
                                                                            percentage,
                                                                            coin_pair,
                                                                            coin,
                                                                            other_coin_pair))
                            continue

                        if ratio_reversed:
                            max_vol_to_buy = min(market1_volume,
                                                 market2_volume,
                                                 market3_volume / buy)
                        else:
                            max_vol_to_buy = min(market1_volume,
                                                 market2_volume,
                                                 market3_volume / sell_other)

                        # Convert the volume from coin to coin_pair, and check
                        # if it's an acceptable value (check the config file)
                        if ((max_vol_to_buy * buy)
                                < self.min_vol_to_trade(coin_pair)):
                            logging.info('Continue because of volume ({} {})'.format(
                                                                   max_vol_to_buy * buy,
                                                                   coin_pair))
                            continue

                        # Max volume will be saved in coin_pair units
                        self.save_to_csv(time.time(), coin_pair, coin,
                                         other_coin_pair, percentage,
                                         max_vol_to_buy * buy)

                        logging.info('Arbitrage: %s -> %s -> %s -> %s' % (coin_pair,
                                                               coin,
                                                               other_coin_pair,
                                                               coin_pair))
                        logging.info('Max vol %s %s' % (max_vol_to_buy, coin))
                        logging.info('Arbitrage oportunity! {:.4f}%'.format(
                                                                percentage))
                        logging.info(BUY_TEMPLATE.format(coin,
                                                  buy,
                                                  coin_pair,
                                                  market1_volume,
                                                  coin
                                                  ))
                        logging.info(SELL_TEMPLATE.format(coin,
                                                   sell_other,
                                                   other_coin_pair,
                                                   market2_volume,
                                                   coin))
                        arbitrage_oportunities.append([
                            ['BUY', coin, buy, coin_pair, market1_volume, coin],
                            ['SELL', coin, sell_other, other_coin_pair, market2_volume, coin]
                            ])
                        if ratio_reversed:
                            logging.info(BUY_TEMPLATE.format(coin_pair,
                                                      ratio[0],
                                                      other_coin_pair,
                                                      market3_volume,
                                                      coin_pair))
                            arbitrage_oportunities[-1].append(
                                ['BUY', coin_pair, ratio[0], other_coin_pair,
                                 market3_volume, coin_pair]
                                )
                        else:
                            logging.info(SELL_TEMPLATE.format(other_coin_pair,
                                                       ratio[1],
                                                       coin_pair,
                                                       market3_volume,
                                                       other_coin_pair))
                            arbitrage_oportunities[-1].append(
                                ['SELL', other_coin_pair, ratio[1], coin_pair,
                                 market3_volume, other_coin_pair]
                                )

                        logging.info('----------------------------')

        await session.close()
        return arbitrage_oportunities

    def _process_symbols(self, symbols):
        for sym in symbols:
            if not sym['trading']:
                continue

            try:
                # Coin pair is the base coin, it can be ETH, BTC, NEO, USDT or
                # KCS, the prices are in this coin
                coin_pair = sym['coinTypePair']

                # Coin is the coin to be traded
                coin = sym['coinType']

                # Best price of people buying in coin_pair units
                buy_price = sym['buy']

                # Best price of people selling in coin_pair units
                sell_price = sym['sell']
            except KeyError:
                # Sometime there's no 'buy' and 'sell' entries
                continue

            assert buy_price <= sell_price

            if coin_pair not in self._symbols.keys():
                self._symbols[coin_pair] = {}

            # Actually, buy price is the price for selling atm, and sell
            # price the price for buying.
            self._symbols[coin_pair][coin] = [sell_price, buy_price]

    def get_symbols(self):
        # ~2-3 seconds
        ret = json.loads(self._get_url_sync(SYMBOLS_URL))
        if not ret['success']:
            raise IOError('Find a better exception to raise')
        return ret['data']

    def save_to_csv(self, *args):
        with open(self.get_csv_file(), 'a+') as f:
            f.write(','.join(map(str, args)) + '\n')

    def run(self):
        self.load_config()
        tstart = time.perf_counter()
        sym = self.get_symbols()
        self._process_symbols(sym)
        t = time.perf_counter() - tstart
        logging.info('Time downloading the symbols page: {:.2f}s'.format(t))
        coroutine = self._get_arbitrage_oportunities()
        # XXX: This is really needed?
        asyncio.set_event_loop(self.loop)
        return self.loop.run_until_complete(coroutine)


if __name__ == '__main__':
    ab = ArbitrageBot()
    ab.run()
