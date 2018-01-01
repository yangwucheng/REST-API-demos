import pymongo
from pymongo import MongoClient

from HuobiService import get_kline, get_symbols
from constants import HUO_BI_EXCHANGE

if __name__ == '__main__':
    # print get_symbols()
    client = MongoClient('localhost', 27017)
    market_data = client.market_data
    symbols_collection = market_data.symbols

    symbols = symbols_collection.find({'exchange':HUO_BI_EXCHANGE})
    periods = ('1min', '5min', '15min', '30min', '60min', '1day', '1mon', '1week', '1year')
    for symbol in symbols:
        for period in periods:
            try:
                pair = symbol['base-currency'] + symbol['quote-currency']
                klines = get_kline(pair, period, 2000)['data']
                for kline in klines:
                    kline['symbol'] = pair
                    kline_collection = pymongo.collection.Collection(market_data, 'huobi_kline_' + period)
                    kline_collection.replace_one({'symbol':pair, 'id':kline['id']}, kline, True)
            except Exception, e:
                print e
