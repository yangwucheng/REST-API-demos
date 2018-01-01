from pymongo import MongoClient

from HuobiService import get_kline, get_symbols
from constants import HUO_BI_EXCHANGE

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    market_data = client.market_data
    symbols_collection = market_data.symbols

    try:
        symbols = get_symbols()['data']
        for symbol in symbols:
            symbol['exchange'] = HUO_BI_EXCHANGE
            symbols_collection.replace_one({
                'base-currency':symbol['base-currency'],
                'quote-currency':symbol['quote-currency'],
                'symbol-partition': symbol['symbol-partition']
            }, symbol, True)
    except Exception, e:
        print e
