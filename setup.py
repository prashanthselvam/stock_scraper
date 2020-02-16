import requests
import shutil
import datetime
import logging

NASDAQ = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt'
OTHER = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def get_tickers():
    """
    Pull list of all Nasdaq tickers and save to a file
    """
    n_req = requests.get(NASDAQ)
    o_req = requests.get(OTHER)
    stocks = []
    invalid_symbols = load_invalids()

    logging.info('Writing NASDAQ tickers to nasdaq_stocks.txt')
    with open('nasdaq_stocks.txt', 'wb') as f:
        f.write(n_req.content)

    logging.info('Writing other tickers to other_stocks.txt')
    with open('other_stocks.txt', 'wb') as f:
        f.write(o_req.content)

    logging.info('Combining valid tickers into stocks.txt')
    with open('stocks.txt', 'w') as s:
        with open('nasdaq_stocks.txt') as f:
            for cnt, line in enumerate(f):
                row = (line.split('|'))
                if row[1] not in ('Symbol', 'Security Name', '') \
                        and len(row[1]) < 8\
                        and row[1] not in invalid_symbols:
                    output = '{stock}|{name}'.format(
                        stock=row[1],
                        name=row[2])
                    s.write(str(output) + '\n')
                    stocks.append(row[1])

        with open('other_stocks.txt') as f:
            for cnt, line in enumerate(f):
                row = (line.split('|'))
                if row[0] not in ('Symbol', 'Security Name', '') \
                        and row[0] not in stocks\
                        and len(row[0]) < 8\
                        and row[0] not in invalid_symbols:
                    output = '{stock}|{name}'.format(
                        stock=row[0],
                        name=row[1])
                    s.write(str(output) + '\n')
                    stocks.append(row[0])


def load_invalids():
    invalid_symbols = []
    f = open('symbols/invalid_symbols.txt', 'r')

    for line in f:
        invalid_symbols.append(line.rstrip('\n'))

    return invalid_symbols


def move_stocks_file(stocks):
    """
    Copy stocks file into symbols directory along with
    its timestamp
    """
    now = datetime.datetime.now()
    dest = 'symbols/stocks_' + str(now.strftime('%Y_%m_%d')) + '.txt'

    logging.info('Copying stocks.txt file to {dest}'.format(dest=dest))
    f = open(dest, "w")
    f.close()

    return shutil.copyfile(stocks, dest)


if __name__ == '__main__':
    get_tickers()
    move_stocks_file('stocks.txt')