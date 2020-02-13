import requests
import shutil
import datetime

NASDAQ = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt'
OTHER = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'


def handle():
    get_tickers()
    move_stocks_file('stocks.txt')


def get_tickers():
    """
    Pull list of all Nasdaq tickers and save to a file
    """
    n_req = requests.get(NASDAQ)
    o_req = requests.get(OTHER)
    stocks = []

    with open('nasdaq_stocks.txt', 'wb') as f:
        f.write(n_req.content)

    with open('other_stocks.txt', 'wb') as f:
        f.write(o_req.content)

    with open('stocks.txt', 'w') as s:
        with open('nasdaq_stocks.txt') as f:
            for cnt, line in enumerate(f):
                row = (line.split('|'))
                if row[1] not in ('Symbol', 'Security Name', '') \
                        and len(row[1]) < 8:
                    output = [row[1], row[2]]
                    s.write(str(output) + '\n')
                    stocks.append(row[1])

        with open('other_stocks.txt') as f:
            for cnt, line in enumerate(f):
                row = (line.split('|'))
                if row[0] not in ('Symbol', 'Security Name', '') \
                        and row[0] not in stocks\
                        and len(row[0]) < 8:
                    output = [row[0], row[1]]
                    s.write(str(output) + '\n')
                    stocks.append(row[0])


def move_stocks_file(stocks):
    """
    Copy stocks file into symbole directory along with
    its timestamp
    """
    now = datetime.datetime.now()
    dest = 'symbols/stocks_' + str(now.strftime('%Y_%m_%d')) + '.txt'
    print(dest)

    f = open(dest, "w")
    f.close()

    return shutil.copyfile(stocks, dest)


if __name__ == '__main__':
    get_tickers()
    move_stocks_file('stocks.txt')

