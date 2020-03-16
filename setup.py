import requests
import shutil
import datetime
import logging
import os
import argparse

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

NASDAQ = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt'
OTHER = 'http://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'


class FinanceSetup():

    def __init__(self, date=datetime.datetime.now(), cron_path=''):
        self.date = date
        self.cron_path = cron_path

    @property
    def invalid_symbols(self):
        invalid_symbols = []
        path = os.path.join(self.cron_path, 'data/working_files/invalid_symbols.txt')
        f = open(path, 'r')

        for line in f:
            invalid_symbols.append(line.rstrip('\n'))

        return invalid_symbols

    def get_tickers(self):
        """
        Pull list of all Nasdaq tickers and save to a file
        """
        n_req = requests.get(NASDAQ)
        o_req = requests.get(OTHER)
        stocks = []

        logging.info('Writing NASDAQ tickers to nasdaq_stocks.txt')
        nasdaq_path = os.path.join(self.cron_path, 'data/working_files/nasdaq_stocks.txt')
        with open(nasdaq_path, 'wb') as f:
            f.write(n_req.content)

        logging.info('Writing other tickers to other_stocks.txt')
        other_path = os.path.join(self.cron_path, 'data/working_files/other_stocks.txt')
        with open(other_path, 'wb') as f:
            f.write(o_req.content)

        logging.info('Combining valid tickers into stocks.txt')
        stocks_path = os.path.join(self.cron_path, 'data/working_files/stocks.txt')
        with open(stocks_path, 'w') as s:
            with open(nasdaq_path) as f:
                for cnt, line in enumerate(f):
                    row = (line.split('|'))
                    if row[1] not in ('Symbol', 'Security Name', '') \
                            and len(row[1]) < 8\
                            and row[1] not in self.invalid_symbols:
                        output = '{stock}|{name}'.format(
                            stock=row[1],
                            name=row[2])
                        s.write(str(output) + '\n')
                        stocks.append(row[1])

            with open(other_path) as f:
                for cnt, line in enumerate(f):
                    row = (line.split('|'))
                    if row[0] not in ('Symbol', 'Security Name', '') \
                            and row[0] not in stocks\
                            and len(row[0]) < 8\
                            and row[0] not in self.invalid_symbols:
                        output = '{stock}|{name}'.format(
                            stock=row[0],
                            name=row[1])
                        s.write(str(output) + '\n')
                        stocks.append(row[0])

    def move_stocks_file(self):
        """
        Copy stocks file into symbols directory along with
        its timestamp
        """
        date = str(self.date.strftime('%Y_%m_%d'))
        origin = os.path.join(self.cron_path, 'data/working_files/stocks.txt')
        dest = os.path.join(self.cron_path, 'data/stocks/stocks_{date}.txt'.format(date=date))

        logging.info('Copying stocks.txt file to {dest}'.format(dest=dest))
        f = open(dest, "w")
        f.close()

        return shutil.copyfile(origin, dest)

    def run(self):
        self.get_tickers()
        self.move_stocks_file()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', default=datetime.datetime.now())
    parser.add_argument('-c', default='')
    args = parser.parse_args()

    date = args.d
    cron_path = args.c

    if not type(date) == datetime.datetime:
        date = datetime.datetime.strptime(args.d, '%Y-%m-%d')

    FinanceSetup(date, cron_path).run()