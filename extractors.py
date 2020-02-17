from bs4 import BeautifulSoup
import requests
import logging
import datetime

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class BaseScraper():

    def __init__(self, date):
        self.date = date

    @property
    def symbols(self):
        dest = 'symbols/stocks_' + str(self.date.strftime('%Y_%m_%d')) + '.txt'
        f = open(dest, 'r')
        symbols = []

        for line in f:
            line_contents = line.split('|')
            symbols.append(line_contents[0])

        return symbols

    @property
    def invalids_file(self):
        invalids_file = open('symbols/invalid_symbols.txt', 'a+')
        return invalids_file

    @staticmethod
    def list_writer(self, destination, content):
        while content:
            text = content.pop(0)
            if content:
                text += '|'
            destination.write(text)
        destination.write('\n')


class FinvizScraper(BaseScraper):

    @property
    def finviz_stats_file(self):
        date = str(self.date.strftime('%Y_%m_%d'))
        finviz_stats_file = open('raw_data/finviz_stats_' + date + '.txt', 'a+')
        return finviz_stats_file

    @property
    def finviz_profile_file(self):
        date = str(self.date.strftime('%Y_%m_%d'))
        finviz_profile_file = open('raw_data/finviz_profile_' + date + '.txt', 'a+')
        return finviz_profile_file

    def finviz_caller(self, symbol, return_headers=False):
        logging.info('Making Finviz request for %s', symbol)
        req = requests.get('https://finviz.com/quote.ashx?t={symbol}'.format(
            symbol=symbol)
        )

        if req.status_code != 200:
            logging.info('Request for {symbol} returned {status_code}'.format(
                symbol=symbol,
                status_code=req.status_code
            ))
            self.invalids_file.write(symbol + '\n')
        else:
            soup = BeautifulSoup(req.content, 'html.parser')
            table = soup.find_all(lambda tag: tag.name == 'table')
            stats_rows = table[8].findAll(lambda tag: tag.name == 'tr')
            profile_rows = table[6].findAll(lambda tag: tag.name == 'a')

            out = []

            try:
                for i in range(len(stats_rows)):
                    td = stats_rows[i].find_all('td')
                    out = out + [x.text.strip() for x in td]

                profile_out = [a.text for a in profile_rows]
                stats_out = [symbol] + out[1::2]

                if return_headers:
                    stats_headers = ['Ticker'] + out[::2]
                    stats_headers[27] = 'EPS Growth Next Y'

                    out_stats_headers = [stats_headers[k] for k in range(len(stats_headers))]
                    out_profile_headers = ['Ticker', 'Name', 'Sector', 'Industry', 'Country']

                    self.list_writer(self, self.finviz_profile_file, out_profile_headers)
                    self.list_writer(self, self.finviz_stats_file, out_stats_headers)

                self.list_writer(self, self.finviz_profile_file, profile_out)
                self.list_writer(self, self.finviz_stats_file, stats_out)

                logging.info('Successfully pulled Finviz data for {symbol}'.format(symbol=symbol))

            except:
                logging.info('Some issue with processing {symbol}'.format(symbol=symbol))
                self.invalids_file.write(symbol + '\n')

    def run(self):

        for symbol in self.symbols:
            if symbol == self.symbols[0]:
                self.finviz_caller(symbol, return_headers=True)
            else:
                self.finviz_caller(symbol)


class YahooScrapper(BaseScraper):

    @property
    def yahoo_invalid_symbols(self):
        # Yahoo has its own set of invalid symbols
        dest = 'symbols/yahoo_invalid_symbols.txt'
        f = open(dest, 'r')
        yahoo_invalid_symbols = []

        for symbol in f:
            yahoo_invalid_symbols.append(symbol.rstrip('\n'))

        return yahoo_invalid_symbols

    @property
    def yahoo_invalids_file(self):
        yahoo_invalids_file = open('symbols/yahoo_invalid_symbols.txt', 'a')
        return yahoo_invalids_file

    @property
    def yahoo_ev_stats_file(self):
        date = str(self.date.strftime('%Y_%m_%d'))
        yahoo_ev_stats_file = open('raw_data/yahoo_ev_stats_' + date + '.txt', 'a+')
        return yahoo_ev_stats_file

    def yahoo_caller(self, symbol, return_headers=False):
        logging.info('Making Yahoo request for %s', symbol)
        req = requests.get('https://finance.yahoo.com/quote/{symbol}/key-statistics?'.format(
            symbol=symbol)
        )

        if req.status_code != 200:
            logging.info('Request for {symbol} returned {status_code}'.format(
                symbol=symbol,
                status_code=req.status_code
            ))
            self.yahoo_invalids_file.write(symbol + '\n')
        elif 'key-statistics' not in str(req.url):
            logging.info('Yahoo does not have key-statistics for {symbol}'.format(
                symbol=symbol,
                status_code=req.status_code
            ))
            self.yahoo_invalids_file.write(symbol + '\n')
        else:
            try:
                soup = BeautifulSoup(req.content, 'html.parser')
                table = soup.find_all(lambda tag: tag.name == 'table')
                rows = table[0].findAll(lambda tag: tag.name == 'tr')
                td = rows[-1].find_all('td')
                evvalue = td[-1].text

                out = [symbol, evvalue]

                if return_headers:
                    ev_headers = ['Ticker', 'EVEBITDA']
                    self.list_writer(self, self.yahoo_ev_stats_file, ev_headers)

                self.list_writer(self, self.yahoo_ev_stats_file, out)
                logging.info('Successfully pulled Yahoo data for {symbol}'.format(symbol=symbol))

            except:
                logging.info('Some issue with processing {symbol}'.format(symbol=symbol))
                self.list_writer(self, self.invalids_file, [symbol])

    def run(self):
        for symbol in self.symbols:
            if symbol not in self.yahoo_invalid_symbols:
                if symbol == self.symbols[0]:
                    self.yahoo_caller(symbol, return_headers=True)
                else:
                    self.yahoo_caller(symbol)


if __name__ == '__main__':
    now = datetime.datetime.now()
    FinvizScraper(now).run()
    YahooScrapper(now).run()