from setup import FinanceSetup
from extractors import FinvizScraper, YahooScraper
from cleaners import DataCleaner
import datetime
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

if __name__ == '__main__':
    now = datetime.datetime.now()

    logging.info('Setting up the stocks to pull')
    FinanceSetup(now).run()

    logging.info('Getting data from Finviz on the stocks')
    FinvizScraper(now).run()

    logging.info('Getting data from Yahoo on the stocks')
    YahooScraper(now).run()

    logging.info('Cleaning up the stock data and writing to json file')
    DataCleaner(now).run()