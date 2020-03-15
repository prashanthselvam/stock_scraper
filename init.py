import datetime
import logging
import argparse
import papermill as pm

from setup import FinanceSetup
from extractors import FinvizScraper, YahooScraper
from cleaners import DataCleaner
from gcloud_transfer import GCLMover


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def get_date():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', default=datetime.datetime.now())
    args = parser.parse_args()

    date = args.d

    if not type(date) == datetime.datetime:
        date = datetime.datetime.strptime(args.d, '%Y-%m-%d')

    return date


if __name__ == '__main__':
    date = get_date()

    logging.info('Setting up the stocks to pull')
    FinanceSetup(date).run()

    logging.info('Getting data from Finviz on the stocks')
    FinvizScraper(date).run()

    logging.info('Getting data from Yahoo on the stocks')
    YahooScraper(date).run()

    logging.info('Cleaning up the stock data and writing to json file')
    DataCleaner(date).run()

    logging.info("Run the O'Shaughnessy Trending Value analysis")
    pm.execute_notebook(
        'analysis.ipynb',
        'analysis.ipynb',
        parameters=dict(date=str(date.strftime('%Y_%m_%d')))
    )

    logging.info("Transferring files over to Google Cloud Storage")
    GCLMover().run()

    logging.info('End-to-end process complete for {date}'.format(
        date=str(date.strftime('%Y_%m_%d'))
    ))
