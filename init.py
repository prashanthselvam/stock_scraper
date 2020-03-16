import datetime
import logging
import argparse
import papermill as pm

from setup import FinanceSetup
from extractors import FinvizScraper, YahooScraper
from cleaners import DataCleaner
from gcloud_transfer import GCLMover


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', default=datetime.datetime.now())
    parser.add_argument('-c', default='')
    args = parser.parse_args()

    date = args.d
    cron_path = args.c

    if not type(date) == datetime.datetime:
        date = datetime.datetime.strptime(args.d, '%Y-%m-%d')

    return date, cron_path


if __name__ == '__main__':
    date, cron_path = get_args()

    logging.info('Setting up the stocks to pull')
    FinanceSetup(date, cron_path).run()

    logging.info('Getting data from Finviz on the stocks')
    FinvizScraper(date, cron_path).run()

    logging.info('Getting data from Yahoo on the stocks')
    YahooScraper(date, cron_path).run()

    logging.info('Cleaning up the stock data and writing to json file')
    DataCleaner(date, cron_path).run()

    logging.info("Run the O'Shaughnessy Trending Value analysis")
    pm.execute_notebook(
        'analysis.ipynb',
        'analysis.ipynb',
        parameters=dict(date=str(date.strftime('%Y_%m_%d')), cron_path=cron_path)
    )

    logging.info("Transferring files over to Google Cloud Storage")
    GCLMover('stock_scrape_bucket', cron_path).run()

    logging.info('End-to-end process complete for {date}'.format(
        date=str(date.strftime('%Y_%m_%d'))
    ))
