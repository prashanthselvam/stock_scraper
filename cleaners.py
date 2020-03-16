import datetime
import json
import logging
import os
import argparse

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class DataCleaner():

    def __init__(self, date=datetime.datetime.now(), cron_path=''):
        self.date = date
        self.cron_path = cron_path

    @staticmethod
    def clean_col_name(col_name):
        col_name = col_name.lower()
        col_name = col_name.replace('(', '').replace(')', '').replace('/', '').\
            replace('%', 'pct').replace('.', '').replace(' ', '_')
        if col_name[:2] == '52':
            col_name.replace('52', 'fifty_two')
        return col_name

    @staticmethod
    def percent_clean(x):
        if '%' in str(x):
            try:
                return round(float(x.replace('%', '').strip())/100, 4)
            except:
                pass
        elif 'N/A' in str(x):
            return None

    @staticmethod
    def big_no_clean(x):
        if 'K' in str(x) or 'k' in str(x):
            return int(int(float(x.replace('k', '').replace('K', '').strip())*1000))
        elif 'M' in str(x) or 'm' in str(x):
            return int(int(float(x.replace('m', '').replace('M', '').strip())*1000000))
        elif 'B' in str(x) or 'b' in str(x):
            return int(int(float(x.replace('b', '').replace('B', '').strip())*1000000000))
        elif 'T' in str(x) or 't' in str(x):
            return int(int(float(x.replace('t', '').replace('T', '').strip())*1000000000000))
        elif 'N/A' in str(x) or '-' in str(x):
            return None
        else:
            return str(x).replace(',', '')

    @staticmethod
    def combine_dicts(finviz_stats_dict, finviz_profile_dict, yahoo_ev_dict):

        dd = {}

        for key in finviz_stats_dict.keys():
            dd[key] = {}
            dd[key].update(finviz_stats_dict[key])
            dd[key].update(finviz_profile_dict[key])
            if key in yahoo_ev_dict.keys():
                dd[key].update(yahoo_ev_dict[key])
            else:
                dd[key]['evebitda'] = None

        return dd

    def output_file(self, dict):
        date = str(self.date.strftime('%Y_%m_%d'))
        output_file = open(os.path.join(
            self.cron_path,
            'data/final_output/clean_data_{date}.json'.format(date=date)
        ), 'a+')

        for value in dict.values():
            s = json.dumps(value)
            output_file.write(s)
            output_file.write('\n')

        output_file.close()

    def create_data_dict(self, file, finviz_stat_processing=False):
        keys = []
        dict_version = {}

        for line in file:
            data = line.rstrip('\n').split('|')
            if data[0] == 'Ticker':
                keys = [self.clean_col_name(col_name) for col_name in data]
            else:
                row = dict(zip(keys, data))
                dict_version[row['ticker']] = row
                dict_version[row['ticker']]['date'] = str(self.date)

        if finviz_stat_processing:
            for ticker in dict_version.keys():
                # Remove % symbol from items which have it and convert to decimal
                for key in ['insider_own', 'perf_week', 'insider_trans', 'perf_month',
                            'inst_own', 'short_float', 'perf_quarter', 'eps_this_y',
                            'inst_trans', 'perf_half_y', 'eps_growth_next_y',
                            'roa', 'perf_year', 'eps_next_5y', 'roe', 'perf_ytd',
                            'eps_past_5y', 'roi', '52w_high', 'dividend_pct', 'sales_past_5y',
                            'gross_margin', '52w_low', 'sales_qq', 'oper_margin',
                            'eps_qq', 'profit_margin', 'payout', 'sma20', 'sma50',
                            'sma200', 'change']:
                    dict_version[ticker][key] = self.percent_clean(dict_version[ticker][key])

                # Convert string numbers into actual numbers
                for key in ['shs_outstand', 'market_cap', 'shs_float', 'income',
                            'sales', 'avg_volume']:
                    dict_version[ticker][key] = self.big_no_clean(dict_version[ticker][key])

                # Change volume to be an actual number
                try:
                    dict_version[ticker]['volume'] = int(dict_version[ticker]['volume'].replace(',', ''))
                except:
                    dict_version[ticker]['volume'] = None

        for ticker in dict_version.keys():
            for key, value in dict_version[ticker].items():
                if value in ['-', 'N/A']:
                    dict_version[ticker][key] = None
                elif key != 'ticker' and type(value) == str:
                    try:
                        dict_version[ticker][key] = float(value)
                    except:
                        pass

        return dict_version

    def process_yahoo_ev_data(self):
        date = str(self.date.strftime('%Y_%m_%d'))
        dest = os.path.join(self.cron_path, 'data/raw_data/yahoo_ev_stats_{date}.txt'.format(date=date))
        f = open(dest, 'r')

        yahoo_ev_dict = self.create_data_dict(f)

        return yahoo_ev_dict

    def process_finviz_profile_data(self):
        date = str(self.date.strftime('%Y_%m_%d'))
        dest = os.path.join(self.cron_path, 'data/raw_data/finviz_profile_{date}.txt'.format(date=date))
        f = open(dest, 'r')
        finviz_profile_dict = self.create_data_dict(f)
        return finviz_profile_dict

    def process_finviz_stats_data(self):
        date = str(self.date.strftime('%Y_%m_%d'))
        dest = os.path.join(self.cron_path, 'data/raw_data/finviz_stats_{date}.txt'.format(date=date))
        f = open(dest, 'r')
        finviz_stats_dict = self.create_data_dict(f, finviz_stat_processing=True)
        return finviz_stats_dict

    def run(self):
        logging.info('Processing all the files and creating final version')
        finviz_stats_dict = self.process_finviz_stats_data()
        finviz_profile_dict = self.process_finviz_profile_data()
        yahoo_ev_dict = self.process_yahoo_ev_data()

        final_dict = self.combine_dicts(finviz_stats_dict,
                                        finviz_profile_dict,
                                        yahoo_ev_dict)

        self.output_file(final_dict)
        logging.info('Final dictionary output to destination')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', default=datetime.datetime.now())
    parser.add_argument('-c', default='')
    args = parser.parse_args()

    date = args.d
    cron_path = args.c

    if not type(date) == datetime.datetime:
        date = datetime.datetime.strptime(args.d, '%Y-%m-%d')

    DataCleaner(date, cron_path).run()

