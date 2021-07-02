import os
import sys
import time
import signal
import yagmail
import logging
import getpass
import schedule
import requests
import argparse

import pandas as pd
from os.path import join

from datetime import datetime


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def expiration(exp_strike):
    return int(exp_strike.split(':')[0].replace('-', ''))

def options_iterator(raw):
    for maptype in ('putExpDateMap', 'callExpDateMap'):
        for exp_strike, data in raw[maptype].items():
            for _, strike in data.items():
                for x in strike:
                    x['expirationDate'] = expiration(exp_strike)
                    yield x


class OptionsCollector:
    def __init__(self, dest):
        self.dest = dest
        self.today = datetime.now().strftime('%Y-%m-%d')

        self.api = 'https://api.tdameritrade.com/v1/marketdata/chains'
        self.api_key = os.getenv('APIKEY')
        if self.api_key is None:
            logger.critical('APIKEY env variable not set.')
            sys.exit(1)

        self.email_user = os.getenv('EMAIL_USER')
        self.email_pwd = None

    def set_date(self, date):
        self.today = date

    def send_request(self, base_params, retry_count=10):
        key_p = { 'apikey': self.api_key }
        params = {**base_params, **key_p}

        for retry in range(retry_count):
            try:
                r = requests.get(self.api, params=params, timeout=60)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.ConnectionError as e:
                logger.error(f'Connection error {e}')
            except requests.exceptions.Timeout as e:
                logger.error(f'Timeout {e}')
            except Exception as e:
                logger.error(f'Unknown error: {e}')
            logger.error(f'retrying... ({retry})')
            time.sleep(3)

    def option_chain(self, symbol):
        ''' Gets the option chain for the given symbol.

        Parameters:
          symbol (str): the underlying symbol

        Return:
          the number of contracts collected
        '''
        symbol = symbol.upper()
        params = { 'symbol': symbol, 'includeQuotes':'TRUE',
                   'strategy': 'ANALYTICAL' }

        r = self.send_request(params)
        if r is None:
            logger.error(f'Giving up on {symbol}')
            return 0
        if r.get('status', '') != 'SUCCESS':
            logger.error(f'Could not get option chain for: {symbol}')
            logger.error(str(r))
            return 0

        contracts = r['numberOfContracts']
        logger.info(f'Contracts: {contracts}')

        fields = ('expirationDate', 'strikePrice', 'putCall', 'symbol',
                  'bid', 'ask', 'last', 'totalVolume', 'openInterest',
                  'delta', 'gamma', 'theta', 'vega', 'rho',
                  'volatility', 'theoreticalVolatility', 'nonStandard')
        def to_list(ov):
            ov['putCall'] = 'p' if ov['putCall'] == 'PUT' else 'c'
            return [ov[x] for x in fields]
        results = [[self.today] + to_list(opt) for opt in options_iterator(r)]

        # Override the default data types
        dtypes = {
            'date': 'datetime64', 'expiration': 'int32', 'strike': 'int16',
            'bid': 'float32', 'ask': 'float32', 'last': 'float32',
            'volume': 'int32', 'openint': 'int32', 'delta': 'float32',
            'gamma': 'float32', 'theta': 'float32', 'vega': 'float32',
            'rho': 'float32', 'impvol': 'float32',
            'theoretical_vol': 'float32', 'non_standard': bool,
        }

        # Prepare the destination
        path = join(self.dest, symbol)
        file = join(path, self.today + '.parquet')
        pq_params = { 'compression': 'brotli' }
        if not path.startswith('s3://'):
            os.makedirs(path, exist_ok=True)
        else:
            endpoint_url = os.getenv('ENDPOINT_URL')
            if endpoint_url is not None:
                pq_params['storage_options'] = {
                    'client_kwargs': {'endpoint_url': endpoint_url}
                }

        # Save the option chain to a file
        df = pd.DataFrame(results, columns=[
            'date', 'expiration', 'strike', 'type', 'symbol', 'bid', 'ask',
            'last', 'volume', 'openint', 'delta', 'gamma', 'theta', 'vega',
            'rho', 'impvol', 'theoretical_vol', 'non_standard'
        ])
        df.astype(dtypes).set_index('date').to_parquet(file, **pq_params)

        if contracts != len(results):
            logger.warning(f'Expected {contracts} options, got {len(results)}')
        return len(results)

    def batch_process(self, symbol_file):
        ''' Download the option chains for all symbols in the given file. '''
        results = []
        with open(symbol_file, 'r') as infile:
            for symbol in infile.readlines():
                symbol = symbol.strip()
                logger.info(f'Processing {symbol}')
                n = self.option_chain(symbol)
                results.append([symbol, n])
                logger.info(f'  {n}')

        try:
            if self.email_pwd is not None:
                with yagmail.SMTP(self.email_user, self.email_pwd) as yag:
                    subject = 'Options collection complete'
                    yag.send(self.email_user, subject, [str(results)])
        except Exception as e:
            logger.warning(f'Could not send email: {e}')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--option', dest='single_option',
                        help='get a single underlying symbol\'s option chain')
    group.add_argument('-f', '--symbol-file', dest='symbol_file',
                        help='a text file of symbols, one per line, '
                        'whose option chains to download')

    parser.add_argument('-e', '--email', action='store_true',
                        help='Email status (requires pwd)')
    parser.add_argument('-d', '--date',
                        help='pretend today is this date (format YYYY-MM-DD)')
    parser.add_argument('--now', action='store_true',
                        help='force a one-time batch download now '
                        '(default is to wait until 9pm)')
    parser.add_argument('--due', default=21,
                        help='which hour (24-based) to collect each day')
    parser.add_argument('--destination', default='.',
                        help='directory or S3 URI where data should be saved')
    args = parser.parse_args()

    tda = OptionsCollector(args.destination)

    # Change the defaults based on command line options
    if args.date:
        tda.set_date(args.date)

    # Get email credentials if email requested
    if args.email:
        if tda.email_user is None:
            logger.critical('EMAIL_USER is not set.')
            sys.exit(1)
        tda.email_pwd = getpass.getpass('Gmail pass: ')

    # Do the required action
    if args.single_option:
        return tda.option_chain(args.single_option)
    elif args.now:
        return tda.batch_process(args.symbol_file)

    def run():
        if datetime.today().weekday() < 5:
            tda.set_date(datetime.now().strftime('%Y-%m-%d'))
            tda.batch_process(args.symbol_file)
        logger.info('Done.')

    # Do continuous collection
    logger.info('Scheduling collection.')
    schedule.every().day.at(f'{args.due:02d}:00').do(run)
    while True:
        schedule.run_pending()
        time.sleep(1)

def signal_handler(signal, frame):
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()

