import argparse
import logging
import os
import retry
import s3fs
import schedule
import signal
import sys
import tempfile
import time
import yagmail

import pandas as pd
import yfinance as yf

from os.path import join

from datetime import datetime
from retry import retry

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def expiration(exp_strike):
    return int(exp_strike.split(':')[0].replace('-', ''))

def get_options_chain(ticker: yf.ticker.Ticker, date: str):
    ''' Extracts call / put option tables for input ticker and expiration date '''
    options_chain = ticker.option_chain(date)
    options_chain.calls['type'] = 'c'
    options_chain.puts['type'] = 'p'

    return (pd.concat([options_chain.calls, options_chain.puts])
            .rename(columns={
                'contractSymbol': 'symbol',
                'lastTradeDate': 'last_trade',
                'lastPrice': 'last',
                'openInterest': 'openint',
                'impliedVolatility': 'impvol',
                'contractSize': 'non_standard',
    }))

@retry(Exception, tries=3, delay=3)
def get_options_chains(symbol: str):
    yfticker = yf.Ticker(symbol)

    def fetch_chain_and_add_expiration(date):
        asint32 = int(datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d'))
        options = (get_options_chain(yfticker, date)
            .drop(columns=['change', 'percentChange', 'currency', 'inTheMoney']))
        options['expiration'] = asint32
        return options

    tables = [fetch_chain_and_add_expiration(date) for date in yfticker.options]
    if len(tables) == 0:
        return None

    combined = pd.concat(tables)
    combined['non_standard'] = combined['non_standard'].replace({'REGULAR': False, '.*': True})
    return combined

def save_option_data(df, filename, s3_client=None):
    if not filename.startswith('s3://'):
        if (base_path := os.path.dirname(filename)):
            os.makedirs(base_path, exist_ok=True)

    dtypes = {
        'expiration': 'int32', 'strike': 'int16',
        'bid': 'float32', 'ask': 'float32', 'last': 'float32',
        'volume': 'int32', 'openint': 'int32', 'impvol': 'float32',
        'non_standard': bool,
    }

    df = df.fillna(0).astype(dtypes).set_index('date')

    pq_params = { 'compression': 'brotli' }
    if s3_client is not None:
        with s3_client.open(filename, 'wb') as f:
            with tempfile.NamedTemporaryFile() as of:
                df.to_parquet(of.name, **pq_params)
                of.seek(0)
                f.write(of.read())
    else:
        df.to_parquet(filename, **pq_params)


class OptionsCollector:

    def __init__(self, dest):
        self.dest = dest
        self.s3_client = None
        if dest.startswith('s3://'):
            self.s3_client = s3fs.S3FileSystem(client_kwargs={
              'endpoint_url': os.getenv('ENDPOINT_URL')
            })

        self.today = datetime.now().strftime('%Y-%m-%d')

        self.email_user = os.getenv('EMAIL_USER')
        self.email_pwd = os.getenv('EMAIL_PWD')

    def set_date(self, date):
        self.today = date

    def option_chain(self, symbol: str):
        ''' Gets the option chain for the given symbol.

        Parameters:
          symbol: the underlying symbol

        Return:
          the number of contracts collected
        '''
        try:
            df = get_options_chains(symbol)
            if df is None:
                return 0
        except Exception as e:
            logger.error(f'Error processing {symbol}', e)
            return 0
    
        df['date'] = self.today
        dest_file = join(self.dest, symbol, self.today + '.parquet')
        save_option_data(df, dest_file, s3_client=self.s3_client)

        return len(df)

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

    oc = OptionsCollector(args.destination)

    # Change the defaults based on command line options
    if args.date:
        oc.set_date(args.date)

    # Get email credentials if email requested
    if args.email:
        if oc.email_user is None:
            logger.critical('EMAIL_USER is not set.')
            sys.exit(1)
        if oc.email_pwd is None:
            logger.critical('EMAIL_PWD is not set.')
            sys.exit(1)

    # Do the required action
    if args.single_option:
        return oc.option_chain(args.single_option)
    elif args.now:
        return oc.batch_process(args.symbol_file)

    def run():
        if datetime.today().weekday() < 5:
            oc.set_date(datetime.now().strftime('%Y-%m-%d'))
            oc.batch_process(args.symbol_file)
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
