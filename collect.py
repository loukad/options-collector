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
from datetime import datetime


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def expiration(exp_strike):
    return exp_strike.split(':')[0].replace('-', '')

def options_iterator(raw):
    for maptype in ('putExpDateMap', 'callExpDateMap'):
        for exp_strike, data in raw[maptype].items():
            for _, strike in data.items():
                for x in strike:
                    x['expirationDate'] = expiration(exp_strike)
                    yield x


class OptionsCollector:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.today = datetime.now().strftime('%Y-%m-%d')

        self.api = 'https://api.tdameritrade.com/v1/marketdata/chains'
        self.api_key = os.getenv('APIKEY')

        self.email_user = os.getenv('EMAIL_USER', 'loukad@gmail.com')
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
        if r['status'] != 'SUCCESS':
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
        results = [to_list(opt) for opt in options_iterator(r)]

        # Save the option chain to a file
        path = os.path.join(self.output_dir, self.today)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, symbol + '.csv'), 'w') as outfile:
            outfile.write('Date,Expiration,Strike,Type,Symbol,Bid,Ask,')
            outfile.write('Last,Volume,OpenInt,Delta,Gamma,Theta,Vega,Rho,')
            outfile.write('ImpVol,TheoreticalVol,NonStandard\n')
            for fields in results:
                outfile.write(self.today + ',')
                outfile.write(','.join(map(str, fields)) + '\n')

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
    parser.add_argument('--outputdir', default='.',
                        help='the directory where to save the downloaded data')
    args = parser.parse_args()

    tda = OptionsCollector(args.outputdir)

    # Change the defaults based on command line options
    if args.date:
        tda.set_date(args.date)

    # Get email credentials if email requested
    if args.email:
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

