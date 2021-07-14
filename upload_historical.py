import os
import re
import sys
import s3fs
import glob
import signal
import logging
import argparse

from tqdm.auto import tqdm
from contextlib import closing
from multiprocessing import get_context

import pandas as pd
from os.path import join, basename, expanduser

from collect import save_option_data

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

def column_remap(col):
    custom = {
        'TheoreticalVol': 'theoretical_vol',
        'NonStandard': 'non_standard'
    }
    if col in custom:
        return custom[col]
    return col.lower()

def convert(item):
    file, dest, s3_client = item

    df = pd.read_csv(file).rename(columns=column_remap)
    date, symbol = df.date[0], re.sub('\.csv.*', '', basename(file))
    fn = join(dest, symbol, f'{date}.parquet')

    if s3_client is not None and s3_client.exists(fn):
        logger.warning(f'Skipping {fn}')
        return
    logger.info(f'{file} => {fn}')
    save_option_data(df, fn, s3_client=s3_client)

def get_file_list(pattern):
    return glob.glob(expanduser(pattern))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', nargs='+',
                        help='CSV options files or to convert (wildcard OK)')
    parser.add_argument('--parallelism', default=4, type=int,
                        help='how many files at a time to process')
    parser.add_argument('--dest', default='.',
                        help='Where to save the converted files')

    args = parser.parse_args()

    s3_client = None
    if args.dest.startswith('s3://'):
        s3_client = s3fs.S3FileSystem(client_kwargs={
            'endpoint_url': os.getenv('ENDPOINT_URL')
        })
    files = sum([get_file_list(p) for p in args.file], [])
    work_items = [(f, args.dest, s3_client) for f in files]
    processes = min(args.parallelism, len(work_items))

    with closing(get_context('spawn').Pool(processes)) as p:
        status = {'desc': 'Processing', 'total': len(files)}
        _ = list(tqdm(p.imap(convert, work_items), **status))


def signal_handler(signal, frame):
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()

