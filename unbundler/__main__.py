import sys

from lib import Unbundler
from lib import Options
import logging


def run_project(args):
    options = Options()
    opts = options.parse(sys.argv[1:])
    v = Unbundler(opts)
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Entering main routine for unbundler\n')
    try:
        v.create_dirs()
        while True:
            v.process_files()
    except Exception as e:
        logging.exception(e)
        raise

if __name__ == '__main__':
    run_project(sys.argv)
