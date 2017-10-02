import sys

from lib import Unbundler
from lib import Options
import logging

if __name__ == '__main__':
    options = Options()
    opts, args = options.parse(sys.argv[1:])
    v = Unbundler(opts)
    logging.basicConfig(filename='logs/unbundler.log', level=logging.DEBUG)
    logging.info('Entering main routine for unbundler\n')
    try:
        v.create_dirs()
        while True:
            v.process_files()
    except Exception as e:
        logging.exception()
        raise
