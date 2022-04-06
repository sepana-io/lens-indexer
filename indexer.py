
from formatter import ElkJsonFormatter
import logging
import time
import warnings
warnings.filterwarnings('ignore')

from script import index_posts_from_lens, index_profiles


jsonhandler = logging.StreamHandler()
jsonhandler.setFormatter(ElkJsonFormatter())
logger = logging.getLogger()
logger.addHandler(jsonhandler)
logger.setLevel(logging.INFO)


if __name__ == '__main__':
    logger.info("Starting lens posts indexing ")
    while True:
        index_profiles()
        index_posts_from_lens()
        time.sleep(10800)
