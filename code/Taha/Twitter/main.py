from twitter_driver import TwitterDriver
from neo4j_driver import Neo4jWrapper
import logging
import random

nj = Neo4jWrapper()
td = TwitterDriver(nj)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def main():
    candidates = get_candidate_tweets('#bugbountytip')
    random.shuffle(candidates)
    for status in candidates:
        if not nj.is_completed(status.user.id):
            crawl_user(status.user.id)
        else:
            log.info('user %d is already crawled')


def crawl_user(user_id):
    td.get_user(user_id)
    td.get_tweets(user_id)
    nj.mark_user_completed(user_id)


def get_candidate_tweets(keyword):
    log.info('getting candidate tweets for ' + keyword)
    search_res = td.search(keyword)
    return search_res


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        exit(-1)
