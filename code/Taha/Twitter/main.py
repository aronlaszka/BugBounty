from twitter_driver import TwitterDriver
import logging
import random


td = TwitterDriver()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def main():
    candidates = get_candidate_tweets('#bugbounty')
    random.shuffle(candidates)
    for status in candidates:
        crawl_user(status.user.id)


def crawl_user(user_id):
    td.get_user(user_id)
    td.get_tweets(user_id)


def get_candidate_tweets(keyword):
    log.info('getting candidate tweets for ' + keyword)
    search_res = td.search(keyword)
    return search_res


if __name__ == '__main__':
    try:
        main()
        # print(random.Random().random())
    except KeyboardInterrupt:
        exit(-1)
