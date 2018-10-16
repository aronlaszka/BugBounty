from twitter_driver import TwitterDriver
from neo4j_driver import Neo4jWrapper
import logging

nj = Neo4jWrapper()
td = TwitterDriver(nj)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def main():
    candidates = get_candidate_tweets('#bugbountytip')
    candidates.sort(reverse=True, key=lambda tweet: tweet.favorite_count)
    for status in candidates:
        if not nj.is_user_something(status.user.id, 'CompletedUser') and not nj.is_user_something(status.user.id,
                                                                                                  'Excluded'):
            crawl_user(status.user.id)
        else:
            log.info('user %d is already crawled')
    # print(nj.bugbounty_ratio(795470076))
    # print(nj.bugbounty_ratio(2433784736))
    # crawl_user(3094698976)


def crawl_user(user_id):
    td.get_user(user_id)
    td.get_tweets(user_id)
    nj.mark_user_something(user_id, 'CompletedUser')


def get_candidate_tweets(keyword):
    log.info('getting candidate tweets for ' + keyword)
    search_res = td.search(keyword)
    return search_res


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        nj.end()
        exit(-1)
