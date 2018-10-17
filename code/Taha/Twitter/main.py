from twitter_driver import TwitterDriver
from neo4j_driver import Neo4jWrapper
import logging
import time

nj = Neo4jWrapper()
td = TwitterDriver(nj)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def main():
    # candidates = get_candidate_tweets('#bugcrowd')
    # candidates.sort(reverse=True, key=lambda tweet: tweet.favorite_count)
    # for status in candidates:
    #     td.crawl_user(status.user.id)
    for user in nj.expansion_candidates(8):
        td.crawl_user(user)


def get_candidate_tweets(keyword):
    log.info('getting candidate tweets for ' + keyword)
    search_res = td.search(keyword)
    return search_res


if __name__ == '__main__':
    try:
        main()
        nj.end()
    except KeyboardInterrupt:
        nj.end()
        exit(-1)
