from twitter_driver import TwitterDriver
from neo4j_driver import Neo4jWrapper
from elastic_driver import ElasticDriver
from threading import Thread
import random
import logging
import time

nj = Neo4jWrapper()
es = ElasticDriver()
td = TwitterDriver(es)
td2 = TwitterDriver(es, consumer_key='pTn8CmSu4DWds42sgriqMTh3D',
                               consumer_secret='PpDLb3oNdiL3WgjnFhgwryQXt4CkehTIJRsh04YRaxvmC6GI5z',
                               access_token_key='1051519670837288965-huNoFYASIRcqZltvR6SM38Tq8o7yRh',
                               access_token_secret='hiq6uVunvOuTbMFG4i7PgtG32SZTRPUMZxjGw7SIEDsHl')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')


class Crawler(Thread):
    def __init__(self, method):
        super().__init__()
        self.method = method

    def run(self):
        self.method()


def main():

    # Crawler(crawl_expand).start()
    # time.sleep(5)
    # Crawler(crawl_mention).start()
    # crawl_search()
    for user in es.get_streamed_tweets():
        td.crawl_user(user)

def crawl_expand():
    for user in nj.expansion_candidates(15000):
        td.crawl_user(user)


def crawl_mention():
    for user in nj.expansion_candidate_by_mention(1000):
        td2.crawl_user(user)


def crawl_search():
    list = ['bugbounty', 'bugbountytip', 'togetherwehitharder', 'ittakesacrowd', 'hackerone',
                           'bugbounties', 'bugcrowd', 'bug bounty']
    random.shuffle(list)
    for keyword in list:
        candidates = get_candidate_tweets(keyword)
        candidates.sort(reverse=True, key=lambda tweet: tweet.favorite_count)
        for status in candidates:
            td.crawl_user(status.user.id)
        candidates = get_candidate_tweets('#'+keyword)
        candidates.sort(reverse=True, key=lambda tweet: tweet.favorite_count)
        for status in candidates:
            td.crawl_user(status.user.id)


def get_candidate_tweets(keyword):
    logging.info('getting candidate tweets for ' + keyword)
    search_res = td.search(keyword)
    return search_res


if __name__ == '__main__':
    try:
        main()
        nj.end()
    except KeyboardInterrupt:
        nj.end()
        exit(-1)
