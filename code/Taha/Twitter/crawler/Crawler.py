from threading import Thread
import random
import logging


class Crawler(Thread):
    def __init__(self, name, keywords, elastic, td):
        super().__init__()
        self.keywords = keywords
        self.es = elastic
        self.td = td
        self.name = name
        self.logger = logging.getLogger(__name__)

    def crawl_search(self):
        random.shuffle(self.keywords)
        for keyword in self.keywords:
            candidates = self.get_candidate_tweets(keyword)
            candidates.sort(reverse=True, key=lambda tweet: tweet.favorite_count)
            for status in candidates:
                self.td.crawl_user(status.user.id)
            candidates = self.get_candidate_tweets('#' + keyword)
            candidates.sort(reverse=True, key=lambda tweet: tweet.favorite_count)
            for status in candidates:
                self.td.crawl_user(status.user.id)

    def get_candidate_tweets(self, keyword):
        self.logger.info('getting candidate tweets for ' + keyword)
        search_res = self.td.search(keyword)
        return search_res

    def run(self):
        while True:
            try:
                self.crawl_search()
            except Exception as e:
                self.logger.error(e)
