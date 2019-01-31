import twitter
import logging
import time
from twitter.error import TwitterError
from exceptions.database_exceptions import UserAlreadyCrawledException


class TwitterDriver:

    def __init__(self,
                 keywords,
                 db,
                 sensitivity,
                 consumer_key,
                 consumer_secret,
                 access_token_key,
                 access_token_secret):

        self.api = twitter.Api(consumer_key=consumer_key,
                               consumer_secret=consumer_secret,
                               access_token_key=access_token_key,
                               access_token_secret=access_token_secret)

        self.keywords = keywords
        self.sensitivity = sensitivity
        self.log = logging
        self.delay = 8
        self.db = db

    def get_user(self, user_id):
        try:
            self.log.info('getting user ' + str(user_id))
            user = self.api.GetUser(user_id)
            self.db.store_user(user)
            self.log.info('user ' + str(user_id) + ' is ' + user.screen_name)
            return user
        except Exception:
            pass

    def get_tweets(self, user_id):
        self.log.info('getting user tweets ' + str(user_id))
        last = -1
        req_count = 0
        while True:
            if self.delay > 1:
                self.log.info('delay: ' + str(self.delay))
            time.sleep(self.delay - 1)
            try:
                timeline = self.api.GetUserTimeline(user_id=user_id, count=200, max_id=None if last == -1 else last - 1)
                req_count += 1
                if len(timeline) == 0 or len(timeline) < 20:
                    break
                last = timeline[-1].id

                if self.delay > 1:
                    self.delay = int(self.delay / 2)

                relevancy_ratio = self.relevancy_ratio([t.text for t in timeline])
                if relevancy_ratio < self.sensitivity:
                    self.log.info('user %d is not relevant. ratio: %f' % (user_id, relevancy_ratio))
                    break

                try:
                    for status in timeline:
                        self.db.store_tweet(status)
                except UserAlreadyCrawledException:
                    break

            except TwitterError as e:
                self.log.error(e)
                if isinstance(e.message, list) and 'message' in e.message[0] and e.message[0][
                    'message'] == 'Rate limit exceeded':
                    self.delay += 2
                    self.log.info('waiting for rate limit window...' + str(self.delay))
                else:
                    break

        self.log.info('collected tweets for ' + str(user_id))

    def search(self, keyword):
        self.log.info('searching for keyword: ' + keyword)
        candidates = self.api.GetSearch(term=keyword, count=100)
        self.log.info(str(len(candidates)) + ' candidates found.')
        return candidates

    def crawl_user(self, user_id):
        self.get_user(user_id)
        self.get_tweets(user_id)
        self.db.mark_user_something(user_id, 'CompletedUser')

    def relevancy_ratio(self, tweets):
        relevant_count = 0
        total_count = 0

        for tweet in tweets:
            total_count += 1
            if self.is_relevant(tweet):
                relevant_count += 1
        return float(relevant_count) / total_count

    def is_relevant(self, tweet):
        for word in self.keywords:
            if word in tweet.lower():
                return True
        return False
