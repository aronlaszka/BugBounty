import twitter
import logging
import time
from twitter.error import TwitterError
from exceptions import UserAlreadyCrawledException


class TwitterDriver:

    def __init__(self, db,
                 consumer_key='lk34GEaQWaaXNk1jK7swHUBgH',
                 consumer_secret='1ItNrVbx9TFS6PGqajOYACSwWuRmZyTOeATImphQBsN3cYQIdv',
                 access_token_key='833314858586296322-32Vo3lUoIaknsK4SbEvNLLAedRRaEmt',
                 access_token_secret='yopvNdiUHZL83aO8KL9iRLd3VRHRjvVbnFYYbziwAC3FC'):

        self.api = twitter.Api(consumer_key= consumer_key,
                               consumer_secret=consumer_secret,
                               access_token_key=access_token_key,
                               access_token_secret=access_token_secret)

        self.log = logging
        self.delay = 32
        self.db = db

    def get_user_friends(self, user_id):
        # self.log.info('getting user friends ' + str(user_id))
        #
        # cursor = -1
        # while True:
        #     if self.delay > 1:
        #         self.log.info('delay: ' + str(self.delay))
        #     time.sleep(self.delay - 1)
        #     try:
        #         fl = self.api.GetFriendsPaged(user_id=user_id, count=20, cursor=cursor)
        #         self.log.debug(len(fl[2]))
        #         if len(fl[2]) == 0:
        #             break
        #         cursor = fl[0]
        #
        #         for user in fl[2]:
        #             self.db.store_user(user)
        #             self.db.following(user_id, user.id)
        #             self.crawl_user(user.id)
        #
        #         if self.delay > 1:
        #             self.delay = int(self.delay / 2)
        #     except TwitterError as e:
        #         self.log.error(e)
        #         self.delay += 2
        #         self.log.info('waiting for rate limit window...' + str(self.delay))
        #
        # self.log.info('finished getting user ' + str(user_id))
        pass

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

                bugbounty_ratio = self.bugbounty_ratio([t.text for t in timeline])
                if bugbounty_ratio < 0.005:
                    self.log.info('user %d is not a bounty hunter. ratio: %f' % (user_id, bugbounty_ratio))
                    # self.db.mark_user_something(user_id, 'Excluded')
                    break

                try:
                    for status in timeline:
                        self.db.store_tweet(status)
                except UserAlreadyCrawledException:
                    break

            except TwitterError as e:
                self.log.error(e)
                if isinstance(e.message, list) and 'message' in e.message[0] and e.message[0]['message'] == 'Rate limit exceeded':
                    self.delay += 2
                    self.log.info('waiting for rate limit window...' + str(self.delay))
                else:
                    break

        self.log.info('collected tweets for ' + str(user_id))

    def search(self, keyword):
        self.log.info('searching for keyword: ' + keyword)
        candidates = self.api.GetSearch(term=keyword, count=100)
        self.log.info(str(len(candidates)) + ' candidates found.')
        #for status in candidates:
        #    try:
        #        self.db.store_tweet(status)
        #    except:
        #        pass
        return candidates

    def crawl_user(self, user_id):
        # if not self.db.is_user_something(user_id, 'CompletedUser') and not self.db.is_user_something(user_id, 'Excluded'):
            self.get_user(user_id)
            self.get_tweets(user_id)
            self.db.mark_user_something(user_id, 'CompletedUser')
        # else:
        #     self.log.info('user %d is already crawled' % user_id)

    def bugbounty_ratio(self, tweets):
        bugbounty_count = 0
        total_count = 0

        for tweet in tweets:
            total_count += 1
            if self.is_bugbounty(tweet):
                bugbounty_count += 1
        return float(bugbounty_count) / total_count

    @staticmethod
    def is_bugbounty(tweet):
        bugbounty_words = ['bugbounty', 'bugbountytip', 'togetherwehitharder', 'ittakesacrowd', 'hackerone',
                           'bugbounties', 'bugcrowd', 'bug bounty']
        for word in bugbounty_words:
            if word in tweet.lower():
                return True
        return False
