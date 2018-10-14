import twitter
import logging
import time
from twitter.error import TwitterError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s -'
                                                       ' %(name)s - %(levelname)s - %(message)s')


class TwitterDriver:

    def __init__(self, nj):
        # self.api = twitter.Api(consumer_key='C64LEG4kait1EUTtZZ5ku5atD',
        #                        consumer_secret='y15FYcKGFUC8w7bDAkGiK50g0DONTRH3DgQb6FxfM82CZkRXEH',
        #                        access_token_key='833314858586296322-Syne61sXWTQ2Y5U9wfc9ZlpDcHSIpxd',
        #                        access_token_secret='CYXvRL4DMQFXjQebQwvQqrpPXZVesy3VNjL9vWThRCsIA')
        self.api = twitter.Api(consumer_key='pTn8CmSu4DWds42sgriqMTh3D',
                               consumer_secret='PpDLb3oNdiL3WgjnFhgwryQXt4CkehTIJRsh04YRaxvmC6GI5z',
                               access_token_key='1051519670837288965-huNoFYASIRcqZltvR6SM38Tq8o7yRh',
                               access_token_secret='hiq6uVunvOuTbMFG4i7PgtG32SZTRPUMZxjGw7SIEDsHl')
        self.log = logging.getLogger(__name__)
        self.delay = 32
        self.nj = nj

    def get_user_friends(self, user_id):
        self.log.info('getting user friends ' + str(user_id))

        cursor = -1
        while True:
            if self.delay > 1:
                self.log.info('delay: ' + str(self.delay))
            time.sleep(self.delay - 1)
            try:
                fl = self.api.GetFriendsPaged(user_id=user_id, count=20, cursor=cursor)
                self.log.debug(len(fl[2]))
                if len(fl[2]) == 0:
                    break
                cursor = fl[0]

                for user in fl[2]:
                    self.nj.store_user(user)
                    self.nj.following(user_id, user.id)

                if self.delay > 1:
                    self.delay = int(self.delay / 2)
            except TwitterError as e:
                self.log.error(e)
                self.delay += 2
                self.log.info('waiting for rate limit window...' + str(self.delay))

        self.log.info('finished getting user ' + str(user_id))

    def get_user(self, user_id):
        self.log.info('getting user ' + str(user_id))
        user = self.api.GetUser(user_id)
        self.nj.store_user(user)
        self.log.info('user ' + str(user_id) + ' is ' + user.screen_name)
        return user

    def get_tweets(self, user_id):
        self.log.info('getting user tweets ' + str(user_id))
        last = -1
        count = 0
        while True:
            if self.delay > 1:
                self.log.info('delay: ' + str(self.delay))
            time.sleep(self.delay - 1)
            try:
                timeline = self.api.GetUserTimeline(user_id=user_id, count=200, max_id=None if last == -1 else last)
                if len(timeline) == 0:
                    break
                last = timeline[-1].id

                for status in timeline:
                    self.nj.store_tweet(status)
                    count += 1

                if self.delay > 1:
                    self.delay = int(self.delay / 2)

            except TwitterError as e:
                self.log.error(e)
                self.delay += 2
                print('total retreived: ' + str(count))
                self.log.info('waiting for rate limit window...' + str(self.delay))
        self.log.info('collected tweets for ' + str(user_id))

    def search(self, keyword):
        self.log.info('searching for keyword: ' + keyword)
        candidates = self.api.GetSearch(term=keyword, count=100)
        self.log.info(str(len(candidates)) + ' candidates found.')
        for status in candidates:
            self.nj.store_tweet(status)
            self.nj.mark_user_important(status.user.id)
        return candidates
