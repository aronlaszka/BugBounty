import tweepy
import logging
from .TwitterDriver import TwitterDriver


class TweepyWrapper(tweepy.StreamListener):

    def __init__(self, td: TwitterDriver):
        super().__init__()
        self.td = td
        self.logger = logging.getLogger(__name__)

    def on_status(self, status):
        self.logger.info(f'Status found for {status.user.screen_name}')
        self.td.crawl_user(status.user.id)
