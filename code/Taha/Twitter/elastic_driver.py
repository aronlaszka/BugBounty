from datetime import datetime
from elasticsearch import Elasticsearch
from exceptions import TweetAlreadyExistsException
import json
import logging
import socket
import time


address = 'localhost'
port = 5000


class ElasticDriver:
    def __init__(self):
        # self.loop = asyncio.get_event_loop()
        # self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.es = Elasticsearch()

    def store_user(self, user):
        # user_json = json.loads(str(user))
        # user_json['@timestamp'] = datetime.strptime(user.created_at, '%a %b %d %X %z %Y').isoformat()
        # self.es.index(index='tweeter', doc_type='user', body=str(user), id=user.id)
        pass

    def store_tweet(self, tweet):
        tweet_dic = json.loads(str(tweet))
        tweet_dic['@timestamp'] = datetime.strptime(tweet.created_at, '%a %b %d %X %z %Y').isoformat()
        # tweet_dic['_id'] = tweet_dic['id_str']

        # del tweet.user_mentions
        # del tweet.hashtags
        # del tweet.urls
        # del tweet.media  # not necessary
        # del tweet.coordinates

        # self.sock.sendto(bytearray(json.dumps(tweet_dic), 'UTF-8'), (address, port))

        res = self.es.index(index='twitter', doc_type='tweet', body=json.dumps(tweet_dic), id=tweet.id)

        if res['result'] == 'updated':
            raise TweetAlreadyExistsException()

    def get_streamed_tweets(self):
        return [tweet['_source']['user']['id'] for tweet in self.es.search(index='logstash*', q='*:*', size='10000')['hits']['hits']]
