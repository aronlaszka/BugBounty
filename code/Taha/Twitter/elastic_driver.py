from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from exceptions import UserAlreadyCrawledException
from requests_aws4auth import AWS4Auth
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

        host = ''
        access_key = ''
        secret_key = ''

        region = ''

        awsauth = AWS4Auth(access_key, secret_key, region, 'es')

        self.es = Elasticsearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

        print(self.es.info())

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
            if self.is_user_completed(tweet_dic['user']['id']):
                raise UserAlreadyCrawledException()
            #pass

    def get_streamed_tweets(self):
        return [tweet['_source']['user']['id'] for tweet in self.es.search(index='logstash*', q='*:*', size='10000')['hits']['hits']]

    def mark_user_something(self, user, what):
        if what == 'CompletedUser':
            self.es.index(index='twitter-finished', doc_type='user', body=json.dumps({'id': user}))

    def is_user_completed(self, user):
        return self.es.search(index='twitter-finished', doc_type='user', q=f'id:{user}')['hits']['total'] > 0
