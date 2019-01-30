import json
from datetime import datetime

from exceptions.database_exceptions import UserAlreadyCrawledException


class ElasticDriver:
    def __init__(self, connection, index):
        self.es = connection
        self.index = index

    def store_tweet(self, tweet):
        tweet_dic = json.loads(str(tweet))
        tweet_dic['@timestamp'] = datetime.strptime(tweet.created_at, '%a %b %d %X %z %Y').isoformat()

        res = self.es.index(index=self.index, doc_type='tweet', body=json.dumps(tweet_dic), id=tweet.id)

        if res['result'] == 'updated':
            if self.is_user_completed(tweet_dic['user']['id']):
                raise UserAlreadyCrawledException()
            #pass

    # def get_streamed_tweets(self):
    #     return [tweet['_source']['user']['id'] for tweet in self.es.search(index='logstash*', q='*:*', size='10000')['hits']['hits']]

    def mark_user_something(self, user, what):
        if what == 'CompletedUser':
            self.es.index(index=f'{self.index}-finished', doc_type='user', body=json.dumps({'id': user}))

    def is_user_completed(self, user):
        return self.es.search(index=f'{self.index}-finished', doc_type='user', q=f'id:{user}')['hits']['total'] > 0

    def clean(self):
        self.es.delete_by_query(index=f'{self.index}', doc_type='', body='{"query": {"match_all": {}}}')
        self.es.delete_by_query(index=f'{self.index}-finished', doc_type='', body='{"query": {"match_all": {}}}')
