import re
import copy
import logging
from queue import Queue, Empty
from threading import Thread
from threading import Event
from neo4j.v1 import GraphDatabase
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s -'
                                                       ' %(name)s - %(levelname)s - %(message)s')


class Neo4jWrapper:

    def __init__(self, uri='bolt://localhost:7687', username='neo4j', password='1'):
        self.log = logging.getLogger(__name__)
        self.queue = Queue()
        self.driver = GraphDatabase.driver(uri=uri,
                                           auth=(username, password))

        self.log = logging.getLogger(__name__)

        self.log.info('Connected to Neo4j at ' + uri)

        self.log.info('running neo4jbatchinsert...')
        self.runner = Neo4jBatchInsert(self.queue, self.driver)
        self.runner.start()

    def end(self):
        self.log.info('finishing neo4j queue...')
        self.runner.end()
        self.runner.join()

    def store_tweet(self, otweet):

        tweet = copy.deepcopy(otweet)

        del tweet.user
        del tweet.user_mentions
        del tweet.hashtags
        del tweet.urls
        del tweet.media  # not necessary
        del tweet.retweeted_status
        del tweet.quoted_status
        del tweet.place  # TODO add these three
        del tweet.coordinates
        del tweet.geo
        del tweet.scopes

        self.queue_query('MERGE (t:Tweet { id: %d }) SET t += %s RETURN t'
                         % (tweet.id, str(tweet)))

        self.store_user(otweet.user)
        self.tweeted(tweet.id, otweet.user.id)

        for m in otweet.user_mentions:
            self.mention(tweet.id, m)

        for t in otweet.hashtags:
            self.tag(tweet.id, t.text)

        for u in otweet.urls:
            self.include(tweet.id, u.expanded_url)

        if otweet.retweeted_status is not None:
            self.store_tweet(otweet.retweeted_status)
            self.retweet(tweet.id, otweet.retweeted_status.id)

        if otweet.quoted_status is not None:
            self.store_tweet(otweet.quoted_status)
            self.quote(tweet.id, otweet.quoted_status.id)

    def mention(self, tweet_id, user):
        self.queue_query('MERGE (u:User { id: %d }) MERGE (t:Tweet { id: %d }) MERGE (t)-[r:Mention]->(u) SET u += %s'
                         % (user.id, tweet_id, str(user)))

    def tag(self, tweet_id, text):
        self.queue_query('MERGE (t:Tweet { id: %d }) MERGE (tag:Tag { text: "%s" }) MERGE (t)-[r:TaggedWith]->(tag)'
                         % (tweet_id, text.lower()))

    def include(self, tweet_id, url):
        self.queue_query('MERGE (t:Tweet { id: %d }) MERGE (u:Url { link: "%s" }) MERGE (t)-[r:Has]->(u)'
                         % (tweet_id, url.lower()))

    def retweet(self, tweet_id, orig_id):
        self.queue_query('MERGE (t:Tweet { id: %d }) MERGE (o:Tweet { id: %d }) MERGE (t)-[r:Retweet]->(o)'
                         % (tweet_id, orig_id))

    def quote(self, tweet_id, quoted_id):
        self.queue_query('MERGE (t:Tweet { id: %d }) MERGE (o:Tweet { id: %d }) MERGE (t)-[r:Quoted]->(o)'
                         % (tweet_id, quoted_id))

    def tweeted(self, tweet_id, user_id):
        self.queue_query('MERGE (t:Tweet { id: %d }) MERGE (u:User { id: %d }) MERGE (u)-[r:Tweeted]->(t)'
                         % (tweet_id, user_id))

    def store_user(self, ouser):
        user = copy.deepcopy(ouser)

        del user.status

        self.queue_query('MERGE (u:User { id: %d }) SET u += %s' % (user.id, str(user)))

    def following(self, user_o_id, user_f_id):
        self.queue_query('MERGE (o:User { id: %d }) MERGE (f:User {id: %d}) MERGE (o)-[r:Following]-(f)'
                         % (user_o_id, user_f_id))

    def mark_user_important(self, user_id):
        self.queue_query('MERGE (u:User { id: %d }) SET u :ImportantUser' % user_id)

    def mark_user_completed(self, user_id):
        self.queue_query('MATCH (u:User { id: %d }) SET u :CompletedUser' % user_id)

    def is_completed(self, user_id):
        with self.driver.session() as session:
            try:
                if 'CompletedUser' in session.run('MATCH (n:User { id: %d }) RETURN LABELS(n)'
                                                               % user_id).single()[0]:
                    return True
                return False
            except:
                return False

    def bugbounty_ratio(self, user_id):
        with self.driver.session() as session:
            bugbounty_count = 0
            total_count = 0
            results = session.run('MATCH (u:User {id: %d})-[:Tweeted]-(t) RETURN t.text' % user_id)
            for record in results:
                total_count += 1
                if self.is_bugbounty(record['t.text']):
                    bugbounty_count += 1
            print(bugbounty_count)
            print(total_count)
            return float(bugbounty_count) / total_count


    @staticmethod
    def is_bugbounty(tweet):
        bugbounty_words = ['bugbounty', 'bugbountytip', 'togetherwehitharder']
        for word in bugbounty_words:
            if word in tweet.lower():
                return True
        return False

    def queue_query(self, query):
        self.queue.put(re.sub(r'(?<!: )(?<!\\)"(\S*?)"', '\\1', query))


class Neo4jBatchInsert(Thread):
    def __init__(self, queue, driver):
        super().__init__()
        self.log = logging.getLogger(__name__)
        self.driver = driver
        self.queue = queue

        self.log.info('Neo4j queue now running...')
        self.finished = Event()

    def end(self):
        self.finished.set()

    def run(self):
        with self.driver.session() as session:
            while True:
                tx = session.begin_transaction()
                try:
                    for i in range(1, 500):
                        tx.run(self.queue.get(timeout=10))
                except Empty:
                    if self.finished.isSet():
                        self.log.info('finishing...')
                        break
                    else:
                        self.log.info('no new query after 10 seconds')
                finally:
                    tx.commit()
                    self.log.info('commit')
