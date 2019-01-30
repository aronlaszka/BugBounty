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


class Neo4jDriver:

    def __init__(self, uri='bolt://localhost:7687', username='neo4j', password='1'):
        self.log = logging
        self.queue = Queue(maxsize=10 * 1000)
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
        del tweet.withheld_in_countries

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

    def mark_user_something(self, user_id, what):
        with self.driver.session() as session:
            session.run('MATCH (u:User { id: %d }) SET u :%s' % (user_id, what))

    def is_user_something(self, user_id, what):
        with self.driver.session() as session:
            try:
                if what in session.run('MATCH (n:User { id: %d }) RETURN LABELS(n)'
                                                               % user_id).single()[0]:
                    return True
                return False
            except:
                return False

    def expansion_candidates(self, skip=0):
        with self.driver.session() as session:
            return [record['u.id'] for record in session.run('match (u:User)-[r:Tweeted]-()'
                                                             ' return u.screen_name, u.id, count(r)'
                                                             ' order by count(r) desc skip %d' % skip)]

    def expansion_candidate_by_mention(self, skip=0):
        with self.driver.session() as session:
            return [record['u.id'] for record in session.run('match (u:User)-[m:Mention]-()'
                                                             ' return u.id, count(m)'
                                                             ' order by count(m) desc skip %d' % skip)]

    def queue_query(self, query):
        self.queue.put(re.sub(r'(?<!: )(?<!\\)"(\S*?)"', '\\1', query))


class Neo4jBatchInsert(Thread):
    def __init__(self, queue, driver):
        super().__init__()
        self.log = logging
        self.driver = driver
        self.queue = queue

        self.log.info('Neo4j queue now running...')
        self.finished = Event()

    def end(self):
        self.finished.set()

    def run(self):
        with self.driver.session() as session:
            while True:
                self.log.info('queue size is %d' % self.queue.qsize())
                tx = session.begin_transaction()
                try:
                    for i in range(0, 2 * 1000):
                        tx.run(self.queue.get(timeout=10))
                except Empty:
                    if self.finished.isSet():
                        self.log.info('finishing...')
                        break
                    else:
                        self.log.info('no new query after 10 seconds')
                finally:
                    self.log.info('before commit...')
                    tx.commit()
                    self.log.info('commit')
