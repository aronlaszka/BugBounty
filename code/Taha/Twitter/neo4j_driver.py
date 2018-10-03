import re
import copy
from neo4j.v1 import GraphDatabase


class Neo4jDriver:

    def __init__(self):
        self.driver = GraphDatabase.driver(uri='bolt://localhost:7687',
                              auth=('neo4j', '1'))

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

        self.run_query('MERGE (t:Tweet { id: %d }) SET t += %s RETURN t'
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
        self.run_query('MERGE (u:User { id: %d }) MERGE (t:Tweet { id: %d }) MERGE (t)-[r:Mention]->(u) SET u += %s'
                  % (user.id, tweet_id, str(user)))

    def tag(self, tweet_id, text):
        self.run_query('MERGE (t:Tweet { id: %d }) MERGE (tag:Tag { text: "%s" }) MERGE (t)-[r:TaggedWith]->(tag)'
                  % (tweet_id, text.lower()))

    def include(self, tweet_id, url):
        self.run_query('MERGE (t:Tweet { id: %d }) MERGE (u:Url { link: "%s" }) MERGE (t)-[r:Has]->(u)'
                  % (tweet_id, url.lower()))

    def retweet(self, tweet_id, orig_id):
        self.run_query('MERGE (t:Tweet { id: %d }) MERGE (o:Tweet { id: %d }) MERGE (t)-[r:Retweet]->(o)'
                  % (tweet_id, orig_id))

    def quote(self, tweet_id, quoted_id):
        self.run_query('MERGE (t:Tweet { id: %d }) MERGE (o:Tweet { id: %d }) MERGE (t)-[r:Quoted]->(o)'
                  % (tweet_id, quoted_id))

    def tweeted(self, tweet_id, user_id):
        self.run_query('MERGE (t:Tweet { id: %d }) MERGE (u:User { id: %d }) MERGE (u)-[r:Tweeted]->(t)'
                  % (tweet_id, user_id))

    def store_user(self, ouser):
        user = copy.deepcopy(ouser)

        del user.status

        self.run_query('MERGE (u:User { id: %d }) SET u += %s' % (user.id, str(user)))

    def following(self, user_o_id, user_f_id):
        self.run_query('MERGE (o:User {id: %d}) MERGE (f:User {id: %d}) MERGE (o)-[r:Following]-(f)')

    def mark_user_completed(self, user_id):
        self.run_query('MATCH (u:User {id: %d}) SET u.completed = true' % user_id)

    def run_query(self, query):
        with self.driver.session() as session:
            return session.write_transaction(self.execute, re.sub(r'(?<!: )(?<!\\)"(\S*?)"', '\\1', query))

    @staticmethod
    def execute(tx, query):
        result = tx.run(query)
        return result.single()
