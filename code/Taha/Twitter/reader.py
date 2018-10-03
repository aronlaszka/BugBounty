import pickle
import re
import copy
from neo4j.v1 import GraphDatabase


driver = GraphDatabase.driver(uri='bolt://localhost:7687',
                              auth=('neo4j', '1'))


def main():
    with open('data', 'rb') as input:
        tweets = pickle.load(input)

    for tweet in tweets:
        try:
            store_tweet(tweet)
        except Exception as e:
            print(str(tweet))
            raise e


def store_tweet(otweet):

    tweet = copy.deepcopy(otweet)

    del tweet.user
    del tweet.user_mentions
    del tweet.hashtags
    del tweet.urls
    del tweet.media
    del tweet.retweeted_status
    del tweet.place
    del tweet.quoted_status
    del tweet.coordinates
    del tweet.geo

    run_query('MERGE (t:Tweet { id: %d }) SET t += %s RETURN t'
              % (tweet.id, str(tweet)))

    for m in otweet.user_mentions:
        mention(tweet.id, m)

    for t in otweet.hashtags:
        tag(tweet.id, t.text)

    for u in otweet.urls:
        include(tweet.id, u.expanded_url)

    if otweet.retweeted_status is not None:
        store_tweet(otweet.retweeted_status)
        retweet(tweet.id, otweet.retweeted_status.id)

    if otweet.quoted_status is not None:
        store_tweet(otweet.quoted_status)
        quote(tweet.id, otweet.quoted_status.id)


def mention(tweet_id, user):
    run_query('MERGE (u:User { id: %d }) MERGE (t:Tweet { id: %d }) MERGE (t)-[r:Mention]->(u) SET u += %s RETURN r'
              % (user.id, tweet_id, str(user)))


def tag(tweet_id, text):
    run_query('MERGE (t:Tweet { id: %d }) MERGE (tag:Tag { text: "%s" }) MERGE (t)-[r:TaggedWith]->(tag) RETURN tag'
              % (tweet_id, text))


def include(tweet_id, url):
    run_query('MERGE (t:Tweet { id: %d }) MERGE (u:Url { link: "%s" }) MERGE (t)-[r:Has]->(u) RETURN u'
              % (tweet_id, url))

def retweet(tweet_id, orig_id):
    run_query('MERGE (t:Tweet { id: %d }) MERGE (o:Tweet { id: %d }) MERGE (t)-[r:Retweet]->(o) RETURN o'
              % (tweet_id, orig_id))


def quote(tweet_id, quoted_id):
    run_query('MERGE (t:Tweet { id: %d }) MERGE (o:Tweet { id: %d }) MERGE (t)-[r:Quoted]->(o) RETURN o'
              % (tweet_id, quoted_id))

def store_user(user):
    user = copy.deepcopy(user)
    run_query('MERGE (u:User { id: %d }) SET u += %s' % (user.id, str(user)))


def run_query(query):
    with driver.session() as session:
        return session.write_transaction(execute, re.sub(r'(?<!: )(?<!\\)"(\S*?)"', '\\1', query))


def execute(tx, query):
    result = tx.run(query)
    return result.single()[0]


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        exit(-2)
