import pickle
import re
import copy
from neo4j.v1 import GraphDatabase


# driver = GraphDatabase.driver(uri='bolt://localhost:7687',
#                               auth=('neo4j', '1'))


def main():
    with open('data', 'rb') as input:
        tweets = pickle.load(input)

    for tweet in tweets:
        try:
            if tweet.media is not None:
                print(tweet)
                break
        except Exception as e:
            print(e.message)


def store_tweet(tweet):

    tweet = copy.deepcopy(tweet)

    del tweet.user
    del tweet.user_mentions
    del tweet.hashtags
    del tweet.urls
    del tweet.media

    run_query('MERGE (t:Tweet { id: %d }) SET t += %s RETURN t'
              % (tweet.id, str(tweet)))


def mention(tweet_id, user_id):
    run_query('MERGE (u:User { id: %d }) MERGE (t:Tweet { id: %d }) MERGE (t)-[r:Mention]->(u) RETURN r'
              % (tweet_id, user_id))


def tag(tweet_id, text):
    run_query('MERGE (t:Tweet { id: %d }) MERGE (tag:Tag { text: %s }) MERGE (t)-[r:TaggedWith]->(tag) RETURN tag'
              % (tweet_id, text))


def include():
    pass


def store_user(user):
    user = copy.deepcopy(user)
    # TODO remove unused properties.
    run_query('MERGE (u:User { id: %d }) SET u += %s' % (user.id, str(user)))


def run_query(query):
    # with driver.session() as session:
    #     return session.write_transaction(execute, re.sub(r'(?<!: )(?<!\\)"(\S*?)"', '\\1', query))
    pass


def execute(tx, query):
    result = tx.run(query)
    return result.single()[0]


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        exit(-2)
