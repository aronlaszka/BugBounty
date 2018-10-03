from neo4j_driver import Neo4jDriver
from twitter_driver import TwitterDriver

nj = Neo4jDriver()
td = TwitterDriver()


def main():
    crawl_user(795470076)
    # u = td.get_user(795470076)
    # del u.asdf


def crawl_user(user_id):
    # print('getting user: ' + str(user_id))
    # nj.store_user(td.get_user(user_id))
    print('getting friends: ' + str(user_id))
    for u in td.get_user_friends(user_id):
        nj.following(user_id, u.id)
    # print('getting tweets: ' + str(user_id))
    # for t in td.get_tweets(user_id):
    #     nj.store_tweet(t)


if __name__ == '__main__':
    main()
