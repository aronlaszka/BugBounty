import twitter
import sys
import pickle
from twitter.error import TwitterError
# from neo4j.v1 import GraphDatabase

api = twitter.Api(consumer_key='C64LEG4kait1EUTtZZ5ku5atD',
                  consumer_secret='y15FYcKGFUC8w7bDAkGiK50g0DONTRH3DgQb6FxfM82CZkRXEH',
                  access_token_key='833314858586296322-Syne61sXWTQ2Y5U9wfc9ZlpDcHSIpxd',
                  access_token_secret='CYXvRL4DMQFXjQebQwvQqrpPXZVesy3VNjL9vWThRCsIA')

# driver = GraphDatabase.driver(uri='bolt://localhost:7687',
#                               auth=('neo4j', '1'))


def main():
    try:
        # print(api.GetUser(user_id=795470076))
        whole = []
        last = -1
        while True:
            timeline = api.GetUserTimeline(user_id=795470076, count=200, max_id=None if last == -1 else last)
            if len(timeline) == 0:
                break

            last = timeline[-1].id
            for s in timeline:
                if s.user.id == 795470076:
                    whole.append(s)

        with open('data', 'wb') as output:
            pickle.dump(whole, output, pickle.HIGHEST_PROTOCOL)

        print(whole[-1])
        print(len(whole))



        # cursor = -1
        # for i in range(1, 10):
        #     fl = api.GetFriendsPaged(user_id=795470076, count=100, cursor=cursor)
        #     cursor = fl[0]
        #     print(str(i) + ' =====> ' + str(fl[2]))
    except TwitterError as te:
        e_print(te.message)


def e_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        exit(-2)
