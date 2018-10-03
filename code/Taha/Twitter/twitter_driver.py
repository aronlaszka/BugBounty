import twitter


class TwitterDriver:

    def __init__(self):
        self.api = twitter.Api(consumer_key='C64LEG4kait1EUTtZZ5ku5atD',
                    consumer_secret='y15FYcKGFUC8w7bDAkGiK50g0DONTRH3DgQb6FxfM82CZkRXEH',
                    access_token_key='833314858586296322-Syne61sXWTQ2Y5U9wfc9ZlpDcHSIpxd',
                    access_token_secret='CYXvRL4DMQFXjQebQwvQqrpPXZVesy3VNjL9vWThRCsIA')

    def get_user_friends(self, user_id):
        whole = []
        cursor = -1
        while True:
            fl = self.api.GetFriendsPaged(user_id=user_id, count=1000, cursor=cursor)
            print(len(fl[2]))
            if len(fl[2]) == 0:
                break
            cursor = fl[0]
            whole += fl[2]
        return whole

    def get_user(self, user_id):
        user = self.api.GetUser(user_id)
        return user

    def get_tweets(self, user_id):
        whole = []
        last = -1
        while True:
            timeline = self.api.GetUserTimeline(user_id=user_id, count=200, max_id=None if last == -1 else last)
            if len(timeline) == 0:
                break
            last = timeline[-1].id
            for s in timeline:
                if s.user.id == user_id:
                    whole.append(s)
        return whole
