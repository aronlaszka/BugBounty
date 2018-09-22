from TwitterAPI import TwitterAPI
import time
import codecs

#replace all XXXXX with appropriate values

#download tweets from the time interval [thisid, oldest_id]
#obtain these numbers by going to twitter accounts, find the
#tweet of the correct date/time, click on the tweet then
#the url will look something like this:
#https://twitter.com/NWS/status/1043161212761120769
#the numbers at the end is the id you use for thisid
#and oldest_id and that id corresponds to 10:33 AM - 21 Sep 2018

#replace the numbers for thisid and oldest_id
thisid = 'XXXXX'
oldest_id = 'XXXXX'

#you should obtain these from your developers account console on twitter
consumer_key = "XXXXX"
consumer_secret = "XXXXX"

api = TwitterAPI(consumer_key, consumer_secret, auth_type='oAuth2')

out_fname = './outfile.json'
txt_fname = './outfile.txt'

r = api.request('application/rate_limit_status')

print (r.text)
for i in range(0,900000):
    time.sleep(1)

    #replace XXXX with query string
    r = api.request('search/tweets', {'q':'XXXXX','result_type':'recent','count':'100','max_id':thisid,})

    for item in r.get_iterator():
        try:
            print (i, item['created_at'], item['id'])
        except KeyError:
            print (i, "keyerror")

        f2 = codecs.open(txt_fname, 'a', 'utf-8')
        f2.write(item['text'])
        f2.close()
        f = open(out_fname, 'a')
        f.write(str(item))
        f.write("\n")
        f.close()
        thisid = item['id']
        thisid = int(thisid) - 1

    if thisid < int(oldest_id):
        print ('Tweet download completed!')
        break

print(out_fname)
