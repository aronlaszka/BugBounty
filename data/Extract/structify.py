from elasticsearch import Elasticsearch
import csv

es = Elasticsearch()

def extract(input):

    pos_1 = input.find(' disclosed a bug submitted by ')
    pos_2 = input.find(':', pos_1)
    pos_3 = input.find('$')
    pos_4 = input.find(' ', pos_3)

    if pos_1 == -1:
        return -1

    company = input[0:pos_1]
    user = input[pos_1 + 30:pos_2]
    if pos_3 == -1:
        return -1
    bounty = input[pos_3 + 1:pos_4].replace('…', '').replace(',','') if pos_3 != -1 else '0'

    return [company, user, bounty]


results = es.search(index='twitter', doc_type='tweet', q='user.screen_name:"disclosedh1"', size=4000)

# orig_sample = 'Chaturbate disclosed a bug submitted by kaustubh:' \
#               ' https://t.co/h5DG8HRpSE #hackerone #bugbounty https://t.co/Z00begFgHT'

with open('disclosedh1.csv', 'w') as file:
    writer = csv.writer(file)

    writer.writerow(['bounty', 'fav'])

    for t in results['hits']['hits']:
        extracted = extract(t['_source']['text'])
        if extracted != -1 and 'favorite_count' in t['_source']:
            extracted.append(t['_source']['favorite_count'])
            writer.writerow(extracted[2:4])
