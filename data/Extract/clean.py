from elasticsearch import Elasticsearch

es = Elasticsearch()

for bucket in es.search(index='twitter', body='''
{
  "aggs": {
    "2": {
      "terms": {
        "field": "user.screen_name.keyword",
        "size": 1000,
        "order": {
          "_count": "asc"
        }
      }
    }
  },
  "size": 0,
  "_source": {
    "excludes": []
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [
    {
      "field": "@timestamp",
      "format": "date_time"
    },
    {
      "field": "created_at",
      "format": "date_time"
    },
    {
      "field": "retweeted_status.created_at",
      "format": "date_time"
    },
    {
      "field": "retweeted_status.user.created_at",
      "format": "date_time"
    },
    {
      "field": "user.created_at",
      "format": "date_time"
    }
  ],
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "@timestamp": {
              "gte": 978328800000,
              "lte": 1546322399999,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "filter": [
        {
          "match_all": {}
        }
      ],
      "should": [],
      "must_not": []
    }
  }
}
''')['aggregations']['2']['buckets']:
    if bucket['doc_count'] < 10:
        es.delete(index='twitter', doc_type='tweet', id=es.search(index='twitter', q='user.screen_name:%s' % bucket['key'])['hits']['hits'][0]['_source']['id'])
