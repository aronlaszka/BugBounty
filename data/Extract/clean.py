from elasticsearch import Elasticsearch
import csv

es = Elasticsearch()

users = {}

def do_bbt_count():

    for bucket in es.search(index='twitter', body='''
    {
      "aggs": {
        "2": {
          "terms": {
            "field": "user.screen_name.keyword",
            "size": 10000,
            "order": {
              "_count": "desc"
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
                  "gte": 1357020000000,
                  "lte": 1546322399999,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "filter": [
            {
              "bool": {
                "should": [
                  {
                    "bool": {
                      "should": [
                        {
                          "match_phrase": {
                            "text": "bugbounty"
                          }
                        }
                      ],
                      "minimum_should_match": 1
                    }
                  },
                  {
                    "bool": {
                      "should": [
                        {
                          "bool": {
                            "should": [
                              {
                                "match_phrase": {
                                  "text": "bug bounty"
                                }
                              }
                            ],
                            "minimum_should_match": 1
                          }
                        },
                        {
                          "bool": {
                            "should": [
                              {
                                "bool": {
                                  "should": [
                                    {
                                      "match_phrase": {
                                        "text": "togetherwehitharder"
                                      }
                                    }
                                  ],
                                  "minimum_should_match": 1
                                }
                              },
                              {
                                "bool": {
                                  "should": [
                                    {
                                      "bool": {
                                        "should": [
                                          {
                                            "match_phrase": {
                                              "text": "bugbountytip"
                                            }
                                          }
                                        ],
                                        "minimum_should_match": 1
                                      }
                                    },
                                    {
                                      "bool": {
                                        "should": [
                                          {
                                            "bool": {
                                              "should": [
                                                {
                                                  "match_phrase": {
                                                    "text": "ittakesacrowd"
                                                  }
                                                }
                                              ],
                                              "minimum_should_match": 1
                                            }
                                          },
                                          {
                                            "bool": {
                                              "should": [
                                                {
                                                  "bool": {
                                                    "should": [
                                                      {
                                                        "match_phrase": {
                                                          "text": "hackerone"
                                                        }
                                                      }
                                                    ],
                                                    "minimum_should_match": 1
                                                  }
                                                },
                                                {
                                                  "bool": {
                                                    "should": [
                                                      {
                                                        "bool": {
                                                          "should": [
                                                            {
                                                              "match_phrase": {
                                                                "text": "bugcrowd"
                                                              }
                                                            }
                                                          ],
                                                          "minimum_should_match": 1
                                                        }
                                                      },
                                                      {
                                                        "bool": {
                                                          "should": [
                                                            {
                                                              "bool": {
                                                                "should": [
                                                                  {
                                                                    "match_phrase": {
                                                                      "entities.hashtags.text.keyword": "bugbounty"
                                                                    }
                                                                  }
                                                                ],
                                                                "minimum_should_match": 1
                                                              }
                                                            },
                                                            {
                                                              "bool": {
                                                                "should": [
                                                                  {
                                                                    "bool": {
                                                                      "should": [
                                                                        {
                                                                          "match_phrase": {
                                                                            "entities.hashtags.text.keyword": "bug bounty"
                                                                          }
                                                                        }
                                                                      ],
                                                                      "minimum_should_match": 1
                                                                    }
                                                                  },
                                                                  {
                                                                    "bool": {
                                                                      "should": [
                                                                        {
                                                                          "bool": {
                                                                            "should": [
                                                                              {
                                                                                "match_phrase": {
                                                                                  "entities.hashtags.text.keyword": "togetherwehitharder"
                                                                                }
                                                                              }
                                                                            ],
                                                                            "minimum_should_match": 1
                                                                          }
                                                                        },
                                                                        {
                                                                          "bool": {
                                                                            "should": [
                                                                              {
                                                                                "bool": {
                                                                                  "should": [
                                                                                    {
                                                                                      "match_phrase": {
                                                                                        "entities.hashtags.text.keyword": "bugbountytip"
                                                                                      }
                                                                                    }
                                                                                  ],
                                                                                  "minimum_should_match": 1
                                                                                }
                                                                              },
                                                                              {
                                                                                "bool": {
                                                                                  "should": [
                                                                                    {
                                                                                      "bool": {
                                                                                        "should": [
                                                                                          {
                                                                                            "match_phrase": {
                                                                                              "entities.hashtags.text.keyword": "ittakesacrowd"
                                                                                            }
                                                                                          }
                                                                                        ],
                                                                                        "minimum_should_match": 1
                                                                                      }
                                                                                    },
                                                                                    {
                                                                                      "bool": {
                                                                                        "should": [
                                                                                          {
                                                                                            "bool": {
                                                                                              "should": [
                                                                                                {
                                                                                                  "match_phrase": {
                                                                                                    "entities.hashtags.text.keyword": "hackerone"
                                                                                                  }
                                                                                                }
                                                                                              ],
                                                                                              "minimum_should_match": 1
                                                                                            }
                                                                                          },
                                                                                          {
                                                                                            "bool": {
                                                                                              "should": [
                                                                                                {
                                                                                                  "match_phrase": {
                                                                                                    "entities.hashtags.text.keyword": "bugcrowd"
                                                                                                  }
                                                                                                }
                                                                                              ],
                                                                                              "minimum_should_match": 1
                                                                                            }
                                                                                          }
                                                                                        ],
                                                                                        "minimum_should_match": 1
                                                                                      }
                                                                                    }
                                                                                  ],
                                                                                  "minimum_should_match": 1
                                                                                }
                                                                              }
                                                                            ],
                                                                            "minimum_should_match": 1
                                                                          }
                                                                        }
                                                                      ],
                                                                      "minimum_should_match": 1
                                                                    }
                                                                  }
                                                                ],
                                                                "minimum_should_match": 1
                                                              }
                                                            }
                                                          ],
                                                          "minimum_should_match": 1
                                                        }
                                                      }
                                                    ],
                                                    "minimum_should_match": 1
                                                  }
                                                }
                                              ],
                                              "minimum_should_match": 1
                                            }
                                          }
                                        ],
                                        "minimum_should_match": 1
                                      }
                                    }
                                  ],
                                  "minimum_should_match": 1
                                }
                              }
                            ],
                            "minimum_should_match": 1
                          }
                        }
                      ],
                      "minimum_should_match": 1
                    }
                  }
                ],
                "minimum_should_match": 1
              }
            }
          ],
          "should": [],
          "must_not": []
        }
      }
    }
    ''')['aggregations']['2']['buckets']:
        # user = es.search(index='twitter', q='user.screen_name:%s' % bucket['key'], size=1)['hits']['hits'][0]['_source']['user']
        users[bucket['key']] = {}
        users[bucket['key']]['bbt'] = bucket['doc_count']


def do_fav_count(field):
    for bucket in es.search(index='twitter', body='''
    {
  "aggs": {
    "2": {
      "terms": {
        "field": "user.screen_name.keyword",
        "size": 10000,
        "order": {
          "1": "desc"
        }
      },
      "aggs": {
        "1": {
          "max": {
            "field": "user.%s"
          }
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
              "gte": 1357020000000,
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
''' % field)['aggregations']['2']['buckets']:
        try:
            users[bucket['key']][field] = int(bucket['1']['value'])
        except:
            pass

do_bbt_count()
do_fav_count('favourites_count')
do_fav_count('statuses_count')
do_fav_count('followers_count')

with open('top_users.csv', 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['user', 'bb','favorite','status','follower'])

    for user in users:
        writer.writerow([
            user,
            users[user]['bbt'],
            users[user]['favourites_count'] if 'favourites_count' in users[user] else 0,
            users[user]['statuses_count'] if 'statuses_count' in users[user] else 0,
            users[user]['followers_count'] if 'followers_count' in users[user] else 0,
        ])