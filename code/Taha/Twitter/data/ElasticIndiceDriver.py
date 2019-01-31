from elasticsearch.client.indices import IndicesClient
from elasticsearch import Elasticsearch

import json


class ElasticIndiceDriver:

    def __init__(self, client: Elasticsearch):
        self.client = IndicesClient(client)

    def create_index(self, index: str, mapping: dict):
        self.client.create(index, json.dumps(mapping))

    def clean_index(self, index: str):
        self.client.delete(index)
        self.client.delete(f'{index}-finished')
