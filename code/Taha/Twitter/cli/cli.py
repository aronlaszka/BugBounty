import json
import logging

import click
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from crawler.Crawler import Crawler
from data.ElasticDriver import ElasticDriver
from wrapper.driver import TwitterDriver

# Configuring logger

# logFormatter = logging.Formatter('%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
# rootLogger = logging.getLogger()

# fileHandler = TimedRotatingFileHandler('log.log', when='d', interval=1, backupCount=1)
# fileHandler.setFormatter(logFormatter)
# rootLogger.addHandler(fileHandler)

# consoleHandler = logging.StreamHandler()
# consoleHandler.setFormatter(logFormatter)
# rootLogger.addHandler(consoleHandler)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
logging.info('Logger initialized')

# Reading config file
with open('config.json') as config_file:
    config = json.load(config_file)


def create_es_connection(host, access_key, secret_key, region) -> Elasticsearch:
    awsauth = AWS4Auth(access_key, secret_key, region, 'es')

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    logging.info(es.info())
    logging.info('ES connection established.')

    return es


@click.command(name='clean')
@click.argument('index')
def clean(index):
    ElasticDriver(create_es_connection(
        config['database']['host'],
        config['database']['access_key'],
        config['database']['secret_key'],
        config['database']['region']), index).clean()


@click.command(name='testDb')
def testDb():
    create_es_connection(
        config['database']['host'],
        config['database']['access_key'],
        config['database']['secret_key'],
        config['database']['region'])


@click.command(name='start')
def start():
    es = create_es_connection(
        config['database']['host'],
        config['database']['access_key'],
        config['database']['secret_key'],
        config['database']['region'])
    threads = []
    for search in config['searches']:
        es = ElasticDriver(es, search['name'])
        for twitterAccount in search['twitterAccounts']:
            td = TwitterDriver(
                search['keywords'],
                es,
                twitterAccount['consumer_key'],
                twitterAccount['consumer_secret'],
                twitterAccount['access_token_key'],
                twitterAccount['access_token_secret']
            )
            threads.append(Crawler(search['keywords'], es, td))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    pass


cli.add_command(clean)
cli.add_command(testDb)
cli.add_command(start)
