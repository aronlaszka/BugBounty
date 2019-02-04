import json
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

import click
import tweepy
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from crawler.Crawler import Crawler
from data.ElasticDriver import ElasticDriver
from wrapper.TwitterDriver import TwitterDriver
from wrapper.TweepyWrapper import TweepyWrapper
from data.ElasticIndiceDriver import ElasticIndiceDriver

# Configuring logger

rootLogger = logging.getLogger()

logFormatter = logging.Formatter('%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')

fileHandler = TimedRotatingFileHandler('logs/log.log', when='d', interval=1, backupCount=1)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

rootLogger.setLevel(logging.INFO)

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
        ed = ElasticDriver(es, search['name'])
        i = 1
        for twitterAccount in search['twitterAccounts']:
            td = TwitterDriver(
                search['keywords'],
                ed,
                search['sensitivity'],
                twitterAccount['consumer_key'],
                twitterAccount['consumer_secret'],
                twitterAccount['access_token_key'],
                twitterAccount['access_token_secret']
            )
            threads.append(Crawler(f'{search["name"]}-{i}', search['keywords'], es, td))
            i += 1

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

@click.command(name='stream')
def stream():
    es = create_es_connection(
        config['database']['host'],
        config['database']['access_key'],
        config['database']['secret_key'],
        config['database']['region'])

    for search in config['searches']:
        ed = ElasticDriver(es, search['name'])
        i = 1
        for twitterAccount in search['twitterAccounts']:
            td = TwitterDriver(
                search['keywords'],
                ed,
                search['sensitivity'],
                twitterAccount['consumer_key'],
                twitterAccount['consumer_secret'],
                twitterAccount['access_token_key'],
                twitterAccount['access_token_secret']
            )
            streamer = TweepyWrapper(td)

            auth = tweepy.OAuthHandler(twitterAccount['consumer_key'],
                twitterAccount['consumer_secret'])
            auth.set_access_token(twitterAccount['access_token_key'],
                twitterAccount['access_token_secret'])

            stream = tweepy.Stream(auth=auth, listener=streamer)
            stream.filter(track=search['keywords'], is_async=True)
            logging.info(f'Listener started for {search["name"]}...')
            i += 1


@click.command(name='create')
def create_indexes():
    es = create_es_connection(
        config['database']['host'],
        config['database']['access_key'],
        config['database']['secret_key'],
        config['database']['region'])
    esi = ElasticIndiceDriver(es)
    with open('mapping.json') as mapping_file:
        mapping = json.load(mapping_file)
    for search in config['searches']:
        esi.create_index(search['name'], {'mappings': {'_doc': mapping}})
        logging.info(f'Index created: {search["name"]}')
        esi.create_index(f'{search["name"]}-finished', {})
        logging.info(f'Index created: {search["name"]}-finished')


@click.command(name='clean')
@click.argument('index')
def clean(index):
    ElasticIndiceDriver(create_es_connection(
        config['database']['host'],
        config['database']['access_key'],
        config['database']['secret_key'],
        config['database']['region'])).clean_index(index)


@click.command(name='reindex')
@click.argument('src')
@click.argument('src_type')
@click.argument('dst')
@click.argument('dst_type')
def reindex(src, src_type, dst, dst_type):
    ElasticDriver(create_es_connection(
        config['database']['host'],
        config['database']['access_key'],
        config['database']['secret_key'],
        config['database']['region']), None).reindex({
            'source': {
                'index': src,
                'type': src_type
            },
            'dest': {
                'index': dst,
                'type': dst_type
            }
    })


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    pass


cli.add_command(clean)
cli.add_command(testDb)
cli.add_command(start)
cli.add_command(create_indexes)
cli.add_command(reindex)
cli.add_command(stream)