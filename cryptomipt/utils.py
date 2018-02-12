import os
import sys
import time
from operator import itemgetter
import argparse
import json
import csv
import requests
import elasticsearch
import progressbar
from functools import wraps
from datetime import datetime

FLUSH_BUFFER = 1000  # Chunk of docs to flush in temp file
CONNECTION_TIMEOUT = 120
TIMES_TO_TRY = 3
RETRY_DELAY = 60
META_FIELDS = ['_id', '_index', '_score', '_type']
__version__ = '6.2.1'


# Retry decorator for functions with exceptions
def retry(ExceptionToCheck, tries=TIMES_TO_TRY, delay=RETRY_DELAY):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries = tries
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    print(e)
                    print('Retrying in %d seconds ...' % delay)
                    time.sleep(delay)
                    mtries -= 1
                else:
                    print('Done.')
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck as e:
                print('Fatal Error: %s' % e)
                exit(1)

        return f_retry

    return deco_retry


class ESfetcher:

    def __init__(self, index='*', host='91.235.136.166', port=9200, data_dir='data/',
                 time_from='2009-01-01', time_to=datetime.now().strftime("%Y-%m-%d")):
        self.host = host
        self.port = port
        self.index = index
        self.data_dir = data_dir
        self.num_results = 0
        self.scroll_ids = []
        self.time_from = time_from
        self.time_to = time_to
        self.scroll_time = '30m'

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def create_connection(self):
        es = elasticsearch.Elasticsearch(self.host, timeout=CONNECTION_TIMEOUT)
        es.cluster.health()
        self.es = es

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def check_indexes(self):
        indexes = self.es.indices.get_alias(self.index)

        for index in indexes:
            self.dump_index(index)

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def unblock_indexes(self):
        indexes = self.es.indices.get_alias(self.index)

        for index in indexes:
            self.unblock_index(index)

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def unblock_index(self, index):
        data = {
            "index": {
                "blocks": {
                    "read_only_allow_delete": "false"
                }
            }
        }
        requests.put('{host}:{port}/{index}/_settings?pretty'.format(host=self.host, port=self.port, index=index), data)

    def dump_index(self, index):
        page = self.es.search(
            index=index,
            scroll=self.scroll_time,
            size=1000,
            body={
                "query": {
                    "range": {  # expect this to return the one result on 2012-12-20
                        "tracker_time": {
                            "gte": self.time_from,
                            "lt": self.time_to,
                        }
                    }
                }
            },
            sort='_id'
        )
        sid = page['_scroll_id']
        scrolled = len(page['hits']['hits'])
        scroll_size = page['hits']['total']

        widgets = ['Downloading {index}'.format(index=str(index)),
                   progressbar.Bar(left='[', marker='#', right=']'),
                   progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
                   progressbar.Percentage(),
                   progressbar.FormatLabel('] [%(elapsed)s] ['),
                   progressbar.ETA(), '] [',
                   progressbar.FileTransferSpeed(unit='docs'), ']'
                   ]
        bar = progressbar.ProgressBar(
            widgets=widgets, maxval=scroll_size).start()
        bar.update(scrolled)

        if self.prepare_file(index, page['hits']['hits']) is None:
            return

        self.write_to_file(page['hits']['hits'])

        while (scrolled < scroll_size):
            # print ("Scrolling...")
            page = self.es.scroll(scroll_id=sid, scroll=self.scroll_time)
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scrolled += len(page['hits']['hits'])
            bar.update(scrolled)

            self.write_to_file(page['hits']['hits'])

    @staticmethod
    def download_dataset(exchange, pair, time_from='', time_to='', data_dir='data/'):
        es = ESfetcher(index='{pair}.{exchange}.*'.format(pair=pair, exchange=exchange),
                       time_from=time_from, time_to=time_to, data_dir=data_dir)

        try:
            es.create_connection()
        except:
            raise ConnectionError

        try:
            es.check_indexes()
        except:
            raise NameError

        return (data_dir + '{pair}_{exchange}_ticker_{time_from}_{time_to}.json'.format(pair=pair, exchange=exchange,
                                                                                        time_from=es.time_from, time_to=es.time_to),
                data_dir + '{pair}_{exchange}_orderbook_{time_from}_{time_to}.json'.format(pair=pair, exchange=exchange,
                                                                                           time_from=es.time_from, time_to=es.time_to))

    def prepare_file(self, index, data, type='json'):

        if len(data) == 0:
            return None

        self.filename = self.data_dir + '{index}_{time_from}_{time_to}.{type}'.format(index=index.replace('.', '_'),
                                                                                      time_from=self.time_from,
                                                                                      time_to=self.time_to,
                                                                                      type=type)
        file = open(self.filename, 'w')

        if type == 'json':
            pass

        if type == 'csv':
            self.csv_headers = get_headers(data[0]['_source'])
            file.write(','.join(self.csv_headers))

        self.file_type = type

        file.close()
        return 1

    def write_to_file(self, data):

        if self.file_type == 'json':
            with open(self.filename, 'a') as file:
                for hit in data:
                    file.write(json.dumps(hit['_source']) + '\n')
                file.close()

        if self.file_type == 'csv':
            self.csv_headers = []

            def to_keyvalue_pairs(source, ancestors=[], header_delimeter=','):
                def is_list(arg):
                    return type(arg) is list

                def is_dict(arg):
                    return type(arg) is dict

                if is_dict(source):
                    for key in source.keys():
                        to_keyvalue_pairs(source[key], ancestors + [key])

                elif is_list(source):
                    # if self.opts.kibana_nested:
                    [to_keyvalue_pairs(item, ancestors) for item in source]
                    # else:
                    #     [to_keyvalue_pairs(item, ancestors + [str(index)]) for index, item in enumerate(source)]
                else:
                    header = header_delimeter.join(ancestors)
                    if header not in self.csv_headers:
                        self.csv_headers.append(header)
                    try:
                        out[header] = '%s%s%s' % (out[header], ',', source)
                    except:
                        out[header] = source

            with open(self.filename, 'a') as tmp_file:
                for hit in data:
                    out = {}
                    if '_source' in hit and len(hit['_source']) > 0:
                        to_keyvalue_pairs(hit['_source'])
                        tmp_file.write('%s\n' % json.dumps(out))
                tmp_file.close()