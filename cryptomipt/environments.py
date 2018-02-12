# -*- coding: utf-8 -*-
import pandas as pd
import os
import re
import json
import asyncio
import logging
from itertools import filterfalse
from .utils import ESfetcher
from datetime import datetime

logger = logging.getLogger('trader')


class ccxt(object):

    fake = True

    def __init__(self, tickers=None, orderbooks=None, data_dir='data/',
                 pair='usdbch', exchange='kraken', latency=0,
                 time_from='2009-01-01', time_to=datetime.now().strftime("%Y-%m-%d"),
                 first_balance={'BCH': 0}, second_balance={'USD': 100}):

        """ Base class for exchange simulators

            first_balance - main currency start balance
            second_balance - base currency start balance
        """

        self.time_from = time_from
        self.time_to = time_to

        self.tickers = None
        self.orderbooks = None

        self.latency = latency

        if tickers is None or orderbooks is None:
            filename_re = re.compile(
                '{pair}_{exchange}_.*_(.*)_(.*)\.json'.format(pair=pair, exchange=exchange))

            datasets = [(datetime.strptime(filename_re.search(f).group(1), "%Y-%m-%d"),
                         datetime.strptime(filename_re.search(f).group(2), "%Y-%m-%d"))
                        for f in os.listdir('./' + data_dir) if filename_re.search(f)]

            datasets = sorted(list(set(datasets)), key=lambda x: x[1])

            isFound = False

            for dataset in datasets:
                if dataset[0].strftime("%Y-%m-%d") == time_from and dataset[1].strftime("%Y-%m-%d") == time_to:
                    tickers = data_dir + '{pair}_{exchange}_ticker_{time_from}_{time_to}.json'\
                        .format(pair=pair, exchange=exchange,
                                time_from=dataset[0].strftime("%Y-%m-%d"),
                                time_to=dataset[1].strftime("%Y-%m-%d"))
                    orderbooks = data_dir + '{pair}_{exchange}_orderbook_{time_from}_{time_to}.json'\
                        .format(pair=pair, exchange=exchange,
                                time_from=dataset[0].strftime("%Y-%m-%d"),
                                time_to=dataset[1].strftime("%Y-%m-%d"))
                    try:
                        self.open_dataset(tickers_path=tickers,
                                          orderbooks_path=orderbooks)
                        isFound = True
                        logger.info('Found local dataset')
                        break
                    except Exception as err:
                        logger.info(str(err))
                        self.remove_dataset(
                            tickers_path=tickers, orderbooks_path=orderbooks)

            if not isFound:
                logger.info('Dataset not found, downloading new dataset')
                tickers, orderbooks = ESfetcher.download_dataset(exchange=exchange, pair=pair,
                                                                 time_from=self.time_from,
                                                                 time_to=self.time_to,
                                                                 data_dir=data_dir)

        if self.tickers is None and self.orderbooks is None:
            self.open_dataset(tickers_path=tickers, orderbooks_path=orderbooks)

        self.first_symbol = list(first_balance.keys())[0]
        self.second_symbol = list(second_balance.keys())[0]
        self.first_balance = first_balance[self.first_symbol]
        self.second_balance = second_balance[self.second_symbol]
        self.sell_price = 0
        self.buy_price = 0
        self.prefetch_order_book()
        self.prefetch_ticker()
        self.timestamp = 0

    def open_dataset(self, tickers_path, orderbooks_path):
        # check files
        try:
            self.tickers = open(tickers_path, 'r')
            self.orderbooks = open(orderbooks_path, 'r')
        except:
            raise FileNotFoundError

    def remove_dataset(self, tickers_path, orderbooks_path):
        try:
            os.remove(tickers_path)
        except:
            logger.debug(
                'No ticker file {}, invalid dataset'.format(tickers_path))

        try:
            os.remove(orderbooks_path)
        except:
            logger.debug(
                'No orderbook file {}, invalid dataset'.format(tickers_path))

    async def fetch_balance(self):
        await asyncio.sleep(self.latency)
        return {
            'info': {self.first_symbol: self.first_balance, self.second_symbol: self.second_balance},
            'timestamp': self.timestamp
        }

    def get_time(self):
        return self.timestamp

    def prefetch_order_book(self, pair=''):
        self.order_book = self.orderbooks.readline()
        if not self.order_book:
            return None
        self.order_book = json.loads(self.order_book)
        self.sell_price = self.order_book['asks'][0][0]
        self.buy_price = self.order_book['bids'][0][0]
        self.timestamp = self.order_book['timestamp']
        return self.order_book

    def prefetch_ticker(self, pair=''):
        self.ticker = self.tickers.readline()
        if not self.ticker:
            return None
        self.ticker = json.loads(self.ticker)
        return self.ticker

    async def fetch_order_book(self, pair=''):
        # if time and time > self.order_book['timestamp']:
        await asyncio.sleep(self.latency)
        self.prefetch_order_book()
        # logger.info('Skipped orderbook: probably trader is too slow')
        return self.order_book

    async def fetch_ticker(self, pair=''):
        # if time and time > self.order_book['timestamp']:
        await asyncio.sleep(self.latency)
        self.prefetch_ticker()
        # logger.info('Skipped ticker: probably trader is too slow')
        return self.ticker

    def executeMarketBuyOrder(self, vol):
        return vol * self.buy_price

    async def createMarketBuyOrder(self, pair, vol):
        self.first_balance += vol
        self.second_balance -= self.executeMarketBuyOrder(vol)

    def executeMarketSellOrder(self, vol):
        return vol * self.sell_price

    async def createMarketSellOrder(self, pair, vol):
        self.first_balance -= vol
        self.second_balance += self.executeMarketSellOrder(vol)

class kraken(ccxt):
    def __init__(self, *args, **kwargs):
        ccxt.__init__(self, *args, **kwargs)
        self.id = self.__class__.__name__