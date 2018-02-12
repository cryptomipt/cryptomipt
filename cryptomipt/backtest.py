import webbrowser
import pandas as pd
from tqdm import tqdm_notebook, tqdm
import time
import asyncio
import logging
import numpy as np
from elasticsearch import Elasticsearch
from datetime import datetime
from concurrent.futures import CancelledError
from concurrent.futures import ThreadPoolExecutor

ELASTIC_SEARCH_ADDR = "91.235.136.166:9200"

# Если запускать в Jupyter или терминале - непонятно, что происходит
# Мало логов, также нет никакого анализа логов -
# Непонятно, я улушаю стратегию или нет

# create logger
logger = logging.getLogger('trader')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

# create console handler and set level to debug
fh = logging.FileHandler('order.tlog')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)

import string
import random
def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

class Backtest(object):

    visualization_url = '''http://91.235.136.166:5601/app/kibana#/visualize/edit/e3ca1560-041b-11e8-b9dc-3bf24d43b2ee?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-5m,mode:quick,to:now))&_a=(filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'0029a1c0-fdf6-11e7-a5d0-bd066e842525',key:trader_name,negate:!f,params:(query:{trader_name},type:phrase),type:phrase,value:{trader_name}),query:(match:(trader_name:(query:{trader_name},type:phrase))))),linked:!f,query:(language:lucene,query:''),uiState:(),vis:(aggs:!((enabled:!t,id:'1',params:(field:base_balance),schema:metric,type:avg),(enabled:!t,id:'2',params:(customInterval:'2h',extended_bounds:(),field:time,interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(enabled:!t,id:'3',params:(field:hodl),schema:metric,type:avg),(enabled:!t,id:'4',params:(field:total_base_balance),schema:metric,type:avg)),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,categoryAxes:!((id:CategoryAxis-1,labels:(show:!t,truncate:100),position:bottom,scale:(type:linear),show:!t,style:(),title:(),type:category)),grid:(categoryLines:!f,style:(color:%23eee)),legendPosition:right,seriesParams:!((data:(id:'1',label:'Average%20base_balance'),drawLinesBetweenPoints:!t,mode:normal,show:true,showCircles:!t,type:line,valueAxis:ValueAxis-1),(data:(id:'3',label:'Average%20hodl'),drawLinesBetweenPoints:!t,mode:normal,show:!t,showCircles:!t,type:line,valueAxis:ValueAxis-1),(data:(id:'4',label:'Average%20total_base_balance'),drawLinesBetweenPoints:!t,mode:normal,show:!t,showCircles:!t,type:line,valueAxis:ValueAxis-1)),times:!(),type:line,valueAxes:!((id:ValueAxis-1,labels:(filter:!f,rotate:0,show:!t,truncate:100),name:LeftAxis-1,position:left,scale:(defaultYExtents:!t,mode:normal,type:linear),show:!t,style:(),title:(text:'Average%20base_balance'),type:value))),title:'{trader_name}%20backtest',type:line))'''

    """ Inner classes for maintaining priority queues of data

        - class contains orderbook/ticker/balance raw data
        - has overloaded compare method for using timestamp as priority value in priority queue
    """

    class Orderbook(object):
        def __init__(self, orderbook):
            self.timestamp = orderbook['timestamp']
            self.orderbook = orderbook

        def __lt__(self, other):
            return self.timestamp < other.timestamp

    class Ticker(object):
        def __init__(self, ticker):
            self.timestamp = ticker['timestamp']
            self.ticker = ticker

        def __lt__(self, other):
            return self.timestamp < other.timestamp

    class Balance(object):
        def __init__(self, balance):
            self.timestamp = time.time()
            self.balance = balance

        def __lt__(self, other):
            return self.timestamp < other.timestamp

    """ Init methods
    """

    def __init__(self, exchange, pair='BCH/USD', max_trades=100,
                 trade_size=1, comission=0, base_symbol=None,
                 quote_symbol=None, log_balance_every_orderbook=100,
                 use_orderbooks=True, use_tickers=True,
                 request_pool_size=100, time_between_requests=0,
                 stop_timeout=4):

        self.time_between_requests = time_between_requests

        self.tickers_pool = asyncio.Queue(maxsize=request_pool_size)
        self.orderbooks_pool = asyncio.Queue(maxsize=request_pool_size)
        self.balances_pool = asyncio.Queue(maxsize=request_pool_size)

        self.tickers = asyncio.PriorityQueue(maxsize=request_pool_size)
        self.orderbooks = asyncio.PriorityQueue(maxsize=request_pool_size)
        self.balances = asyncio.PriorityQueue(maxsize=request_pool_size)

        self.exchange = exchange

        self._use_orderbooks = use_orderbooks
        self._use_tickers = use_tickers

        self.last_timestamp = None
        self.log_balance_every_orderbook = 10 ** 10
        self.history_base = []
        self.history_quote = []
        self.history_total = []
        self.history_hodl = []
        self.history_ts = []

        self.orderbook_counter = 0
        self.ticker_counter = 0

        self.comission = comission
        self.trade_size = trade_size
        self.log_balance_every_orderbook = log_balance_every_orderbook

        self.pair = pair
        self.max_trades = max_trades

        self.__stop_timeout = stop_timeout
        self.__checkpoint = time.time()

        self.isBacktest = False
        if hasattr(exchange, 'fake'):
            self.isBacktest = True

        self.base_symbol = base_symbol
        if base_symbol is None:
            self.base_symbol = pair.split('/')[1]

        self.quote_symbol = quote_symbol
        if quote_symbol is None:
            self.quote_symbol = pair.split('/')[0]

        try:
            self.init_elastic_search()
            self.log_es = True
            logger.info('Logging to remote elasticsearch')
        except:
            self.log_es = False
            logger.info('Elasticsearch is unreachable, only text logging')

    def init_elastic_search(self):
        try:
            self.es = Elasticsearch(ELASTIC_SEARCH_ADDR)
        except:
            raise TimeoutError
        self.data_index = self.base_symbol.lower() + self.quote_symbol.lower()
        test_type = 'production'
        if self.isBacktest:
            test_type = 'backtest'
        self.data_index = '{data_index}.{ex_id}.{tt}'.format(data_index=self.data_index,
                                                             ex_id=self.exchange.id, tt=test_type)
        create_index(self.es, self.data_index)

    """ get_* methods are supposed to get the most recent data

        - get the most recent available data from priority queue
        - skip and delete all other data from queue
        - log number of skipped data
    """

    async def get_balances(self):

        balances = await self.balances.get()
        while not self.balances.empty():
            balances = await self.balances.get()

        balances = balances.balance['info']
        base, quote = 0, 0
        if self.base_symbol in balances:
            base = balances[self.base_symbol]
        else:
            logger.debug(
                'Symbol {} not found in balances info'.format(self.base_symbol))

        if self.quote_symbol in balances:
            quote = balances[self.quote_symbol]
        else:
            logger.debug(
                'Symbol {} not found in balances info'.format(self.quote_symbol))

        return (float(base), float(quote))

    async def get_ticker(self):
        skipped = -1
        ticker = self.last_ticker
        while ((not self.tickers.empty()) or (ticker == self.last_ticker)) and self._use_tickers:
            ticker = await self.tickers.get()
            skipped += 1
        ticker = ticker.ticker
        self.last_ticker = ticker
        if skipped:
            logger.info(
                'Skipped {} tickers. Too slow trader or too high backtest speed'.format(skipped))
        self.last_price = (ticker['bid'] + ticker['ask']) / 2
        return ticker

    async def get_orderbook(self):
        skipped = -1
        orderbook = self.last_orderbook
        while ((not self.orderbooks.empty()) or (orderbook == self.last_orderbook)) and self._use_orderbooks:
            orderbook = await self.orderbooks.get()
            skipped += 1
        orderbook = orderbook.orderbook
        self.last_orderbook = orderbook
        if skipped:
            logger.info(
                'Skipped {} orderbooks. Too slow trader or too high backtest speed'.format(skipped))
        return orderbook

    """ fetch_* methods are supposed to fetch data from exchange

        - await for exchange response
        - if response is correct put data to priority queue
        - else log error and continue
        - delete task from task pool
    """

    async def fetch_balances(self):
        try:
            balances = await self.exchange.fetch_balance()
            logger.debug('got balance {}'.format(balances))
            await self.balances.put(Backtest.Balance(balances))
        except Exception as e:
            logger.debug(str(e))
        await self.balances_pool.get()

    async def fetch_orderbook(self):
        try:
            orderbook = await self.exchange.fetch_order_book(self.pair)
            logger.debug('got orderbook {}'.format(orderbook))
            await self.orderbooks.put(Backtest.Orderbook(orderbook))
            self.orderbook_counter += 1
        except Exception as e:
            logger.debug(str(e))
        await self.orderbooks_pool.get()

    async def fetch_ticker(self):
        try:
            ticker = await self.exchange.fetch_ticker(self.pair)
            logger.debug('got ticker {}'.format(ticker))
            await self.tickers.put(Backtest.Ticker(ticker))
            self.ticker_counter += 1
        except Exception as e:
            logger.debug(str(e))
        await self.tickers_pool.get()

    """ *_requester coroutines are supposed to continuously fetch data from exchange

        - if number of fetchers less than pool_size run another fetcher
        - else wait for queue availability
    """

    async def orderbook_requester(self):
        try:
            while True:
                await self.orderbooks_pool.put(asyncio.ensure_future(self.fetch_orderbook()))
                await asyncio.sleep(self.time_between_requests)
        except CancelledError:
            logger.info('Shutting down orderbook requester...')

    async def ticker_requester(self):
        try:
            while True:
                await self.tickers_pool.put(asyncio.ensure_future(self.fetch_ticker()))
                await asyncio.sleep(self.time_between_requests)
        except CancelledError:
            logger.info('Shutting down ticker requester...')

    async def balance_requester(self):
        try:
            while True:
                await self.balances_pool.put(asyncio.ensure_future(self.fetch_balances()))
                await asyncio.sleep(self.time_between_requests)
        except CancelledError:
            logger.info('Shutting down balance requester...')

    def __stop_coroutines(self, reason=''):
        if reason:
            reason = ': ' + reason
        logger.info('Shutting down{reason}'.format(reason=reason))
        for task in asyncio.Task.all_tasks():
            task.cancel()

    """ Redundant method (?)
    TODO: remove update_balance
    """

    async def update_balance(self):
        self.last_timestamp = time.time() + time.timezone
        self.base_balance, self.quote_balance = await self.get_balances()
        # self.last_price = self.get_price()
        self.total_base_balance = self.base_balance + \
            self.quote_balance * self.last_price

        self.history_base.append(self.base_balance)
        self.history_quote.append(self.quote_balance)
        self.history_total.append(self.total_base_balance)
        self.history_hodl.append(
            self.last_price * (self.start_quote_balance + self.start_base_balance / self.first_price))
        self.history_ts.append(self.last_timestamp)

        self.log_balance()

    def __report_alive(self):
        self.__checkpoint = time.time()

    async def __status_monitor(self):
        while time.time() - self.__checkpoint < self.__stop_timeout:
            await asyncio.sleep(self.__stop_timeout)
            logger.info('Orderbooks: {}it/s Tickers: {}it/s'.format(self.orderbook_counter / self.__stop_timeout,
                                                                    self.ticker_counter / self.__stop_timeout))
            self.ticker_counter = self.orderbook_counter = 0
        self.__stop_coroutines('status monitor timeout')

    """ Main synchronous test method that call async fetchers and async _test
    """

    def test(self, trader, name_prefix='', open_in_browser=False):

        if not isinstance(trader, Trader):
            print("Please pass Trader class.")
            return self._end_backtest()

        self.trader = trader
        self.stop_coroutines = False

        self.trader_name = name_prefix + '_' + trader.__class__.__name__
        if open_in_browser:
            webbrowser.open(self.visualization_url.format(
                trader_name=self.trader_name), new=0, autoraise=True)

        loop = asyncio.get_event_loop()
        tasks = [
            self.orderbook_requester(),
            self.ticker_requester(),
            self.balance_requester(),
            self.__status_monitor(),
            self.__test()
        ]
        try:
            self.__report_alive()
            loop.run_until_complete(asyncio.gather(*tasks))
        except CancelledError:
            logger.info('Stopped')
        # loop.stop()
        # loop.close()

    async def __test(self):
        self.start_base_balance, self.start_quote_balance = await self.get_balances()

        self.last_orderbook = {}
        self.last_ticker = {}
        __iterations = 0

        ticker = await self.get_ticker()
        self.first_price = ticker["last"]
        try:
            while __iterations < self.max_trades:
                __iterations += 1
                self.__report_alive()

                ticker = await self.get_ticker()
                orderbook = await self.get_orderbook()

                action = self.trader.step(ticker, orderbook)

                if action:
                    self.log_action(action)
                    self.last_timestamp = time.time() + time.timezone
                    await self._action(action)

                if __iterations % self.log_balance_every_orderbook == 0:
                    # await self.update_balance()
                    pass

            if __iterations == self.max_trades:
                self.__stop_coroutines(reason='max steps acheived')

        except Exception as e:
            # print(e)
            # Чтобы можно было нажать Keyboard Interrupt
            pass
        finally:
            return self._end_backtest(__iterations)

    async def _action(self, action):
        base, quote = await self.get_balances()

        acted = False
        # if action < 0 -> BUY quote / SELL base
        if action < 0:
            amount_to_buy = self.trade_size / \
                self.last_price * (1 - self.comission)
            if base < self.trade_size:
                self.log_str("Can't BUY {} {} for {} {} with that balance".format(
                    amount_to_buy, self.quote_symbol, self.trade_size, self.base_symbol
                ))
                return
            try:
                await self.exchange.createMarketBuyOrder(
                    self.pair, amount_to_buy)
                self.log_buy_order()
                acted = True
            except Exception as e:
                self.log_str("Can't BUY: {}".format(e))
                acted = False
            asyncio.ensure_future(self.update_balance())

        # if action > 0 -> SELL quote / BUY base
        elif action > 0:
            amount_to_sell = self.trade_size / \
                self.last_price * (1 - self.comission)
            if quote < amount_to_sell:
                self.log_str("Can't SELL {} {} for {} {} with that balance".format(
                    amount_to_sell, self.quote_symbol, self.trade_size, self.base_symbol
                ))
                return
            try:
                await self.exchange.createMarketSellOrder(self.pair, amount_to_sell)
                self.log_sell_order()
                acted = True
            except Exception as e:
                self.log_str("Can't SELL: {}".format(e))
                acted = False
            asyncio.ensure_future(self.update_balance())

        # asyncio.ensure_future(self.update_balance())
        # return self.history_total[-1]  # current_total

    def _end_backtest(self, steps=0):
        logger.info('Passed {steps} steps, taken {actions} actions'.format(
            steps=steps, actions=len(self.history_total)))
        if len(self.history_total) == 0:
            return None
        return pd.DataFrame([self.history_total, self.history_base[2:], self.history_hodl],
                            columns=self.history_ts,
                            index=["Total Balance", self.base_symbol.upper(), "HODL"]).T

    """ Log methods
    """

    def log_balance(self):
        logger.info('Total balance: {} {} ({} {} and {} {})'.format(
            self.total_base_balance, self.base_symbol,
            self.base_balance, self.base_symbol,
            self.quote_balance, self.quote_symbol
        ))
        if self.log_es:
            self.log_to_es()

    def log_to_es(self):
        state = {
            "timestamp": self.last_timestamp,
            "time": datetime.fromtimestamp(self.last_timestamp),
            "hodl":  self.history_hodl[-1],
            "total_base_balance": self.history_total[-1],
            "trader_name": self.trader_name,
            "base_balance": self.history_base[-1]
        }
        if self.isBacktest:
            state['time'] = datetime.fromtimestamp(
                self.exchange.get_time() / 1000)
        self.send_data(state)

    def log_action(self, action):
        action_name = "BUY" if action == -1 else "SELL" if action == 1 else "IDLE"
        logger.info('{trader_name} suggests to {action}'.format(
            trader_name=self.trader_name,
            action=action_name
        ))

    def log_str(self, string):
        logger.info(string)

    def log_buy_order(self):
        logger.info('SUCCESS BUY {qamount} {quote} FOR {bamount} {base}'.format(
            bamount=self.trade_size,
            quote=self.quote_symbol,
            qamount=self.trade_size /
            self.last_price * (1 - self.comission),
            base=self.base_symbol
        ))

    def log_sell_order(self):
        logger.info('SUCCESS SELL {qamount} {base} FOR {bamount} {quote}'.format(
            qamount=self.trade_size / self.last_price * (1 - self.comission),
            quote=self.base_symbol,
            bamount=self.trade_size,
            base=self.quote_symbol
        ))

    def send_data(self, state):
        self.es.create(index=self.data_index, id=state["timestamp"] * 1000,
                       doc_type='log', body=state)


def create_index(es, index):
    if (es.indices.exists(index)):
        logger.info('Already Exists, Skipping:! ' + index)
    else:
        es.indices.create(index)
