from .errors import NotImplemented
import numpy as np
import random

class Trader(object):
    """Base Trader class. Just inherit and implement proper methods"""

    def __init__(self):
        pass

    def step(self, ticker, orderbook):

        self.ticker = ticker

        self.ask_volumes = np.array(orderbook['asks'])[:, 1]
        self.bid_volumes = np.array(orderbook['bids'])[:, 1]

        self.ask_prices = np.array(orderbook['asks'])[:, 0]
        self.bid_prices = np.array(orderbook['bids'])[:, 0]

        self.current_price = (self.ask_prices[0] + self.bid_prices[0]) / 2

        return self.action()

    def action(self):
        """ Return values:
              -1 - Buy
              1  - Sell
              0  - Idle
        """

        raise NotImplemented('action() is not implemented yet')

class Random_trader(Trader):

    def __init__(self, threshold=0, predict_proba=1.0):
        super(Random_trader, self).__init__()
        self.predict_proba = predict_proba
        self.threshold = threshold

        self.state = 0
        self.action_proba = 0.5

    def predict(self):
        current_state = 1 if self.action_proba > self.threshold else \
            -1 if self.action_proba < -self.threshold else 0

        if current_state == self.state:
            # self.state = 0
            return 0

        self.state = np.sign(current_state)
        return self.state

    def preprocess_data(self):
        self.action_proba = (
            (np.sum(self.ask_volumes) / np.sum(self.bid_volumes)) ** 2) - 1

    def action(self):
        self.preprocess_data()

        if random.random() < self.predict_proba:
            return self.predict()
        return 0

class Simple_trader(Trader):

    def __init__(self, threshold=0, reverse=False):
        super(Simple_trader, self).__init__()
        self.threshold = threshold
        self.reverse = reverse

        self.state = -1
        self.current_price = 0
        self.last_action_price = 0

    def check_state(self):
        if self.state == 1:  # we want to buy
            if self.last_action_price > self.current_price * (1 + self.threshold):
                self.state = -1
                self.last_action_price = self.current_price
                return self.state * (-1)
        if self.state == -1:  # we want to sell
            if self.last_action_price < self.current_price * (1 - self.threshold):
                self.state = 1
                self.last_action_price = self.current_price
                return self.state * (-1)
        return 0

    def action(self):
        mul = -1 if self.reverse else 1
        return mul * self.check_state()

class Hodl_trader(Trader):

    def __init__(self):
        super(Hodl_trader, self).__init__()

    def action(self):
        return 0