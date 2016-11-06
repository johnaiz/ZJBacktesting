

import logging

from trading import Order

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BaseStrategy(object):

    def __init__(self, account):
        self._account = account
        self._account.setStrategy(self)
        self._engine = self._account.getEngine()

    def getAccount(self):
        return self._account

    def newOrder(self, pos, commission = None):
        self._account.newOrder(Order(pos, commission=commission))

    def history(self):
        return self._engine.getDataHistory()

    def record(self, **kwargs):
        self._account.getRecorder().record(**kwargs)

    def getRecords(self):
        return self._account.getRecorder().getRecords()

    def getCurrentDate(self):
        return self._engine.getCurrentDate()

    def getMarketData(self):
        return self._engine.getMarketData()

    def handleData(self):
        logger.info('In handle data')
        order = {}
        logger.info(str(self._account.getCurrentPositions()))
        if not self._account.getCurrentPositions():
            #buy sp500
            order = {'SP500': self._account.getCurrentCash()}
            self.newOrder(order)
            logger.info('new order')

        wealth = self._account.getCurrentWealth()
        positions = self._account.getCurrentPositions()
        cash = self._account.getCurrentCash()

        self.record(wealth = wealth, positions = [positions.copy()], cash = cash, order = [order.copy()])