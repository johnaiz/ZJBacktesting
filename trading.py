
import pandas as pd

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Account(object):

    def __init__(self, name, backTestingEngine, startCash = 0):
        self._name = name
        self._currentCash = startCash
        self._currentPosition = {}
        self._strategy = None
        self._record = PortfolioRecord(self) #TODO
        self._engine = backTestingEngine
        self._engine.registerAccount(self)
        self._orderCache = []
        self._recorder = Recorder(self._engine, self)
        self._officialRecorder = Recorder(self._engine, self)
        logger.info('Initialize Account %s', name)

    def getName(self):
        return self._name

    def setStrategy(self, strategy):
        self._strategy = strategy

    def getRecorder(self):
        return self._recorder

    def getOfficialRecorder(self):
        return self._officialRecorder

    def getEngine(self):
        return self._engine

    def getCurrentDate(self):
        return self._engine.getCurrentDate()

    def getCurrentCash(self):
        return self._currentCash

    def getCurrentPositions(self):
        return self._currentPosition

    def getCurrentWealth(self):
        return self.getCurrentCash() + sum(self.getCurrentPositions().values())

    def doTrading(self):
        # called by engine, main running function per day
        self._strategy.handleData()

    def eod(self): #TODO
        self._updatePosValue()
        self._officialRecorder.record(wealth = self.getCurrentWealth())

    def newOrder(self, order):
        # called by strategy
        trade = self._sendOrder(order)
        if trade:
            self._processTrade(trade)

    def _updatePosValue(self):
        logger.debug('eod updating positions values')
        marketData = self._engine.getMarketDataEOD()
        for pos, value in self.getCurrentPositions().iteritems():
            try:
                self.getCurrentPositions()[pos] = value * (1 + marketData.get_value(pos, 'Return'))
            except KeyError:
                pass
        logger.debug(self.getCurrentPositions())

    def _processTrade(self, trade):
        self._currentCash += trade.getCashDelta()
        posDelta = trade.getPositionsDelta()
        for pos, value in posDelta.iteritems():
            if pos in self._currentPosition:
                self._currentPosition[pos] += value
            else:
                self._currentPosition[pos] = value
        for p, q in self._currentPosition.items():
            if q == 0:
                del self._currentPosition[p]


    def _sendOrder(self, order):
        self._checkOrder(order)
        if order:
            trade = self._engine.processOrder(order, self)
            return trade
        else:
            logger.info('Warning: No order sent')
            return None

    def _checkOrder(self, order):
        return order


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


class Recorder(object):

    def __init__(self, engine, account):
        self._account = account
        self._engine = engine
        self._records = pd.DataFrame()

    def record(self, **kwargs):
        rec = pd.DataFrame(data = kwargs, index = [self._engine.getCurrentDate()])
        logger.debug('Record info: %s', str(rec))
        self._records = self._records.append(rec)

    def getRecords(self):
        return self._records

class PortfolioRecord(object):
    # Record positions/cash change
    def __init__(self, account):
        self._account = account
        self.cashSeries = []
        self.positionsSeries = []
        self.valueSeries = []
        self.dates = []

    def record(self):
        cash = self._account.getCurrentCash()
        positions = self._account.getCurrentPositions()
        totalValue = cash + sum(positions.values())
        date = self._account.getCurrentDate()
        self.cashSeries.append(cash)
        self.positionsSeries.append(positions)
        self.valueSeries.append(totalValue)
        self.dates.append(date)
        print """
Account %s %s overnight Record:
    Cash: %s
    Positions: %s
    Total Value: %s
""" %(self._account.getName(), str(date.date()), str(cash), str(positions), str(totalValue))


class Order(object):
    # Extentable to more complicated order
    def __init__(self, pos, commission = None):
        self._pos = pos
        self._commission = commission

    def getPositions(self):
        return self._pos

    def execute(self, market):
        if not self._commission:
            self._commission = market.getCommission()
        return self._pos

    def getCommissionFee(self):
        return self._commission.getFee(self) if self._commission else 0.0

class Trade(object):
    # the trade is delta of cash and positions
    def __init__(self, cashDelta, positionsDelta):
        assert isinstance(cashDelta, (float, int)), 'cash change should only be numbers'
        assert isinstance(positionsDelta, dict), 'positions change should be {p1: number, p2: number, ...}'
        self._cashDelta = cashDelta
        self._positionsDelta = positionsDelta

    def getCashDelta(self):
        return self._cashDelta

    def getPositionsDelta(self):
        return self._positionsDelta



class BaseCommission(object):
    def __init__(self):
        pass

    def getFee(self, order):
        raise NotImplementedError('Need to implement getFee function')


class FlatCommission(BaseCommission):
    def __init__(self, flatFee = 0):
        super(FlatCommission, self).__init__()
        self._flatFee = flatFee

    def getFee(self, order):
        return self._flatFee * len(order.getPositions())


class FixedRateCommission(BaseCommission):
    def __init__(self, rate):
        super(FlatCommission, self).__init__()
        self._rate = rate

    def getFee(self, order):
        # return sum([self._rate * ])
        pass