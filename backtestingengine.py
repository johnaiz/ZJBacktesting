import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import trading

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DatetimeManager(object):
    def __init__(self, dateRange):
        self._dateRange = dateRange
        self._currentDate = None
        self._currentIdx = None
        self._start = False
        self._endDate = None
        self._endIdx = None
        self._dataDate = None
        self._dataIdx = None

    def start(self, startDate = None, endDate = None):

        startDate = pd.Timestamp(startDate) if startDate else None
        endDate = pd.Timestamp(endDate) if endDate else None
        if startDate:
            self._currentIdx = self._findClosestDateIdx(startDate)
            self._currentDate = self._dateRange[self._currentIdx]
        else:
            self._currentIdx = 0
            self._currentDate = self._dateRange[self._currentIdx]

        if endDate:
            self._endIdx = self._findClosestDateIdx(endDate)
            self._endDate = self._dateRange[self._endIdx]
        else:
            self._endIdx = len(self._dateRange) - 1
            self._endDate = self._dateRange[self._endIdx]
        if self._endIdx < self._currentIdx:
            logger.error('End date %s should be greater than start date %s',
                         str(self._endDate), str(self._currentDate))
            return

        self._start = True
        logger.info('Start from %s to %s', str(self._currentDate), str(self._endDate))

    def _findClosestDate(self, date):
        d = self._dateRange.asof(date)
        if not isinstance(d, pd.Timestamp):
            return self._dateRange[0]
        return d

    def _findClosestDateIdx(self, date):
        return self._dateRange.get_loc(self._findClosestDate(date))

    def getCurrentDate(self):
        if not self._start:
            raise 'Datetime Manager not started yet'
        return self._currentDate

    def getPreviousDate(self):
        if not self._start:
            raise 'Datetime Manager not started yet'
        if self._currentIdx == 0:
            return None
        return self._dateRange[self._currentIdx - 1]

    def next(self):
        if not self._start:
            raise 'Datetime Manager not started yet'
        self._currentIdx += 1
        if self._currentIdx > self._endIdx:
            logger.info('End at %s', str(self._currentDate))
        self._currentDate = self._dateRange[self._currentIdx]
        logger.info('Date move to %s', str(self._currentDate))

    def hasNext(self):
        if not self._start:
            raise 'Datetime Manager not started yet'
        return self._currentIdx < self._endIdx

    def setCurrentDate(self, date):
        if not self._start:
            raise 'Datetime Manager not started yet'
        date = pd.Timestamp(date)
        idx = self._findClosestDateIdx(date)
        if idx > self._endIdx:
            logger.error('Cannot set date %s later than stop date %s', str(date), str(self._endDate))
        self._currentIdx = idx
        self._currentDate = self._dateRange[idx]

class Engine(object):

    def __init__(self, datafeed, *args, **kwargs):

        self._datafeed = datafeed
        self._accounts = [] #accounts registered
        self._defaultCommission = trading.FlatCommission()
        self._marketData = None
        self._DTManager = DatetimeManager(datafeed.getDateRange())

        # self._marketDate = None

    def start(self, startDate = None, endDate = None):
        self._DTManager.start(startDate, endDate)
        self.run()
        self.statsAndPlot()

    def getCommission(self):
        return self._defaultCommission

    def getCurrentDate(self):
        return self._DTManager.getCurrentDate()

    def getPreviousDate(self):
        return self._DTManager.getPreviousDate()

    def setDate(self, date):
        self._DTManager.setCurrentDate(date)

    def getMarketData(self):
        return self._datafeed.getMarketData(self.getPreviousDate())

    def getMarketDataEOD(self):
        return self._datafeed.getMarketData(self.getCurrentDate())

    def getDataHistory(self):
        return self._datafeed.getHistoricalData(self.getPreviousDate())

    def _nextDay(self):
        # next date frame

        # 1. market move:
        self._dateMove()

        # 3. all accounts do daily trading
        self._trading()

        # 2. end of day:
        self._eod()


    def _dateMove(self):
        self._DTManager.next()

    def _trading(self):
        logger.debug('inform all accounts for trading')
        for acc in self._accounts:
            acc.doTrading()

    def _eod(self):
        for acc in self._accounts:
            acc.eod()

    def _hasNext(self):
        return self._DTManager.hasNext()

    def run(self):
        logger.info('Start Backtesting')
        self._trading()
        self._eod()
        while(self._hasNext()):
            self._nextDay()
        logger.info('Backtesting finished')

    def processOrder(self, order, account):
        # Used by client account to invoke to process order
        pos = order.execute(self)
        tradeValue = sum(pos.values()) + order.getCommissionFee()
        if tradeValue <= account.getCurrentCash():
            return trading.Trade(cashDelta = -tradeValue, positionsDelta = pos)
        else:
            return None

    def registerAccount(self, account):
        self._accounts.append(account)

    def statsAndPlot(self):
        logger.info('Statistics and Plots')
        for account in self._accounts:
            logger.info('Account: %s', account.getName())
            logger.info(account.getRecorder().getRecords())

            logger.info('Official Report')
            logger.info(account.getOfficialRecorder().getRecords())



records = account.getRecorder().getRecords()
ore = account.getOfficialRecorder().getRecords()
lastLine = records.iloc[-1]
lastLine.set_value('wealth', ore.iloc[-1].get_value('wealth'))
lastLine.name += pd.Timedelta(days = 1)
records = records.append(lastLine)
records['trades'] = records.order.apply(len)
records['positionsNum'] = records.positions.apply(len)


fig = plt.figure()
ax1 = fig.add_subplot(2, 2, 1)
ax2 = fig.add_subplot(2, 2, 2)
ax3 = fig.add_subplot(2, 2, 3)

ax1.plot(records['wealth'])
ax1.set_title('Wealth Curve')
for label in ax1.get_xticklabels():
    label.set_rotation(20)
ax2.plot(records['positionsNum'])
ax2.set_title('Positions Number')
for label in ax2.get_xticklabels():
    label.set_rotation(20)
ts = records['trades'].groupby(pd.TimeGrouper(freq = '1M')).sum()
y_pos = np.arange(len(ts))
ax3.bar(y_pos, ts)
ax3.set_xticks(y_pos[::3])
ax3.set_xticklabels(ts.index.strftime('%Y-%m')[::3], rotation=20)
fmt = '%.0f%%' # Format you want the ticks, e.g. '40%'
yticks = mtick.FormatStrFormatter(fmt)
ax3.yaxis.set_major_formatter(yticks)
ax3.set_title('Turnover Rate')
fig.show()


anualdates = pd.Series(records.index, index = records.index)\
    .groupby(pd.TimeGrouper(freq = 'A')).min()
annual