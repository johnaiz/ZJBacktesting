import pandas_datareader.data as webdata
import pandas as pd
import numpy as np
import datetime
import logging

import calculators

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DataFeed(object):
    def __init__(self):
        self._currentDatetimeFrame = None
        self._currentDatetimeIdx = -1
        self._startFeeding = False

    def getData(self):
        raise NotImplementedError('Need to implement getData function')

    def startFeeding(self):
        self._startFeeding = True

    def _initDateFrame(self):
        raise NotImplementedError('Need to implement _initDateFrame function')

    def nextDataPoint(self):
        raise NotImplementedError('Need to implement _initDateFrame function')


class MemoryDataFeed(DataFeed):
    # All Data Stored in Memory

    def __init__(self, data):
        super(MemoryDataFeed, self).__init__()
        self._dataInMemory = data

    def getData(self):
        return self._dataInMemory


class CSVFeed(MemoryDataFeed):
    # used this time for csv file
    def __init__(self, csvFileName, enriched=True):
        if enriched:
            data = pd.read_csv(csvFileName,
                               dtype={'ID': np.str, 'Return': np.float64, 'IN_Flag': np.bool, 'Beta': np.float64},
                               parse_dates=['Pointdate'], date_parser=lambda x: pd.datetime.strptime(x, '%Y-%m-%d'))
        else:
            data = pd.read_csv(csvFileName, dtype={'ID': np.str, 'Return': np.float64, 'IN_Flag': np.bool},
                               parse_dates=['Pointdate'], date_parser=lambda x: pd.datetime.strptime(x, '%Y-%m-%d'))

            benchmark = self._getBenchmark()
            data = data.append(benchmark)
        data.set_index(['Pointdate', 'ID'], inplace=True)
        data.sort_index(inplace=True)
        self._dateArray = data.index.levels[0].unique()
        super(CSVFeed, self).__init__(data)

    def _betaEnrich(self):
        self._dataInMemory = calculators.BetaEnrich(self._dataInMemory)
        self._dataInMemory.sort_index(inplace=True)

    def _getBenchmark(self):
        start = datetime.datetime(2004, 12, 31)
        end = datetime.datetime(2008, 12, 31)
        f = webdata.DataReader("^GSPC", 'yahoo', start, end)
        adjclose = f['Adj Close']
        ret = adjclose / adjclose.shift(1)
        ret = ret[1:]
        ret = pd.DataFrame({'Pointdate': ret.index, 'Return': ret})
        ret['IN_Flag'] = False
        ret['ID'] = 'SP500'
        return ret

    def getDateRange(self):
        return self._dateArray

    def getHistoricalData(self, date):
        return self._dataInMemory.loc[:date]

    def getMarketData(self, date):
        return self._dataInMemory.loc[date]


if __name__ == '__main__':
    feed = CSVFeed('codeTest.csv')
    feed.startFeeding()
    feed.nextDataPoint()
    print feed.getCurrentDataPoint()
    feed.nextDataPoint()
    print feed.getCurrentDataPoint()
    feed.nextDataPoint()
    print feed.getHistoricalData()
