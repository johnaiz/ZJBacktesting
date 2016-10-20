import pandas as pd
import numpy as np
import trading
import datafeed
import backtestingengine
import calculators

reload(trading)
reload(datafeed)
reload(backtestingengine)
reload(calculators)

# if __name__ == '__main__':
csvFeed = datafeed.CSVFeed('codeTest.csv')
csvFeed.betaEnrich()
engine = backtestingengine.Engine(csvFeed, stopDate = pd.Timestamp('2005-02-01'))
account = trading.Account('TestAccount', engine, startCash = 1000000.0)
strat = trading.BaseStrategy(account, datafeed)
print 'starting engine now'
engine.run()
print 'end'


sp500ss = sp500s[:252]
stock1ss = stock1s[50:252]


ROLLINGWINDOW = 252
MINCONTINUOUES = 189
STARTDATE = pd.Timestamp('20060101')

def dataAlignment(benchmark, stock):
# Return aligned benchmark and stock with
# at least MINCONTINUOUES continuoues datapoint and no more than ROLLINGWINDOW
    if stock[-1] == None:
        return None
    join = benchmark.align(stock)
    newbm = join[0][-ROLLINGWINDOW:]
    newstock = join[1][-ROLLINGWINDOW:]
    try:
        lastnan = pd.isnull(newstock).tolist()[::-1].index(True)
        if lastnan < MINCONTINUOUES:
            return None
        else:
            return (newbm[-lastnan:], newstock[-lastnan:])
    except ValueError:
        return (newbm, newstock)

def calcBeta(benchmark, stock):
    afterAlign = dataAlignment(benchmark, stock)
    if not afterAlign:
        return None
    covmat = np.cov(afterAlign[1], afterAlign[0])
    beta = covmat[0, 1] / covmat[1, 1]
    return beta


# data = data.loc[STARTDATE:]
def foo():
    data['Beta'] = None
    calcDateArray = data.loc[STARTDATE:].index.unique().tolist()[:3]
    calcDateArray = filter(lambda x: x > STARTDATE, calcDateArray)
    ids = data['ID'].unique().tolist()
    ids.remove('SP500')
    ids = ids[:10]
    for date in calcDateArray[:1]:
        dataPart = data.loc[:date]
        benchmark = dataPart[dataPart.ID == 'SP500']['Return']
        for id in ids:
            beta = calcBeta(benchmark, dataPart[dataPart.ID == id]['Return'])
            data.loc[(data.index == date) & (data.ID == id), 'Beta'] = beta
            print 'finished calc for %s at %s' %(id, str(date))
    print 'finished'


def foo2():
    data['Beta'] = None
    calcDateArray = data.index.levels[0].unique().tolist()
    calcDateArray = filter(lambda x: x > STARTDATE, calcDateArray)

    for date in calcDateArray[:1]:
        dataPart = data.sortlevel(level = 0).loc[:date]
        benchmark = dataPart.xs('SP500', level = 1)['Return']
        ids = dataPart.loc[date].index.unique().tolist()
        ids.remove('SP500')
        ids = ids
        for id in ids:
            beta = calcBeta(benchmark, dataPart.xs(id, level = 1)['Return'])
            data.set_value((date, id), 'Beta', beta)
            print 'finished calc for %s at %s' %(id, str(date))
    print 'finished'

cProfile.run('foo2()')


import logging
import pandas as pd
import trading
import datafeed
import backtestingengine
import calculators

reload(datafeed)

reload(trading)
reload(backtestingengine)
reload(calculators)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
csvFeed = datafeed.CSVFeed('dataEnriched2.csv')
engine = backtestingengine.Engine(csvFeed)
account = trading.Account('ZJTestAccount', engine, startCash = 1000000.0)
strat = trading.BaseStrategy(account)
logging.info('starting engine now')
engine.start('20060103', '20060131')
logging.info('end')




import logging
root = logging.getLogger()
root.addHandler(logging.StreamHandler())
logging.info('lalalal')