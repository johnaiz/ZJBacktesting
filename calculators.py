
import pandas as pd
import numpy as np

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


def BetaEnrich(data):
    # data['Beta'] = None
    calcDateArray = data.index.levels[0].unique().tolist()
    # calcDateArray = filter(lambda x: x > STARTDATE, calcDateArray)
    calcDateArray = [pd.Timestamp('20051230')]
    for date in calcDateArray:
        dataPart = data.sortlevel(level=0).loc[:date]
        benchmark = dataPart.xs('SP500', level=1)['Return']
        ids = dataPart.loc[date].index.unique().tolist()
        ids.remove('SP500')
        ids = ids
        for id in ids:
            beta = calcBeta(benchmark, dataPart.xs(id, level=1)['Return'])
            data.set_value((date, id), 'Beta', beta)
            print 'finished calc for %s at %s' % (id, str(date))
    print 'finished beta enrichment'
    return data