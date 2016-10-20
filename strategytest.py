

import logging
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

class BetaStrategy(trading.BaseStrategy):
    def handleData(self):

        records = self.getRecords()
        trades = records.get('order')
        doBetaTrade = True
        if trades is not None and not trades.empty:
            realtrades = trades[trades != {}]
            if not realtrades.empty:
                lastTradeTime = realtrades.index[-1]
                if self.getCurrentDate().month == lastTradeTime.month \
                        and self.getCurrentDate().year == lastTradeTime.year:
                    doBetaTrade = False

        order = {}
        logger.info('before dobetatrade')
        if doBetaTrade:
            logger.info('in do betatrade')
            marketData = self.getMarketData()
            pos = self.getAccount().getCurrentPositions()
            longsin = set([p for p, v in pos.iteritems() if v > 0])
            shortsin = set([p for p, v in pos.iteritems() if v < 0])
            universe = marketData[marketData['IN_Flag'] & marketData['Beta'].notnull()]
            if len(universe) > 400: # means the beta is calculated
                shorts = set(universe.nlargest(50, 'Beta').index)
                longs = set(universe.nsmallest(50, 'Beta').index)
                tolongs = longs.difference(longsin)
                toclose = longsin.difference(longs).union(shortsin.difference(shorts))
                toshorts = shorts.difference(shortsin)
                order = dict([(p, 1) for p in tolongs])
                order.update(dict([(p, -1) for p in toshorts]))
                order.update(dict([(p, -pos[p]) for p in toclose]))
                self.newOrder(order)
                logger.debug('Make Order: %s', str(order))

        wealth = self.getAccount().getCurrentWealth()
        positions = self.getAccount().getCurrentPositions()
        cash = self.getAccount().getCurrentCash()

        self.record(wealth=wealth, positions=[positions.copy()], cash=cash, order=[order.copy()])


# if __name__ == '__main__':
csvFeed = datafeed.CSVFeed('dataEnriched3.csv')
engine = backtestingengine.Engine(csvFeed)
account = trading.Account('ZJBetaAccount', engine, startCash=100.0)
strat = BetaStrategy(account)
logging.info('starting engine now')
engine.start('20060103')
logging.info('end')


import matplotlib.pyplot as plot