
import pandas as pd
import logging

from empyrical import (
    alpha_beta_aligned,
    annual_volatility,
    cum_returns,
    downside_risk,
    max_drawdown,
    sharpe_ratio,
    annual_return,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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


class Stats(object):
    METRIC_NAMES = (
        'alpha',
        'beta',
        'sharpe',
        'strategy_volatility',
        'benchmark_volatility',
        'downside_risk',
        'max_drawdown',
        'annual_return',
    )

    @classmethod
    def getStats(cls, returns, benchmark_returns):
        _alpha, _beta = alpha_beta_aligned(
            returns,
            benchmark_returns,
        )

        _sharpe = sharpe_ratio(
            returns
        )

        _downside_risk = downside_risk(
            returns
        )

        _max_drawdown = max_drawdown(
            returns
        )

        _annual_volatility = annual_volatility(
            returns
        )

        _benchmark_volatility = annual_volatility(
            benchmark_returns
        )

        _annual_return = annual_return(
            returns
        )

        _cum_return = cum_returns(
            returns
        )

        return {
            'cum_return' : _cum_return,
            'annual_return' : _annual_return,
            'annual_volatility' : _annual_volatility,
            'benchmark_volatility' : _benchmark_volatility,
            'max_drawdown' : _max_drawdown,
            'downside_risk' : _downside_risk,
            'sharpe ratio' : _sharpe,
            'alpha' : _alpha,
            'beta' : _beta,
        }
