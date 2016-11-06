#
# import datafeed
#
#
# class Context(object):
#
#     def __init__(self, fileName, startDate = None, stopDate = None):
#         self._data = datafeed.CSVFeed(fileName)
#         self._accounts = []  # accounts registered
#         self._orders = []  # cache orders by client accounts in current dateframe
#         self._defaultCommission = trading.FlatCommission()
#         self._startDate = startDate
#         self._stopDate = stopDate or pd.Timestamp('2099-12-31')
#
#         self._marketData = None
#         self._marketDate = None