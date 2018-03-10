'''
(c) 2011, 2012 Georgia Tech Research Corporation
This source code is released under the New BSD license.  Please see
http://wiki.quantsoftware.org/index.php?title=QSTK_License
for license details.

Created on January, 23, 2013

@author: Sourabh Bajaj
@contact: sourabhbajaj@gatech.edu
@summary: Event Profiler Tutorial
'''


import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep

"""
Accepts a list of symbols along with start and end date
Returns the Event Matrix which is a pandas Datamatrix
Event matrix has the following structure :
    |IBM |GOOG|XOM |MSFT| GS | JP |
(d1)|nan |nan | 1  |nan |nan | 1  |
(d2)|nan | 1  |nan |nan |nan |nan |
(d3)| 1  |nan | 1  |nan | 1  |nan |
(d4)|nan |  1 |nan | 1  |nan |nan |
...................................
...................................
Also, d1 = start date
nan = no information about any event.
1 = status bit(positively confirms the event occurence)
"""

class EventDates(object):

    def __init__(self):
        csv_file_name = 'eventstudyAVdates.csv'

        df = pd.read_csv(csv_file_name,
                         encoding="ISO-8859-1",
                         usecols=["Date", "Value"],
                         parse_dates=['Date'],
                         index_col='Date')

        # Sort by date
        df = df.sort_index()

        # Select rows after a certain date.
        df = df.loc['2012-01-01':'2015-12-31']
        df = df[df['Value'] >= 7]
        df = df.drop(['Value'], axis=1)

        self.event_dates_df = df

def find_events(ls_symbols, d_data):
    ''' Finding the event dataframe '''
    df_close = d_data['close']
    #ts_market = df_close['SPY']

    print("Finding Events")
    '''
    To Do:
    For events that only have dates and not times, probably should set the event on the first trading day 
    after the event and use the open price. (To avoid look ahead bias)
    '''

    # Creating an empty dataframe
    df_events = copy.deepcopy(df_close)
    df_events = df_events * np.NAN

    # Time stamps for the event range
    ldt_timestamps = df_close.index

    #print(ldt_timestamps)

    eventDates_df = EventDates().event_dates_df
    print("num events:" + str(len(eventDates_df)))
    #print(eventDates_df.index)

    for index, row in eventDates_df.iterrows():
        index_df_events = df_events.index.get_loc(index, method="backfill")
        #print(df_events.iloc[index_df_events])
        for s_sym in ls_symbols:
            df_events.iloc[index_df_events] = 1
            #print(df_events.iloc[index_df_events])

    # show all the rows with events
    #print(df_events[(df_events == 1.0).any(axis=1)])

    return df_events


if __name__ == '__main__':
    print("Collecting symbols and prices")
    dt_start = dt.datetime(2000, 1, 1)
    dt_end = dt.datetime(2017, 12, 31)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

    dataobj = da.DataAccess('AlphaVantage')
    #dataobj = da.DataAccess('Yahoo')
    #ls_symbols = dataobj.get_symbols_from_list('sp5002012')
    #ls_symbols = ['ATI', 'AVY', 'AZO']
    #ls_symbols = ['AOBC', 'OLN', 'RGR']
    #ls_symbols = ['AAXN', 'AOBC', 'OLN', 'RGR']
    #ls_symbols = ['RGR']
    ls_symbols = ['AAPL', 'MSFT']
    ls_symbols.append('SPY')

    # Yahoo
    #ls_keys = ['open', 'high', 'low', 'close']

    # AlphaVantage
    ls_keys = ['open', 'high', 'low', 'close']
    ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    d_data = dict(list(zip(ls_keys, ldf_data)))

    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method='ffill')
        d_data[s_key] = d_data[s_key].fillna(method='bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)

    df_events = find_events(ls_symbols, d_data)

    print("Creating Study")
    ep.eventprofiler(df_events, d_data, i_lookback=20, i_lookforward=20,
                #s_filename='AAPL_Yahoo.pdf', b_market_neutral=True, b_errorbars=True,
                s_filename='eventstudyAV.pdf', b_market_neutral=True, b_errorbars=True,
                s_market_sym='SPY')
