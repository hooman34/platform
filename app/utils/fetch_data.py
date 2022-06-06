# For data manipulation
import pandas as pd
from fredapi import Fred
import json
from pathlib import Path
import quandl
import investpy
import requests
from .log import get_logger

# auth
basepath = Path(__file__).parent.parent

logger = get_logger(__name__)

with open(str(basepath) + '/keys/keys.json', 'r') as key_file:
    keys = json.load(key_file)

fred = Fred(api_key=keys['fred'])
quandl.ApiConfig.api_key = keys['quandl']


def fundamental_metric(soup, metric):
    return soup.find(text=metric).find_next(class_='snapshot-td2').text


def get_fundamental_data(df):
    for symbol in df.index:
        try:
            url = ("http://finviz.com/quote.ashx?t=" + symbol.lower())
            soup = bs(requests.get(url).content)
            for m in df.columns:
                df.loc[symbol, m] = fundamental_metric(soup, m)
        except Exception as e:
            print(symbol, 'not found')
    return df


def convert_date_format(d, format):
    """"
    input format for date should be 'YYYY-MM-DD'
    format: fred, investing.com
    """
    y, m, d = d.split('-')

    if format == 'Fred':
        return m + '/' + d + '/' + y
    elif format == 'YMD':
        return y + '-' + m + '-' + d
    elif format == 'Investing.com':
        return d + '/' + m + '/' + y


def fred_quandl(indx, start_date, end_date):
    """
    indx
    start_date, end_date: 'YYYY-MM-DD'
    """
    df = quandl.get('FRED/' + indx, start_date=start_date, end_date=end_date)
    df = df.reset_index()
    df.columns = ['Date', indx]
    return df


def fred_fred(code, observation_start=None, observation_end=None):
    """
    date: yyyy-mm--dd
    """
    logger.info("Fetching data from fred: {}, from {} to {}.".format(code, observation_start, observation_end))

    observation_start = convert_date_format(observation_start, 'Fred')
    observation_end = convert_date_format(observation_end, 'Fred')

    df = fred.get_series(code, observation_start=observation_start, observation_end=observation_end)
    df = pd.DataFrame(df).reset_index()
    df.columns = ['date', 'v']
    df.loc[:, 'code'] = code

    df.loc[:, 'p_key'] = df['date'].astype(str).str.replace("-", "_") + "_" + df['code']
    return df


def investing_api(call_type, ticker, from_date, to_date, country='united states'):
    """
    call_type: etf, stock, fund, index
    ticker: str. ticker name
    from_date: yyyy-mm--dd
    to_date: yyyy-mm--dd
    """

    from_date = convert_date_format(from_date, 'Investing.com')
    to_date = convert_date_format(to_date, 'Investing.com')

    if call_type == 'etf':
        logger.info("Fetching etf from investing_api: {}, from {} to {}".format(ticker, from_date, to_date))

        # search name from ticker and return
        etfs = investpy.etfs.get_etfs(country=country)
        etf_name = etfs.loc[(etfs.symbol == ticker), 'name'].tolist()[0]
        data = investpy.get_etf_historical_data(etf=etf_name, country=country,
                                                from_date=from_date,
                                                to_date=to_date).reset_index()
    elif call_type == 'stock':
        logger.info("Fetching stock from investing_api: {}, from {} to {}".format(ticker, from_date, to_date))

        data = investpy.stocks.get_stock_historical_data(stock=ticker, country=country,
                                                         from_date=from_date,
                                                         to_date=to_date).reset_index()
    elif call_type == 'index':
        logger.info("Fetching index from investing_api: {}, from {} to {}".format(ticker, from_date, to_date))
        data = investpy.get_index_historical_data(index=ticker, country=country,
                                                         from_date=from_date,
                                                         to_date=to_date).reset_index()
        
    else:
        logger.info("not supported call type")
        
    data.loc[:, 'ticker'] = ticker
    data.loc[:, 'type'] = call_type
    data.loc[:, 'p_key'] = data['Date'].astype(str).str.replace("-", "_") + "_" + data['ticker']
    return data
