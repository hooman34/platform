# For data manipulation
import pandas as pd
from fredapi import Fred
import json
from pathlib import Path
import quandl
import investpy
import requests
from urllib.request import urlopen
from .log import get_logger

# auth
basepath = Path(__file__).parent.parent

logger = get_logger(__name__)

with open(str(basepath) + '/keys/keys.json', 'r') as key_file:
    keys = json.load(key_file)

fred = Fred(api_key=keys['fred'])
quandl.ApiConfig.api_key = keys['quandl']
financial_modeling_prep = keys['financial_modeling_prep']

# TODO: Standardize the dataformat for each stock, index, fund etc

# def fundamental_metric(soup, metric):
#     return soup.find(text=metric).find_next(class_='snapshot-td2').text
#
#
# def get_fundamental_data(df):
#     for symbol in df.index:
#         try:
#             url = ("http://finviz.com/quote.ashx?t=" + symbol.lower())
#             soup = bs(requests.get(url).content)
#             for m in df.columns:
#                 df.loc[symbol, m] = fundamental_metric(soup, m)
#         except Exception as e:
#             print(symbol, 'not found')
#     return df


def convert_date_format(date, format):
    """
    Change the date format so that it matches the required format for each type of API.
    This function standardizes the date format, reducing the effort to match the date format for each API

    Args:
        date (str): 'YYYY-MM-DD' format string
        format (str): The type of format we want to match.
                      Options are: Fred, YMD, Investing.com

    Returns:
        date string, with the corresponding format
    """
    y, m, d = date.split('-')

    if format == 'Fred':
        return m + '/' + d + '/' + y
    elif format == 'YMD':
        return y + '-' + m + '-' + d
    elif format == 'Investing.com':
        return d + '/' + m + '/' + y


def fred_quandl(indx, start_date, end_date):
    """
    Fetch FRED data from quandl.

    Args:
        indx (str): code for the index
        start_date (str): 'YYYY-MM-DD' format date
        end_date (str): 'YYYY-MM-DD' format date

    Returns:
        df (pd.DataFrame): index data
    """
    df = quandl.get('FRED/' + indx, start_date=start_date, end_date=end_date)
    df = df.reset_index()
    df.columns = ['Date', indx]
    return df


def fred_fred(code, observation_start=None, observation_end=None):
    """
    Fetch FRED data from the Fred API.

    Args:
        code (str): code for the index
        observation_start (Union[str, None]): 'YYYY-MM-DD' format date. If None, calls every possible date.
        observation_end (Union[str, None]): 'YYYY-MM-DD' format date. If None, calls every possible date.

    Returns:
        df (pd.DataFrame): dataframe of the index data
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


def investing_api(call_type, ticker, from_date, to_date, country='united states', interval='daily'):
    """
    Fetch data from Investing.com

    Args:
        call_type (str): type of product. Options are: etf, stock, fund, index
        ticker (str): ticker name
        from_date (str): Start date of the data. 'YYYY-MM-DD' format date
        to_date (str): End date of the data. 'YYYY-MM-DD' format date
        country (str): country the ticker belongs to
        interval (str): interval of the dat points.

    Returns:
        df (pd.DataFrame): dataset from investing.com.
    """
    from_date = convert_date_format(from_date, 'Investing.com')
    to_date = convert_date_format(to_date, 'Investing.com')

    if call_type == 'etf':
        # search name from ticker and return
        etfs = investpy.etfs.get_etfs(country=country)
        etf_name = etfs.loc[(etfs.symbol == ticker), 'name'].tolist()[0]
        data = investpy.get_etf_historical_data(etf=etf_name, country=country,
                                                from_date=from_date,
                                                to_date=to_date,
                                                interval=interval).reset_index()
        logger.info("Fetching {} etf from investing_api: {}, from {} to {}".format(interval, ticker, from_date, to_date))
        
    elif call_type == 'stock':
        data = investpy.stocks.get_stock_historical_data(stock=ticker, country=country,
                                                         from_date=from_date,
                                                         to_date=to_date,
                                                         interval=interval).reset_index()
        logger.info("Fetching {} stock from investing_api: {}, from {} to {}".format(interval, ticker, from_date, to_date))

    elif call_type == 'index':
        data = investpy.get_index_historical_data(index=ticker, country=country,
                                                  from_date=from_date,
                                                  to_date=to_date,
                                                  interval=interval).reset_index()
        logger.info("Fetching {} index from investing_api: {}, from {} to {}".format(interval, ticker, from_date, to_date))
        
    else:
        logger.info("not supported call type")
        
    data.loc[:, 'ticker'] = ticker
    data.loc[:, 'type'] = call_type
    data.loc[:, 'p_key'] = data['Date'].astype(str).str.replace("-", "_") + "_" + data['ticker']
    return data

def alpha_vantage_api(call_type, ticker, currency='USD', key=keys['alpha_vantage']):
    """
    Fetch data from alpha vantage

    Args:
        call_type (str): type of data. Options are: stock
        ticker (str): ticker name
        currency (str): currency of the index data. Used to match the format from other sources.
        key (str): key for alpha vantage api

    Returns:
        df (pd.DataFrame): relevant data
    """
    if call_type=='stock':
        url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}&outputsize=full'.format(ticker, key)
        r = requests.get(url)
        data = r.json()
        
        data = pd.DataFrame(data['Time Series (Daily)']).T
        data.reset_index(inplace=True)
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        logger.info("Fetching stock from alpha vantage api: {}".format(ticker))
        
        
    data.loc[:, 'Currency'] = currency
    data.loc[:,'ticker'] = ticker
    data.loc[:,'type'] = call_type
    data.loc[:,'p_key'] = data['Date'].astype(str).str.replace("-", "_") + "_" + data['ticker']
        
    return data


class FMP:
    """
    Class object for FinancialModelingPrep
    """

    def __init__(self):
        self.key = financial_modeling_prep
        logger.info("Financial Modeling Prep api ready.")

    def get_jsonparsed_data(self, url):
        """
        Receive the content of ``url``, parse it as JSON and return the object.

        Args:
            url (str): url of the API

        Returns:
            parsed_content (json): parsed content returned from the API
        """
        response = urlopen(url)
        data = response.read().decode("utf-8")
        return json.loads(data)

    def get_historical_insider_trade_ticker(self, ticker, num_pages=1):
        """
        Retrieve historical data

        Args:
            ticker (str): ticker name
            num_pages (int): number of pages of reports to call. 1 page = 1 API call

        Returns:
            df (pd.DataFrame): insider trade historical data
        """
        logger.info("Fetching {} pages of {} insider trade data.".format(num_pages, ticker))

        df = pd.DataFrame()

        for page in range(num_pages):
            url = "https://financialmodelingprep.com/api/v4/insider-trading?symbol={}&page={}&apikey={}".format(ticker, page, self.key)
            insider_trade = self.get_jsonparsed_data(url)
            df = pd.concat([df, pd.DataFrame(insider_trade)], axis=0)

        df = df.reset_index(inplace=False)
        df['transactionDate'] = pd.to_datetime(df['transactionDate'], format='%Y-%m-%d')

        return df

    def get_stock_split_history(self, ticker):
        """
        Fetch stock split history of the ticker

        Args:
            ticker (str): ticker name

        Returns:
            df (pd.DataFrame): dataframe of the
        """

        logger.info("Fetching stock split history for {}".format(ticker))

        url = "https://financialmodelingprep.com/api/v3/historical-price-full/stock_split/{}?apikey={}".format(ticker, self.key)
        stock_split = self.get_jsonparsed_data(url)

        df = pd.DataFrame(stock_split['historical'])
        df['symbol'] = stock_split['symbol']
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

        return df