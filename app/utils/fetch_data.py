import pandas as pd
from fredapi import Fred
import json
from pathlib import Path
import investpy
import requests
from urllib.request import urlopen
import yfinance as yf
from .log import get_logger

# auth
basepath = Path(__file__).parent.parent

logger = get_logger(__name__)

with open(str(basepath) + '/keys/keys.json', 'r') as key_file:
    keys = json.load(key_file)

fred = Fred(api_key=keys['fred'])
alpha_vantage = keys['alpha_vantage']
financial_modeling_prep = keys['financial_modeling_prep']

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

    if format == 'fred':
        return m + '/' + d + '/' + y
    elif format == 'YMD':
        return y + '-' + m + '-' + d
    elif format == 'investing':
        return d + '/' + m + '/' + y

def standardize_data(source, df, symbol, call_type=None, interval=None, currency=None):
    """
    A core function that standardizes data from different sources.
    The standardization process requires creating a key column, date column, and an interval column
    The output of the dataset should have the same column name, regardless of the data source.

    Args:
        source (str): source of data. 'fred', 'investing', 'alpha_vantage', 'alpha_vantage_financial_statements', 'yf'
        df (pd.DataFrame): dataframe to work on
        symbol(str): symbol of the data. it could be a ticker or a code if it's an index
        call_type (str): type of investment
        interval (str): daily, weekly or monthly
        currency (str): currency of the target

    Returns:
        df (pd.DataFrame): standardized dataframe
    """

    if source=='fred':
        df.columns = ['Date', 'v']
        df.loc[:, 'symbol'] = symbol
        df['type'] = call_type
        df['interval'] = interval
        df['unit'] = currency
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        df.loc[:, 'p_key'] = df['Date'].astype(str).str.replace("-", "_") + "_" + df['symbol']

    elif source=='investing':
        df.loc[:, 'symbol'] = symbol
        df.loc[:, 'type'] = call_type
        df.loc[:, 'interval'] = interval
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        df.loc[:, 'p_key'] = df['Date'].astype(str).str.replace("-", "_") + "_" + df['symbol']

    elif source=='alpha_vantage':
        df.loc[:, 'Currency'] = currency
        df.loc[:, 'symbol'] = symbol
        df.loc[:, 'type'] = call_type
        df['interval'] = interval
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        df.loc[:, 'p_key'] = df['Date'].astype(str).str.replace("-", "_") + "_" + df['symbol']

    elif source=='alpha_vantage_financial_statements':
        df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'], format='%Y-%m-%d')
        df['symbol'] = symbol
        df.loc[:, 'p_key'] = df['fiscalDateEnding'].astype(str).str.replace("-", "_") + "_" + df['symbol']

    elif source=='yf':
        df.loc[:, 'Currency'] = currency
        df['symbol'] = symbol
        df['type'] = call_type
        df['interval'] = interval
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        df.loc[:, 'p_key'] = df['Date'].astype(str).str.replace("-", "_") + "_" + df['symbol']

    return df

def fred_fred(symbol, observation_start=None, observation_end=None, call_type='index'):
    """
    Fetch FRED data from the Fred API.

    Args:
        symbol (str): code for the index
        observation_start (Union[str, None]): 'YYYY-MM-DD' format date. If None, calls every possible date.
        observation_end (Union[str, None]): 'YYYY-MM-DD' format date. If None, calls every possible date.

    Returns:
        df (pd.DataFrame): dataframe of the index data
    """
    logger.info("Fetching data from fred: {}, from {} to {}.".format(symbol, observation_start, observation_end))

    observation_start = convert_date_format(observation_start, 'fred')
    observation_end = convert_date_format(observation_end, 'fred')

    fred_search_df = fred.search(symbol).reset_index()
    interval = fred_search_df.frequency.values[0].lower()
    unit = fred_search_df.units.values[0].lower()

    df = fred.get_series(symbol, observation_start=observation_start, observation_end=observation_end)
    df = pd.DataFrame(df).reset_index()

    df = standardize_data('fred', df, symbol=symbol, call_type=call_type, interval=interval, currency=unit)

    return df


def investing_api(call_type, symbol, from_date, to_date, interval='daily', country='united states'):
    """
    Fetch data from Investing.com

    Args:
        call_type (str): type of product. Options are: etf, stock, fund, index, etf
        symbol (str): ticker name or index symbol.
        from_date (str): Start date of the data. 'YYYY-MM-DD' format date
        to_date (str): End date of the data. 'YYYY-MM-DD' format date
        country (str): country the ticker belongs to
        interval (str): interval of the dat points. daily, weekly, monthly

    Returns:
        df (pd.DataFrame): dataset from investing.com.
    """
    from_date = convert_date_format(from_date, 'investing')
    to_date = convert_date_format(to_date, 'investing')

    if call_type == 'etf':
        # search name from ticker and return
        etfs = investpy.etfs.get_etfs(country=country)
        etf_name = etfs.loc[(etfs.symbol == symbol), 'name'].tolist()[0]
        data = investpy.get_etf_historical_data(etf=etf_name, country=country,
                                                from_date=from_date,
                                                to_date=to_date,
                                                interval=interval).reset_index()
        logger.info("Fetching {} etf from investing_api: {}, from {} to {}".format(interval, symbol, from_date, to_date))
        
    elif call_type == 'stock':
        data = investpy.stocks.get_stock_historical_data(stock=symbol, country=country,
                                                         from_date=from_date,
                                                         to_date=to_date,
                                                         interval=interval).reset_index()
        logger.info("Fetching {} stock from investing_api: {}, from {} to {}".format(interval, symbol, from_date, to_date))

    elif call_type == 'index':
        index_df = investpy.indices.get_indices('united states')
        index_name = index_df.loc[index_df.symbol == symbol, 'name'].tolist()[0]
        data = investpy.get_index_historical_data(index=index_name, country=country,
                                                  from_date=from_date,
                                                  to_date=to_date,
                                                  interval=interval).reset_index()
        logger.info("Fetching {} index from investing_api: {}, from {} to {}".format(interval, symbol, from_date, to_date))

    elif call_type == 'fund':
        funds = investpy.funds.get_funds()
        fund_name = funds.loc[funds.symbol == symbol, 'name'].tolist()[0]
        data = investpy.get_fund_historical_data(fund=fund_name, country=country,
                                                  from_date=from_date,
                                                  to_date=to_date,
                                                  interval=interval).reset_index()
    else:
        logger.info("not supported call type")
        assert False, "Not implemented"

    data = standardize_data('investing', data, symbol=symbol, call_type=call_type, interval=interval)

    return data


def yf_api(call_type, symbol, from_date=None, to_date=None, interval='daily', currency='USD'):
    """
    Fetch data from yahoo finance

    Args:
        call_type (str): type of product. Options are: stock, fund, etf
        symbol (str): ticker name or index symbol.
        from_date (str): Start date of the data. 'YYYY-MM-DD' format date
        to_date (str): End date of the data. 'YYYY-MM-DD' format date
        interval (str): interval of the dat points. daily, weekly, monthly

    Returns:
        df (pd.DataFrame): dataset from Yahoo Finance

    """

    logger.info("Fetching data from YahooFinance: {}, from {} to {}.".format(symbol, from_date, to_date))
    data = yf.Ticker(symbol)

    if interval=='daily':
        interval_yf = '1d'
    elif interval=='weekly':
        interval_yf = '1wk'
    elif interval=='monthly':
        interval_yf = '1mo'
    else:
        assert False, "Not supported interval."

    data = data.history(period="max", interval=interval_yf).reset_index()
    data = standardize_data('yf', data, symbol, call_type=call_type, interval=interval, currency=currency)
    if (from_date is not None) and (to_date is not None):
        data = data.loc[(data.Date>from_date) & (data.Date<to_date)]
        data = data.reset_index(drop=True)

    return data


def alpha_vantage_api(call_type, symbol, from_date=None, to_date=None, interval='daily', currency='USD', key=alpha_vantage):
    """
    Fetch data from alpha vantage

    Args:
        call_type (str): type of data. Options are: stock
        symbol (str): ticker name
        interval (str): interval
        currency (str): currency of the index data. Used to match the format from other sources.
        key (str): key for alpha vantage api

    Returns:
        df (pd.DataFrame): relevant data
    """
    if interval=='daily':
        input_interval = 'TIME_SERIES_DAILY'
        data_column = 'Time Series (Daily)'
    elif interval=='weekly':
        input_interval = 'TIME_SERIES_WEEKLY'
        data_column = 'Weekly Adjusted Time Series'
    elif interval=='monthly':
        input_interval = 'TIME_SERIES_MONTHLY'
        data_column = 'Monthly Adjusted Time Series'

    if call_type=='stock':
        url = 'https://www.alphavantage.co/query?function={}&symbol={}&apikey={}&outputsize=full'.format(input_interval, symbol, key)
        r = requests.get(url)
        data = r.json()
        
        data = pd.DataFrame(data[data_column]).T
        data.reset_index(inplace=True)
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        logger.info("Fetching stock from alpha vantage api: {}".format(symbol))
    else:
        logger.info("not supported call type")
        assert False, "Not implemented"

    data = standardize_data('alpha_vantage', data, symbol=symbol, call_type=call_type, interval=interval, currency=currency)
    if (from_date is not None) and (to_date is not None):
        data = data.loc[(data.Date>from_date) & (data.Date<to_date)]
        data = data.reset_index(drop=True)
        
    return data

def alpha_vantage_api_financial_statements(call_type, symbol, key=alpha_vantage):
    """
    Fetch financial statements from the past 5 years

    Args:
        call_type (str): available options are: income statement, balance sheet, cash flow, earnings
        symbol (str): ticker of the company
        key (str): key for alpha vantage api

    Returns:
        data (pd.DataFrame): data of financial statement.

    """

    if call_type=='income statement':
        function = 'INCOME_STATEMENT'
        data_column = 'annualReports'
    elif call_type=='balance sheet':
        function = 'BALANCE_SHEET'
        data_column = 'annualReports'
    elif call_type=='cash flow':
        function = 'CASH_FLOW'
        data_column = 'annualReports'
    elif call_type=='earnings':
        function = 'EARNINGS'
        data_column = 'annualEarnings'

    url = 'https://www.alphavantage.co/query?function={}&symbol={}&apikey={}&outputsize=full'.format(function,
                                                                                                     symbol, key)
    r = requests.get(url)
    data = r.json()
    data = pd.DataFrame(data[data_column])

    data = standardize_data('alpha_vantage_financial_statements', data, symbol=symbol, call_type=call_type)

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