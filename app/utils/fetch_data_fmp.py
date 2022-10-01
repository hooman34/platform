from subprocess import call
import pandas as pd
import json
from pathlib import Path
import requests
from urllib.request import urlopen
from .log import get_logger

# auth
basepath = Path(__file__).parent.parent

logger = get_logger(__name__)

with open(str(basepath) + '/keys/keys.json', 'r') as key_file:
    keys = json.load(key_file)

financial_modeling_prep = keys['financial_modeling_prep']


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

    def get_historical_daily_price(self, call_type, symbol, start_date, end_date):
        """
        Retrieve historical price data. daily.
        
        Args:
            call_type (str): index or stock
            symbol (str): symbol
        
        """
        logger.info("Fetching daily price data of {}".format(symbol))

        if call_type == "index":

            url = "https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}".format(symbol, self.key)
        
        elif call_type == "stock":
            url = "https://financialmodelingprep.com/api/v3/historical-price-full/{}?from={}&to={}&apikey={}".format(symbol, start_date, end_date, self.key)

        else:
            assert False, "Not supported call type"

        daily_price = self.get_jsonparsed_data(url)

        df = pd.DataFrame(daily_price['historical'])
        df['symbol'] = daily_price['symbol']
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

        return df
    
    def get_economic_index(self, symbol, start_date, end_date):
        """
        Retrieve economic index
        
        Total list of available indices: updated 2022-10-01
        
        "GDP", "realGDP", "nominalPotentialGDP", "realGDPPerCapita",
        "federalFunds", "CPI", "inflationRate", "inflation", "retailSales", "consumerSentiment", "durableGoods",
        "unemploymentRate", "totalNonfarmPayroll", "initialClaims", "industrialProductionTotalIndex",
        "newPrivatelyOwnedHousingUnitsStartedTotalUnits", "totalVehicleSales", "retailMoneyFunds",
        "smoothedUSRecessionProbabilities", "3MonthOr90DayRatesAndYieldsCertificatesOfDeposit",
        "commercialBankInterestRateOnCreditCardPlansAllAccounts", "30YearFixedRateMortgageAverage",
        "15YearFixedRateMortgageAverage"
        """
        logger.info("Fetching economic index data: {}".format(symbol))
        
        url = "https://financialmodelingprep.com/api/v4/economic?name={}&from={}&to={}&apikey={}".format(symbol, start_date, end_date, self.key)
        historical_index = self.get_jsonparsed_data(url)
        
        df = pd.DataFrame(historical_index)
        df.columns = ['date', symbol]
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        
        return df

    def get_historical_insider_trade_ticker(self, ticker, num_pages=1):
        """
        Retrieve historical insider trade data

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

        