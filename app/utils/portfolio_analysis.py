import pandas as pd
import datetime
import requests

# auth
basepath = Path(__file__).parent.parent

with open(str(basepath) + '/keys/keys.json', 'r') as key_file:
    keys = json.load(key_file)

class portfolio_analysis:

    def __init__(self, ticker_list, period_type='annual'):
        assert period_type in ['quarterly', 'annual']

        self.period_type = period_type
        self.ticker_list = ticker_list
        # year
        currentDateTime = datetime.datetime.now()
        self.date = currentDateTime.date()
        self.year = int(date.strftime("%Y"))

    def create_estimates(self, data_type):
        estimate_dict = dict()
        for ticker in self.ticker_list:
            estimate_dict[ticker] = dict()
            estimate_dict[ticker]['data'] = self._seekingAlpha_estimates(ticker, data_type)
            estimate_dict[ticker]['average_growth'] = estimate_dict[ticker]['data']['v_pct_change'].mean()
            estimate_dict[ticker]['growth_type'] = self._determine_growth_type(estimate_dict[ticker]['average_growth'])

        self.estimate_dict = estimate_dict
        print("Created `estimate_dict`")

    def calculate_price_range(self, multiplier=None):
        """
        For stable grower, min max estimation based on diluted eps is fairly accurate.
        However, when it comes to fast grower, market sentiment become relatively more important.
        For slow grower, it refers to divident paying stocks. Thus min max price should be calculated differently.
        """
        if multiplier is None:
            multiplier = [1.5, 2]

        for ticker in self.ticker_list:
            next_year_dil_eps = self.estimate_dict[ticker]['data'].loc[ \
                self.estimate_dict[ticker]['data'].year == self.year + 1, 'consensus'].values[0]
            average_growth = self.estimate_dict[ticker]['average_growth']
            self.estimate_dict[ticker]['price_range'] = [next_year_dil_eps * average_growth * m for m in multiplier]
        print("Created price_range")

    def _determine_growth_type(self, growth):
        if (growth > 10) & (growth < 20):
            return "stable grower"
        elif growth >= 20:
            return "fast grower"
        elif growth <= 10:
            return "slow grower"

    def _seekingAlpha_estimates(self, ticker, data_type, key=keys['rapidAPI_seekingalpha']):
        assert data_type in ['eps', 'revenues']
        url = "https://seeking-alpha.p.rapidapi.com/symbols/get-estimates"

        querystring = {"symbol": ticker.lower(),
                       "data_type": data_type,
                       "period_type": self.period_type}
        headers = {
            'x-rapidapi-key': key,
            'x-rapidapi-host': "seeking-alpha.p.rapidapi.com"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        df = pd.DataFrame([response.json()['data'][i]['attributes'] for i in range(len(response.json()['data']))])

        df = df.loc[df.year.isin([self.year - 2, self.year - 1, self.year, self.year + 1])]

        df['v'] = df['actual']
        df.loc[df.actual.isna(), 'v'] = df.loc[df.actual.isna(), 'consensus']
        df['v_pct_change'] = df.v.pct_change() * 100

        return df