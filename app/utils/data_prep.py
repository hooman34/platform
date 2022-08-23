import pandas_market_calendars as mcal
import itertools




def create_price_features(df):
    """
    Feature engineering
    """
    df['upper_shadow'] = df['High'] / df[['Close', 'Open']].max(axis=1)
    df['lower_shadow'] = df[['Close', 'Open']].min(axis=1) / df['Low']
    df['open2close'] = df['Close'] / df['Open']
    df['high2low'] = df['High'] / df['Low']
    mean_price = df[['Open', 'High', 'Low', 'Close']].mean(axis=1)
    median_price = df[['Open', 'High', 'Low', 'Close']].median(axis=1)
    df['high2mean'] = df['High'] / mean_price
    df['low2mean'] = df['Low'] / mean_price
    df['high2median'] = df['High'] / median_price
    df['low2median'] = df['Low'] / median_price
    return df


class master_df:
    # TODO: need to finish this class.
    
    def __init__(self, interval, start_date, end_date, data_fetch_config):
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.data_fetch_config = data_fetch_config
        
        self.integrity_check()
        
        
    def integrity_check(self):
        config_flat_list = [c['config'] for c in self.data_fetch_config]
        self.config_flat_list = list(itertools.chain(*config_flat_list))
        
        intervals_choices = set([c['interval'] for c in self.config_flat_list])
        
        if len(intervals_choices) > 1:
            print("Caution: There are multiple choices of intervals : {}.\
                  \nKeep in mind that it might cause problems in the future".format(intervals_choices))
            self.mixed_intervals = True
        else:
            self.mixed_intervals = False
        
        
    def fetch_data(self):
        data_dict_format = dict()
        
        for data_config in self.data_fetch_config:
            if data_config['source'] == 'fred':
                for config in data_config['config']:
                    data_dict_format[config['alias']] = fred_fred(config['symbol'] , 
                                                                  observation_start = self.start_date, 
                                                                  observation_end = self.end_date)
                    
            elif data_config['source'] == 'investing':
                for config in data_config['config']:
                    data_dict_format[config['alias']] = investing_api(call_type = config['call_type'],
                                                                      symbol = config['symbol'],
                                                                      from_date = self.start_date, 
                                                                      to_date = self.end_date, 
                                                                      interval = config['interval'])
                    
            elif data_config['source'] == 'alpha_vantage':
                for config in data_config['config']:
                    data_dict_format[config['alias']] = alpha_vantage_api(call_type = config['call_type'],
                                                                          symbol = config['symbol'], 
                                                                          from_date = self.start_date,
                                                                          to_date = self.end_date, 
                                                                          interval = config['interval'],
                                                                          currency = config['currency'])
                    
            elif data_config['source'] == 'alpha_vantage_financial_statements':
                for config in data_config['config']:
                    data_dict_format[config['alias']] = alpha_vantage_api_financial_statements(call_type = config['call_type'],
                                                                                               symbol = config['symbol'])
                    
            elif data_config['source'] == 'yf':
                for config in data_config['config']:
                    data_dict_format[config['alias']] = yf_api(call_type = config['call_type'], 
                                                               symbol = config['symbol'], 
                                                               from_date = self.start_date, 
                                                               to_date = self.end_date, 
                                                               interval = config['interval'], 
                                                               currency = config['currency'])
                    
            else:
                assert False, "Source not supported."
            
        self.data_dict_format = data_dict_format

# Input format for the master_df class
# data_fetch_config = [
    
#     {'source':'investing',
#      'config': [
#          {'symbol':'SPX',
#           'call_type':'index',
#           'interval':'monthly',
#           'currency':'usd',
#           'alias':'sp500'},
         
#          {'symbol':'IXIC',
#           'call_type':'index',
#           'interval':'monthly',
#           'currency':'usd',
#           'alias':'nasdaq'}
#      ]},
    
#     {'source':'fred',
#      'config': [
#          {'symbol':'REAINTRATREARAT10Y',
#           'call_type':'index',
#           'interval':'monthly',
#           'currency':'usd',
#           'alias':'ir_10y'},
         
#          {'symbol':'REAINTRATREARAT1YE',
#           'call_type':'index',
#           'interval':'monthly',
#           'currency':'usd',
#           'alias':'ir_1y'}
#      ]}
    
# ]

# df = master_df('monthly', start_date, today, data_fetch_config)