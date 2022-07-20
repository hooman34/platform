

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


