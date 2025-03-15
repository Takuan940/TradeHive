import pandas as pd

def process_market_data(bars_trades, bars_bid_ask):
    """
    Converts trade and bid/ask bar data into DataFrames, processes timestamps,
    merges them, and aggregates into 1-Min, 5-Min, and 15-Min intervals.

    :param bars_trades: List of trade bar objects
    :param bars_bid_ask: List of bid/ask bar objects
    :return: Tuple of DataFrames (df_1min, df_5min, df_15min)
    """

    # Convert trade data to DataFrame
    df_trades = pd.DataFrame([{
        'date': bar.date,
        'open': bar.open,
        'high': bar.high,
        'low': bar.low,
        'close': bar.close,
        'volume': getattr(bar, 'volume', 0)  # Falls Volumen fehlt, ersetze mit 0
    } for bar in bars_trades])

    # Convert BID/ASK data to DataFrame
    df_bid_ask = pd.DataFrame([{
        'date': bar.date,
        'bid_price': getattr(bar, 'bid', None),  # Sicherstellen, dass Attribute existieren
        'ask_price': getattr(bar, 'ask', None),
        'bid_size': getattr(bar, 'bidSize', None),
        'ask_size': getattr(bar, 'askSize', None)
    } for bar in bars_bid_ask])

    # Ensure dataframes are not empty before processing
    if df_trades.empty:
        print("⚠️ Warning: Trade data is empty!")
        return None, None, None

    if df_bid_ask.empty:
        print("⚠️ Warning: Bid/Ask data is empty!")

    # Convert 'date' to datetime and set as index
    df_trades['date'] = pd.to_datetime(df_trades['date'])
    df_trades.set_index('date', inplace=True)

    if not df_bid_ask.empty:
        df_bid_ask['date'] = pd.to_datetime(df_bid_ask['date'])
        df_bid_ask.set_index('date', inplace=True)

        # Ensure bid/ask data has only one row per timestamp
        df_bid_ask = df_bid_ask.groupby('date').apply(
            lambda x: pd.Series({
                'bid_price': (x['bid_price'] * x['bid_size']).sum() / (x['bid_size'].sum() + 1e-9),
                'ask_price': (x['ask_price'] * x['ask_size']).sum() / (x['ask_size'].sum() + 1e-9),
                'bid_size': x['bid_size'].sum(),
                'ask_size': x['ask_size'].sum()
            })
        )

    # Merge trade and bid/ask data (left join: keep all trades, add bid/ask if available)
    df_1min = df_trades.merge(df_bid_ask, on='date', how='left')

    # Fill missing bid/ask values with last known values
    df_1min[['bid_price', 'ask_price', 'bid_size', 'ask_size']] = df_1min[
        ['bid_price', 'ask_price', 'bid_size', 'ask_size']
    ].fillna(method='ffill').fillna(method='bfill')

    # Aggregate 5-Min and 15-Min data (without bid/ask columns)
    df_5min = df_1min[['open', 'high', 'low', 'close', 'volume']].resample('5T').agg(
        {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()

    df_15min = df_1min[['open', 'high', 'low', 'close', 'volume']].resample('15T').agg(
        {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()

    return df_1min, df_5min, df_15min
