import pandas as pd
import numpy as np


def calculate_indicators(df, include_order_book=False):
    """
    Calculates various technical indicators for a given DataFrame.
    - If `include_order_book=True`, it calculates Order Book Imbalance & Weighted Bid/Ask Price.

    :param df: DataFrame containing 'open', 'high', 'low', 'close', and 'volume' columns.
    :param include_order_book: Boolean flag to include Order Book indicators.
    :return: DataFrame with calculated indicators.
    """
    df = df.copy()

    # VWAP (Volume Weighted Average Price)
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    df['VWAP'] = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()

    # EMA (Exponential Moving Average)
    df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()

    # RSI (Relative Strength Index)
    delta = df['close'].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain, index=df.index).ewm(span=14, adjust=False).mean()
    avg_loss = pd.Series(loss, index=df.index).ewm(span=14, adjust=False).mean()
    rs = avg_gain / avg_loss
    df['RSI_14'] = 100 - (100 / (1 + rs))

    # Momentum (14)
    df['Momentum_14'] = df['close'].diff(14)

    # ATR (Average True Range, 14)
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    true_range = np.maximum(high_low, np.maximum(high_close, low_close))
    df['ATR_14'] = pd.Series(true_range).ewm(span=14, adjust=False).mean()

    # MACD (Moving Average Convergence Divergence)
    df['MACD'] = df['close'].ewm(span=12, adjust=False).mean() - df['close'].ewm(span=26, adjust=False).mean()
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()

    # ADX (Average Directional Index, 14)
    up_move = df['high'].diff()
    down_move = df['low'].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

    plus_di = 100 * (pd.Series(plus_dm).ewm(span=14, adjust=False).mean() / df['ATR_14'])
    minus_di = 100 * (pd.Series(minus_dm).ewm(span=14, adjust=False).mean() / df['ATR_14'])
    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)

    df['ADX_14'] = pd.Series(dx).ewm(span=14, adjust=False).mean()

    # Order Book Indicators (only for df_1min)
    if include_order_book:
        df['Order_Imbalance'] = (df['bid_size'] - df['ask_size']) / (df['bid_size'] + df['ask_size'] + 1e-9)
        df['Weighted_BidAsk'] = (df['bid_price'] * df['bid_size'] + df['ask_price'] * df['ask_size']) / (
                df['bid_size'] + df['ask_size'] + 1e-9)

    return df.round(2)
