import pandas as pd
import numpy as np

def synchronize_timeframes(df_1min, df_5min, df_15min):
    """
    Ensures that all timeframes start at the same timestamp by:
    - Removing leading NaN and NULL values from indicators
    - Synchronizing start timestamps across all DataFrames

    :param df_1min: DataFrame with 1-minute data
    :param df_5min: DataFrame with 5-minute data
    :param df_15min: DataFrame with 15-minute data
    :return: Synchronized DataFrames
    """
    if df_1min.empty or df_5min.empty or df_15min.empty:
        raise ValueError("❌ One or more DataFrames are empty. Cannot synchronize timeframes!")

    # 🧹 Function to remove leading invalid values (NULL, NaN, 0)
    def remove_leading_invalid(df, num_rows=14):
        """
        Removes the first `num_rows` rows to eliminate invalid indicator values at the start.

        :param df: DataFrame to process
        :param num_rows: Number of rows to remove from the top
        :return: Cleaned DataFrame
        """
        return df.iloc[num_rows:] if len(df) > num_rows else df

    df_1min = remove_leading_invalid(df_1min)
    df_5min = remove_leading_invalid(df_5min)
    df_15min = remove_leading_invalid(df_15min)

    # 🔄 Ensure index is sorted
    df_1min.sort_index(inplace=True)
    df_5min.sort_index(inplace=True)
    df_15min.sort_index(inplace=True)

    # ⏳ Find the latest common timestamp
    common_timestamp = max(df_1min.index.min(), df_5min.index.min(), df_15min.index.min())

    if pd.isna(common_timestamp):
        raise ValueError("❌ Synchronization failed: No valid common timestamp found!")

    print(f"🕒 Synchronizing all timeframes to start from: {common_timestamp}")

    # ✂ Trim all DataFrames to start at the common timestamp
    df_1min = df_1min[df_1min.index >= common_timestamp]
    df_5min = df_5min[df_5min.index >= common_timestamp]
    df_15min = df_15min[df_15min.index >= common_timestamp]

    return df_1min, df_5min, df_15min
