import pandas as pd

def split_optimization_test(df_1min, df_5min, df_15min, optimization_ratio=0.8):
    """
    Splits data into optimization and test sets using a common timestamp.

    :param df_1min: DataFrame with 1-minute data
    :param df_5min: DataFrame with 5-minute data
    :param df_15min: DataFrame with 15-minute data
    :param optimization_ratio: Ratio of data used for optimization (default: 0.8)
    :return: Tuple of DataFrames (optimization and test sets for all timeframes)
    """
    if df_1min.empty or df_5min.empty or df_15min.empty:
        raise ValueError("❌ One or more DataFrames are empty. Cannot split data!")

    # 🔄 Ensure index is sorted
    df_1min.sort_index(inplace=True)
    df_5min.sort_index(inplace=True)
    df_15min.sort_index(inplace=True)

    # 📌 Define split timestamp from the 15-minute data (coarse resolution)
    split_index = int(len(df_15min) * optimization_ratio)
    test_start_timestamp = df_15min.index[split_index]

    print(f"🔹 Splitting data at timestamp: {test_start_timestamp}")

    # ✂ Split 1-Minute Data
    df_1min_optimization = df_1min[df_1min.index < test_start_timestamp]
    df_1min_test = df_1min[df_1min.index >= test_start_timestamp]

    # ✂ Split 5-Minute Data
    df_5min_optimization = df_5min[df_5min.index < test_start_timestamp]
    df_5min_test = df_5min[df_5min.index >= test_start_timestamp]

    # ✂ Split 15-Minute Data
    df_15min_optimization = df_15min[df_15min.index < test_start_timestamp]
    df_15min_test = df_15min[df_15min.index >= test_start_timestamp]

    return (
        df_1min_optimization, df_1min_test,
        df_5min_optimization, df_5min_test,
        df_15min_optimization, df_15min_test
    )
