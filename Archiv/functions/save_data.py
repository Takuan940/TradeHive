import pandas as pd
import os

def save_market_data(symbol, df_1min_optimization, df_1min_test,
                     df_5min_optimization, df_5min_test,
                     df_15min_optimization, df_15min_test, save_dir="saved_data"):
    """
    Saves market data as Parquet files in a structured directory.

    :param symbol: Stock symbol (e.g., "SPY")
    :param df_1min_optimization: 1-minute optimization data
    :param df_1min_test: 1-minute test data
    :param df_5min_optimization: 5-minute optimization data
    :param df_5min_test: 5-minute test data
    :param df_15min_optimization: 15-minute optimization data
    :param df_15min_test: 15-minute test data
    :param save_dir: Directory to store the Parquet files (default: "saved_data")
    """

    # 🔹 Ensure the save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # 📌 Define file paths
    file_paths = {
        "1min_optimization": os.path.join(save_dir, f"{symbol}_optimization_1min.parquet"),
        "1min_test": os.path.join(save_dir, f"{symbol}_testdata_1min.parquet"),
        "5min_optimization": os.path.join(save_dir, f"{symbol}_optimization_5min.parquet"),
        "5min_test": os.path.join(save_dir, f"{symbol}_testdata_5min.parquet"),
        "15min_optimization": os.path.join(save_dir, f"{symbol}_optimization_15min.parquet"),
        "15min_test": os.path.join(save_dir, f"{symbol}_testdata_15min.parquet"),
    }

    # 🔄 Save each DataFrame
    df_1min_optimization.to_parquet(file_paths["1min_optimization"])
    df_1min_test.to_parquet(file_paths["1min_test"])
    df_5min_optimization.to_parquet(file_paths["5min_optimization"])
    df_5min_test.to_parquet(file_paths["5min_test"])
    df_15min_optimization.to_parquet(file_paths["15min_optimization"])
    df_15min_test.to_parquet(file_paths["15min_test"])

    print("✅ Market data successfully saved:")
    for key, path in file_paths.items():
        print(f"   - {key}: {path}")
