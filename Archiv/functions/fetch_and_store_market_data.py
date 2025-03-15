from ib_insync import IB, Stock
from datetime import datetime, timedelta
from functions.load_data import load_1min_data
from functions.process_data import process_market_data
from functions.indicator_calculations import calculate_indicators
from functions.synchronize_data import synchronize_timeframes
from functions.split_data import split_optimization_test
import functions.save_data
import time

def fetch_and_store_market_data(symbol: str, days_back: int, optimization_ratio: float):
    """
    Fetches historical market data for a given stock, aggregates it into 1-Min, 5-Min, and 15-Min intervals,
    calculates indicators, synchronizes the data, and splits it into optimization and test sets.

    :param symbol: Stock symbol (e.g., "SPY")
    :param days_back: Number of days to fetch (e.g., 15*30 for 15 months)
    :param optimization_ratio: Percentage of data used for optimization (e.g., 0.8 for 80%)
    """

    print(f"🔄 Connecting to IBKR API...")

    # Connect to IBKR API
    ib = IB()
    try:
        ib.connect('127.0.0.1', 7497, clientId=10)
        print("✅ Connected successfully!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    # Define stock contracts
    contract_trades = Stock(symbol, 'SMART', 'USD')
    contract_bid_ask = contract_trades  # Same contract for BID/ASK data

    # Define time range
    today = datetime.today()
    start_date = today - timedelta(days=days_back)

    print(f"📊 Fetching historical market data for {symbol} from {start_date.date()} to {today.date()}...")

    # Fetch trade and bid/ask data
    try:
        bars_trades = load_1min_data(ib, contract_trades, start_date, today, "TRADES")
        time.sleep(10)
        bars_bid_ask = load_1min_data(ib, contract_bid_ask, start_date, today, "MIDPOINT")
    except Exception as e:
        print(f"❌ Error fetching market data: {e}")
        ib.disconnect()
        return
    print(bars_bid_ask[:10])  # Zeigt die ersten 10 empfangenen Datenobjekte

    print("✅ Data successfully retrieved!")

    # Process Data
    print("🔄 Processing market data...")
    df_1min, df_5min, df_15min = process_market_data(bars_trades, bars_bid_ask)
    print("✅ Data processing complete!")

    # Calculate Indicators
    print("🔄 Calculating indicators...")
    df_1min = calculate_indicators(df_1min)
    df_5min = calculate_indicators(df_5min)
    df_15min = calculate_indicators(df_15min)
    print("✅ Indicators calculated!")

    # Synchronize all timeframes
    print("🔄 Synchronizing all timeframes...")
    df_1min, df_5min, df_15min = synchronize_timeframes(df_1min, df_5min, df_15min)
    print("✅ Timeframes synchronized!")

    # Split data into optimization and test sets
    print("🔄 Splitting data into optimization and test sets...")
    (df_1min_optimization, df_1min_test,
     df_5min_optimization, df_5min_test,
     df_15min_optimization, df_15min_test) = split_optimization_test(df_1min, df_5min, df_15min, optimization_ratio)
    print("✅ Data successfully split!")

    # Save market data
    print(f"💾 Saving market data for {symbol}...")
    functions.save_data.save_market_data(
        symbol=symbol,
        df_1min_optimization=df_1min_optimization, df_1min_test=df_1min_test,
        df_5min_optimization=df_5min_optimization, df_5min_test=df_5min_test,
        df_15min_optimization=df_15min_optimization, df_15min_test=df_15min_test
    )

    print(f"✅ {symbol} Data successfully saved!")

    # Disconnect from IBKR
    ib.disconnect()
    print("🔌 Disconnected from IBKR API.")
