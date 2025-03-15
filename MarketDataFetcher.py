import pandas as pd
import numpy as np
import time
from ib_insync import IB, Stock
from datetime import datetime


class MarketDataFetcher:
    def __init__(self, symbol, days, train_ratio):
        self.symbol = symbol
        self.days = days
        self.train_ratio = train_ratio
        self.ib = IB()

    def process_and_save_data(self):
        print("🔄 Connecting to IBKR API...")
        self.ib.connect('127.0.0.1', 7497, clientId=1)
        print("✅ Connected successfully!")

        # 1-Minuten-Daten abrufen (30-Tage-Blöcke)
        df_1min = self._fetch_1min_data_in_chunks()
        df_5min = self._resample_data(df_1min, '5T')
        df_15min = self._resample_data(df_1min, '15T')

        # Indikatoren berechnen
        df_1min = self._calculate_indicators(df_1min)
        df_5min = self._calculate_indicators(df_5min)
        df_15min = self._calculate_indicators(df_15min)

        # Entferne die ersten 50 Zeilen von df_15min
        df_15min = df_15min.iloc[50:]

        # Synchronisiere 1-Min- und 5-Min-Daten mit 15-Min-Startzeitpunkt
        first_timestamp = df_15min.index[0]
        df_1min = df_1min[df_1min.index >= first_timestamp]
        df_5min = df_5min[df_5min.index >= first_timestamp]

        # Train-Test-Split mit df_15min als Referenz
        train_size = int(len(df_15min) * self.train_ratio)
        train_timestamp = df_15min.index[train_size]

        df_1min_train, df_1min_test = df_1min[df_1min.index < train_timestamp], df_1min[df_1min.index >= train_timestamp]
        df_5min_train, df_5min_test = df_5min[df_5min.index < train_timestamp], df_5min[df_5min.index >= train_timestamp]
        df_15min_train, df_15min_test = df_15min[df_15min.index < train_timestamp], df_15min[df_15min.index >= train_timestamp]

        # Speichern
        self._save_data(df_1min_train, "train_1min")
        self._save_data(df_5min_train, "train_5min")
        self._save_data(df_15min_train, "train_15min")
        self._save_data(df_1min_test, "test_1min")
        self._save_data(df_5min_test, "test_5min")
        self._save_data(df_15min_test, "test_15min")

        print("✅ Data processing complete!")

    def _fetch_1min_data_in_chunks(self):
        """ Lädt 1-Minuten-Daten in 30-Tage-Blöcken und fügt sie zusammen. """
        contract = Stock(self.symbol, 'SMART', 'USD')
        self.ib.qualifyContracts(contract)

        all_data = []
        end_date_dt = datetime.now()  # Startpunkt: Heute
        num_batches = self.days // 30  # Anzahl der 30-Tage-Blöcke

        for i in range(num_batches):
            start_date_str = (end_date_dt - pd.Timedelta(days=30)).strftime("%d.%m.%Y %H:%M:%S")
            end_date_str = end_date_dt.strftime("%d.%m.%Y %H:%M:%S")

            print(f"🔄 Fetching 1-Min data from {start_date_str} to {end_date_str}...")

            bars = self.ib.reqHistoricalData(
                contract, endDateTime=end_date_dt.strftime("%Y%m%d %H:%M:%S"), durationStr="30 D",
                barSizeSetting="1 min", whatToShow="TRADES", useRTH=True, formatDate=1
            )

            if not bars:
                print("❌ No more data available.")
                break

            df = pd.DataFrame(bars)
            df.set_index("date", inplace=True)
            all_data.append(df)

            # Setze neues Enddatum (30 Tage weiter in die Vergangenheit)
            end_date_dt -= pd.Timedelta(days=30)

            time.sleep(10)  # IBKR-Limit beachten

        # Alle geladenen Daten kombinieren
        df_1min = pd.concat(all_data).sort_index()
        df_1min = df_1min[~df_1min.index.duplicated(keep="first")]  # Doppelte Einträge entfernen
        return df_1min

    def _resample_data(self, df, timeframe):
        """ Aggregiert Daten auf das gewünschte Zeitintervall """
        return df.resample(timeframe).agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()

    def _calculate_indicators(self, df):
        """ Berechnet technische Indikatoren und entfernt erste Zeilen mit NaN-Werten """
        df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean().round(2)
        df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean().round(2)
        df['ATR_14'] = self._calculate_atr(df, 14).round(2)
        df['Momentum_14'] = df['close'].diff(14).round(2)
        df['ADX_14'] = self._calculate_adx(df, 14).round(2)

        # Entferne die ersten 50 Zeilen mit NaN-Werten
        return df.iloc[50:]

    def _calculate_atr(self, df, period=14):
        """ Berechnet den Average True Range (ATR) """
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())

        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(period).mean()

    def _calculate_adx(self, df, period=14):
        """ Berechnet den Average Directional Index (ADX) """
        plus_dm = df['high'].diff()
        minus_dm = df['low'].diff()

        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm > 0] = 0

        tr = self._calculate_atr(df, period)
        plus_di = 100 * (plus_dm.rolling(period).mean() / tr)
        minus_di = abs(100 * (minus_dm.rolling(period).mean() / tr))
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        return dx.rolling(period).mean()

    def _save_data(self, df, filename):
        """ Speichert die Daten im Parquet-Format """
        df.to_parquet(f"saved_data/SPY_{filename}.parquet")
        print(f"✅ Saved {filename}.parquet")


# Test
if __name__ == "__main__":
    fetcher = MarketDataFetcher("SPY", 15 * 30, 0.8)
    fetcher.process_and_save_data()
