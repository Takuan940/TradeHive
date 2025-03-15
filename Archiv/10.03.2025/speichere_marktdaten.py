from ib_insync import IB, Stock
import pandas as pd

# Verbindung zur IBKR API herstellen
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)  # Falls Paper-Trading: Port 7497, sonst 7496

# SPY Kontrakt definieren
contract = Stock('SPY', 'SMART', 'USD')

# Zeitrahmen & Speicherdateien
zeitrahmen = {
    "1-Minuten": ("spy_optimierung_1min.parquet", "spy_testdaten_1min.parquet", "1 min"),
    "5-Minuten": ("spy_optimierung_5min.parquet", "spy_testdaten_5min.parquet", "5 mins"),
    "15-Minuten": ("spy_optimierung_15min.parquet", "spy_testdaten_15min.parquet", "15 mins"),
}

# Zeitraum festlegen
optimierungszeitraum = "12 M"
testzeitraum = "3 M"

for timeframe, (optimierung_file, test_file, bar_size) in zeitrahmen.items():
    print(f"\n🔄 Lade {timeframe}-Daten...")

    # Daten für den Optimierungszeitraum abrufen
    bars_opt = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr=optimierungszeitraum,
        barSizeSetting=bar_size,
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    # Daten für den Testzeitraum abrufen
    bars_test = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr=testzeitraum,
        barSizeSetting=bar_size,
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    # In DataFrame umwandeln
    df_opt = pd.DataFrame(bars_opt)
    df_test = pd.DataFrame(bars_test)

    # Falls Daten vorhanden, VWAP berechnen
    if not df_opt.empty:
        df_opt['VWAP'] = (df_opt['high'] + df_opt['low'] + df_opt['close']) / 3
        df_opt['VWAP'] = (df_opt['VWAP'] * df_opt['volume']).cumsum() / df_opt['volume'].cumsum()

    if not df_test.empty:
        df_test['VWAP'] = (df_test['high'] + df_test['low'] + df_test['close']) / 3
        df_test['VWAP'] = (df_test['VWAP'] * df_test['volume']).cumsum() / df_test['volume'].cumsum()

    # Speichern als Parquet
    df_opt.to_parquet(optimierung_file, index=False)
    df_test.to_parquet(test_file, index=False)

    print(f"✅ {timeframe}-Daten mit VWAP erfolgreich gespeichert!")

# Verbindung trennen
ib.disconnect()
print("\n🚀 Alle Marktdaten inkl. VWAP erfolgreich gespeichert!")
