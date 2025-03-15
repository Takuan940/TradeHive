from ib_insync import IB, Stock
import pandas as pd

# Verbindung zur IBKR API herstellen
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)  # Falls Paper-Trading: Port 7497, sonst 7496

# SPY Kontrakt definieren
contract = Stock('SPY', 'SMART', 'USD')

# Historische Daten abrufen (5-Minuten-Intervall als Beispiel)
bars = ib.reqHistoricalData(
    contract,
    endDateTime='',
    durationStr='1 D',
    barSizeSetting='5 mins',
    whatToShow='TRADES',
    useRTH=True,
    formatDate=1
)

# In DataFrame umwandeln
df = pd.DataFrame(bars)

# Prüfen, ob VWAP enthalten ist
print("📊 Erhaltene Spalten:", df.columns)

# Falls VWAP enthalten ist, einige Werte ausgeben
if 'VWAP' in df.columns:
    print("✅ IBKR liefert VWAP!")
    print(df[['date', 'VWAP']].head())
else:
    print("❌ Kein VWAP in den Daten!")

# Verbindung schließen
ib.disconnect()
