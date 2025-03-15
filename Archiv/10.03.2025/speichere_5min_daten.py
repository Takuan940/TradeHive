import pandas as pd
from ib_insync import *
from datetime import datetime, timedelta
import time

# Verbindung zur IBKR API herstellen
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=9)  # Neuer Client ID, falls andere Sessions laufen

# SPY definieren
contract = Stock('SPY', 'SMART', 'USD')

# Heutiges Datum
today = datetime.today()

# Startdatum für die Testdaten: 3 Monate zurück
test_start = today - timedelta(days=90)
test_start_str = test_start.strftime('%Y%m%d %H:%M:%S')

# Startdatum für die Optimierungsdaten: 12 Monate vor Testbeginn
optimierung_start = test_start - timedelta(days=365)
optimierung_start_str = optimierung_start.strftime('%Y%m%d %H:%M:%S')

# Enddatum für die Optimierungsdaten = Startdatum der Testdaten
optimierung_end_str = test_start_str

# Funktion zum Abrufen von 5-Minuten-Daten in 90-Tage-Blöcken
def lade_historische_daten(start_date, end_date):
    """ Lädt historische 5-Minuten-Daten in 90-Tage-Blöcken ohne Überschneidung. """
    all_data = []
    current_date = datetime.strptime(start_date, "%Y%m%d %H:%M:%S")

    while current_date < datetime.strptime(end_date, "%Y%m%d %H:%M:%S"):
        next_date = current_date + timedelta(days=90)  # Max. 90 Tage pro Abfrage
        if next_date > datetime.strptime(end_date, "%Y%m%d %H:%M:%S"):
            next_date = datetime.strptime(end_date, "%Y%m%d %H:%M:%S")

        next_date_str = next_date.strftime('%Y%m%d %H:%M:%S')

        print(f"🔄 Lade 5-Minuten-Daten von {current_date.strftime('%d.%m.%Y')} bis {next_date.strftime('%d.%m.%Y')}...")

        bars = ib.reqHistoricalData(
            contract,
            endDateTime=next_date_str,
            durationStr='90 D',
            barSizeSetting='5 mins',
            whatToShow='TRADES',
            useRTH=True
        )

        if bars:
            all_data.extend(bars)
        else:
            print(f"⚠️ Keine Daten für 5 Min von {current_date.strftime('%d.%m.%Y')} bis {next_date.strftime('%d.%m.%Y')}")

        # Warten, um IBKR-Rate-Limit zu vermeiden
        time.sleep(5)

        # Weiter zum nächsten Block
        current_date = next_date

    return all_data

# Daten abrufen
bars_optimierung = lade_historische_daten(optimierung_start_str, optimierung_end_str)
bars_test = lade_historische_daten(test_start_str, today.strftime('%Y%m%d %H:%M:%S'))

# In DataFrames umwandeln
df_optimierung = pd.DataFrame([{
    'date': bar.date, 'open': bar.open, 'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume
} for bar in bars_optimierung])

df_test = pd.DataFrame([{
    'date': bar.date, 'open': bar.open, 'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume
} for bar in bars_test])

# Doppelte Einträge vor dem Speichern entfernen
df_optimierung = df_optimierung.drop_duplicates(subset=['date'])
df_test = df_test.drop_duplicates(subset=['date'])

# Fehlende 5-Minuten-Intervalle auffüllen
df_optimierung['date'] = pd.to_datetime(df_optimierung['date'])
df_optimierung = df_optimierung.set_index('date').asfreq('5min')

df_test['date'] = pd.to_datetime(df_test['date'])
df_test = df_test.set_index('date').asfreq('5min')

# VWAP berechnen
for df, name in zip([df_optimierung, df_test], ["Optimierung", "Test"]):
    df['VWAP'] = (df['high'] + df['low'] + df['close']) / 3
    df['VWAP'] = (df['VWAP'] * df['volume']).cumsum() / df['volume'].cumsum()
    print(f"📊 VWAP für {name}-Daten berechnet!")

# Bereinigte Daten speichern
df_optimierung.to_parquet('spy_optimierung_5min.parquet', index=True)
df_test.to_parquet('spy_testdaten_5min.parquet', index=True)

print("✅ 5-Minuten-Daten erfolgreich gespeichert!")
print(f"🔹 Optimierungsdaten: {optimierung_start.strftime('%d. %B %Y')} bis {test_start.strftime('%d. %B %Y')}")
print(f"🔹 Testdaten: {test_start.strftime('%d. %B %Y')} bis {today.strftime('%d. %B %Y')}")

ib.disconnect()
