import pandas as pd

df = pd.read_parquet('spy_optimierung_1min.parquet')

# Doppelte Zeitstempel prüfen
print(df.duplicated(subset=['date']).sum())  # Anzahl doppelter Zeitstempel

# Fehlende Zeiträume prüfen
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date')
print(df.asfreq('T').isna().sum())  # Fehlt irgendwo eine 1-Minuten-Kerze?
