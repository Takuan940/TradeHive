import pandas as pd

# Liste der Zeitrahmen und zugehörigen Dateien
zeitrahmen = {
    "1-Minuten": ("spy_optimierung_1min.parquet", "spy_testdaten_1min.parquet", "1min"),
    "5-Minuten": ("spy_optimierung_5min.parquet", "spy_testdaten_5min.parquet", "5min"),
    "15-Minuten": ("spy_optimierung_15min.parquet", "spy_testdaten_15min.parquet", "15min"),
}

# Ergebnisse speichern
ergebnisse = []

for timeframe, (optimierung_file, test_file, freq) in zeitrahmen.items():
    print(f"\n🔍 Prüfe {timeframe}-Daten...")

    try:
        # Dateien einlesen
        df_optimierung = pd.read_parquet(optimierung_file)
        df_test = pd.read_parquet(test_file)

        # Falls die `date`-Spalte im Index steckt, zurückholen
        if df_optimierung.index.name == 'date':
            df_optimierung = df_optimierung.reset_index()
        if df_test.index.name == 'date':
            df_test = df_test.reset_index()

        # Prüfen, ob die Spalte `date` vorhanden ist
        if 'date' not in df_optimierung.columns or 'date' not in df_test.columns:
            print(f"⚠️ Fehler: `date`-Spalte fehlt in {timeframe}-Daten!")
            continue

        # Doppelte Zeitstempel prüfen
        duplikate_optimierung = df_optimierung.duplicated(subset=['date']).sum()
        duplikate_test = df_test.duplicated(subset=['date']).sum()

        # Prüfung auf fehlende Zeitstempel: Vergleich mit vorherigem Timestamp
        df_optimierung['date'] = pd.to_datetime(df_optimierung['date'])
        df_optimierung = df_optimierung.set_index('date')

        df_test['date'] = pd.to_datetime(df_test['date'])
        df_test = df_test.set_index('date')

        # Berechnung der erwarteten Zeitabstände (1min, 5min, 15min)
        timedelta_map = {"1min": "1T", "5min": "5T", "15min": "15T"}

        fehlende_optimierung = (df_optimierung.index.to_series().diff() != pd.Timedelta(timedelta_map[freq])).sum()
        fehlende_test = (df_test.index.to_series().diff() != pd.Timedelta(timedelta_map[freq])).sum()

        # Ergebnisse speichern
        ergebnisse.append({
            "Zeitrahmen": timeframe,
            "Optimierungsdaten": len(df_optimierung),
            "Testdaten": len(df_test),
            "Doppelte Zeitstempel Optimierung": duplikate_optimierung,
            "Doppelte Zeitstempel Test": duplikate_test,
            "Fehlende Zeitstempel Optimierung": fehlende_optimierung,
            "Fehlende Zeitstempel Test": fehlende_test
        })

        # Ausgabe der Ergebnisse
        print(f"🔹 Optimierungsdaten: {len(df_optimierung)} Einträge")
        print(f"🔹 Testdaten: {len(df_test)} Einträge")
        print(f"🚨 Doppelte Zeitstempel in Optimierungsdaten: {duplikate_optimierung}")
        print(f"🚨 Doppelte Zeitstempel in Testdaten: {duplikate_test}")
        print(f"⚠️ Fehlende Zeitstempel in Optimierungsdaten: {fehlende_optimierung}")
        print(f"⚠️ Fehlende Zeitstempel in Testdaten: {fehlende_test}")

    except Exception as e:
        print(f"❌ Fehler beim Prüfen der {timeframe}-Daten: {e}")

# Finale Zusammenfassung
print("\n📊 **Zusammenfassung:**")
df_ergebnisse = pd.DataFrame(ergebnisse)
print(df_ergebnisse)
