from ib_insync import *

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=12)

contract = Stock('SPY', 'SMART', 'USD')

# Teste den BID/ASK-Abruf
bars = ib.reqHistoricalData(
    contract,
    endDateTime='',
    durationStr='1 D',
    barSizeSetting='1 min',
    whatToShow='BID_ASK',  # Wichtiger Punkt!
    useRTH=True
)

if bars:
    print(f"✅ BID_ASK Daten verfügbar! {len(bars)} Einträge geladen.")
else:
    print("❌ Keine BID_ASK-Daten. Prüfe dein IBKR-Abonnement!")

ib.disconnect()
