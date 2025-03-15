import time
from datetime import datetime, timedelta

def load_1min_data(ib, contract, start, end, data_type="TRADES"):
    """ Fetches historical 1-minute data in 30-day blocks. """
    all_data = []
    current_date = start

    while current_date < end:
        next_date = min(current_date + timedelta(days=30), end)

        print(f"🔄 Fetching {data_type} data from {current_date.strftime('%d.%m.%Y')} to {next_date.strftime('%d.%m.%Y')}...")

        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=next_date.strftime('%Y%m%d %H:%M:%S'),
                durationStr='30 D',
                barSizeSetting='1 min',
                whatToShow=data_type,
                useRTH=True
            )
        except Exception as e:
            print(f"❌ Error fetching {data_type} data: {e}")
            bars = []

        if bars:
            all_data.extend(bars)
        else:
            print(f"⚠️ No data for {data_type} from {current_date.strftime('%d.%m.%Y')} to {next_date.strftime('%d.%m.%Y')}")

        time.sleep(5)
        current_date = next_date

    return all_data if all_data else None
