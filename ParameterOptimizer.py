import pandas as pd
import numpy as np
import multiprocessing
import itertools
import random
import time
import threading
from backtester import Backtester
from MomentumBreakoutAgent import MomentumBreakoutAgent

# Laden der Marktdaten
df_5min = pd.read_parquet("saved_data/SPY_train_5min.parquet")
df_15min = pd.read_parquet("saved_data/SPY_train_15min.parquet")

# Parameterbereiche für die Optimierung
param_grid = {
    "breakout_window": [10, 20, 30, 50],
    "ema_trend_filter": [True],
    "atr_multiplier_sl": [2.0, 2.5, 3.0, 4],
    "atr_multiplier_tp": [2.5, 3.0, 4.0, 5],
    "min_candle_body_ratio": [0.3, 0.5, 0.7],
    "min_adx_15m": [10, 20],
    "min_atr_threshold": [0.05, 0.1, 0.2]
}

num_combinations = 100
param_combinations = random.sample(list(itertools.product(*param_grid.values())), num_combinations)


def is_valid_params(params):
    param_dict = dict(zip(param_grid.keys(), params))
    return param_dict["atr_multiplier_sl"] < param_dict["atr_multiplier_tp"]


def run_backtest_with_timeout(params, timeout=60):
    if not is_valid_params(params):
        return None

    agent_params = dict(zip(param_grid.keys(), params))
    agent = MomentumBreakoutAgent(**agent_params)
    backtester = Backtester(agent, df_5min, df_15min, visualize=False)
    result = {}

    def target():
        nonlocal result
        try:
            result = backtester.run_backtest()
        except Exception as e:
            print(f"❌ Fehler im Backtest: {e}")
            result = None

    thread = threading.Thread(target=target)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        print("⚠️ Timeout: Backtest hat zu lange gedauert.")
        return None

    if not isinstance(result, dict):
        return None

    return {
        **agent_params,
        "final_balance": result.get("Final Balance", 0),
        "sharpe_ratio": result.get("Sharpe Ratio", 0),
        "num_trades": result.get("Total Trades", 0),
        "max_drawdown": result.get("Max Drawdown", 0),
        "win_rate": result.get("Win Rate (%)", 0)  # Winrate hinzufügen
    }


if __name__ == '__main__':
    num_cores = max(1, multiprocessing.cpu_count() - 1)
    print(f"🔄 Starte Parameteroptimierung mit {num_cores} Kernen...")

    start_time = time.time()
    results = []

    with multiprocessing.Pool(num_cores) as pool:
        for i, res in enumerate(pool.imap_unordered(run_backtest_with_timeout, param_combinations), 1):
            if res:
                results.append(res)
                progress = (i / num_combinations) * 100
                elapsed_time = time.time() - start_time
                estimated_total_time = (elapsed_time / i) * num_combinations
                remaining_time = estimated_total_time - elapsed_time
                print(
                    f"🚀 Fortschritt: {progress:.2f}% - Verstrichen: {elapsed_time:.1f}s - Geschätzt: {remaining_time:.1f}s verbleibend")

    df_results = pd.DataFrame(results)
    df_results.to_csv("optimization_results.csv", index=False)
    print(f"✅ Optimierung abgeschlossen! Ergebnisse gespeichert: optimization_results.csv")
