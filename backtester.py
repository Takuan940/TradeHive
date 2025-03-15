import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import logging

# Logging einrichten
logging.basicConfig(filename="backtest_log.txt", level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Backtester:
    def __init__(self, agent, df_5min, df_15min, initial_balance=10000, slippage=0.01, fee_per_trade=0.0001,
                 visualize=False):
        """
        Generic Backtester for trading agents.
        """
        self.agent = agent
        self.df_5min = df_5min
        self.df_15min = df_15min
        self.initial_balance = initial_balance
        self.slippage = slippage
        self.fee_per_trade = fee_per_trade
        self.visualize = visualize

        self.balance = initial_balance
        self.equity_curve = []
        self.trades = []
        self.current_trade = None

        logging.info("Backtester initialisiert.")

    def run_backtest(self):
        """Run the backtest over the available market data."""
        for i in range(len(self.df_5min)):
            current_time = self.df_5min.index[i]
            df_5min_slice = self.df_5min.iloc[:i] if i > 0 else self.df_5min.iloc[:1]
            df_15min_slice = self.df_15min[self.df_15min.index <= current_time]

            if df_5min_slice.empty or df_15min_slice.empty:
                continue  # Verhindert Zugriff auf leere DataFrames

            # Erhalte das Trading-Signal
            signal, stop_loss, take_profit = self.agent.get_signal(df_5min_slice, df_15min_slice)
            logging.debug(f"Zeitschritt {i}: Signal={signal}, SL={stop_loss}, TP={take_profit}")

            # Prüfe zuerst, ob der aktuelle Trade geschlossen werden muss
            if not df_5min_slice.empty:
                self._check_exit_conditions(df_5min_slice.iloc[-1])
                self._manage_trade(signal, stop_loss, take_profit, df_5min_slice.iloc[-1])

            self.equity_curve.append(self.balance)

        logging.info("Backtest abgeschlossen.")
        return self._calculate_metrics()

    def _check_exit_conditions(self, last_candle):
        """Prüft, ob der aktuelle Trade geschlossen werden muss."""
        if self.current_trade is None:
            return

        trade_type = self.current_trade['type']
        logging.debug(f"Prüfe Exit-Bedingungen für Trade: {self.current_trade}")

        if trade_type == "BUY CALL":
            if last_candle['low'] <= self.current_trade['stop_loss']:
                self._close_trade(self.current_trade['stop_loss'] - self.slippage)
            elif last_candle['high'] >= self.current_trade['take_profit']:
                self._close_trade(self.current_trade['take_profit'] - self.slippage)

        elif trade_type == "BUY PUT":
            if last_candle['high'] >= self.current_trade['stop_loss']:
                self._close_trade(self.current_trade['stop_loss'] + self.slippage)
            elif last_candle['low'] <= self.current_trade['take_profit']:
                self._close_trade(self.current_trade['take_profit'] + self.slippage)

    def _manage_trade(self, signal, stop_loss, take_profit, last_candle):
        """Verwaltet Trades und eröffnet neue, falls notwendig."""
        if self.current_trade:
            if ((self.current_trade['type'] == "BUY CALL" and signal == "BUY PUT") or
                    (self.current_trade['type'] == "BUY PUT" and signal == "BUY CALL")):
                self._close_trade(last_candle['close'])
            else:
                return

        if signal in ["BUY CALL", "BUY PUT"]:
            entry_price = last_candle['close'] + self.slippage if signal == "BUY CALL" else last_candle[
                                                                                                'close'] - self.slippage
            trade_size = self.balance / entry_price
            trade = {
                'type': signal,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'size': trade_size,
                'entry_balance': self.balance
            }
            self.current_trade = trade
            self.trades.append(trade)
            self.balance = 0

    def _close_trade(self, exit_price):
        """Schließt den aktuellen Trade mit dem gegebenen Exit-Preis und speichert das Ergebnis."""
        if self.current_trade:
            logging.debug(f"Schließe Trade: {self.current_trade}")

            if exit_price is None or np.isnan(exit_price):
                logging.error(f"❌ Fehler: exit_price ist None oder NaN für Trade {self.current_trade}")
                exit_price = self.df_5min.iloc[-1]['close']
                logging.warning(f"⚠ Verwende letzten bekannten Schlusskurs als Fallback: {exit_price}")

            logging.info(f"📌 Trade wird geschlossen mit exit_price={exit_price}")

            profit = (exit_price - self.current_trade['entry_price']) * self.current_trade['size']
            if self.current_trade['type'] == "BUY PUT":
                profit *= -1

            profit -= self.current_trade['entry_balance'] * self.fee_per_trade
            self.balance += self.current_trade['entry_balance'] + profit

            self.current_trade['exit_price'] = exit_price
            self.current_trade['closed'] = True

            logging.debug(f"✅ Abgeschlossener Trade: {self.current_trade}")

            self.trades.append(self.current_trade.copy())
            self.current_trade = None

    def _calculate_metrics(self):
        """Berechnet Performance-Kennzahlen."""
        equity_array = np.array(self.equity_curve, dtype=np.float64)
        returns = np.diff(equity_array) / np.maximum(equity_array[:-1], 1)
        returns = returns[~np.isnan(returns)]

        sharpe_ratio = round((np.mean(returns) / np.std(returns) * np.sqrt(252)) if np.std(returns) > 0 else 0, 2)
        running_max = np.maximum.accumulate(equity_array)
        drawdowns = running_max - equity_array
        max_drawdown = round(np.max(drawdowns), 2)

        results = {
            "Final Balance": round(self.balance, 2),
            "Total Trades": len(self.trades),
            "Sharpe Ratio": sharpe_ratio,
            "Max Drawdown": max_drawdown
        }
        logging.info(f"📊 Backtest-Ergebnisse: {results}")
        return results
