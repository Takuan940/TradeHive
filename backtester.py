import numpy as np
import logging
import pandas as pd

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
        self.trades = []
        self.current_trade = None

    def run_backtest(self):
        """Run the backtest over the available market data."""
        for i in range(len(self.df_5min)):
            current_time = self.df_5min.index[i]
            df_5min_slice = self.df_5min.iloc[:i]
            df_15min_slice = self.df_15min[self.df_15min.index <= current_time]

            if df_5min_slice.empty or df_15min_slice.empty:
                continue  # Verhindert Zugriff auf leere DataFrames

            # Erhalte das Trading-Signal
            signal, stop_loss, take_profit = self.agent.get_signal(df_5min_slice, df_15min_slice)

            if signal not in ["BUY CALL", "BUY PUT", "HOLD"]:
                raise Exception(f"Ungültiges Signal erhalten: {signal}")

            # Prüfe zuerst, ob der aktuelle Trade geschlossen werden muss
            if self.current_trade is not None:
                self._check_exit_conditions(signal, df_5min_slice.iloc[-1])

            if self.current_trade is None and signal != "HOLD":
                self._manage_trade(signal, stop_loss, take_profit, df_5min_slice.iloc[-1])

        return self._calculate_metrics()

    def _check_exit_conditions(self, signal, last_candle):
        """Prüft, ob der aktuelle Trade geschlossen werden muss."""
        trade_type = self.current_trade['type']

        if trade_type not in ["BUY CALL", "BUY PUT"]:
            raise Exception(f"Ungültiger Trade-Typ: {trade_type}")

        if trade_type == "BUY CALL":
            # Falls ein Trade in PUT Richtung aktiv ist, wird dieser geschlossen
            if signal == "BUY PUT":
                self._close_trade(last_candle['close'])
                return
            # Falls der StopLoss getroffen wurde, wird der Trade geschlossen
            if last_candle['low'] <= self.current_trade['stop_loss']:
                self._close_trade(self.current_trade['stop_loss'] - self.slippage)
            # Falls der TakeProfit getroffen wurde, wird der Trade geschlossen
            elif last_candle['high'] >= self.current_trade['take_profit']:
                self._close_trade(self.current_trade['take_profit'] - self.slippage)

        elif trade_type == "BUY PUT":
            # Falls ein Trade in CALL Richtung aktiv ist, wird dieser geschlossen
            if signal == "BUY CALL":
                self._close_trade(last_candle['close'])
                return
            # Falls der StopLoss getroffen wurde, wird der Trade geschlossen
            if last_candle['high'] >= self.current_trade['stop_loss']:
                self._close_trade(self.current_trade['stop_loss'] + self.slippage)
            # Falls der TakeProfit getroffen wurde, wird der Trade geschlossen
            elif last_candle['low'] <= self.current_trade['take_profit']:
                self._close_trade(self.current_trade['take_profit'] + self.slippage)

    def _manage_trade(self, signal, stop_loss, take_profit, last_candle):
        """Verwaltet Trades und eröffnet neue, falls notwendig."""
        entry_price = last_candle['close'] + self.slippage if signal == "BUY CALL" else last_candle['close'] - self.slippage
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
        self.balance = 0

    def _close_trade(self, exit_price):
        """Schließt den aktuellen Trade mit dem gegebenen Exit-Preis und speichert das Ergebnis."""
        profit = (exit_price - self.current_trade['entry_price']) * self.current_trade['size']
        if self.current_trade['type'] == "BUY PUT":
            profit *= -1

        profit -= self.current_trade['entry_balance'] * self.fee_per_trade
        self.balance = self.current_trade['entry_balance'] + profit

        self.current_trade['exit_price'] = exit_price
        self.current_trade['exit_balance'] = self.balance
        self.current_trade['closed'] = True

        self.trades.append(self.current_trade)
        self.current_trade = None

    def _calculate_max_drawdown(self):
        """Berechnet den maximalen Drawdown basierend auf exit_balance-Werten."""
        if not self.trades:
            return 0  # Kein Drawdown, falls keine Trades existieren

        # Liste aller Kapitalstände nach jedem Trade
        balances = [self.initial_balance] + [t['exit_balance'] for t in self.trades if 'exit_balance' in t]
        balances = np.array(balances)

        # Höchstes Kapital bis zu jedem Zeitpunkt (laufendes Maximum)
        running_max = np.maximum.accumulate(balances)

        # Drawdowns berechnen: Wie weit fällt das Kapital vom höchsten Stand?
        drawdowns = (running_max - balances) / running_max

        # Maximalen Drawdown als Prozentwert zurückgeben
        max_drawdown = round(np.max(drawdowns) * 100, 2) if drawdowns.size > 0 else 0

        return max_drawdown

    def _calculate_metrics(self):
        """Berechnet Performance-Kennzahlen."""
        total_trades = len(self.trades)
        win_trades = sum(1 for t in self.trades if 'exit_price' in t and
                         ((t['type'] == "BUY CALL" and t['exit_price'] > t['entry_price']) or
                          (t['type'] == "BUY PUT" and t['exit_price'] < t['entry_price'])))

        win_rate = round((win_trades / total_trades) * 100, 2) if total_trades > 0 else 0

        # Sharpe Ratio berechnen
        returns = [(t['exit_price'] / t['entry_price'] - 1) if t['type'] == "BUY CALL"
            else (t['entry_price'] / t['exit_price'] - 1) for t in self.trades if 'exit_price' in t]

        if len(returns) > 1 and np.std(returns) > 0:
            sharpe_ratio = round(np.mean(returns) / np.std(returns) * np.sqrt(252), 2)
        else:
            sharpe_ratio = 0

        # Max Drawdown berechnen
        max_drawdown = self._calculate_max_drawdown()

        return {
            "Final Balance": round(self.balance, 2),
            "Total Trades": total_trades,
            "Win Rate (%)": win_rate,
            "Sharpe Ratio": sharpe_ratio,
            "Max Drawdown": max_drawdown
        }
