import pandas as pd

class MomentumBreakoutAgent:
    def __init__(self,
                 breakout_window=20,
                 ema_trend_filter=True,
                 atr_multiplier_sl=1.5,
                 atr_multiplier_tp=2.5,
                 volume_confirmation=True,
                 min_candle_body_ratio=0.6,
                 min_adx_15m=20,
                 min_atr_threshold=0.1,
                 slippage_adjustment=0.01,
                 debug=False):
        """
        Initializes the Momentum Breakout Agent with customizable parameters.
        """
        self.breakout_window = breakout_window
        self.ema_trend_filter = ema_trend_filter
        self.atr_multiplier_sl = atr_multiplier_sl
        self.atr_multiplier_tp = atr_multiplier_tp
        self.volume_confirmation = volume_confirmation
        self.min_candle_body_ratio = min_candle_body_ratio
        self.min_adx_15m = min_adx_15m
        self.min_atr_threshold = min_atr_threshold
        self.slippage_adjustment = slippage_adjustment
        self.debug = debug

    def get_signal(self, df_5min, df_15min):
        """
        Determines a trading signal based on the provided 5min and 15min market data.
        """
        # Prüfen, ob genügend Daten vorhanden sind
        if df_5min is None or df_15min is None or df_5min.empty or df_15min.empty or len(df_5min) < self.breakout_window + 2 or len(df_15min) < 50:
            return "HOLD", None, None

        try:
            # 🎯 Extract latest values
            last_low_5m = df_5min['low'].iloc[-1]
            last_close_5m = df_5min['close'].iloc[-1]
            last_high_5m = df_5min['high'].iloc[-1]
            last_atr_5m = df_5min['ATR_14'].iloc[-1]
            last_volume_5m = df_5min['volume'].iloc[-1]
            last_adx_15m = df_15min['ADX_14'].iloc[-1]
            ema_20_15m = df_15min['EMA_20'].iloc[-1]
            ema_50_15m = df_15min['EMA_50'].iloc[-1]
            breakout_high = df_5min['high'].rolling(self.breakout_window).max().iloc[-1]
            breakout_low = df_5min['low'].rolling(self.breakout_window).min().iloc[-1]

            # ✅ Trendbestätigung (15-Min-Chart)
            trend_long = ema_20_15m > ema_50_15m if self.ema_trend_filter else True
            trend_short = ema_20_15m < ema_50_15m if self.ema_trend_filter else True

            # ✅ Weitere Bedingungen
            valid_atr = last_atr_5m > self.min_atr_threshold
            valid_adx = last_adx_15m >= self.min_adx_15m
            body_size_5m = abs(last_close_5m - df_5min['open'].iloc[-1])
            candle_size_5m = abs(last_high_5m - last_low_5m)
            valid_candle_body = (body_size_5m / candle_size_5m) >= self.min_candle_body_ratio if candle_size_5m != 0 else False
            valid_volume = last_volume_5m > df_5min['volume'].rolling(self.breakout_window).mean().iloc[-1] if self.volume_confirmation else True

            # 📌 Korrektur der Slippage in Stop-Loss und Take-Profit Berechnung
            entry_price_long = last_close_5m + self.slippage_adjustment
            stop_loss_long = entry_price_long - (self.atr_multiplier_sl * last_atr_5m)
            take_profit_long = entry_price_long + (self.atr_multiplier_tp * last_atr_5m)

            entry_price_short = last_close_5m - self.slippage_adjustment
            stop_loss_short = entry_price_short + (self.atr_multiplier_sl * last_atr_5m)
            take_profit_short = entry_price_short - (self.atr_multiplier_tp * last_atr_5m)

            # 🟢 LONG-Setup
            if last_close_5m > breakout_high and trend_long and valid_atr and valid_adx and valid_candle_body and valid_volume:
                return "BUY CALL", stop_loss_long, take_profit_long

            # 🔴 SHORT-Setup
            if last_close_5m < breakout_low and trend_short and valid_atr and valid_adx and valid_candle_body and valid_volume:
                return "BUY PUT", stop_loss_short, take_profit_short

        except IndexError:
            return "HOLD", None, None

        return "HOLD", None, None
