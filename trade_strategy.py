import pandas as pd
import numpy as np


class TradingStrategy:
    """
    Implements a trading strategy that uses technical indicators
    such as Keltner Channels, Bollinger Bands, and the Commodity Channel Index (CCI).
    """
    def __init__(self, df: pd.DataFrame, supabase):
        """Initialize the trading strategy with a DataFrame and Supabase connection."""
        self.df = df
        self.supabase = supabase
    
    def calculate_keltner_channels(self, period, atr_multiplier):
        """Calculate Keltner Channels using Exponential Moving Average (EMA) and Average True Range (ATR)."""
        self.df['EMA'] = self.df['close'].ewm(span=period, adjust=False).mean()
        self.df['ATR'] = self.calculate_average_true_range()
        self.df['KeltnerUpper'] = self.df['EMA'] + (atr_multiplier * self.df['ATR'])
        self.df['KeltnerLower'] = self.df['EMA'] - (atr_multiplier * self.df['ATR'])

    def calculate_cci(self, period):
        """Calculate Commodity Channel Index (CCI) to measure market trend strength."""
        self.df['SMA'] = self.df['close'].rolling(window=period).mean()
        self.df['mean_deviation'] = (self.df['close'] - self.df['SMA']).abs().rolling(window=period).mean()
        self.df['CCI'] = (self.df['close'] - self.df['SMA']) / (0.015 * self.df['mean_deviation'])

    def calculate_bollinger_bands(self, period, std_multiplier):
        """Calculate Bollinger Bands using Simple Moving Average (SMA) and standard deviation."""
        self.df['SMA'] = self.df['close'].rolling(window=period).mean()
        self.df['StdDev'] = self.df['close'].rolling(window=period).std()
        self.df['BollingerUpper'] = self.df['SMA'] + (std_multiplier * self.df['StdDev'])
        self.df['BollingerLower'] = self.df['SMA'] - (std_multiplier * self.df['StdDev'])

    def calculate_average_true_range(self) -> pd.Series:
        """Calculate the Average True Range (ATR), an indicator of market volatility."""
        high_low = self.df['high'] - self.df['low']
        high_close = abs(self.df['high'] - self.df['close'].shift())
        low_close = abs(self.df['low'] - self.df['close'].shift())
        return pd.Series(np.maximum(np.maximum(high_low, high_close), low_close), index=self.df.index)

    def generate_signals(self):
        """Generate buy (long) and sell (short) signals based on trading indicators."""
        self.df['LongSignal'] = False
        self.df['ShortSignal'] = False
        last_index = self.df.index[-1]  # Only consider the last row

        if (self.df.at[last_index, 'close'] < self.df.at[last_index, 'KeltnerLower'] and
            self.df.at[last_index, 'CCI'] < -100 and
            abs(self.df.at[last_index, 'BollingerLower'] - self.df.at[last_index, 'KeltnerLower']) / self.df.at[last_index, 'KeltnerLower'] < 0.005):
            self.df.at[last_index, 'LongSignal'] = True

        elif (self.df.at[last_index, 'close'] > self.df.at[last_index, 'KeltnerUpper'] and
              self.df.at[last_index, 'CCI'] > 100 and
              abs(self.df.at[last_index, 'BollingerUpper'] - self.df.at[last_index, 'KeltnerUpper']) / self.df.at[last_index, 'KeltnerUpper'] < 0.005):
            self.df.at[last_index, 'ShortSignal'] = True

    def calculate_take_profit_stop_loss(self, entry_price: float, atr: float, tp_level:float,sl_level:float):
        """Calculate take profit and stop loss levels based on ATR."""
        take_profit = entry_price + (tp_level * atr)
        stop_loss = entry_price - (sl_level * atr)
        buy_fee = entry_price * 0.005  # 0.5% fee
        return take_profit, stop_loss, buy_fee

    def execute_trades(self, budget: float, tp_level:float,sl_level: float):
        """Execute trades based on generated signals and store data in Supabase."""
        last_index = self.df.index[-1]
        entry_price = self.df.at[last_index, 'close']
        atr = self.df.at[last_index, 'ATR']
        sol_amount = budget / entry_price if budget > 0 else 0
        budget = 0  # Set budget to zero after trade execution

        long_take_profit, long_stop_loss, buy_fee = None, None, None
        short_take_profit, short_stop_loss = None, None

        if self.df.at[last_index, 'LongSignal']:
            long_take_profit, long_stop_loss, buy_fee = self.calculate_take_profit_stop_loss(entry_price, atr,tp_level,sl_level)
           

        if self.df.at[last_index, 'ShortSignal']:
            short_take_profit, short_stop_loss, buy_fee = self.calculate_take_profit_stop_loss(entry_price, atr,tp_level,sl_level)
           

        trade_status = self.insert_trade_to_supabase(budget, sol_amount, entry_price, self.df.at[last_index, 'ShortSignal'],
                                      self.df.at[last_index, 'LongSignal'], short_take_profit, short_stop_loss,
                                      long_take_profit, long_stop_loss, buy_fee)
        return trade_status
    def insert_trade_to_supabase(self, budget: float, sol_amount: float, sol_price: float, short_signal: bool,
                                 long_signal: bool, short_take_profit: float, short_stop_loss: float,
                                 long_take_profit: float, long_stop_loss: float, buy_fee: float):
        """Insert trade data into the Supabase database."""
        trade_status = 'OPEN' if short_signal or long_signal else 'CLOSED'
        #change to kafka producer 
        
        try:
            response = self.supabase.table('trades').insert({
                "budget": budget,
                "sol_amount": sol_amount,
                "sol_price": sol_price,
                "short_signal": int(short_signal),
                "long_signal": int(long_signal),
                "entry_price": sol_price,
                "short_take_profit": short_take_profit,
                "short_stop_loss": short_stop_loss,
                "long_take_profit": long_take_profit,
                "long_stop_loss": long_stop_loss,
                "buy_fee": buy_fee,
                "status": trade_status
            }).execute()
            
            if response.data:
                print("Trade data successfully inserted into Supabase!")
            else:
                print("Failed to insert trade data into Supabase.")
        except Exception as e:
            print(f"Error inserting trade data into Supabase: {e}")
        return trade_status

