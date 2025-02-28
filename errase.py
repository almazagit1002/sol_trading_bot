import pandas as pd
import numpy as np
import boto3
from typing import Tuple, List
import pyarrow.parquet as pq

class TradingStrategy:
    def __init__(self, bucket_name: str, file_path: str):
        """
        Initialize the trading strategy with S3 credentials and data location
        
        Args:
            bucket_name (str): S3 bucket name
            file_path (str): Path to parquet file in S3
        """
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.file_path = file_path
        
    def fetch_data(self) -> pd.DataFrame:
        """Fetch parquet data from S3 and convert to DataFrame"""
        response = self.s3.get_object(Bucket=self.bucket_name, Key=self.file_path)
        parquet_data = pq.read_table(response['Body'])
        df = parquet_data.to_pandas()
        
        # Ensure datetime index
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        return df
        
    def aggregate_candles(self, df: pd.DataFrame, timeframe: str = '1H') -> pd.DataFrame:
        """
        Aggregate 15-minute data into specified timeframe candles
        
        Args:
            df (pd.DataFrame): Raw 15-minute data
            timeframe (str): Target timeframe for aggregation (e.g., '1H' for 1 hour)
        """
        return df.resample(timeframe).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all required technical indicators"""
        
        # ATR Calculation
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = np.abs(df['high'] - df['close'].shift())
        df['low_close'] = np.abs(df['low'] - df['close'].shift())
        df['tr'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
        df['atr'] = df['tr'].ewm(span=20, adjust=False).mean()
        
        # EMA for Keltner Channel
        df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
        
        # Keltner Channels
        df['keltner_upper'] = df['ema20'] + (2 * df['atr'])
        df['keltner_lower'] = df['ema20'] - (2 * df['atr'])
        
        # Bollinger Bands
        df['sma20'] = df['close'].rolling(window=20).mean()
        df['stddev'] = df['close'].rolling(window=20).std()
        df['bollinger_upper'] = df['sma20'] + (2 * df['stddev'])
        df['bollinger_lower'] = df['sma20'] - (2 * df['stddev'])
        
        # CCI
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        mean_dev = pd.Series(
            [abs(typical_price[i] - typical_price[i-13:i+1].mean()) for i in range(13, len(typical_price))]
        )
        df['cci'] = (typical_price - typical_price.rolling(14).mean()) / (0.015 * mean_dev)
        
        return df
        
    def check_entry_conditions(self, row: pd.Series) -> Tuple[bool, str]:
        """
        Check if entry conditions are met
        
        Returns:
            Tuple[bool, str]: (entry_signal, position_type)
        """
        # Check long entry conditions
        long_conditions = (
            row['close'] < row['keltner_lower'] and
            row['cci'] < -100 and
            abs(row['bollinger_lower'] - row['keltner_lower']) / row['keltner_lower'] < 0.005
        )
        
        # Check short entry conditions
        short_conditions = (
            row['close'] > row['keltner_upper'] and
            row['cci'] > 100 and
            abs(row['bollinger_upper'] - row['keltner_upper']) / row['keltner_upper'] < 0.005
        )
        
        if long_conditions:
            return True, 'LONG'
        elif short_conditions:
            return True, 'SHORT'
        
        return False, ''
        
    def calculate_exit_levels(self, entry_price: float, atr: float, position_type: str) -> Tuple[float, float]:
        """Calculate take profit and stop loss levels"""
        if position_type == 'LONG':
            take_profit = entry_price + (1.5 * atr)
            stop_loss = entry_price - (0.8 * atr)
        else:  # SHORT
            take_profit = entry_price - (1.5 * atr)
            stop_loss = entry_price + (0.8 * atr)
            
        return take_profit, stop_loss
        
    def run_strategy(self) -> List[dict]:
        """
        Execute the trading strategy and return trade signals
        
        Returns:
            List[dict]: List of trade signals with entry/exit conditions
        """
        # Fetch and prepare data
        raw_data = self.fetch_data()
        candles = self.aggregate_candles(raw_data)
        df = self.calculate_indicators(candles)
        
        trades = []
        in_position = False
        
        for index, row in df.iterrows():
            if not in_position:
                entry_signal, position_type = self.check_entry_conditions(row)
                
                if entry_signal:
                    entry_price = row['close']
                    take_profit, stop_loss = self.calculate_exit_levels(
                        entry_price, row['atr'], position_type
                    )
                    
                    trades.append({
                        'entry_time': index,
                        'position_type': position_type,
                        'entry_price': entry_price,
                        'take_profit': take_profit,
                        'stop_loss': stop_loss,
                        'atr': row['atr']
                    })
                    
                    in_position = True
                    
            else:
                current_trade = trades[-1]
                
                # Check if take profit or stop loss hit
                if position_type == 'LONG':
                    if row['high'] >= current_trade['take_profit']:
                        current_trade['exit_time'] = index
                        current_trade['exit_price'] = current_trade['take_profit']
                        current_trade['exit_reason'] = 'take_profit'
                        in_position = False
                    elif row['low'] <= current_trade['stop_loss']:
                        current_trade['exit_time'] = index
                        current_trade['exit_price'] = current_trade['stop_loss']
                        current_trade['exit_reason'] = 'stop_loss'
                        in_position = False
                else:  # SHORT
                    if row['low'] <= current_trade['take_profit']:
                        current_trade['exit_time'] = index
                        current_trade['exit_price'] = current_trade['take_profit']
                        current_trade['exit_reason'] = 'take_profit'
                        in_position = False
                    elif row['high'] >= current_trade['stop_loss']:
                        current_trade['exit_time'] = index
                        current_trade['exit_price'] = current_trade['stop_loss']
                        current_trade['exit_reason'] = 'stop_loss'
                        in_position = False
        
        return trades

def main():
    # Initialize strategy with your S3 bucket details
    strategy = TradingStrategy(
        bucket_name='your-bucket-name',
        file_path='path/to/your/data.parquet'
    )
    
    # Run strategy and get trades
    trades = strategy.run_strategy()
    
    # Convert trades to DataFrame for analysis
    trades_df = pd.DataFrame(trades)
    
    # Calculate basic statistics
    if not trades_df.empty:
        trades_df['profit'] = trades_df.apply(
            lambda x: x['exit_price'] - x['entry_price'] if x['position_type'] == 'LONG'
            else x['entry_price'] - x['exit_price'],
            axis=1
        )
        
        print("\nTrading Strategy Results:")
        print(f"Total Trades: {len(trades_df)}")
        print(f"Profitable Trades: {len(trades_df[trades_df['profit'] > 0])}")
        print(f"Win Rate: {(len(trades_df[trades_df['profit'] > 0]) / len(trades_df)) * 100:.2f}%")
        print(f"Total Profit: {trades_df['profit'].sum():.2f}")
        print(f"Average Profit per Trade: {trades_df['profit'].mean():.2f}")
        
        # Save results to CSV
        trades_df.to_csv('trading_results.csv')

if __name__ == "__main__":
    main()