import os
import pandas as pd
import numpy as np
from supabase import create_client
import boto3
import json
from datetime import datetime, timedelta
import io
from dotenv import load_dotenv

###############################################################################
# S3JSONToDataFrame Class
# Purpose: Retrieves historical price data from S3 and converts it to a DataFrame
###############################################################################

class S3JSONToDataFrame:
    def __init__(self, bucket_name, base_path='candles/15min/'):
        """
        Initialize the class with bucket name and base path.
        
        Args:
            bucket_name (str): The name of the S3 bucket
            base_path (str): The base path in the bucket where files are stored
        """
        # Initialize the S3 client using boto3 library
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.base_path = base_path
        
    def _construct_path(self, year, month, day):
        """
        Construct the path for a specific date.
        Follows the partitioning pattern: base_path/year=YYYY/month=MM/day=DD/
        
        Args:
            year (int): Year
            month (int): Month
            day (int): Day
            
        Returns:
            str: The constructed path
        """
        # Format month and day with leading zeros
        return f"{self.base_path}year={year}/month={month:02d}/day={day:02d}/"
    
    def _list_files(self, prefix):
        """
        List files in a specific path in the S3 bucket.
        
        Args:
            prefix (str): The prefix to list files from
            
        Returns:
            list: List of file keys
        """
        try:
            # Request object listing from S3
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            # Extract file keys if files exist
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except Exception as e:
            print(f"Error listing files: {e}")
            return []
    
    def _get_previous_date(self, year, month, day):
        """
        Get the previous date (day before).
        Used for traversing backward in time to fetch more data.
        
        Args:
            year (int): Year
            month (int): Month
            day (int): Day
            
        Returns:
            tuple: (year, month, day) for the previous date
        """
        # Create datetime object for current date
        current_date = datetime(year, month, day)
        # Subtract one day
        previous_date = current_date - timedelta(days=1)
        # DEBUG: Print the previous date components
        print(f"Moving to previous day: {previous_date.year}-{previous_date.month:02d}-{previous_date.day:02d}")
        return previous_date.year, previous_date.month, previous_date.day
    
    def _get_file_content(self, file_key):
        """
        Get the content of a file from S3.
        
        Args:
            file_key (str): The key of the file to read
            
        Returns:
            dict: The parsed JSON content or None if error
        """
        try:
            # Request the object from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            # Read and decode the content
            content = response['Body'].read().decode('utf-8')
            # Parse JSON content
            return json.loads(content)
        except Exception as e:
            print(f"Error reading file {file_key}: {e}")
            return None
    
    def get_last_n_files(self, n, specific_date=None):
        """
        Get the last n files from the S3 bucket.
        Traverses backward in time if needed to collect enough files.
        
        Args:
            n (int): Number of files to retrieve
            specific_date (tuple): Optional (year, month, day) to start from
            
        Returns:
            list: List of file contents (parsed JSON)
        """
        files_content = []
        files_to_fetch = n
        
        # Start from specified date or today
        if specific_date:
            year, month, day = specific_date
        else:
            now = datetime.now()
            year, month, day = now.year, now.month, now.day
        
        # Continue fetching until we have enough files or hit safety limit
        while files_to_fetch > 0:
            # Construct the path for the current date
            path = self._construct_path(year, month, day)
            # List files in the path
            files = self._list_files(path)
            
            # Sort files by name (assuming they have timestamp in the name)
            # Reverse to get newest first
            files.sort(reverse=True)
            
            # Process each file in the current date directory
            for file_key in files:
                # Exit early if we have enough files
                if files_to_fetch <= 0:
                    break
                    
                # Get and store file content
                content = self._get_file_content(file_key)
                if content:
                    files_content.append(content)
                    files_to_fetch -= 1
            
            # Logic to move to previous day
            if files_to_fetch > 0:
                if len(files) > 0:
                    # We processed files but need more, move to previous day
                    year, month, day = self._get_previous_date(year, month, day)
                else:
                    # No files found for current day, move to previous day
                    year, month, day = self._get_previous_date(year, month, day)
            else:
                # We have enough files
                break
                
            # Safety check to avoid infinite loops or excessive fetching
            if len(files_content) + files_to_fetch > n * 2:
                print(f"Warning: Could only retrieve {len(files_content)} files out of {n} requested")
                break
        
        return files_content
    
    def create_dataframe(self, n=20, specific_date=None):
        """
        Create a DataFrame from the last n files.
        
        Args:
            n (int): Number of files to retrieve
            specific_date (tuple): Optional (year, month, day) to start from
            
        Returns:
            pandas.DataFrame: DataFrame with the data, sorted chronologically
        """
        # Get file contents
        files_content = self.get_last_n_files(n, specific_date)
        
        # Return empty DataFrame if no files were found
        if not files_content:
            print("Warning: No files found, returning empty DataFrame")
            return pd.DataFrame()
        
        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(files_content)
        
        # Define time-related columns that might be redundant
        time_columns = ['candle_start', 'candle_end', 'timestamp', 'open_time', 'close_time', 'generated_at', 'volume']
        
        # Convert timestamp to datetime and set as index
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
        else:
            print("Warning: 'timestamp' column not found in data")
        
        # Drop redundant time columns but keep timestamp as index
        for col in time_columns:
            if col in df.columns and col != 'timestamp':
                df = df.drop(columns=[col])
        
        # Sort by index to ensure chronological order
        df = df.sort_index()
        
        return df


###############################################################################
# TradingStrategy Class
# Purpose: Implements trading strategy using technical indicators 
# and generates trading signals
###############################################################################

class TradingStrategy:
    """
    Implements a trading strategy that uses technical indicators
    such as Keltner Channels, Bollinger Bands, and the Commodity Channel Index (CCI).
    """
    def __init__(self, df: pd.DataFrame, supabase):
        """
        Initialize the trading strategy with a DataFrame and Supabase connection.
        
        Args:
            df (pd.DataFrame): DataFrame containing price data with OHLC columns
            supabase: Supabase client for database operations
        """
        self.df = df
        self.supabase = supabase
    
    def calculate_keltner_channels(self, period, atr_multiplier):
        """
        Calculate Keltner Channels using Exponential Moving Average (EMA) and Average True Range (ATR).
        Keltner Channels are volatility-based envelopes set above and below an EMA.
        
        Args:
            period (int): The period for the EMA calculation
            atr_multiplier (float): Multiplier for the ATR to determine channel width
        """
        # Calculate Exponential Moving Average
        self.df['EMA'] = self.df['close'].ewm(span=period, adjust=False).mean()
        
        # Calculate Average True Range
        self.df['ATR'] = self.calculate_average_true_range()
        
        # Calculate upper and lower bands
        self.df['KeltnerUpper'] = self.df['EMA'] + (atr_multiplier * self.df['ATR'])
        self.df['KeltnerLower'] = self.df['EMA'] - (atr_multiplier * self.df['ATR'])
        
        # Log the percentage of NaN values in Keltner Channels (due to warm-up period)
        nan_upper = self.df['KeltnerUpper'].isna().sum()
        nan_lower = self.df['KeltnerLower'].isna().sum()
        total_rows = len(self.df)
        if total_rows > 0:
            print(f"Keltner Channels NaN values: Upper={nan_upper}/{total_rows} ({nan_upper/total_rows:.2%}), Lower={nan_lower}/{total_rows} ({nan_lower/total_rows:.2%})")

    def calculate_cci(self, period):
        """
        Calculate Commodity Channel Index (CCI) to measure market trend strength.
        CCI measures the current price level relative to an average price level over a period of time.
        
        Args:
            period (int): The lookback period for calculation
        """
        # First ensure we have enough data
        if len(self.df) < period:
            print(f"Warning: Not enough data points for CCI calculation. Have {len(self.df)}, need at least {period}.")
            # Initialize columns with default values
            self.df['SMA'] = self.df['close'].rolling(window=period, min_periods=1).mean()
            self.df['mean_deviation'] = 0.0
            self.df['CCI'] = 0.0
            return
            
        # Calculate Simple Moving Average
        self.df['SMA'] = self.df['close'].rolling(window=period).mean()
        
        # Calculate mean deviation (average distance of price from SMA)
        mean_deviation = (self.df['close'] - self.df['SMA']).abs().rolling(window=period).mean()
        self.df['mean_deviation'] = mean_deviation
        
        # Calculate CCI with proper NaN and division by zero handling
        # Formula: CCI = (Typical Price - SMA) / (0.015 * Mean Deviation)
        with np.errstate(divide='ignore', invalid='ignore'):  # Suppress numpy warnings
            raw_cci = (self.df['close'] - self.df['SMA']) / (0.015 * mean_deviation)
        
        # Replace infinity and NaN with 0
        self.df['CCI'] = np.where(
            np.isfinite(raw_cci),  # Only use values that are finite (not inf or NaN)
            raw_cci,
            0.0  # Default for invalid calculations
        )
        
        # Log the percentage of zero values in CCI
        zero_count = (self.df['CCI'] == 0).sum()
        total_rows = len(self.df)
        print(f"CCI zeros: {zero_count}/{total_rows} ({zero_count/total_rows:.2%})")

    def calculate_bollinger_bands(self, period, std_multiplier):
        """
        Calculate Bollinger Bands using Simple Moving Average (SMA) and standard deviation.
        Bollinger Bands consist of a middle band (SMA) with upper and lower bands
        at standard deviation levels above and below the middle band.
        
        Args:
            period (int): The period for SMA calculation
            std_multiplier (float): Number of standard deviations for the bands
        """
        # Calculate Simple Moving Average
        self.df['SMA'] = self.df['close'].rolling(window=period).mean()
        
        # Calculate Standard Deviation
        self.df['StdDev'] = self.df['close'].rolling(window=period).std()
        
        # Calculate upper and lower bands
        self.df['BollingerUpper'] = self.df['SMA'] + (std_multiplier * self.df['StdDev'])
        self.df['BollingerLower'] = self.df['SMA'] - (std_multiplier * self.df['StdDev'])
        
        # Log the percentage of NaN values in Bollinger Bands (due to warm-up period)
        nan_upper = self.df['BollingerUpper'].isna().sum()
        nan_lower = self.df['BollingerLower'].isna().sum()
        total_rows = len(self.df)
        if total_rows > 0:
            print(f"Bollinger Bands NaN values: Upper={nan_upper}/{total_rows} ({nan_upper/total_rows:.2%}), Lower={nan_lower}/{total_rows} ({nan_lower/total_rows:.2%})")

    def calculate_average_true_range(self) -> pd.Series:
        """
        Calculate the Average True Range (ATR), an indicator of market volatility.
        ATR is the average of true ranges over the specified period.
        True Range is the greatest of:
        1. Current High - Current Low
        2. |Current High - Previous Close|
        3. |Current Low - Previous Close|
        
        Returns:
            pd.Series: Series containing ATR values
        """
        # Calculate the three components of True Range
        high_low = self.df['high'] - self.df['low']  # Current High - Current Low
        high_close = abs(self.df['high'] - self.df['close'].shift())  # |Current High - Previous Close|
        low_close = abs(self.df['low'] - self.df['close'].shift())  # |Current Low - Previous Close|
        
        # Take the maximum of the three components
        true_range = pd.Series(
            np.maximum(np.maximum(high_low, high_close), low_close), 
            index=self.df.index
        )
        
        # For ATR, we should technically calculate the rolling average of true range
        # But this function returns just the true range, not the average
        # The actual averaging is done in calculate_keltner_channels
        
        return true_range

    def generate_signals(self, period_kernel_chanel, cci_period, period_bolinger_band):
        """
        Generate buy (long) and sell (short) signals based on trading indicators.
        
        Signal logic:
        - Long (Buy) Signal: 
          1. Price below lower Keltner Channel
          2. CCI below -100 (oversold)
          3. Bollinger and Keltner lower bands are close (< 0.5% difference)
        
        - Short (Sell) Signal:
          1. Price above upper Keltner Channel
          2. CCI above 100 (overbought)
          3. Bollinger and Keltner upper bands are close (< 0.5% difference)
        
        Args:
            period_kernel_chanel (int): Period used for Keltner Channels
            cci_period (int): Period used for CCI
            period_bolinger_band (int): Period used for Bollinger Bands
        """
        # Initialize signal columns
        self.df['LongSignal'] = False
        self.df['ShortSignal'] = False

        # Ensure we have enough data for our indicators
        min_required_rows = max(period_kernel_chanel, cci_period, period_bolinger_band) + 1
        if len(self.df) < min_required_rows:
            print(f"Warning: Not enough data points ({len(self.df)}). Need at least {min_required_rows}.")
            return
            
        # Check if DataFrame is empty or doesn't have enough data
        if len(self.df) == 0:
            print("Warning: Empty DataFrame, cannot generate signals")
            return
            
        # We only generate signals for the most recent candle
        last_index = self.df.index[-1]
        
        # Add error handling for missing columns or NaN values
        required_columns = ['close', 'KeltnerLower', 'KeltnerUpper', 'CCI', 'BollingerLower', 'BollingerUpper']
        for col in required_columns:
            if col not in self.df.columns:
                print(f"Warning: Missing column {col}, cannot generate signals")
                return
            if pd.isna(self.df.at[last_index, col]):
                print(f"Warning: NaN value in column {col}, cannot generate signals")
                return

        # Check for division by zero
        if self.df.at[last_index, 'KeltnerLower'] == 0 or self.df.at[last_index, 'KeltnerUpper'] == 0:
            print("Warning: KeltnerLower or KeltnerUpper is zero, cannot generate signals")
            return

        # Long signal conditions
        # 1. Price below lower Keltner Channel
        # 2. CCI below -100 (oversold)
        # 3. Bollinger and Keltner lower bands are close (< 0.5% difference)
        if (self.df.at[last_index, 'close'] < self.df.at[last_index, 'KeltnerLower'] and
            self.df.at[last_index, 'CCI'] < -100 and
            abs(self.df.at[last_index, 'BollingerLower'] - self.df.at[last_index, 'KeltnerLower']) / self.df.at[last_index, 'KeltnerLower'] < 0.005):
            self.df.at[last_index, 'LongSignal'] = True
            print("SIGNAL: LONG signal generated")

        # Short signal conditions
        # 1. Price above upper Keltner Channel
        # 2. CCI above 100 (overbought)
        # 3. Bollinger and Keltner upper bands are close (< 0.5% difference)
        elif (self.df.at[last_index, 'close'] > self.df.at[last_index, 'KeltnerUpper'] and
              self.df.at[last_index, 'CCI'] > 100 and
              abs(self.df.at[last_index, 'BollingerUpper'] - self.df.at[last_index, 'KeltnerUpper']) / self.df.at[last_index, 'KeltnerUpper'] < 0.005):
            self.df.at[last_index, 'ShortSignal'] = True
            print("SIGNAL: SHORT signal generated")
        else:
            print("SIGNAL: No trading signals generated")
        

    def calculate_take_profit_stop_loss(self, entry_price: float, atr: float, tp_level: float, sl_level: float):
        """
        Calculate take profit and stop loss levels based on ATR.
        
        Args:
            entry_price (float): The entry price of the trade
            atr (float): The Average True Range value
            tp_level (float): Multiplier for ATR to determine take profit distance
            sl_level (float): Multiplier for ATR to determine stop loss distance
            
        Returns:
            tuple: (take_profit, stop_loss, buy_fee)
        """
        # Calculate take profit level (entry price + ATR * multiplier)
        take_profit = entry_price + (tp_level * atr)
        
        # Calculate stop loss level (entry price - ATR * multiplier)
        stop_loss = entry_price - (sl_level * atr)
        
        # Calculate trading fee (0.5% of entry price)
        buy_fee = entry_price * 0.005  # 0.5% fee
        
        return take_profit, stop_loss, buy_fee

    def execute_trades(self, budget: float, tp_level: float, sl_level: float):
        """
        Execute trades based on generated signals and store data in Supabase.
        
        Args:
            budget (float): Available budget for trading
            tp_level (float): Take profit level multiplier for ATR
            sl_level (float): Stop loss level multiplier for ATR
            
        Returns:
            str: Trade status ('OPEN', 'CLOSED', or 'ERROR')
        """
        # Check if DataFrame is empty
        if len(self.df) == 0:
            print("Warning: Empty DataFrame, cannot execute trades")
            return 'ERROR'
            
        # We only execute trades based on the most recent candle
        last_index = self.df.index[-1]
        
        # Check for missing columns
        required_columns = ['close', 'ATR', 'LongSignal', 'ShortSignal']
        for col in required_columns:
            if col not in self.df.columns:
                print(f"Warning: Missing column {col}, cannot execute trades")
                return 'ERROR'
                
        # Check for NaN values
        for col in required_columns:
            if pd.isna(self.df.at[last_index, col]):
                print(f"Warning: NaN value in column {col}, cannot execute trades")
                return 'ERROR'
                
        # Get current price and ATR
        entry_price = self.df.at[last_index, 'close']
        atr = self.df.at[last_index, 'ATR']
        
        # Validate values
        if entry_price <= 0:
            print("Warning: Invalid entry price (must be positive)")
            return 'ERROR'
            
        # Calculate SOL amount based on budget
        sol_amount = budget / entry_price if budget > 0 else 0
        
        # Set budget to zero after trade execution
        # FIXME: Should probably keep track of remaining budget instead of setting to zero
        budget_after_trade = 0  
        
        # Initialize take profit, stop loss, and fee variables
        long_take_profit, long_stop_loss, buy_fee = None, None, None
        short_take_profit, short_stop_loss = None, None

        # Calculate levels for long signal
        if self.df.at[last_index, 'LongSignal']:
            long_take_profit, long_stop_loss, buy_fee = self.calculate_take_profit_stop_loss(
                entry_price, atr, tp_level, sl_level
            )
            print(f"TRADE: Long signal - TP: {long_take_profit}, SL: {long_stop_loss}, Fee: {buy_fee}")
           
        # Calculate levels for short signal
        if self.df.at[last_index, 'ShortSignal']:
            short_take_profit, short_stop_loss, buy_fee = self.calculate_take_profit_stop_loss(
                entry_price, atr, tp_level, sl_level
            )
            print(f"TRADE: Short signal - TP: {short_take_profit}, SL: {short_stop_loss}, Fee: {buy_fee}")
           
        # Insert trade data into Supabase
        trade_status = self.insert_trade_to_supabase(
            budget_after_trade, 
            sol_amount, 
            entry_price, 
            self.df.at[last_index, 'ShortSignal'],
            self.df.at[last_index, 'LongSignal'], 
            short_take_profit, 
            short_stop_loss,
            long_take_profit, 
            long_stop_loss, 
            buy_fee
        )
        
        return trade_status
        
    def insert_trade_to_supabase(self, budget: float, sol_amount: float, sol_price: float, short_signal: bool,
                                 long_signal: bool, short_take_profit: float, short_stop_loss: float,
                                 long_take_profit: float, long_stop_loss: float, buy_fee: float):
        """
        Insert trade data into the Supabase database.
        
        Args:
            budget (float): Remaining budget after trade
            sol_amount (float): Amount of SOL purchased/sold
            sol_price (float): Current SOL price
            short_signal (bool): Whether a short signal was generated
            long_signal (bool): Whether a long signal was generated
            short_take_profit (float): Take profit level for short trades
            short_stop_loss (float): Stop loss level for short trades
            long_take_profit (float): Take profit level for long trades
            long_stop_loss (float): Stop loss level for long trades
            buy_fee (float): Trading fee
            
        Returns:
            str: Trade status ('OPEN', 'CLOSED', or 'ERROR')
        """
        # Determine trade status based on signals
        trade_status = 'OPEN' if short_signal or long_signal else 'CLOSED'
        
        # TODO: Future enhancement - change to kafka producer
        
        try:
            # Insert trade data into Supabase
            response = self.supabase.table('trades').insert({
                "budget": budget,
                "sol_amount": sol_amount,
                "sol_price": sol_price,
                "short_signal": int(short_signal),  # Convert boolean to int (0/1)
                "long_signal": int(long_signal),    # Convert boolean to int (0/1)
                "entry_price": sol_price,
                "short_take_profit": short_take_profit,
                "short_stop_loss": short_stop_loss,
                "long_take_profit": long_take_profit,
                "long_stop_loss": long_stop_loss,
                "buy_fee": buy_fee,
                "status": trade_status
            }).execute()
            
            # Check for errors in response
            if hasattr(response, 'error') and response.error is not None:
                print(f"Failed to insert trade: {response.error}")
                return 'ERROR'
                
            print("Trade data successfully inserted into Supabase!")
            return trade_status
            
        except Exception as e:
            print(f"Error inserting trade data: {e}")
            return 'ERROR'


###############################################################################
# Main Function
# Purpose: Orchestrates the entire trading system
###############################################################################

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Get environment variables for Supabase connection
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    # Check if environment variables are available
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Missing SUPABASE_URL or SUPABASE_KEY environment variables")
        return

    # Define strategy parameters
    # These control the behavior of the technical indicators and risk management
    PERIOD_KERNEL_CHANEL = 20   # Period for Keltner Channels
    ATR_MULTIPLIER = 2          # ATR multiplier for Keltner Channels width
    CCI_PERIOD = 14             # Period for Commodity Channel Index
    PERIOD_BOLINGER_BAND = 20   # Period for Bollinger Bands
    STD_MULTIPLIER = 2          # Standard deviation multiplier for Bollinger Bands
    TP_LEVEL = 1.5              # Take profit level (ATR multiplier)
    SL_LEVEL = 0.7              # Stop loss level (ATR multiplier)
    CANDLES = 100               # Number of candles to retrieve
                                # This needs to be at least max(PERIOD_KERNEL_CHANEL, CCI_PERIOD, PERIOD_BOLINGER_BAND) + buffer

    try:
        # Initialize Supabase client for database operations
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase")
        
        # Define S3 bucket name
        bucket_name = "sol-price-storage-ctech-bot"
        
        # Create an instance of the S3JSONToDataFrame class
        s3_to_df = S3JSONToDataFrame(bucket_name)
        print(f"Fetching {CANDLES} candles from S3 bucket: {bucket_name}")
        
        # Get DataFrame with historical price data
        df = s3_to_df.create_dataframe(n=CANDLES)
        
        # Check if DataFrame is empty
        if df.empty:
            print("Error: No data retrieved from S3")
            return
            
        print(f"Successfully retrieved {len(df)} candles from S3")

        # Initialize the trading strategy with the price data
        strategy = TradingStrategy(df, supabase)
        
        # Calculate technical indicators
        print("Calculating Keltner Channels...")
        strategy.calculate_keltner_channels(period=PERIOD_KERNEL_CHANEL, atr_multiplier=ATR_MULTIPLIER)
        
        print("Calculating CCI...")
        strategy.calculate_cci(period=CCI_PERIOD)
        
        print("Calculating Bollinger Bands...")
        strategy.calculate_bollinger_bands(period=PERIOD_BOLINGER_BAND, std_multiplier=STD_MULTIPLIER)
        
        # Generate trading signals
        print("Generating trading signals...")
        strategy.generate_signals(PERIOD_KERNEL_CHANEL, CCI_PERIOD, PERIOD_BOLINGER_BAND)
        
        # Execute trades with a budget of 100 (currency units)
        print("Executing trades...")
        trade_status = strategy.execute_trades(100, tp_level=TP_LEVEL, sl_level=SL_LEVEL)

        print(f"Trade status: {trade_status}")
        
    except Exception as e:
        print(f"Error in main function: {e}")
        # FIXME: Consider adding more robust error handling or logging here

# Entry point of the script
if __name__ == "__main__":
    print("Starting Solana trading system...")
    main()