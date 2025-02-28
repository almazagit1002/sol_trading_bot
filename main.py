import os

from dotenv import load_dotenv
from supabase import create_client

from candle_agregator import DfCandleAgregator
from trade_strategy import TradingStrategy
from trade_monitoring import TradeMonitor

# Load environment variables
load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
FILE_PATH = os.getenv("FILE_PATH")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

#startegy parameters
PERIOD_KERNEL_CHANEL = 20
ATR_MULTIPLIER = 2
CCI_PERIOD = 14
PERIOD_BOLINGER_BAND = 20
STD_MULTIPLIER = 2
TP_LEVEL = 1.5
SL_LEVEL = 0.7


supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
def main():
    """Main function to fetch and process candle data from S3."""
    if not BUCKET_NAME or not FILE_PATH:
        print("❌ Missing required environment variables. Check .env file.")
        return

    # Initialize the Candle Aggregator
    aggregator = DfCandleAgregator(bucket_name=BUCKET_NAME, prefix=FILE_PATH)

    # Fetch and process latest candle data
    df = aggregator.fetch_latest_files(60)
    ohlc = aggregator.aggregate_candles(df)

    # Initialize the strategy
    strategy = TradingStrategy(ohlc,supabase)
    # Calculate the indicators
    strategy.calculate_keltner_channels(period=PERIOD_KERNEL_CHANEL, atr_multiplier=ATR_MULTIPLIER)
    strategy.calculate_cci(period=CCI_PERIOD )
    strategy.calculate_bollinger_bands(period=PERIOD_BOLINGER_BAND, std_multiplier=STD_MULTIPLIER)
    # Generate buy/sell signals
    strategy.generate_signals()
    # Execute trades
    trade_status = strategy.execute_trades(100,  tp_level=TP_LEVEL,sl_level=SL_LEVEL)
    #send to kafka 
    print(trade_status)

    # Initialize TradeMonitor
    if trade_status == "OPEN":
        trade_monitor = TradeMonitor(supabase)
  



if __name__ == "__main__":
    main()
