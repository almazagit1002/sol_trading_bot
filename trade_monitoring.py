
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
class TradeMonitor:
    def __init__(self, supabase, get_current_price):
        """
        Initialize Trade Monitor.
        :param supabase: Supabase client instance
        :param get_current_price: Function to fetch the current SOL price
        """
        self.supabase = supabase
        self.get_current_price = get_current_price

    def check_and_close_trades(self):
        """Check open trades and close if TP or SL is hit."""
        try:
            # Fetch all OPEN trades from Supabase
            response = self.supabase.table('trades').select('*').eq('status', 'OPEN').execute()
            
            if not response.data:
                print("No open trades found.")
                return

            open_trades = response.data

            for trade in open_trades:
                trade_id = trade['id']
                entry_price = trade['entry_price']
                short_tp = trade['short_take_profit']
                short_sl = trade['short_stop_loss']
                long_tp = trade['long_take_profit']
                long_sl = trade['long_stop_loss']
                
                # Get current SOL price
                current_price = self.get_current_price()
                
                close_trade = False
                reason = ""

                # Check Short Trade Conditions
                if trade['short_signal']:
                    if current_price <= short_tp:
                        reason = "Short TP Hit"
                        close_trade = True
                    elif current_price >= short_sl:
                        reason = "Short SL Hit"
                        close_trade = True

                # Check Long Trade Conditions
                if trade['long_signal']:
                    if current_price >= long_tp:
                        reason = "Long TP Hit"
                        close_trade = True
                    elif current_price <= long_sl:
                        reason = "Long SL Hit"
                        close_trade = True

                # Close trade if TP or SL is hit
                if close_trade:
                    self.close_trade(trade_id, reason)

        except Exception as e:
            print(f"Error checking trades: {e}")

    def close_trade(self, trade_id, reason):
        """Close the trade and update status in Supabase."""
        try:
            response = self.supabase.table('trades').update({'status': 'CLOSED'}).eq('id', trade_id).execute()
            if response.data:
                print(f"✅ Trade {trade_id} closed: {reason}")
            else:
                print(f"⚠️ Failed to close trade {trade_id}")
        except Exception as e:
            print(f"Error closing trade {trade_id}: {e}")

    def start_monitoring(self, interval=1):
        """Start background job to check trades every 'interval' minutes."""
        print("🚀 Trade Monitoring Started!")


      

 