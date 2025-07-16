# src/data/data_ingestion/macro_data.py
import pandas as pd
import yfinance as yf
from datetime import datetime
from .base_data import BaseData

class MacroData(BaseData):
    def __init__(self, symbol: str, interval: str = "4h"):
        """Initialize MacroData.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Interval (default '4h')
        """
        super().__init__(symbol, interval)
        self.logger.info(f"MacroData initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_macro_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch macro-economic data (S&P 500 returns).
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: Macro data (sp500_returns)
        """
        try:
            self.logger.info(f"Requesting macro data (S&P 500) from {start_date} to {end_date}")
            sp500 = yf.Ticker("^GSPC")
            df = sp500.history(start=start_date, end=end_date + pd.Timedelta(days=1), interval="1d")
            df = df[['Close']].reset_index()
            df['timestamp'] = pd.to_datetime(df['Date'])
            df['sp500_returns'] = df['Close'].pct_change().fillna(0)
            df = df[['timestamp', 'sp500_returns']]
            df = df.set_index('timestamp').resample('4H').ffill().reset_index()
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
            
            self.logger.info(f"Fetched {len(df)} macro records")
            return df
        except Exception as e:
            self.logger.exception(f"Error fetching macro data: {e}")
            return pd.DataFrame()

    def fetch_and_store(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch and store macro data.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: Macro data
        """
        is_up_to_date, df = self._check_local_data('macro')
        if is_up_to_date:
            self.logger.info(f"Using local macro data with {len(df)} records")
            return df

        try:
            macro_data = self.fetch_macro_data(start_date, end_date)
            if not macro_data.empty:
                self._save_data(macro_data, 'macro')
            return macro_data
        except Exception as e:
            self.logger.exception(f"Error fetching and storing macro data: {e}")
            raise