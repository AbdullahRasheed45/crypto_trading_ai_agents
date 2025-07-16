# src/data/data_ingestion/fundamental_data.py
import requests
import pandas as pd
from datetime import datetime
from .base_data import BaseData

class FundamentalData(BaseData):
    def __init__(self, symbol: str, interval: str = "4h"):
        """Initialize FundamentalData.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Interval (default '4h')
        """
        super().__init__(symbol, interval)
        self.logger.info(f"FundamentalData initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_fundamental_data(self, coin: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch fundamental data from CoinGecko.
        
        Args:
            coin (str): Coin name (e.g., 'bitcoin', 'ethereum')
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: Fundamental data
        """
        try:
            endpoint = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
            days = (end_date - start_date).days + 1
            params = {"vs_currency": "usd", "days": days, "interval": "daily"}
            self.logger.info(f"Requesting fundamental data for {coin} from {start_date} to {end_date}")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame({
                'timestamp': pd.to_datetime([d[0] for d in data['prices']], unit='ms'),
                'market_cap': [d[1] for d in data['market_caps']],
                'total_volume': [d[1] for d in data['total_volumes']]
            })
            df = df.set_index('timestamp').resample('4H').ffill().reset_index()
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
            self.logger.info(f"Fetched {len(df)} fundamental records for {coin}")
            return df
        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"HTTP error during fundamental data fetch: {req_err}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.exception(f"Unexpected error while fetching fundamental data: {e}")
            return pd.DataFrame()

    def fetch_and_store(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch and store fundamental data.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: Fundamental data
        """
        is_up_to_date, df = self._check_local_data('fundamentals')
        if is_up_to_date:
            self.logger.info(f"Using local fundamental data with {len(df)} records")
            return df

        try:
            coin_name = self.symbol.replace('USDT', '').lower()
            fundamental_data = self.fetch_fundamental_data(coin_name, start_date, end_date)
            if not fundamental_data.empty:
                self._save_data(fundamental_data, 'fundamentals')
            return fundamental_data
        except Exception as e:
            self.logger.exception(f"Error fetching and storing fundamental data: {e}")
            raise