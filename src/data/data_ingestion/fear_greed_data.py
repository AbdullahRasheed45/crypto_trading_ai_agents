# src/data/data_ingestion/fear_greed_data.py
import requests
import pandas as pd
from datetime import datetime
from .base_data import BaseData

class FearGreedData(BaseData):
    def __init__(self, symbol: str, interval: str = "4h"):
        """Initialize FearGreedData.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Interval (default '4h')
        """
        super().__init__(symbol, interval)
        self.logger.info(f"FearGreedData initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_fear_and_greed_index(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch historical Fear and Greed Index.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: Fear and Greed Index data
        """
        try:
            endpoint = "https://api.alternative.me/v2/ticker/Bitcoin/"
            self.logger.info(f"Requesting Fear and Greed Index for {start_date} to {end_date}")
            response = requests.get(endpoint)
            response.raise_for_status()
            data = response.json()

            dates = pd.date_range(start=start_date, end=end_date, freq='4H')
            fear_greed = data['data']['1']['fear_greed']
            df = pd.DataFrame({
                'timestamp': dates,
                'fear_greed_index': [fear_greed] * len(dates)
            })
            self.logger.info(f"Fetched Fear and Greed Index (simulated) for {len(df)} periods")
            return df
        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"HTTP error during Fear and Greed fetch: {req_err}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.exception(f"Unexpected error while fetching Fear and Greed Index: {e}")
            return pd.DataFrame()

    def fetch_and_store(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch and store Fear and Greed Index data.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: Fear and Greed data
        """
        is_up_to_date, df = self._check_local_data('fear_greed')
        if is_up_to_date:
            self.logger.info(f"Using local Fear and Greed data with {len(df)} records")
            return df

        try:
            fear_greed = self.fetch_fear_and_greed_index(start_date, end_date)
            if not fear_greed.empty:
                self._save_data(fear_greed, 'fear_greed')
            return fear_greed
        except Exception as e:
            self.logger.exception(f"Error fetching and storing Fear and Greed data: {e}")
            raise