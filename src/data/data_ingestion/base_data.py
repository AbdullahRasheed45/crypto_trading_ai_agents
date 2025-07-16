# src/data/data_ingestion/base_data.py
import pandas as pd
import os
from datetime import datetime, timedelta
from src.utils.logger import get_logger
from src.config_loader import load_config

class BaseData:
    def __init__(self, symbol: str, interval: str = "4h"):
        """Initialize BaseData with symbol and interval.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Kline interval (default '4h')
        """
        self.symbol = symbol.upper()
        self.interval = interval
        config = load_config()
        self.data_dir = config['data_paths']['raw']
        os.makedirs(self.data_dir, exist_ok=True)
        self.logger = get_logger(__name__)

    def _check_local_data(self, data_type: str) -> tuple[bool, pd.DataFrame]:
        """Check if local data exists and is up-to-date for the given data type.
        
        Args:
            data_type (str): Type of data (e.g., 'ohlcv', 'orderbook')
        
        Returns:
            tuple[bool, pd.DataFrame]: (is_up_to_date, data)
        """
        file_path = os.path.join(self.data_dir, f"{self.symbol}_{self.interval}_{data_type}.csv")
        if not os.path.exists(file_path):
            self.logger.info(f"No local data found for {data_type} at {file_path}")
            return False, pd.DataFrame()

        df = pd.read_csv(file_path)
        if df.empty:
            self.logger.info(f"Empty local data for {data_type} at {file_path}")
            return False, pd.DataFrame()

        latest_timestamp = pd.to_datetime(df['open_time' if data_type == 'ohlcv' else 'timestamp'].max())
        if datetime.now() - latest_timestamp < timedelta(hours=4):
            self.logger.info(f"Local {data_type} data is up-to-date at {file_path}")
            return True, df
        self.logger.info(f"Local {data_type} data is outdated at {file_path}")
        return False, df

    def _save_data(self, df: pd.DataFrame, data_type: str):
        """Save DataFrame to CSV.
        
        Args:
            df (pd.DataFrame): Data to save
            data_type (str): Type of data (e.g., 'ohlcv')
        """
        if not df.empty:
            file_path = os.path.join(self.data_dir, f"{self.symbol}_{self.interval}_{data_type}.csv")
            df.to_csv(file_path, index=False)
            self.logger.info(f"Saved {data_type} data to {file_path} | Records: {len(df)}")
        else:
            self.logger.warning(f"No {data_type} data to save for {self.symbol}")