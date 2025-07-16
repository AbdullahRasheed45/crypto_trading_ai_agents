# src/data/data_ingestion/data_manager.py
import pandas as pd
from datetime import datetime
from .ohlcv_data import OHLCVData
from .orderbook_data import OrderBookData
from .technical_data import TechnicalData
from .sentiment_data import SentimentData
from .fear_greed_data import FearGreedData
from .fundamental_data import FundamentalData
from src.utils.logger import get_logger
from src.config_loader import load_config
import os

class DataManager:
    def __init__(self, symbol: str, interval: str = "4h"):
        """Initialize DataManager.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Interval (default '4h')
        """
        self.symbol = symbol.upper()
        self.interval = interval
        config = load_config()
        self.data_dir = config['data_paths']['raw']
        self.logger = get_logger(__name__)
        self.ohlcv_fetcher = OHLCVData(symbol, interval)
        self.orderbook_fetcher = OrderBookData(symbol, interval)
        self.technical_fetcher = TechnicalData(symbol, interval)
        self.sentiment_fetcher = SentimentData(symbol, interval)
        self.fear_greed_fetcher = FearGreedData(symbol, interval)
        self.fundamental_fetcher = FundamentalData(symbol, interval)
        self.logger.info(f"DataManager initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_and_store_all(self):
        """Fetch and store all data types."""
        try:
            # Fetch OHLCV first
            ohlcv_data = self.ohlcv_fetcher.fetch_and_store()
            if ohlcv_data.empty:
                self.logger.warning("No OHLCV data available, skipping other data types")
                return

            # Get date range from OHLCV
            start_date = ohlcv_data['open_time'].min()
            end_date = datetime.now()

            # Fetch other data types
            self.orderbook_fetcher.fetch_and_store(ohlcv_data)
            self.technical_fetcher.fetch_and_store(ohlcv_data)
            self.sentiment_fetcher.fetch_and_store(start_date, end_date)
            self.fear_greed_fetcher.fetch_and_store(start_date, end_date)
            self.fundamental_fetcher.fetch_and_store(start_date, end_date)

            self.logger.info(f"Completed fetching and storing all data for {self.symbol}")
        except Exception as e:
            self.logger.exception(f"Error in fetch_and_store_all: {e}")
            raise

    def merge_data_for_training(self) -> pd.DataFrame:
        """Merge all data types into a single DataFrame.
        
        Returns:
            pd.DataFrame: Combined data
        """
        try:
            data_types = ['ohlcv', 'orderbook', 'technical', 'sentiment', 'fear_greed', 'fundamentals']
            combined_data = None
            for data_type in data_types:
                file_path = os.path.join(self.data_dir, f"{self.symbol}_{self.interval}_{data_type}.csv")
                if not os.path.exists(file_path):
                    self.logger.warning(f"No {data_type} data found at {file_path}")
                    continue

                df = pd.read_csv(file_path)
                key = 'open_time' if data_type == 'ohlcv' else 'timestamp'
                df[key] = pd.to_datetime(df[key])
                if combined_data is None:
                    combined_data = df.rename(columns={key: 'open_time'})
                else:
                    combined_data = pd.merge_asof(
                        combined_data.sort_values('open_time'),
                        df.sort_values(key),
                        left_on='open_time',
                        right_on=key,
                        direction='nearest',
                        tolerance=pd.Timedelta(hours=4)
                    )
                    combined_data = combined_data.drop(columns=[key])

            if combined_data is not None:
                combined_path = os.path.join(self.data_dir, f"{self.symbol}_{self.interval}_combined.csv")
                combined_data.to_csv(combined_path, index=False)
                self.logger.info(f"Saved combined data to {combined_path} | Records: {len(combined_data)}")
                return combined_data
            else:
                self.logger.warning(f"No data available to merge for {self.symbol}")
                return pd.DataFrame()
        except Exception as e:
            self.logger.exception(f"Error merging data: {e}")
            return pd.DataFrame()