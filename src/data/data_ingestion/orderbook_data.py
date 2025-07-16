# src/data/data_ingestion/orderbook_data.py
import requests
import pandas as pd
from .base_data import BaseData

class OrderBookData(BaseData):
    def __init__(self, symbol: str, interval: str = "4h", base_url: str = "https://api.binance.com"):
        """Initialize OrderBookData.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Interval (default '4h')
            base_url (str): Binance API base URL
        """
        super().__init__(symbol, interval)
        self.base_url = base_url
        self.logger.info(f"OrderBookData initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_order_book_snapshot(self, limit: int = 100, timestamp: int = None) -> pd.DataFrame:
        """Fetch an order book snapshot from Binance.
        
        Args:
            limit (int): Number of order book entries (max 5000)
            timestamp (int, optional): Unix timestamp (ms) for alignment
        
        Returns:
            pd.DataFrame: Order book data (aggregated features)
        """
        endpoint = f"{self.base_url}/api/v3/depth"
        params = {
            "symbol": self.symbol,
            "limit": limit,
        }

        try:
            self.logger.info(f"Requesting order book snapshot | Symbol: {self.symbol}, Limit: {limit}")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            bids = pd.DataFrame(data['bids'], columns=['bid_price', 'bid_quantity'], dtype=float)
            asks = pd.DataFrame(data['asks'], columns=['ask_price', 'ask_quantity'], dtype=float)
            
            order_book = pd.DataFrame({
                'timestamp': [pd.to_datetime(timestamp or data['lastUpdateId'], unit='ms')],
                'bid_price_top': [bids['bid_price'].iloc[0] if not bids.empty else None],
                'bid_quantity_top': [bids['bid_quantity'].iloc[0] if not bids.empty else None],
                'ask_price_top': [asks['ask_price'].iloc[0] if not asks.empty else None],
                'ask_quantity_top': [asks['ask_quantity'].iloc[0] if not asks.empty else None],
                'bid_ask_spread': [(asks['ask_price'].iloc[0] - bids['bid_price'].iloc[0]) if not bids.empty and not asks.empty else None],
                'total_bid_volume': [bids['bid_quantity'].sum() if not bids.empty else 0],
                'total_ask_volume': [asks['ask_quantity'].sum() if not asks.empty else 0]
            })
            order_book['symbol'] = self.symbol

            self.logger.info(f"Fetched order book snapshot for {self.symbol}")
            return order_book

        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"HTTP error during order book fetch: {req_err}")
            raise
        except Exception as e:
            self.logger.exception(f"Unexpected error while fetching order book data: {e}")
            raise

    def fetch_and_store(self, ohlcv_data: pd.DataFrame) -> pd.DataFrame:
        """Fetch and store order book data aligned with OHLCV timestamps.
        
        Args:
            ohlcv_data (pd.DataFrame): OHLCV data for timestamp alignment
        
        Returns:
            pd.DataFrame: Order book data
        """
        is_up_to_date, df = self._check_local_data('orderbook')
        if is_up_to_date:
            self.logger.info(f"Using local order book data with {len(df)} records")
            return df

        if ohlcv_data.empty:
            self.logger.warning("No OHLCV data provided for order book alignment")
            return pd.DataFrame()

        try:
            all_order_books = []
            for timestamp in ohlcv_data['open_time']:
                order_book = self.fetch_order_book_snapshot(timestamp=int(timestamp.timestamp() * 1000))
                all_order_books.append(order_book)
            if all_order_books:
                full_order_book = pd.concat(all_order_books, ignore_index=True)
                self._save_data(full_order_book, 'orderbook')
                return full_order_book
            return pd.DataFrame()
        except Exception as e:
            self.logger.exception(f"Error fetching and storing order book data: {e}")
            raise