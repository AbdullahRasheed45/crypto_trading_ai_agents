# src/data/data_ingestion/ohlcv_data.py
import requests
import pandas as pd
from datetime import datetime
from .base_data import BaseData

class OHLCVData(BaseData):
    def __init__(self, symbol: str, interval: str = "4h", base_url: str = "https://api.binance.com"):
        """Initialize OHLCVData.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Kline interval (default '4h')
            base_url (str): Binance API base URL
        """
        super().__init__(symbol, interval)
        self.base_url = base_url
        self.logger.info(f"OHLCVData initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def _get_symbol_listing_date(self) -> int:
        """Get the approximate listing date of the symbol from Binance exchange info.
        
        Returns:
            int: Unix timestamp (ms) of the earliest available data.
        """
        try:
            endpoint = f"{self.base_url}/api/v3/exchangeInfo"
            response = requests.get(endpoint)
            response.raise_for_status()
            data = response.json()
            for symbol_info in data['symbols']:
                if symbol_info['symbol'] == self.symbol:
                    klines = self.fetch_historical_ohlcv(limit=1, start_time=0)
                    if klines.empty:
                        self.logger.error(f"No historical data available for {self.symbol}")
                        raise ValueError(f"No historical data for {self.symbol}")
                    return int(klines['open_time'].iloc[0].timestamp() * 1000)
            self.logger.error(f"Symbol {self.symbol} not found in exchange info")
            raise ValueError(f"Symbol {self.symbol} not found")
        except Exception as e:
            self.logger.exception(f"Error fetching listing date for {self.symbol}: {e}")
            raise

    def fetch_historical_ohlcv(self, limit: int = 1000, start_time: int = None) -> pd.DataFrame:
        """Fetch historical OHLCV data from Binance.
        
        Args:
            limit (int): Number of records to fetch per request (max 1000)
            start_time (int, optional): Unix timestamp (ms) to start from
        
        Returns:
            pd.DataFrame: OHLCV data
        """
        endpoint = f"{self.base_url}/api/v3/klines"
        params = {
            "symbol": self.symbol,
            "interval": self.interval,
            "limit": limit,
        }
        if start_time:
            params["startTime"] = start_time

        try:
            self.logger.info(f"Requesting OHLCV data | Symbol: {self.symbol}, Interval: {self.interval}, Limit: {limit}")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            if not data:
                self.logger.warning(f"No OHLCV data returned for {self.symbol}")
                return pd.DataFrame()

            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "num_trades",
                "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
            ])

            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
            df = df[["open_time", "open", "high", "low", "close", "volume", "close_time"]]
            df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

            self.logger.info(f"Fetched {len(df)} OHLCV records for {self.symbol}")
            return df

        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"HTTP error during OHLCV fetch: {req_err}")
            raise
        except Exception as e:
            self.logger.exception(f"Unexpected error while fetching OHLCV data: {e}")
            raise

    def fetch_and_store(self) -> pd.DataFrame:
        """Fetch and store OHLCV data if missing or outdated.
        
        Returns:
            pd.DataFrame: OHLCV data
        """
        is_up_to_date, df = self._check_local_data('ohlcv')
        if is_up_to_date:
            self.logger.info(f"Using local OHLCV data with {len(df)} records")
            return df

        try:
            start_time = self._get_symbol_listing_date()
            all_ohlcv = []
            current_time = start_time
            while True:
                df_ohlcv = self.fetch_historical_ohlcv(limit=1000, start_time=current_time)
                if df_ohlcv.empty:
                    break
                all_ohlcv.append(df_ohlcv)
                current_time = int(df_ohlcv['close_time'].iloc[-1].timestamp() * 1000) + 1
                if len(df_ohlcv) < 1000:
                    break

            if all_ohlcv:
                full_ohlcv = pd.concat(all_ohlcv, ignore_index=True)
                self._save_data(full_ohlcv, 'ohlcv')
                return full_ohlcv
            return pd.DataFrame()
        except Exception as e:
            self.logger.exception(f"Error fetching and storing OHLCV data: {e}")
            raise