# src/data/data_ingestion/technical_data.py
import pandas as pd
import talib
from .base_data import BaseData

class TechnicalData(BaseData):
    def __init__(self, symbol: str, interval: str = "4h"):
        """Initialize TechnicalData.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Interval (default '4h')
        """
        super().__init__(symbol, interval)
        self.logger.info(f"TechnicalData initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def compute_technical_indicators(self, ohlcv_data: pd.DataFrame) -> pd.DataFrame:
        """Compute technical indicators for the OHLCV data.
        
        Args:
            ohlcv_data (pd.DataFrame): OHLCV data
        
        Returns:
            pd.DataFrame: Data with technical indicators
        """
        try:
            if ohlcv_data.empty:
                self.logger.warning("No OHLCV data provided for technical indicators")
                return pd.DataFrame()

            df = ohlcv_data.copy()
            df['timestamp'] = df['open_time']
            df['sma50'] = talib.SMA(df['close'], timeperiod=50)
            df['sma200'] = talib.SMA(df['close'], timeperiod=200)
            df['rsi'] = talib.RSI(df['close'], timeperiod=14)
            df['macd'], df['macd_signal'], _ = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
            df['upper_band'], df['middle_band'], df['lower_band'] = talib.BBANDS(df['close'], timeperiod=20)
            df = df[['timestamp', 'sma50', 'sma200', 'rsi', 'macd', 'macd_signal', 'upper_band', 'middle_band', 'lower_band']]
            self.logger.info(f"Computed technical indicators for {len(df)} records")
            return df
        except Exception as e:
            self.logger.exception(f"Error computing technical indicators: {e}")
            return pd.DataFrame()

    def fetch_and_store(self, ohlcv_data: pd.DataFrame) -> pd.DataFrame:
        """Compute and store technical indicators.
        
        Args:
            ohlcv_data (pd.DataFrame): OHLCV data
        
        Returns:
            pd.DataFrame: Technical indicators
        """
        is_up_to_date, df = self._check_local_data('technical')
        if is_up_to_date:
            self.logger.info(f"Using local technical data with {len(df)} records")
            return df

        try:
            technical_data = self.compute_technical_indicators(ohlcv_data)
            if not technical_data.empty:
                self._save_data(technical_data, 'technical')
            return technical_data
        except Exception as e:
            self.logger.exception(f"Error computing and storing technical data: {e}")
            raise