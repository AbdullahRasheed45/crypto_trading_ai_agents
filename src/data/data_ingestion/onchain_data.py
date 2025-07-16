# src/data/data_ingestion/onchain_data.py
import requests
import pandas as pd
from datetime import datetime
from .base_data import BaseData

class OnChainData(BaseData):
    def __init__(self, symbol: str, interval: str = "4h", base_url: str = "https://community-api.coinmetrics.io/v4"):
        """Initialize OnChainData.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Interval (default '4h')
            base_url (str): CoinMetrics API base URL
        """
        super().__init__(symbol, interval)
        self.base_url = base_url
        self.coin = 'btc' if 'BTC' in symbol.upper() else symbol.replace('USDT', '').lower()
        self.logger.info(f"OnChainData initialized | Symbol: {self.symbol}, Coin: {self.coin}, Interval: {self.interval}")

    def fetch_onchain_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch on-chain data from CoinMetrics.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: On-chain data (tx_volume, active_addresses)
        """
        try:
            endpoint = f"{self.base_url}/timeseries/asset-metrics"
            params = {
                "assets": self.coin,
                "metrics": "TxCnt,AdrActCnt",
                "start_time": start_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "end_time": end_date.strftime('%Y-%m-%dT%H:%M:%S'),
                "frequency": "4h"
            }
            self.logger.info(f"Requesting on-chain data for {self.coin} from {start_date} to {end_date}")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()['data']
            
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['time'])
            df = df.rename(columns={'TxCnt': 'tx_volume', 'AdrActCnt': 'active_addresses'})
            df = df[['timestamp', 'tx_volume', 'active_addresses']].dropna()
            df[['tx_volume', 'active_addresses']] = df[['tx_volume', 'active_addresses']].astype(float)
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
            df = df.set_index('timestamp').resample('4H').ffill().reset_index()
            
            self.logger.info(f"Fetched {len(df)} on-chain records for {self.coin}")
            return df
        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"HTTP error during on-chain data fetch: {req_err}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.exception(f"Unexpected error while fetching on-chain data: {e}")
            return pd.DataFrame()

    def fetch_and_store(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch and store on-chain data.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: On-chain data
        """
        is_up_to_date, df = self._check_local_data('onchain')
        if is_up_to_date:
            self.logger.info(f"Using local on-chain data with {len(df)} records")
            return df

        try:
            onchain_data = self.fetch_onchain_data(start_date, end_date)
            if not onchain_data.empty:
                self._save_data(onchain_data, 'onchain')
            return onchain_data
        except Exception as e:
            self.logger.exception(f"Error fetching and storing on-chain data: {e}")
            raise