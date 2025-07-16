import requests
import pandas as pd
import threading
import json
from websocket import WebSocketApp
from typing import Callable
from src.logger import get_logger

logger = get_logger(__name__)

class BinanceMarketData:
    def __init__(self, symbol: str, interval: str, base_url: str = "https://api.binance.com"):
        """Initialize BinanceMarketData with a trading pair symbol and interval.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT', 'ETHUSDT')
            interval (str): Kline interval (e.g., '15m', '1h')
            base_url (str): Binance API base URL
        """
        self.symbol = symbol.upper()
        self.interval = interval
        self.base_url = base_url
        self.ws_url = "wss://stream.binance.com:9443/ws"
        logger.info(f"BinanceMarketData instance initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_historical_ohlcv(self, symbol: str = None, interval: str = None, limit: int = 500) -> pd.DataFrame:
        """Fetch historical OHLCV data from Binance.
        
        Args:
            symbol (str, optional): Trading pair, defaults to instance symbol
            interval (str, optional): Kline interval, defaults to instance interval
            limit (int): Number of records to fetch
        """
        symbol = (symbol or self.symbol).upper()
        interval = interval or self.interval
        endpoint = f"{self.base_url}/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }

        try:
            logger.info(f"Requesting OHLCV data | Symbol: {symbol}, Interval: {interval}, Limit: {limit}")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            logger.debug("Raw OHLCV data received. Parsing into DataFrame.")
            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "num_trades",
                "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
            ])

            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
            df = df[["open_time", "open", "high", "low", "close", "volume", "close_time"]]
            df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

            logger.info(f"Successfully fetched and processed {len(df)} OHLCV records for {symbol}")
            return df

        except requests.exceptions.RequestException as req_err:
            logger.error(f"HTTP error during OHLCV fetch: {req_err}")
            raise

        except Exception as e:
            logger.exception(f"Unexpected error while fetching OHLCV data: {e}")
            raise

    def stream_live_price(self, on_message_callback: Callable[[dict], None], symbol: str = None):
        """Stream live price data for a symbol.
        
        Args:
            on_message_callback (Callable): Callback to handle trade data
            symbol (str, optional): Trading pair, defaults to instance symbol
        
        Returns:
            WebSocketApp: The WebSocket instance for control
        """
        symbol = (symbol or self.symbol).lower()
        stream = f"{symbol}@trade"

        def on_message(ws, message):
            try:
                data = json.loads(message)
                trade_data = {
                    "price": float(data["p"]),
                    "quantity": float(data["q"]),
                    "timestamp": pd.to_datetime(data["T"], unit="ms")
                }
                logger.debug(f"Received trade: {trade_data}")
                on_message_callback(trade_data)
            except Exception as e:
                logger.exception("Error in WebSocket message handler")

        def on_error(ws, error):
            logger.error(f"WebSocket error: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"WebSocket closed | Code: {close_status_code}, Message: {close_msg}")

        logger.info(f"Opening WebSocket connection for stream: {stream}")
        ws = WebSocketApp(
            f"{self.ws_url}/{stream}",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        thread = threading.Thread(target=ws.run_forever)
        thread.daemon = True
        thread.start()
        self.ws = ws  # Store WebSocket instance
        return ws  # Return WebSocketApp for control