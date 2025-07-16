# tests/test_market_data.py
from src.data.data_ingestion.market_data import BinanceMarketData
import pandas as pd
import time

def print_live(msg):
    print(f"Live: {msg['timestamp']} | Price: {msg['price']} | Qty: {msg['quantity']}")

def test_binance_market_data():
    bmd = BinanceMarketData(symbol='BTCUSDT', interval='15m')
    
    print("Fetching Historical Data...")
    df = bmd.fetch_historical_ohlcv()
    assert isinstance(df, pd.DataFrame), "Expected DataFrame from fetch_historical_ohlcv"
    assert len(df) > 0, "Expected non-empty DataFrame"
    print(df.tail())

    print("Streaming Live Price Data...")
    thread = bmd.stream_live_price(on_message_callback=print_live)
    time.sleep(30)  # Let it stream for 30 seconds
    # Note: No direct way to close WebSocket thread gracefully; let it run as daemon

if __name__ == "__main__":
    test_binance_market_data()