# src/data/data_ingestion/sentiment_data.py
import pandas as pd
from datetime import datetime
from .base_data import BaseData
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

nltk.download('vader_lexicon', quiet=True)

class SentimentData(BaseData):
    def __init__(self, symbol: str, interval: str = "4h"):
        """Initialize SentimentData.
        
        Args:
            symbol (str): Trading pair (e.g., 'BTCUSDT')
            interval (str): Interval (default '4h')
        """
        super().__init__(symbol, interval)
        self.sid = SentimentIntensityAnalyzer()
        self.logger.info(f"SentimentData initialized | Symbol: {self.symbol}, Interval: {self.interval}")

    def fetch_social_sentiment(self, coin: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch sentiment from X posts for the given coin and date range.
        
        Args:
            coin (str): Coin name (e.g., 'Bitcoin', 'Ethereum')
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: Sentiment scores
        """
        try:
            # Simulate X post fetching (replace with actual X API)
            self.logger.info(f"Simulating sentiment analysis for {coin} from {start_date} to {end_date}")
            dates = pd.date_range(start=start_date, end=end_date, freq='4H')
            sentiment_scores = [self.sid.polarity_scores(f"{coin} price is stable")['compound'] for _ in dates]
            df = pd.DataFrame({
                'timestamp': dates,
                'social_sentiment': sentiment_scores
            })
            self.logger.info(f"Simulated {len(df)} sentiment scores for {coin}")
            return df
        except Exception as e:
            self.logger.exception(f"Error fetching social sentiment: {e}")
            return pd.DataFrame()

    def fetch_and_store(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch and store sentiment data.
        
        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
        
        Returns:
            pd.DataFrame: Sentiment data
        """
        is_up_to_date, df = self._check_local_data('sentiment')
        if is_up_to_date:
            self.logger.info(f"Using local sentiment data with {len(df)} records")
            return df

        try:
            coin_name = self.symbol.replace('USDT', '').lower()
            sentiment_data = self.fetch_social_sentiment(coin_name, start_date, end_date)
            if not sentiment_data.empty:
                self._save_data(sentiment_data, 'sentiment')
            return sentiment_data
        except Exception as e:
            self.logger.exception(f"Error fetching and storing sentiment data: {e}")
            raise