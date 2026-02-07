import os

from dotenv import load_dotenv
from typing import List

load_dotenv()

class Config:
    """ Configuration for stock data producer """

    # API Configuration
    POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
    ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

    # Kafka Configuration
    KAFKA_BOOSTRAP_SERVERS = os.getenv(
        'KAFKA_BOOSTRAP_SERVERS',
        'localhost:9902'
    )
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'stock-prices')

    # Stock symbols to track
    STOCK_SYMBOLS = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT,TSLA,AMZN').split(',')

     # Fetch interval (seconds)
    FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', 60))

    # Market hours (Eastern Time)
    MARKET_OPEN_HOUR = 9
    MARKET_OPEN_MINUTE = 30
    MARKET_CLOSE_HOUR = 16
    MARKET_CLOSE_MINUTE = 0
    
    # Producer settings
    PRODUCER_ACKS = 'all'
    PRODUCER_RETRIES = 3
    PRODUCER_COMPRESSION = 'gzip'

    @classmethod
    def validate(cls):
        """ Validate Required Configuration"""
        if not cls.POLYGON_API_KEY and not cls.ALPHA_VANTAGE_API_KEY:
            raise ValueError("At least one API key is required")
        
        if not cls.KAFKA_BOOSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOSTRAP_SERVERS required")
        
        return True

