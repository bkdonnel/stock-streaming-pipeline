import requests
import json
from datetime import datetime, time
import pytz
from typing import Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def is_market_open() -> bool:
    """Check if US stock market is currently open"""
    est = pytz.timezone('US/Eastern')
    now = datetime.now(est)
    
    # Check if weekday (Monday=0, Sunday=6)
    if now.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Check if within market hours (9:30 AM - 4:00 PM EST)
    market_open = time(9, 30)
    market_close = time(16, 0)
    current_time = now.time()
    
    return market_open <= current_time <= market_close

def fetch_stock_data_polygon(symbol: str, api_key: str) -> Optional[Dict]:
    """
    Fetch real-time stock data from Polygon.io
    
    Args:
        symbol: Stock ticker symbol
        api_key: Polygon.io API key
        
    Returns:
        Dictionary with stock data or None if failed
    """
    try:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
        params = {'apiKey': api_key}
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('results') and len(data['results']) > 0:
            result = data['results'][0]
            return {
                'symbol': symbol,
                'timestamp': datetime.fromtimestamp(result['t'] / 1000).isoformat(),
                'open': float(result['o']),
                'high': float(result['h']),
                'low': float(result['l']),
                'close': float(result['c']),
                'volume': int(result['v']),
                'vwap': float(result.get('vw', 0)),
                'source': 'polygon'
            }
        
        logger.warning(f"No results returned for {symbol} from Polygon")
        return None
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {symbol} from Polygon: {e}")
        return None
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error parsing data for {symbol} from Polygon: {e}")
        return None

def fetch_stock_data_alpha_vantage(symbol: str, api_key: str) -> Optional[Dict]:
    """
    Fetch real-time stock data from Alpha Vantage (backup)
    
    Args:
        symbol: Stock ticker symbol
        api_key: Alpha Vantage API key
        
    Returns:
        Dictionary with stock data or None if failed
    """
    try:
        url = "https://www.alphavantage.co/query"
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol,
            'apikey': api_key
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'Global Quote' in data and data['Global Quote']:
            quote = data['Global Quote']
            return {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'open': float(quote['02. open']),
                'high': float(quote['03. high']),
                'low': float(quote['04. low']),
                'close': float(quote['05. price']),
                'volume': int(quote['06. volume']),
                'vwap': 0.0,  # Not available in Alpha Vantage
                'source': 'alpha_vantage'
            }
        
        logger.warning(f"No results returned for {symbol} from Alpha Vantage")
        return None
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {symbol} from Alpha Vantage: {e}")
        return None
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error parsing data for {symbol} from Alpha Vantage: {e}")
        return None

def validate_stock_data(data: Dict) -> bool:
    """
    Validate stock data before sending to Kafka
    
    Args:
        data: Stock data dictionary
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
    
    # Check all required fields exist
    if not all(field in data for field in required_fields):
        logger.error(f"Missing required fields in data: {data}")
        return False
    
    # Validate price data is positive
    price_fields = ['open', 'high', 'low', 'close']
    if any(data[field] <= 0 for field in price_fields):
        logger.error(f"Invalid price data (must be positive): {data}")
        return False
    
    # Validate high >= low
    if data['high'] < data['low']:
        logger.error(f"High price less than low price: {data}")
        return False
    
    # Validate volume is non-negative
    if data['volume'] < 0:
        logger.error(f"Invalid volume (must be non-negative): {data}")
        return False
    
    return True

def format_stock_message(data: Dict) -> str:
    """
    Format stock data for logging
    
    Args:
        data: Stock data dictionary
        
    Returns:
        Formatted string
    """
    return (
        f"{data['symbol']} | "
        f"Close: ${data['close']:.2f} | "
        f"Volume: {data['volume']:,} | "
        f"Time: {data['timestamp']}"
    )