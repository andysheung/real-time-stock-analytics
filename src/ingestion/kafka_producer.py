#!/usr/bin/env python3
"""
Kafka Producer for Stock Data Ingestion

Fetches real-time stock data from Yahoo Finance and publishes to Kafka topics.
"""

import json
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockDataProducer:
    """Produces stock data to Kafka topics."""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        stock_symbols: List[str] = None,
        fetch_interval: int = 5
    ):
        """
        Initialize the Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker address
            stock_symbols: List of stock symbols to fetch
            fetch_interval: Seconds between data fetches
        """
        self.bootstrap_servers = bootstrap_servers
        self.stock_symbols = stock_symbols or ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        self.fetch_interval = fetch_interval
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3
        )
        
        logger.info(f"Initialized Kafka producer for {bootstrap_servers}")
    
    def fetch_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch real-time stock data from Yahoo Finance.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Dictionary containing stock data
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Get current price data
            hist = ticker.history(period='1d', interval='1m')
            
            if hist.empty:
                logger.warning(f"No data available for {symbol}")
                return None
            
            latest = hist.iloc[-1]
            
            data = {
                'symbol': symbol,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'price': float(latest['Close']),
                'open': float(latest['Open']),
                'high': float(latest['High']),
                'low': float(latest['Low']),
                'volume': int(latest['Volume']),
                'previous_close': info.get('previousClose', None),
                'market_cap': info.get('marketCap', None),
                'currency': info.get('currency', 'USD')
            }
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def publish_to_kafka(self, data: Dict[str, Any], topic: str):
        """
        Publish data to Kafka topic.
        
        Args:
            data: Data dictionary to publish
            topic: Kafka topic name
        """
        if data is None:
            return
        
        try:
            # Use symbol as key for partitioning
            future = self.producer.send(
                topic,
                key=data['symbol'],
                value=data
            )
            
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Published {data['symbol']} to topic {topic} "
                f"[partition: {record_metadata.partition}, offset: {record_metadata.offset}]"
            )
            
        except KafkaError as e:
            logger.error(f"Failed to publish to Kafka: {e}")
    
    def run(self):
        """Main loop to continuously fetch and publish stock data."""
        logger.info(f"Starting stock data producer for symbols: {self.stock_symbols}")
        
        try:
            while True:
                for symbol in self.stock_symbols:
                    # Fetch stock data
                    price_data = self.fetch_stock_data(symbol)
                    
                    if price_data:
                        # Publish to stock-prices topic
                        self.publish_to_kafka(price_data, 'stock-prices')
                        
                        # Extract volume data and publish separately
                        volume_data = {
                            'symbol': price_data['symbol'],
                            'timestamp': price_data['timestamp'],
                            'volume': price_data['volume'],
                            'price': price_data['price']
                        }
                        self.publish_to_kafka(volume_data, 'stock-volumes')
                
                # Wait before next fetch
                time.sleep(self.fetch_interval)
                
        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        finally:
            self.producer.close()
            logger.info("Producer closed")


def main():
    """Entry point for the script."""
    producer = StockDataProducer(
        bootstrap_servers='localhost:9092',
        stock_symbols=['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA'],
        fetch_interval=5  # Fetch every 5 seconds
    )
    producer.run()


if __name__ == '__main__':
    main()
