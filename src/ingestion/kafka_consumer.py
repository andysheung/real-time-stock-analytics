#!/usr/bin/env python3
"""
Simple Kafka Consumer for Testing

Consumes messages from Kafka topics to verify data flow.
"""

import json
import logging
import yaml
from pathlib import Path
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_kafka_config(config_path=None):
    """Load Kafka configuration from YAML file."""
    if config_path is None:
        project_root = Path(__file__).parent.parent.parent
        config_path = project_root / "config" / "kafka_config.yaml"
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config.get('kafka', {})


def consume_stock_prices(topic: str = None, bootstrap_servers: str = None, config_path: str = None):
    """Consume and print stock price messages from Kafka."""
    # Load config
    kafka_config = load_kafka_config(config_path)
    
    # Use config values or provided parameters
    bootstrap_servers = bootstrap_servers or kafka_config.get('bootstrap_servers', 'localhost:9092')
    topic = topic or kafka_config.get('topics', {}).get('stock_prices', 'stock-prices')
    consumer_config = kafka_config.get('consumer', {})
    group_id = consumer_config.get('group_id', 'stock-analytics-group')
    auto_offset_reset = consumer_config.get('auto_offset_reset', 'earliest')
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset=auto_offset_reset,
            consumer_timeout_ms=5000,  # Stop after 5 seconds of no messages
            group_id=group_id
        )
        
        logger.info(f"Consuming messages from topic: {topic}")
        
        for message in consumer:
            data = message.value
            logger.info(
                f"Received: {data['symbol']} - "
                f"Price: ${data['price']:.2f}, "
                f"Volume: {data['volume']:,}, "
                f"Time: {data['timestamp']}"
            )
            
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()


if __name__ == '__main__':
    consume_stock_prices()
