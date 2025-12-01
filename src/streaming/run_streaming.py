#!/usr/bin/env python3
"""
Simple script to run the Spark streaming application.

This script provides an easy way to start the streaming pipelines.
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from streaming.streaming_app import StockStreamingApp
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Run Spark Structured Streaming for Stock Data')
    parser.add_argument(
        '--mode',
        choices=['all', 'prices', 'volumes', 'aggregated'],
        default='all',
        help='Which streams to run (default: all)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Path to Spark config file (default: config/spark_config.yaml)'
    )
    
    args = parser.parse_args()
    
    app = None
    try:
        app = StockStreamingApp(config_path=args.config)
        
        if args.mode == 'all':
            logger.info("Running all streaming pipelines...")
            app.run_all_streams()
        elif args.mode == 'prices':
            logger.info("Running price stream only...")
            query = app.run_price_stream()
            query.awaitTermination()
        elif args.mode == 'volumes':
            logger.info("Running volume stream only...")
            query = app.run_volume_stream()
            query.awaitTermination()
        elif args.mode == 'aggregated':
            logger.info("Running aggregated streams only...")
            queries = app.run_aggregated_streams()
            for query in queries:
                query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Error running streaming application: {e}", exc_info=True)
        raise
    finally:
        if app:
            app.stop()
            logger.info("Streaming application stopped")


if __name__ == '__main__':
    main()
