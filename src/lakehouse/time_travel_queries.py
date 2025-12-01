#!/usr/bin/env python3
"""
Time Travel Query Examples

Demonstrates time travel queries on Apache Iceberg tables.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lakehouse.iceberg_tables import IcebergTableManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TimeTravelQueries:
    """Examples of time travel queries on Iceberg tables."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize time travel queries.
        
        Args:
            spark: SparkSession instance
        """
        self.manager = IcebergTableManager(spark=spark)
        self.spark = self.manager.spark
        logger.info("Initialized TimeTravelQueries")
    
    def list_snapshots(self, table_name: str):
        """
        List all snapshots for a table.
        
        Args:
            table_name: Full table name (catalog.database.table)
        """
        logger.info(f"Listing snapshots for table: {table_name}")
        
        try:
            snapshots = self.spark.sql(f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC")
            snapshots.show(truncate=False)
            return snapshots
        except Exception as e:
            logger.error(f"Error listing snapshots: {e}", exc_info=True)
            raise
    
    def query_at_snapshot(self, table_name: str, snapshot_id: int):
        """
        Query table at a specific snapshot.
        
        Args:
            table_name: Full table name (catalog.database.table)
            snapshot_id: Snapshot ID to query
        """
        logger.info(f"Querying table {table_name} at snapshot {snapshot_id}")
        
        try:
            query = f"SELECT * FROM {table_name} VERSION AS OF {snapshot_id} LIMIT 100"
            df = self.spark.sql(query)
            df.show(truncate=False)
            return df
        except Exception as e:
            logger.error(f"Error querying snapshot: {e}", exc_info=True)
            raise
    
    def query_at_timestamp(self, table_name: str, timestamp: str):
        """
        Query table at a specific timestamp.
        
        Args:
            table_name: Full table name (catalog.database.table)
            timestamp: Timestamp string (YYYY-MM-DD HH:MM:SS)
        """
        logger.info(f"Querying table {table_name} at timestamp {timestamp}")
        
        try:
            query = f"SELECT * FROM {table_name} TIMESTAMP AS OF '{timestamp}' LIMIT 100"
            df = self.spark.sql(query)
            df.show(truncate=False)
            return df
        except Exception as e:
            logger.error(f"Error querying timestamp: {e}", exc_info=True)
            raise
    
    def compare_snapshots(self, table_name: str, snapshot_id_1: int, snapshot_id_2: int):
        """
        Compare data between two snapshots.
        
        Args:
            table_name: Full table name (catalog.database.table)
            snapshot_id_1: First snapshot ID
            snapshot_id_2: Second snapshot ID
        """
        logger.info(f"Comparing snapshots {snapshot_id_1} and {snapshot_id_2} for table {table_name}")
        
        try:
            query = f"""
            SELECT 
                s1.symbol,
                s1.price as price_v1,
                s2.price as price_v2,
                s2.price - s1.price as price_change
            FROM {table_name} VERSION AS OF {snapshot_id_1} s1
            JOIN {table_name} VERSION AS OF {snapshot_id_2} s2
            ON s1.symbol = s2.symbol AND s1.timestamp = s2.timestamp
            LIMIT 100
            """
            
            df = self.spark.sql(query)
            df.show(truncate=False)
            return df
        except Exception as e:
            logger.error(f"Error comparing snapshots: {e}", exc_info=True)
            raise
    
    def get_table_history(self, table_name: str):
        """
        Get table history (all changes).
        
        Args:
            table_name: Full table name (catalog.database.table)
        """
        logger.info(f"Getting history for table: {table_name}")
        
        try:
            history = self.spark.sql(f"SELECT * FROM {table_name}.history ORDER BY made_current_at DESC")
            history.show(truncate=False)
            return history
        except Exception as e:
            logger.error(f"Error getting history: {e}", exc_info=True)
            raise
    
    def rollback_to_snapshot(self, table_name: str, snapshot_id: int):
        """
        Rollback table to a specific snapshot.
        
        Args:
            table_name: Full table name (catalog.database.table)
            snapshot_id: Snapshot ID to rollback to
        """
        logger.warning(f"Rolling back table {table_name} to snapshot {snapshot_id}")
        
        try:
            rollback_sql = f"CALL stock_analytics.system.rollback_to_snapshot(table => '{table_name}', snapshot_id => {snapshot_id})"
            self.spark.sql(rollback_sql)
            logger.info(f"Successfully rolled back table to snapshot {snapshot_id}")
        except Exception as e:
            logger.error(f"Error rolling back: {e}", exc_info=True)
            raise
    
    def stop(self):
        """Stop Spark session."""
        if self.manager:
            self.manager.stop()


def main():
    """Entry point for time travel queries."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Time travel queries on Iceberg tables')
    parser.add_argument(
        '--table',
        type=str,
        required=True,
        help='Table name (catalog.database.table)'
    )
    parser.add_argument(
        '--operation',
        type=str,
        choices=['list-snapshots', 'query-snapshot', 'query-timestamp', 'compare', 'history'],
        required=True,
        help='Operation to perform'
    )
    parser.add_argument(
        '--snapshot-id',
        type=int,
        default=None,
        help='Snapshot ID (for query-snapshot or compare)'
    )
    parser.add_argument(
        '--snapshot-id-2',
        type=int,
        default=None,
        help='Second snapshot ID (for compare)'
    )
    parser.add_argument(
        '--timestamp',
        type=str,
        default=None,
        help='Timestamp (YYYY-MM-DD HH:MM:SS) for query-timestamp'
    )
    
    args = parser.parse_args()
    
    queries = None
    try:
        queries = TimeTravelQueries()
        
        if args.operation == 'list-snapshots':
            queries.list_snapshots(args.table)
        elif args.operation == 'query-snapshot':
            if args.snapshot_id is None:
                logger.error("--snapshot-id is required for query-snapshot")
                sys.exit(1)
            queries.query_at_snapshot(args.table, args.snapshot_id)
        elif args.operation == 'query-timestamp':
            if args.timestamp is None:
                logger.error("--timestamp is required for query-timestamp")
                sys.exit(1)
            queries.query_at_timestamp(args.table, args.timestamp)
        elif args.operation == 'compare':
            if args.snapshot_id is None or args.snapshot_id_2 is None:
                logger.error("--snapshot-id and --snapshot-id-2 are required for compare")
                sys.exit(1)
            queries.compare_snapshots(args.table, args.snapshot_id, args.snapshot_id_2)
        elif args.operation == 'history':
            queries.get_table_history(args.table)
        
    except Exception as e:
        logger.error(f"Time travel query failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if queries:
            queries.stop()


if __name__ == '__main__':
    main()
