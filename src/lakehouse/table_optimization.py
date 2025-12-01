#!/usr/bin/env python3
"""
Iceberg Table Optimization Scripts

Performs compaction and expiration operations on Iceberg tables.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, List

from pyspark.sql import SparkSession

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lakehouse.iceberg_tables import IcebergTableManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TableOptimizer:
    """Optimizes Iceberg tables."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize table optimizer.
        
        Args:
            spark: SparkSession instance
        """
        self.manager = IcebergTableManager(spark=spark)
        self.spark = self.manager.spark
        self.config = self.manager.config
        logger.info("Initialized TableOptimizer")
    
    def compact_table(self, table_name: str, target_file_size_mb: int = 128):
        """
        Compact an Iceberg table.
        
        Args:
            table_name: Full table name (catalog.database.table)
            target_file_size_mb: Target file size in MB
        """
        logger.info(f"Compacting table: {table_name}")
        
        try:
            # Use Iceberg's rewrite_data_files procedure
            compact_sql = f"""
            CALL stock_analytics.system.rewrite_data_files(
                table => '{table_name}',
                options => map(
                    'target-file-size-bytes', '{target_file_size_mb * 1024 * 1024}'
                )
            )
            """
            
            self.spark.sql(compact_sql)
            logger.info(f"Successfully compacted table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error compacting table {table_name}: {e}", exc_info=True)
            raise
    
    def expire_snapshots(self, table_name: str, older_than_days: int = 7):
        """
        Expire old snapshots from an Iceberg table.
        
        Args:
            table_name: Full table name (catalog.database.table)
            older_than_days: Keep snapshots newer than this many days
        """
        logger.info(f"Expiring snapshots older than {older_than_days} days for table: {table_name}")
        
        try:
            expire_sql = f"""
            CALL stock_analytics.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{older_than_days} days ago'
            )
            """
            
            self.spark.sql(expire_sql)
            logger.info(f"Successfully expired old snapshots for table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error expiring snapshots for table {table_name}: {e}", exc_info=True)
            raise
    
    def remove_orphan_files(self, table_name: str, older_than_hours: int = 24):
        """
        Remove orphan files from an Iceberg table.
        
        Args:
            table_name: Full table name (catalog.database.table)
            older_than_hours: Remove files older than this many hours
        """
        logger.info(f"Removing orphan files older than {older_than_hours} hours for table: {table_name}")
        
        try:
            remove_sql = f"""
            CALL stock_analytics.system.remove_orphan_files(
                table => '{table_name}',
                older_than => TIMESTAMP '{older_than_hours} hours ago'
            )
            """
            
            self.spark.sql(remove_sql)
            logger.info(f"Successfully removed orphan files for table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error removing orphan files for table {table_name}: {e}", exc_info=True)
            raise
    
    def optimize_all_tables(self):
        """Optimize all Iceberg tables."""
        tables = [
            "stock_analytics.stock_analytics.stock_prices",
            "stock_analytics.stock_analytics.stock_volumes",
            "stock_analytics.stock_analytics.daily_aggregates"
        ]
        
        optimization_config = self.config.get('optimization', {})
        compaction_config = optimization_config.get('compaction', {})
        expiration_config = optimization_config.get('expiration', {})
        
        target_file_size = compaction_config.get('target_file_size_mb', 128)
        snapshot_retention = expiration_config.get('snapshot_retention_days', 7)
        orphan_retention = expiration_config.get('orphan_file_retention_hours', 24)
        
        logger.info("Optimizing all Iceberg tables...")
        
        for table in tables:
            try:
                # Compact table
                if compaction_config.get('enabled', True):
                    self.compact_table(table, target_file_size)
                
                # Expire snapshots
                if expiration_config.get('enabled', True):
                    self.expire_snapshots(table, snapshot_retention)
                    self.remove_orphan_files(table, orphan_retention)
                
            except Exception as e:
                logger.warning(f"Failed to optimize table {table}: {e}")
                continue
        
        logger.info("Completed optimization of all tables")
    
    def stop(self):
        """Stop Spark session."""
        if self.manager:
            self.manager.stop()


def main():
    """Entry point for table optimization."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Optimize Iceberg tables')
    parser.add_argument(
        '--table',
        type=str,
        default=None,
        help='Table to optimize (catalog.database.table). If not specified, optimizes all tables.'
    )
    parser.add_argument(
        '--operation',
        type=str,
        choices=['compact', 'expire', 'remove-orphans', 'all'],
        default='all',
        help='Operation to perform'
    )
    parser.add_argument(
        '--target-file-size-mb',
        type=int,
        default=128,
        help='Target file size in MB for compaction'
    )
    parser.add_argument(
        '--snapshot-retention-days',
        type=int,
        default=7,
        help='Snapshot retention in days'
    )
    
    args = parser.parse_args()
    
    optimizer = None
    try:
        optimizer = TableOptimizer()
        
        if args.table:
            table_name = args.table
        else:
            # Optimize all tables
            optimizer.optimize_all_tables()
            return
        
        if args.operation == 'all':
            optimizer.compact_table(table_name, args.target_file_size_mb)
            optimizer.expire_snapshots(table_name, args.snapshot_retention_days)
            optimizer.remove_orphan_files(table_name)
        elif args.operation == 'compact':
            optimizer.compact_table(table_name, args.target_file_size_mb)
        elif args.operation == 'expire':
            optimizer.expire_snapshots(table_name, args.snapshot_retention_days)
        elif args.operation == 'remove-orphans':
            optimizer.remove_orphan_files(table_name)
        
    except Exception as e:
        logger.error(f"Optimization failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if optimizer:
            optimizer.stop()


if __name__ == '__main__':
    main()
