#!/usr/bin/env python3
"""
HDFS Health Check Utility

Checks HDFS cluster health and reports status.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.hdfs_client import HDFSClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_hdfs_health() -> Dict[str, Any]:
    """
    Perform comprehensive HDFS health check.
    
    Returns:
        Dictionary with health check results
    """
    client = HDFSClient()
    
    results = {
        'namenode_url': client.get_namenode_url(),
        'health_status': None,
        'directories_exist': {},
        'directories_accessible': {},
        'overall_healthy': False
    }
    
    # Check health status
    logger.info("Checking HDFS cluster health...")
    health = client.get_health_status()
    results['health_status'] = health
    
    if not health['healthy']:
        logger.error("HDFS cluster health check failed")
        return results
    
    # Check required directories
    paths_config = client.config.get('paths', {})
    directories_to_check = [
        ('base', paths_config.get('base', '/stock-data')),
        ('raw_prices', paths_config.get('raw', {}).get('prices', '/stock-data/raw/prices')),
        ('raw_volumes', paths_config.get('raw', {}).get('volumes', '/stock-data/raw/volumes')),
        ('lakehouse_prices', paths_config.get('lakehouse', {}).get('prices', '/stock-data/lakehouse/prices')),
        ('lakehouse_volumes', paths_config.get('lakehouse', {}).get('volumes', '/stock-data/lakehouse/volumes')),
    ]
    
    logger.info("Checking HDFS directories...")
    all_directories_exist = True
    for name, path in directories_to_check:
        exists = client.exists(path)
        results['directories_exist'][name] = {
            'path': path,
            'exists': exists
        }
        if not exists:
            logger.warning(f"Directory does not exist: {path}")
            all_directories_exist = False
        else:
            logger.info(f"✓ Directory exists: {path}")
    
    # Try to list directories to check accessibility
    logger.info("Checking directory accessibility...")
    for name, path in directories_to_check:
        try:
            files = client.ls(path)
            results['directories_accessible'][name] = {
                'path': path,
                'accessible': True,
                'file_count': len(files)
            }
            logger.info(f"✓ Directory accessible: {path} ({len(files)} items)")
        except Exception as e:
            results['directories_accessible'][name] = {
                'path': path,
                'accessible': False,
                'error': str(e)
            }
            logger.error(f"✗ Directory not accessible: {path} - {e}")
    
    # Overall health
    results['overall_healthy'] = (
        health['healthy'] and
        all_directories_exist and
        all(results['directories_accessible'].values())
    )
    
    return results


def main():
    """Main entry point for health check."""
    print("=" * 60)
    print("HDFS Health Check")
    print("=" * 60)
    print()
    
    results = check_hdfs_health()
    
    print(f"NameNode URL: {results['namenode_url']}")
    print()
    
    if results['health_status']['healthy']:
        print("✓ HDFS Cluster: HEALTHY")
        print(results['health_status']['report'])
    else:
        print("✗ HDFS Cluster: UNHEALTHY")
        print(f"Error: {results['health_status']['error']}")
        sys.exit(1)
    
    print()
    print("Directory Status:")
    for name, info in results['directories_exist'].items():
        status = "✓" if info['exists'] else "✗"
        print(f"  {status} {name}: {info['path']}")
    
    print()
    print("Accessibility Status:")
    for name, info in results['directories_accessible'].items():
        if info.get('accessible'):
            status = "✓"
            print(f"  {status} {name}: {info['path']} ({info.get('file_count', 0)} items)")
        else:
            status = "✗"
            print(f"  {status} {name}: {info['path']} - {info.get('error', 'Unknown error')}")
    
    print()
    if results['overall_healthy']:
        print("=" * 60)
        print("OVERALL STATUS: ✓ HEALTHY")
        print("=" * 60)
        sys.exit(0)
    else:
        print("=" * 60)
        print("OVERALL STATUS: ✗ UNHEALTHY")
        print("=" * 60)
        sys.exit(1)


if __name__ == '__main__':
    main()
