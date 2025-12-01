#!/usr/bin/env python3
"""
HDFS Client Utility

Provides utilities for interacting with HDFS for reading and writing data.
"""

import logging
import yaml
from pathlib import Path
from typing import Optional, List, Dict, Any
import subprocess
import os

logger = logging.getLogger(__name__)


class HDFSClient:
    """Client for HDFS operations."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize HDFS client.
        
        Args:
            config_path: Path to HDFS config file
        """
        self.config = self._load_config(config_path)
        self.namenode_host = self.config.get('namenode_host', 'localhost')
        self.namenode_port = self.config.get('namenode_port', 9000)
        self.namenode_url = f"hdfs://{self.namenode_host}:{self.namenode_port}"
        
        logger.info(f"Initialized HDFS client for {self.namenode_url}")
    
    @staticmethod
    def _load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load HDFS configuration from YAML file.
        
        Args:
            config_path: Path to config file
            
        Returns:
            Configuration dictionary
        """
        if config_path is None:
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "hdfs_config.yaml"
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config.get('hdfs', {})
    
    def _run_hdfs_command(self, command: List[str]) -> tuple:
        """
        Run HDFS command via subprocess.
        
        Args:
            command: List of command parts
            
        Returns:
            Tuple of (returncode, stdout, stderr)
        """
        try:
            # Set HDFS environment
            env = os.environ.copy()
            env['HADOOP_CONF_DIR'] = '/etc/hadoop'
            
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            return result.returncode, result.stdout, result.stderr
            
        except subprocess.TimeoutExpired:
            logger.error(f"HDFS command timed out: {' '.join(command)}")
            return -1, "", "Command timed out"
        except Exception as e:
            logger.error(f"Error running HDFS command: {e}")
            return -1, "", str(e)
    
    def mkdir(self, path: str, create_parents: bool = True) -> bool:
        """
        Create directory in HDFS.
        
        Args:
            path: HDFS path
            create_parents: Create parent directories if they don't exist
            
        Returns:
            True if successful, False otherwise
        """
        cmd = ['hdfs', 'dfs', '-mkdir']
        if create_parents:
            cmd.append('-p')
        cmd.append(path)
        
        returncode, stdout, stderr = self._run_hdfs_command(cmd)
        
        if returncode == 0:
            logger.info(f"Created HDFS directory: {path}")
            return True
        else:
            # Directory might already exist, which is okay
            if 'File exists' in stderr:
                logger.debug(f"HDFS directory already exists: {path}")
                return True
            logger.warning(f"Failed to create HDFS directory {path}: {stderr}")
            return False
    
    def ls(self, path: str, recursive: bool = False) -> List[str]:
        """
        List files in HDFS directory.
        
        Args:
            path: HDFS path
            recursive: List recursively
            
        Returns:
            List of file paths
        """
        cmd = ['hdfs', 'dfs', '-ls']
        if recursive:
            cmd.append('-R')
        cmd.append(path)
        
        returncode, stdout, stderr = self._run_hdfs_command(cmd)
        
        if returncode == 0:
            lines = stdout.strip().split('\n')
            # Parse output and extract file paths
            files = []
            for line in lines:
                if line.strip() and not line.startswith('Found'):
                    parts = line.split()
                    if len(parts) > 7:
                        files.append(parts[-1])
            return files
        else:
            logger.warning(f"Failed to list HDFS path {path}: {stderr}")
            return []
    
    def exists(self, path: str) -> bool:
        """
        Check if path exists in HDFS.
        
        Args:
            path: HDFS path
            
        Returns:
            True if path exists, False otherwise
        """
        cmd = ['hdfs', 'dfs', '-test', '-e', path]
        returncode, _, _ = self._run_hdfs_command(cmd)
        return returncode == 0
    
    def rm(self, path: str, recursive: bool = False) -> bool:
        """
        Remove file or directory from HDFS.
        
        Args:
            path: HDFS path
            recursive: Remove recursively
            
        Returns:
            True if successful, False otherwise
        """
        cmd = ['hdfs', 'dfs', '-rm']
        if recursive:
            cmd.append('-r')
        cmd.append(path)
        
        returncode, stdout, stderr = self._run_hdfs_command(cmd)
        
        if returncode == 0:
            logger.info(f"Removed HDFS path: {path}")
            return True
        else:
            logger.warning(f"Failed to remove HDFS path {path}: {stderr}")
            return False
    
    def put(self, local_path: str, hdfs_path: str) -> bool:
        """
        Copy file from local filesystem to HDFS.
        
        Args:
            local_path: Local file path
            hdfs_path: HDFS destination path
            
        Returns:
            True if successful, False otherwise
        """
        cmd = ['hdfs', 'dfs', '-put', local_path, hdfs_path]
        
        returncode, stdout, stderr = self._run_hdfs_command(cmd)
        
        if returncode == 0:
            logger.info(f"Copied {local_path} to HDFS: {hdfs_path}")
            return True
        else:
            logger.error(f"Failed to copy to HDFS: {stderr}")
            return False
    
    def get(self, hdfs_path: str, local_path: str) -> bool:
        """
        Copy file from HDFS to local filesystem.
        
        Args:
            hdfs_path: HDFS source path
            local_path: Local destination path
            
        Returns:
            True if successful, False otherwise
        """
        cmd = ['hdfs', 'dfs', '-get', hdfs_path, local_path]
        
        returncode, stdout, stderr = self._run_hdfs_command(cmd)
        
        if returncode == 0:
            logger.info(f"Copied HDFS {hdfs_path} to local: {local_path}")
            return True
        else:
            logger.error(f"Failed to copy from HDFS: {stderr}")
            return False
    
    def chmod(self, path: str, mode: str, recursive: bool = False) -> bool:
        """
        Change permissions of HDFS path.
        
        Args:
            path: HDFS path
            mode: Permission mode (e.g., '777', '755')
            recursive: Apply recursively
            
        Returns:
            True if successful, False otherwise
        """
        cmd = ['hdfs', 'dfs', '-chmod']
        if recursive:
            cmd.append('-R')
        cmd.extend([mode, path])
        
        returncode, stdout, stderr = self._run_hdfs_command(cmd)
        
        if returncode == 0:
            logger.info(f"Changed permissions of {path} to {mode}")
            return True
        else:
            logger.warning(f"Failed to change permissions: {stderr}")
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get HDFS cluster health status.
        
        Returns:
            Dictionary with health status information
        """
        cmd = ['hdfs', 'dfsadmin', '-report']
        returncode, stdout, stderr = self._run_hdfs_command(cmd)
        
        status = {
            'healthy': returncode == 0,
            'report': stdout if returncode == 0 else None,
            'error': stderr if returncode != 0 else None
        }
        
        return status
    
    def get_namenode_url(self) -> str:
        """
        Get NameNode URL for Spark.
        
        Returns:
            NameNode URL
        """
        return self.namenode_url
    
    def setup_directories(self) -> bool:
        """
        Set up standard HDFS directories for stock data.
        
        Returns:
            True if successful, False otherwise
        """
        paths_config = self.config.get('paths', {})
        
        directories = [
            paths_config.get('base', '/stock-data'),
            paths_config.get('raw', {}).get('prices', '/stock-data/raw/prices'),
            paths_config.get('raw', {}).get('volumes', '/stock-data/raw/volumes'),
            paths_config.get('lakehouse', {}).get('prices', '/stock-data/lakehouse/prices'),
            paths_config.get('lakehouse', {}).get('volumes', '/stock-data/lakehouse/volumes'),
        ]
        
        success = True
        for directory in directories:
            if not self.mkdir(directory, create_parents=True):
                success = False
        
        # Set permissions
        if success:
            self.chmod(paths_config.get('base', '/stock-data'), '777', recursive=True)
        
        return success


def main():
    """Test HDFS client functionality."""
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    client = HDFSClient()
    
    # Test connection
    print(f"NameNode URL: {client.get_namenode_url()}")
    
    # Setup directories
    print("Setting up HDFS directories...")
    if client.setup_directories():
        print("✓ Directories created successfully")
    else:
        print("✗ Failed to create directories")
        sys.exit(1)
    
    # Check health
    print("\nHDFS Health Status:")
    health = client.get_health_status()
    if health['healthy']:
        print("✓ HDFS cluster is healthy")
        print(health['report'])
    else:
        print("✗ HDFS cluster health check failed")
        print(health['error'])


if __name__ == '__main__':
    main()
