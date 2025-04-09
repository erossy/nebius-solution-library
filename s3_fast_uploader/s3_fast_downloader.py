"""
High-Performance S3 File Downloader

This script provides a fast and memory-efficient way to download multiple files from S3
to host RAM. It's specifically optimized for high-performance scenarios with large numbers
of files and high-memory environments.

Features:
- Parallel downloading using thread pool
- Memory-efficient batch processing
- Progress tracking with detailed statistics
- Automatic resource cleanup
- Memory usage monitoring
- S3 Transfer Acceleration support
- Optimized chunk sizes based on available system resources

Usage:
    python s3_fast_downloader.py --bucket <bucket_name> [options]

Arguments:
    --bucket        S3 bucket name (required)
    --prefix        S3 prefix to filter files (default: '')
    --batch-size    Number of files to download in each batch (default: 5)
    --region        AWS region (default: None, uses default configuration)
    --access-key    AWS access key (default: None, uses default configuration)
    --secret-key    AWS secret key (default: None, uses default configuration)
    --endpoint-url  Custom endpoint URL for S3-compatible storage (default: None)

Examples:
    # Download from AWS S3
    python s3_fast_downloader.py --bucket my-bucket --prefix data/ --batch-size 10

    # Download from MinIO or other S3-compatible storage
    python s3_fast_downloader.py --bucket my-bucket --endpoint-url http://minio.example.com:9000 \
        --access-key minioadmin --secret-key minioadmin --batch-size 10

Performance Considerations:
1. Memory Usage:
   - Files are downloaded in batches to prevent memory overflow
   - Each batch is processed independently with memory monitoring
   - Pre-allocated buffers for optimal memory usage

2. Concurrency:
   - Uses thread pool for I/O-bound operations
   - Automatically scales based on CPU cores
   - Configurable batch size for optimal performance

3. Network:
   - S3 Transfer Acceleration enabled
   - Optimized chunk sizes for transfer
   - Connection pooling for better performance

Requirements:
    See requirements.txt for dependencies
"""

import os
import boto3
import io
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict
from tqdm import tqdm
import time
from botocore.exceptions import ClientError
import logging
import psutil
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import threading
import atexit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3FastDownloader:
    def __init__(self, bucket_name: str, aws_access_key_id: str = None, 
                 aws_secret_access_key: str = None, region_name: str = None,
                 endpoint_url: str = None, max_retries: int = 3, num_processes: int = None,
                 multipart_threshold: int = 8 * 1024 * 1024,  # 8MB
                 max_concurrency: int = 10,
                 multipart_chunksize: int = 8 * 1024 * 1024):  # 8MB
        """
        Initialize the S3 fast downloader.

        Args:
            bucket_name (str): Name of the S3 bucket
            aws_access_key_id (str, optional): AWS access key
            aws_secret_access_key (str, optional): AWS secret key
            region_name (str, optional): AWS region
            max_retries (int): Maximum number of retries for failed downloads
            num_processes (int, optional): Number of processes to use (defaults to CPU count)
        """
        self.bucket_name = bucket_name
        self.max_retries = max_retries
        self.num_processes = num_processes or cpu_count()

        # Configure boto3 for maximum performance
        config = Config(
            max_pool_connections=50,  # Increase connection pool size
            retries={'max_attempts': max_retries},
            tcp_keepalive=True,
            read_timeout=300,  # 5 minutes
            connect_timeout=300,  # 5 minutes
            s3={'use_accelerate_endpoint': True}  # Enable S3 Transfer Acceleration
        )

        # Configure transfer settings
        self.transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
            use_threads=True
        )

        # Initialize S3 client with optimized configuration
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=config
        )

        # Dictionary to store downloaded files in memory
        self.files_in_memory = {}

        # Initialize thread pool for concurrent operations
        self.thread_pool = ThreadPoolExecutor(max_workers=max_concurrency)

        # Register cleanup on exit
        atexit.register(self.cleanup)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=True)
        if hasattr(self, 'files_in_memory'):
            self.files_in_memory.clear()
        # Ensure process pool is cleaned up
        if hasattr(self, '_pool'):
            self._pool.close()
            self._pool.join()

    def check_memory_usage(self):
        """Check if there's enough memory available."""
        memory = psutil.virtual_memory()
        if memory.available < 1024 * 1024 * 1024:  # Less than 1GB available
            raise MemoryError("Not enough memory available to continue downloads")

    def get_object_size(self, file_key: str) -> int:
        """Get the size of an S3 object before downloading."""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
            return response['ContentLength']
        except ClientError as e:
            logger.error(f"Error getting size for {file_key}: {str(e)}")
            raise

    def download_file_to_memory(self, file_key: str) -> Tuple[str, bytes]:
        """
        Download a single file from S3 to memory with retries.

        Args:
            file_key (str): The key of the file in S3 bucket

        Returns:
            Tuple[str, bytes]: Tuple of (file_key, file_content)
        """
        # Check memory availability before download
        self.check_memory_usage()

        # Get file size first
        file_size = self.get_object_size(file_key)

        retries = 0
        while retries < self.max_retries:
            try:
                # Create a BytesIO object with pre-allocated buffer
                file_obj = io.BytesIO(bytearray(file_size))

                # Download the file directly to memory with optimized transfer config
                self.s3_client.download_fileobj(
                    Bucket=self.bucket_name,
                    Key=file_key,
                    Fileobj=file_obj,
                    Config=self.transfer_config
                )

                # Get the content and return
                return file_key, file_obj.getvalue()

            except ClientError as e:
                logger.error(f"Error downloading {file_key}: {str(e)}")
                retries += 1
                if retries == self.max_retries:
                    raise
                time.sleep(2 ** retries)  # Exponential backoff

    def download_files(self, file_keys: List[str], batch_size: int = 5) -> dict:
        """
        Download multiple files in parallel using thread pool.

        Args:
            file_keys (List[str]): List of file keys to download
            batch_size (int): Number of files to download in each batch

        Returns:
            dict: Dictionary mapping file keys to their contents in memory
        """
        logger.info(f"Starting parallel download of {len(file_keys)} files using thread pool")

        # Calculate total size of all files
        total_size = 0
        file_sizes = {}
        for key in file_keys:
            size = self.get_object_size(key)
            file_sizes[key] = size
            total_size += size

        logger.info(f"Total download size: {total_size / (1024*1024):.2f} MB")

        # Process files in batches to prevent memory overload
        results = {}
        for i in range(0, len(file_keys), batch_size):
            batch = file_keys[i:i + batch_size]
            batch_size_mb = sum(file_sizes[key] for key in batch) / (1024*1024)
            logger.info(f"Processing batch {i//batch_size + 1}, size: {batch_size_mb:.2f} MB")

            # Submit batch of downloads to thread pool
            future_to_key = {
                self.thread_pool.submit(self.download_file_to_memory, key): key 
                for key in batch
            }

            # Track progress with tqdm
            with tqdm(total=len(batch), desc=f"Batch {i//batch_size + 1}") as pbar:
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        file_key, content = future.result()
                        results[file_key] = content
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error downloading {key}: {str(e)}")
                        pbar.update(1)

            # Force garbage collection after each batch
            self.check_memory_usage()

        # Store results
        self.files_in_memory = results
        logger.info(f"Successfully downloaded {len(self.files_in_memory)} files to memory")

        return self.files_in_memory

def get_performance_metrics():
    """Get current system performance metrics."""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    return {
        'cpu_percent': cpu_percent,
        'memory_percent': memory.percent,
        'memory_available': memory.available / (1024 * 1024 * 1024),  # GB
        'memory_used': memory.used / (1024 * 1024 * 1024)  # GB
    }

def main():
    """Main function demonstrating the usage of S3FastDownloader."""
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fast S3 file downloader')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', default='', help='S3 prefix to filter files')
    parser.add_argument('--batch-size', type=int, default=5, help='Number of files to download in each batch')
    parser.add_argument('--region', default=None, help='AWS region')
    parser.add_argument('--access-key', default=None, help='AWS access key')
    parser.add_argument('--secret-key', default=None, help='AWS secret key')
    parser.add_argument('--endpoint-url', default=None, help='Custom endpoint URL for S3-compatible storage')
    args = parser.parse_args()

    try:
        # List files in the bucket
        s3_client = boto3.client('s3',
            aws_access_key_id=args.access_key,
            aws_secret_access_key=args.secret_key,
            region_name=args.region,
            endpoint_url=args.endpoint_url
        )

        # Get list of files
        paginator = s3_client.get_paginator('list_objects_v2')
        file_keys = []
        for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
            if 'Contents' in page:
                file_keys.extend(obj['Key'] for obj in page['Contents'])

        if not file_keys:
            logger.error(f"No files found in bucket {args.bucket} with prefix {args.prefix}")
            return

        logger.info(f"Found {len(file_keys)} files to download")

        # Calculate optimal settings based on system resources
        memory = psutil.virtual_memory()
        available_memory = memory.available
        file_count = len(file_keys)
        optimal_chunk_size = min(
            8 * 1024 * 1024,  # 8MB default
            max(1 * 1024 * 1024, available_memory // (file_count * 2))  # At least 1MB
        )

        # Initialize and use downloader with context manager
        with S3FastDownloader(
            bucket_name=args.bucket,
            aws_access_key_id=args.access_key,
            aws_secret_access_key=args.secret_key,
            region_name=args.region,
            endpoint_url=args.endpoint_url,
            num_processes=cpu_count(),
            multipart_threshold=optimal_chunk_size,
            multipart_chunksize=optimal_chunk_size,
            max_concurrency=min(32, cpu_count() * 4)
        ) as downloader:

            # Download files with progress tracking
            start_time = time.time()
            files = downloader.download_files(file_keys, batch_size=args.batch_size)
            end_time = time.time()

            # Calculate and display performance metrics
            total_size = sum(len(content) for content in files.values())
            duration = end_time - start_time
            speed = total_size / duration / (1024 * 1024)  # MB/s
            metrics = get_performance_metrics()

            # Print detailed performance report
            logger.info("\nPerformance Report:")
            logger.info("-" * 50)
            logger.info(f"Files downloaded: {len(files)} of {len(file_keys)}")
            logger.info(f"Total size: {total_size / (1024 * 1024):.2f} MB")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Average speed: {speed:.2f} MB/s")
            logger.info(f"CPU Usage: {metrics['cpu_percent']}%")
            logger.info(f"Memory Usage: {metrics['memory_percent']}%")
            logger.info(f"Available Memory: {metrics['memory_available']:.2f} GB")
            logger.info(f"Used Memory: {metrics['memory_used']:.2f} GB")
            logger.info("-" * 50)

    except KeyboardInterrupt:
        logger.info("\nDownload interrupted by user")
    except Exception as e:
        logger.error(f"Error during download: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
