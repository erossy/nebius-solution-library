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
   - S3 Transfer Acceleration (when using standard AWS S3)
   - Optimized chunk sizes for transfer
   - Connection pooling for better performance
   - Support for custom S3-compatible endpoints

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
        # Calculate optimal connection pool size based on system resources
        optimal_pool_size = min(
            max(100, max_concurrency * cpu_count() * 4),  # Scale with CPU and concurrency
            2000  # Maximum of 2000 connections for high throughput
        )

        # Log system resources and configuration
        logger.info("System Configuration:")
        logger.info(f"- CPU cores: {cpu_count()}")
        logger.info(f"- Max concurrency: {max_concurrency}")
        logger.info(f"- Connection pool size: {optimal_pool_size}")
        memory = psutil.virtual_memory()
        logger.info(f"- Available memory: {memory.available / (1024**3):.1f} GB")

        config_params = {
            'max_pool_connections': optimal_pool_size,
            'retries': {'max_attempts': max_retries},
            'tcp_keepalive': True,
            'read_timeout': 300,  # 5 minutes
            'connect_timeout': 300,  # 5 minutes
        }
        logger.info(f"Configured connection pool with {optimal_pool_size} connections")

        # Enable S3 Transfer Acceleration only when using standard AWS S3 (no custom endpoint)
        if not endpoint_url:
            config_params['s3'] = {'use_accelerate_endpoint': True}
            logger.info("Using S3 Transfer Acceleration for improved performance")
        else:
            logger.info(f"Using custom endpoint: {endpoint_url} (S3 Transfer Acceleration disabled)")

        config = Config(**config_params)

        # Log configuration details
        logger.info(f"Configured with: {cpu_count()} CPUs, {max_concurrency} concurrent transfers")
        logger.info(f"Chunk size: {multipart_chunksize / (1024*1024):.1f}MB, Retries: {max_retries}")

        # Initialize S3 client with optimized configuration
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=config
        )

        # Verify endpoint accessibility
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info("Successfully connected to S3 endpoint")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to S3 endpoint: {str(e)}")

        # Configure transfer settings
        self.transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
            use_threads=True
        )

        # Dictionary to store downloaded files in memory
        self.files_in_memory = {}

        # Initialize thread pool for concurrent operations within batches
        thread_pool_size = min(
            max(cpu_count() * 4, max_concurrency),  # At least 4 threads per CPU
            128  # Cap at 128 threads
        )
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        logger.info(f"- Thread pool size: {thread_pool_size}")

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

        # Calculate optimal number of concurrent batches based on available memory and CPU
        memory = psutil.virtual_memory()
        available_memory = memory.available
        avg_file_size = total_size / len(file_keys)
        max_concurrent_batches = min(
            max(1, int(available_memory / (avg_file_size * batch_size * 2))),  # Based on memory
            max(1, cpu_count()),  # Use all CPU cores
            16  # Cap at 16 concurrent batches
        )

        # Log batch processing configuration
        logger.info("\nBatch Processing Configuration:")
        logger.info(f"- Concurrent batches: {max_concurrent_batches}")
        logger.info(f"- Batch size: {batch_size}")
        logger.info(f"- Average file size: {avg_file_size / (1024*1024):.2f} MB")
        logger.info(f"- Total files: {len(file_keys)}")
        logger.info(f"- Total size: {total_size / (1024*1024*1024):.2f} GB")

        # Process files in parallel batches
        results = {}
        failed_downloads = []
        start_time = time.time()
        total_downloaded_size = 0

        with ThreadPoolExecutor(max_workers=max_concurrent_batches) as batch_executor:
            # Submit batches for parallel processing
            batch_futures = []
            for i in range(0, len(file_keys), batch_size):
                batch = file_keys[i:i + batch_size]
                batch_futures.append(batch_executor.submit(
                    self._process_batch, batch, i//batch_size + 1, file_sizes
                ))

            # Track overall progress
            with tqdm(total=len(file_keys), desc="Total Progress") as total_pbar:
                for future in as_completed(batch_futures):
                    try:
                        batch_results = future.result()
                        results.update(batch_results)

                        # Update progress and statistics
                        successful_files = len(batch_results)
                        total_pbar.update(successful_files)

                        # Calculate downloaded size and speed
                        batch_size = sum(file_sizes[key] for key in batch_results.keys())
                        total_downloaded_size += batch_size
                        elapsed = time.time() - start_time
                        current_speed = total_downloaded_size / elapsed / (1024*1024) if elapsed > 0 else 0

                        total_pbar.set_postfix({
                            'Speed': f'{current_speed:.1f} MB/s',
                            'Success': f'{len(results)}/{len(file_keys)}'
                        })

                    except Exception as e:
                        logger.error(f"Batch processing error: {str(e)}")
                        failed_downloads.extend(batch)

        # Calculate final statistics
        end_time = time.time()
        total_time = end_time - start_time
        avg_speed = total_downloaded_size / total_time / (1024*1024) if total_time > 0 else 0
        success_rate = len(results) / len(file_keys) * 100

        # Get system resource usage
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        memory_used = memory.used / (1024*1024*1024)  # GB
        memory_available = memory.available / (1024*1024*1024)  # GB

        # Print performance summary
        logger.info("\nDownload Summary:")
        logger.info("-" * 50)
        logger.info("Files:")
        logger.info(f"- Total processed: {len(file_keys)}")
        logger.info(f"- Successfully downloaded: {len(results)}")
        logger.info(f"- Failed downloads: {len(failed_downloads)}")
        logger.info(f"- Success rate: {success_rate:.1f}%")

        logger.info("\nPerformance:")
        logger.info(f"- Total time: {total_time:.1f} seconds")
        logger.info(f"- Average speed: {avg_speed:.1f} MB/s")
        logger.info(f"- Total data transferred: {total_downloaded_size / (1024*1024*1024):.2f} GB")

        logger.info("\nSystem Resources:")
        logger.info(f"- CPU Usage: {cpu_percent:.1f}%")
        logger.info(f"- Memory Used: {memory_used:.1f} GB")
        logger.info(f"- Memory Available: {memory_available:.1f} GB")
        logger.info(f"- Memory Utilization: {memory.percent:.1f}%")

        # Log failed downloads if any
        if failed_downloads:
            logger.warning("\nFailed downloads:")
            for key in failed_downloads:
                logger.warning(f"- {key}")

        # Store final results
        self.files_in_memory = results

        # Clean up any partially downloaded data
        if failed_downloads:
            self.check_memory_usage()

        return self.files_in_memory

    def _process_batch(self, batch: List[str], batch_num: int, file_sizes: Dict[str, int]) -> Dict[str, bytes]:
        """Process a single batch of files with performance monitoring and retries."""
        batch_results = {}
        failed_files = set()
        batch_size_mb = sum(file_sizes[key] for key in batch) / (1024*1024)
        start_time = time.time()
        max_file_retries = 3

        logger.info(f"Processing batch {batch_num}, size: {batch_size_mb:.2f} MB")

        # Monitor memory before batch
        mem_before = psutil.virtual_memory()

        # Track files that need downloading (initially all files)
        remaining_files = set(batch)
        retry_count = 0

        while remaining_files and retry_count < max_file_retries:
            if retry_count > 0:
                logger.info(f"Retry attempt {retry_count + 1} for {len(remaining_files)} files in batch {batch_num}")
                time.sleep(2 ** retry_count)  # Exponential backoff

            # Submit remaining files to thread pool
            future_to_key = {
                self.thread_pool.submit(self.download_file_to_memory, key): key 
                for key in remaining_files
            }

            # Track batch progress with performance monitoring
            successful_downloads = len(batch_results)
            total_downloaded = sum(file_sizes[key] for key in batch_results)
            retry_files = set()

            with tqdm(total=len(remaining_files), 
                     desc=f"Batch {batch_num}" + (f" (Retry {retry_count + 1})" if retry_count > 0 else "")) as pbar:
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        file_key, content = future.result()
                        batch_results[file_key] = content
                        successful_downloads += 1
                        total_downloaded += file_sizes[key]

                        # Calculate and display current speed
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            speed = total_downloaded / elapsed / (1024*1024)  # MB/s
                            pbar.set_postfix({
                                'Speed': f'{speed:.1f} MB/s',
                                'Success': f'{successful_downloads}/{len(batch)}'
                            })

                    except Exception as e:
                        logger.error(f"Error downloading {key} (attempt {retry_count + 1}): {str(e)}")
                        retry_files.add(key)
                    finally:
                        pbar.update(1)

            # Update remaining files for next retry
            remaining_files = retry_files
            retry_count += 1

        # Add any remaining failed files to the failed set
        failed_files.update(remaining_files)

        # Monitor memory and calculate statistics
        mem_after = psutil.virtual_memory()
        memory_used = (mem_after.used - mem_before.used) / (1024*1024)  # MB
        elapsed_time = time.time() - start_time
        avg_speed = batch_size_mb / elapsed_time if elapsed_time > 0 else 0

        # Log batch completion status
        success_rate = (len(batch) - len(failed_files)) / len(batch) * 100
        logger.info(f"Batch {batch_num} completed:")
        logger.info(f"- Speed: {avg_speed:.1f} MB/s")
        logger.info(f"- Memory used: {memory_used:.1f} MB")
        logger.info(f"- Success rate: {success_rate:.1f}% ({len(batch) - len(failed_files)}/{len(batch)})")

        if failed_files:
            logger.warning(f"Failed to download {len(failed_files)} files in batch {batch_num} after {max_file_retries} attempts")
            # Clean up memory for failed files
            for key in failed_files:
                if key in batch_results:
                    del batch_results[key]

        # Check memory after batch
        self.check_memory_usage()

        return batch_results

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
