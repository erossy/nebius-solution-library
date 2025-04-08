"""
High-Performance S3 Downloader for RAM - Optimized for High-Performance Hardware

This implementation is specifically optimized for high-performance hardware:
- 128 vCPUs
- 1600 GiB RAM
- High-bandwidth network

Key optimizations:
1. Larger multipart chunk sizes for better throughput
2. Optimized process and thread count for many CPUs
3. Memory pre-allocation for large downloads
4. Network optimization parameters
5. Adaptive concurrency based on file sizes
6. Enhanced progress monitoring with memory usage stats
"""

import os
import time
import boto3
import io
import concurrent.futures
import threading
import multiprocessing
import psutil
from multiprocessing import Process, Queue, cpu_count, Manager
from typing import List, Optional, Dict, Any, Union, Tuple
import traceback


class HighPerformanceS3DownloaderRAMOptimized:
    """
    An optimized high-performance S3 downloader specifically designed for
    high-performance hardware with many CPUs and large RAM.
    """

    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str = None,
        processes: int = None,
        concurrency_per_process: int = None,
        multipart_size_mb: int = 128,  # Increased for better throughput
        max_attempts: int = 5,
        verbose: bool = True,
        memory_limit_gb: float = 1500,  # Leave some RAM for system
        network_timeout: int = 300,  # Increased timeout for large files
        tcp_keepalive: bool = True,
        max_pool_connections: int = 200  # Increased for better concurrency
    ):
        """
        Initialize the optimized downloader with hardware-specific parameters.

        Args:
            endpoint_url: S3 endpoint URL
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret access key
            region_name: AWS region name (optional)
            processes: Number of processes (defaults to CPU count - 4)
            concurrency_per_process: Concurrent downloads per process (auto-calculated)
            multipart_size_mb: Size of multipart chunks in MB
            max_attempts: Maximum number of retry attempts
            verbose: Whether to print detailed information
            memory_limit_gb: Maximum RAM usage in GB
            network_timeout: Network timeout in seconds
            tcp_keepalive: Enable TCP keepalive
            max_pool_connections: Maximum connection pool size
        """
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        # Optimize process count for 128 vCPUs
        # Leave some CPUs for system and network I/O
        self.processes = processes if processes else max(1, cpu_count() - 4)

        # Auto-calculate optimal concurrency based on available resources
        if concurrency_per_process is None:
            total_memory_mb = psutil.virtual_memory().total / (1024 * 1024)
            memory_per_process_mb = (memory_limit_gb * 1024) / self.processes
            self.concurrency_per_process = min(
                50,  # Maximum concurrent downloads per process
                int(memory_per_process_mb / multipart_size_mb)
            )
        else:
            self.concurrency_per_process = concurrency_per_process

        self.multipart_size_mb = multipart_size_mb
        self.max_attempts = max_attempts
        self.verbose = verbose
        self.memory_limit_gb = memory_limit_gb
        self.network_timeout = network_timeout
        self.tcp_keepalive = tcp_keepalive
        self.max_pool_connections = max_pool_connections

        if self.verbose:
            print(f"\nInitialized Optimized S3 Downloader with:")
            print(f"  Hardware Configuration:")
            print(f"    CPUs: {cpu_count()}")
            print(f"    RAM: {psutil.virtual_memory().total / (1024**3):.1f} GB")
            print(f"  Download Configuration:")
            print(f"    Processes: {self.processes}")
            print(f"    Concurrency per process: {self.concurrency_per_process}")
            print(f"    Multipart chunk size: {self.multipart_size_mb} MB")
            print(f"    Memory limit: {self.memory_limit_gb} GB")
            print(f"    Network timeout: {self.network_timeout}s")
            print(f"    TCP keepalive: {self.tcp_keepalive}")
            print(f"    Max pool connections: {self.max_pool_connections}")

    def download_files(
        self,
        bucket: str,
        keys: List[str],
        report_interval_sec: int = 5
    ) -> Dict[str, Any]:
        """
        Download multiple files from S3 using optimized multiprocessing.

        Args:
            bucket: S3 bucket name
            keys: List of S3 object keys to download
            report_interval_sec: Progress reporting interval

        Returns:
            Dict containing performance statistics and downloaded data
        """
        if self.verbose:
            print(f"\nStarting optimized RAM download of {len(keys)} files")
            print(f"Bucket: {bucket}")

        start_time = time.time()

        # Create manager for shared data
        manager = Manager()
        result_queue = manager.Queue()
        data_store = manager.dict()

        # Get file sizes and calculate optimal distribution
        session = self._create_session()
        s3_client = session.client('s3', endpoint_url=self.endpoint_url)

        file_sizes = self._get_file_sizes(s3_client, bucket, keys)
        total_size_gb = sum(file_sizes.values()) / (1024**3)

        if self.verbose:
            print(f"\nTotal download size: {total_size_gb:.2f} GB")
            print(f"Available memory: {psutil.virtual_memory().available / (1024**3):.2f} GB")

        # Check if we have enough memory
        if total_size_gb > self.memory_limit_gb:
            raise MemoryError(
                f"Total download size ({total_size_gb:.2f} GB) exceeds "
                f"memory limit ({self.memory_limit_gb} GB)"
            )

        # Distribute files optimally across processes based on size
        keys_per_process = self._distribute_files(keys, file_sizes)

        if self.verbose:
            print("\nDistributed files across processes:")
            for i, proc_keys in enumerate(keys_per_process):
                if proc_keys:
                    size_gb = sum(file_sizes[k] for k in proc_keys) / (1024**3)
                    print(f"  Process {i}: {len(proc_keys)} files, {size_gb:.2f} GB")

        # Start download processes
        processes = []
        for i in range(self.processes):
            if not keys_per_process[i]:
                continue

            p = Process(
                target=self._process_worker,
                args=(
                    keys_per_process[i],
                    bucket,
                    data_store,
                    result_queue,
                    i,
                    file_sizes,
                )
            )
            p.daemon = True
            p.start()
            processes.append(p)

        # Monitor progress with enhanced reporting
        try:
            self._monitor_progress(processes, result_queue, len(keys), report_interval_sec)
        except KeyboardInterrupt:
            print("\nStopping downloads...")
            for p in processes:
                p.terminate()

        # Wait for processes to finish
        for p in processes:
            p.join(timeout=2)
            if p.is_alive():
                p.terminate()

        # Calculate final statistics
        stats = self._calculate_statistics(
            start_time,
            result_queue,
            data_store,
            len(keys),
            file_sizes
        )

        return stats

    def _create_session(self) -> boto3.Session:
        """Create an optimized boto3 session with enhanced network settings"""
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

        # Configure session with optimized network settings
        config = boto3.Config(
            connect_timeout=self.network_timeout,
            read_timeout=self.network_timeout,
            retries={'max_attempts': self.max_attempts},
            max_pool_connections=self.max_pool_connections,
            tcp_keepalive=self.tcp_keepalive
        )

        return session

    def _get_file_sizes(
        self,
        s3_client: boto3.client,
        bucket: str,
        keys: List[str]
    ) -> Dict[str, int]:
        """Get sizes of all files to be downloaded"""
        file_sizes = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_key = {
                executor.submit(
                    s3_client.head_object,
                    Bucket=bucket,
                    Key=key
                ): key for key in keys
            }

            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    response = future.result()
                    file_sizes[key] = response['ContentLength']
                except Exception as e:
                    print(f"Error getting size for {key}: {e}")
                    file_sizes[key] = 0

        return file_sizes

    def _distribute_files(
        self,
        keys: List[str],
        file_sizes: Dict[str, int]
    ) -> List[List[str]]:
        """Distribute files across processes optimally based on size"""
        # Sort files by size in descending order
        sorted_keys = sorted(keys, key=lambda k: file_sizes[k], reverse=True)

        # Initialize process buckets
        process_buckets = [[] for _ in range(self.processes)]
        process_sizes = [0] * self.processes

        # Distribute files using a modified bin-packing algorithm
        for key in sorted_keys:
            # Find process with smallest current load
            target_process = min(range(self.processes), key=lambda i: process_sizes[i])
            process_buckets[target_process].append(key)
            process_sizes[target_process] += file_sizes[key]

        return process_buckets

    def _process_worker(
        self,
        keys: List[str],
        bucket: str,
        data_store: Dict,
        result_queue: Queue,
        process_id: int,
        file_sizes: Dict[str, int]
    ):
        """Optimized worker process for downloading files"""
        worker_id = f"Process-{process_id}"

        try:
            # Create optimized session and client
            session = self._create_session()
            s3_client = session.client('s3', endpoint_url=self.endpoint_url)

            # Calculate optimal concurrency for this process based on file sizes
            total_size = sum(file_sizes[k] for k in keys)
            optimal_concurrency = min(
                self.concurrency_per_process,
                max(1, int(total_size / (self.multipart_size_mb * 1024 * 1024)))
            )

            if self.verbose:
                print(f"[{worker_id}] Using optimal concurrency: {optimal_concurrency}")

            # Create thread pool with optimal concurrency
            with concurrent.futures.ThreadPoolExecutor(max_workers=optimal_concurrency) as executor:
                futures = []
                for key in keys:
                    future = executor.submit(
                        self._download_file,
                        s3_client,
                        bucket,
                        key,
                        data_store,
                        result_queue,
                        process_id,
                        file_sizes[key]
                    )
                    futures.append(future)

                # Wait for all downloads to complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"[{worker_id}] Download error: {e}")

        except Exception as e:
            print(f"[{worker_id}] Critical error: {e}")
            traceback.print_exc()
            result_queue.put({
                'status': 'process_error',
                'process_id': process_id,
                'error': str(e)
            })

    def _download_file(
        self,
        s3_client: boto3.client,
        bucket: str,
        key: str,
        data_store: Dict,
        result_queue: Queue,
        process_id: int,
        file_size: int
    ):
        """Download a single file with optimized parameters"""
        thread_id = f"Process-{process_id}-Thread-{threading.get_ident()}"

        for attempt in range(self.max_attempts):
            try:
                start_time = time.time()

                # Pre-allocate buffer based on file size
                buffer = io.BytesIO(bytearray(file_size))

                # Configure optimized transfer parameters
                config = boto3.s3.transfer.TransferConfig(
                    multipart_threshold=self.multipart_size_mb * 1024 * 1024,
                    multipart_chunksize=self.multipart_size_mb * 1024 * 1024,
                    max_concurrency=min(20, max(4, int(file_size / (100 * 1024 * 1024)))),
                    use_threads=True,
                    num_download_attempts=self.max_attempts
                )

                # Download file
                s3_client.download_fileobj(
                    Bucket=bucket,
                    Key=key,
                    Fileobj=buffer,
                    Config=config
                )

                # Store data
                buffer.seek(0)
                data_store[key] = buffer.getvalue()

                duration = time.time() - start_time
                mb_per_sec = (file_size / (1024 * 1024)) / duration if duration > 0 else 0

                result_queue.put({
                    'status': 'success',
                    'key': key,
                    'size': file_size,
                    'duration': duration,
                    'mb_per_sec': mb_per_sec,
                    'process_id': process_id
                })

                if self.verbose:
                    print(f"[{thread_id}] Downloaded {key} ({file_size / (1024 * 1024):.2f} MB) "
                          f"at {mb_per_sec:.2f} MB/s")

                return True

            except Exception as e:
                print(f"[{thread_id}] Error downloading {key} (attempt {attempt + 1}/{self.max_attempts}): {e}")
                if attempt < self.max_attempts - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    result_queue.put({
                        'status': 'error',
                        'key': key,
                        'error': str(e),
                        'process_id': process_id
                    })
                    return False

    def _monitor_progress(
        self,
        processes: List[Process],
        result_queue: Queue,
        total_files: int,
        report_interval_sec: int
    ):
        """Monitor download progress with enhanced reporting"""
        completed = 0
        errors = 0
        total_bytes = 0
        start_time = time.time()
        last_report_time = start_time
        last_bytes = 0

        while any(p.is_alive() for p in processes) or not result_queue.empty():
            try:
                while not result_queue.empty():
                    result = result_queue.get_nowait()
                    if result['status'] == 'success':
                        completed += 1
                        total_bytes += result.get('size', 0)
                    elif result['status'] == 'error':
                        errors += 1

                current_time = time.time()
                if current_time - last_report_time >= report_interval_sec:
                    elapsed = current_time - start_time
                    recent_throughput = (total_bytes - last_bytes) / (1024 * 1024 * report_interval_sec)
                    overall_throughput = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0

                    memory_used = psutil.Process().memory_info().rss / (1024 * 1024 * 1024)
                    memory_available = psutil.virtual_memory().available / (1024 * 1024 * 1024)

                    print(f"\nProgress: {completed}/{total_files} files "
                          f"({completed/total_files*100:.1f}%)")
                    print(f"Errors: {errors}")
                    print(f"Throughput: {recent_throughput:.2f} MB/s (recent), "
                          f"{overall_throughput:.2f} MB/s (average)")
                    print(f"Memory: {memory_used:.1f} GB used, {memory_available:.1f} GB available")

                    last_report_time = current_time
                    last_bytes = total_bytes

                time.sleep(0.1)  # Prevent busy waiting

            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"Error in progress monitoring: {e}")
                traceback.print_exc()

    def _calculate_statistics(
        self,
        start_time: float,
        result_queue: Queue,
        data_store: Dict,
        total_files: int,
        file_sizes: Dict[str, int]
    ) -> Dict[str, Any]:
        """Calculate final performance statistics"""
        end_time = time.time()
        duration = end_time - start_time

        # Process any remaining results
        completed = 0
        errors = 0
        total_bytes = 0
        successful_files = []
        error_files = []

        while not result_queue.empty():
            try:
                result = result_queue.get_nowait()
                if result['status'] == 'success':
                    completed += 1
                    total_bytes += result.get('size', 0)
                    successful_files.append(result['key'])
                elif result['status'] == 'error':
                    errors += 1
                    error_files.append(result['key'])
            except:
                break

        # Calculate performance metrics
        mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
        gbits_per_sec = (mb_per_sec * 8) / 1024

        stats = {
            'duration_seconds': duration,
            'files_completed': completed,
            'files_failed': errors,
            'total_files': total_files,
            'bytes_downloaded': total_bytes,
            'mb_per_second': mb_per_sec,
            'gbits_per_second': gbits_per_sec,
            'successful_files': successful_files,
            'error_files': error_files,
            'processes_used': self.processes,
            'concurrency_per_process': self.concurrency_per_process,
            'multipart_size_mb': self.multipart_size_mb,
            'data': data_store
        }

        if self.verbose:
            print("\nDownload Statistics:")
            print(f"  Duration: {duration:.2f} seconds")
            print(f"  Files: {completed}/{total_files} completed, {errors} failed")
            print(f"  Total Data: {total_bytes / (1024**3):.2f} GB")
            print(f"  Throughput: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbps)")
            print(f"  Memory Used: {psutil.Process().memory_info().rss / (1024**3):.2f} GB")

        return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='High Performance S3 Downloader to RAM (Optimized)')
    parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
    parser.add_argument('--access-key', required=True, help='AWS access key ID')
    parser.add_argument('--secret-key', required=True, help='AWS secret access key')
    parser.add_argument('--region', default=None, help='AWS region name')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
    parser.add_argument('--processes', type=int, default=None, help='Number of processes to use')
    parser.add_argument('--concurrency', type=int, default=None, help='Concurrent downloads per process')
    parser.add_argument('--multipart-size-mb', type=int, default=128, help='Multipart chunk size in MB')
    parser.add_argument('--memory-limit-gb', type=float, default=1500, help='Maximum RAM usage in GB')
    parser.add_argument('--report-interval', type=int, default=5, help='Progress reporting interval in seconds')
    parser.add_argument('--max-files', type=int, default=None, help='Maximum number of files to download')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('--save-to-file', help='Save downloaded data to a file (pickle format)')

    args = parser.parse_args()

    # Create optimized downloader
    downloader = HighPerformanceS3DownloaderRAMOptimized(
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
        processes=args.processes,
        concurrency_per_process=args.concurrency,
        multipart_size_mb=args.multipart_size_mb,
        memory_limit_gb=args.memory_limit_gb,
        verbose=args.verbose
    )

    # List objects in bucket
    session = boto3.Session(
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region
    )
    s3_client = session.client('s3', endpoint_url=args.endpoint_url)

    print(f"Listing objects in bucket '{args.bucket}' with prefix '{args.prefix}'...")

    keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
        if 'Contents' in page:
            keys.extend([obj['Key'] for obj in page['Contents']])

    if args.max_files and len(keys) > args.max_files:
        print(f"Limiting to {args.max_files} files (from {len(keys)} total)")
        keys = keys[:args.max_files]
    else:
        print(f"Found {len(keys)} files to download")

    if not keys:
        print("No files found to download.")
        exit(0)

    # Start downloading to RAM
    result = downloader.download_files(
        bucket=args.bucket,
        keys=keys,
        report_interval_sec=args.report_interval
    )

    # Save data to file if requested
    if args.save_to_file:
        import pickle
        print(f"Saving downloaded data to {args.save_to_file}")
        with open(args.save_to_file, 'wb') as f:
            pickle.dump(result['data'], f)
        print("Data saved successfully")

    print("\nDownload complete!")
