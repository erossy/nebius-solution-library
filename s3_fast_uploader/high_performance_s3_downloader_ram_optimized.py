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
from botocore.config import Config
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
        chunk_threshold_mb: int = 1024,  # Files larger than this will use parallel downloading
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
            chunk_threshold_mb: Files larger than this will use parallel downloading (MB)
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
        self.chunk_threshold_mb = chunk_threshold_mb

        # Use provided process count or default to CPU count - 4
        default_processes = max(1, cpu_count() - 4)
        self.processes = processes if processes is not None else default_processes

        # Use provided concurrency or default to 10 threads per process
        self.concurrency_per_process = concurrency_per_process if concurrency_per_process is not None else 10

        # Validate parameters
        total_memory_mb = psutil.virtual_memory().total / (1024 * 1024)
        max_concurrent_chunks = self.processes * self.concurrency_per_process
        memory_per_chunk_mb = multipart_size_mb * 2  # Account for buffers and overhead
        total_required_memory_mb = max_concurrent_chunks * memory_per_chunk_mb

        if total_required_memory_mb > memory_limit_gb * 1024:
            print(f"\nWarning: Current configuration might require up to {total_required_memory_mb/1024:.1f} GB RAM:")
            print(f"  - {self.processes} processes × {self.concurrency_per_process} threads = {max_concurrent_chunks} concurrent chunks")
            print(f"  - Each {multipart_size_mb} MB chunk might use up to {memory_per_chunk_mb} MB RAM")
            print(f"  - Memory limit is set to {memory_limit_gb} GB")

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
            print(f"    Available CPUs: {cpu_count()}")
            print(f"    Available RAM: {psutil.virtual_memory().total / (1024**3):.1f} GB")
            print(f"  Parallel Processing Configuration:")
            print(f"    Processes: {self.processes}")
            print(f"    Threads per process: {self.concurrency_per_process}")
            print(f"    Total concurrent downloads: {self.processes * self.concurrency_per_process}")
            print(f"  Chunk Configuration:")
            print(f"    Chunk size: {self.multipart_size_mb} MB")
            print(f"    Parallel threshold: {self.chunk_threshold_mb} MB")
            print(f"    Max memory usage: {total_required_memory_mb/1024:.1f} GB")
            print(f"    Memory limit: {self.memory_limit_gb} GB")
            print(f"  Network Configuration:")
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
        session, s3_client = self._create_session()

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

    def _create_session(self) -> Tuple[boto3.Session, boto3.client]:
        """Create an optimized boto3 session and client with enhanced network settings"""
        # Create optimized configuration
        config = Config(
            connect_timeout=self.network_timeout,
            read_timeout=self.network_timeout,
            retries={'max_attempts': self.max_attempts},
            max_pool_connections=self.max_pool_connections,
            tcp_keepalive=self.tcp_keepalive
        )

        # Create session with credentials
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

        # Create client with optimized config
        client = session.client('s3', endpoint_url=self.endpoint_url, config=config)

        return session, client

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
            _, s3_client = self._create_session()

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
        """Download a single file with optimized parameters, using parallel downloads for large files"""
        # For large files (>chunk_threshold_mb), use parallel chunk downloading
        LARGE_FILE_THRESHOLD = self.chunk_threshold_mb * 1024 * 1024
        if file_size > LARGE_FILE_THRESHOLD:
            return self._download_large_file(
                s3_client, bucket, key, data_store, result_queue, process_id, file_size
            )
        else:
            return self._download_single_file(
                s3_client, bucket, key, data_store, result_queue, process_id, file_size
            )

    def _download_large_file(
        self,
        s3_client: boto3.client,
        bucket: str,
        key: str,
        data_store: Dict,
        result_queue: Queue,
        process_id: int,
        file_size: int
    ):
        """Download a large file in parallel chunks"""
        thread_id = f"Process-{process_id}-Thread-{threading.get_ident()}"

        try:
            start_time = time.time()

            # Use configured chunk size
            chunk_size = self.multipart_size_mb * 1024 * 1024
            num_chunks = (file_size + chunk_size - 1) // chunk_size

            # Calculate chunk range for this process
            chunks_per_process = (num_chunks + self.processes - 1) // self.processes
            start_chunk = process_id * chunks_per_process
            end_chunk = min(start_chunk + chunks_per_process, num_chunks)

            if self.verbose:
                print(f"[Process-{process_id}] Handling chunks {start_chunk} to {end_chunk-1} "
                      f"of {num_chunks} total chunks ({chunk_size / (1024*1024):.1f}MB each)")

            # Pre-allocate buffer for the entire file
            file_buffer = bytearray(file_size)

            # Download assigned chunks using thread pool
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.concurrency_per_process
            ) as executor:
                futures = []

                # Only process chunks assigned to this process
                for chunk_index in range(start_chunk, end_chunk):
                    start_byte = chunk_index * chunk_size
                    end_byte = min(start_byte + chunk_size, file_size) - 1

                    future = executor.submit(
                        self._download_chunk,
                        s3_client,
                        bucket,
                        key,
                        start_byte,
                        end_byte,
                        file_buffer,
                        thread_id,
                        result_queue,
                        process_id,
                        num_chunks,
                        file_size
                    )
                    futures.append(future)

                # Wait for all chunks to complete
                for future in concurrent.futures.as_completed(futures):
                    if not future.result():
                        raise Exception("Chunk download failed")

            # Store the complete file
            data_store[key] = bytes(file_buffer)

            duration = time.time() - start_time
            mb_per_sec = (file_size / (1024 * 1024)) / duration if duration > 0 else 0

            result_queue.put({
                'status': 'success',
                'key': key,
                'size': file_size,
                'duration': duration,
                'mb_per_sec': mb_per_sec,
                'process_id': process_id,
                'chunks': num_chunks
            })

            if self.verbose:
                print(f"[{thread_id}] Downloaded {key} ({file_size / (1024 * 1024):.2f} MB) "
                      f"in {num_chunks} chunks at {mb_per_sec:.2f} MB/s")

            return True

        except Exception as e:
            print(f"[{thread_id}] Error downloading {key} in parallel: {e}")
            traceback.print_exc()
            result_queue.put({
                'status': 'error',
                'key': key,
                'error': str(e),
                'process_id': process_id
            })
            return False

    def _download_chunk(
        self,
        s3_client: boto3.client,
        bucket: str,
        key: str,
        start_byte: int,
        end_byte: int,
        file_buffer: bytearray,
        thread_id: str,
        result_queue: Queue,
        process_id: int,
        num_chunks: int,
        file_size: int
    ) -> bool:
        """Download a specific byte range of a file"""
        chunk_size = end_byte - start_byte + 1

        for attempt in range(self.max_attempts):
            try:
                response = s3_client.get_object(
                    Bucket=bucket,
                    Key=key,
                    Range=f'bytes={start_byte}-{end_byte}'
                )

                chunk_start_time = time.time()
                chunk_data = response['Body'].read()
                chunk_duration = time.time() - chunk_start_time

                file_buffer[start_byte:end_byte + 1] = chunk_data

                # Calculate chunk transfer rate
                chunk_mb_per_sec = (chunk_size / (1024 * 1024)) / chunk_duration if chunk_duration > 0 else 0

                # Report chunk progress
                result_queue.put({
                    'status': 'chunk_complete',
                    'key': key,
                    'chunk_size': chunk_size,
                    'start_byte': start_byte,
                    'end_byte': end_byte,
                    'process_id': process_id,
                    'thread_id': thread_id,
                    'total_chunks': num_chunks,
                    'total_size': file_size,
                    'chunk_mb_per_sec': chunk_mb_per_sec
                })

                if self.verbose:
                    print(f"[{thread_id}] Downloaded chunk {start_byte}-{end_byte} "
                          f"({chunk_size / (1024*1024):.1f}MB)")

                return True

            except Exception as e:
                print(f"[{thread_id}] Error downloading chunk {start_byte}-{end_byte}: {e}")
                if attempt < self.max_attempts - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    return False

    def _download_single_file(
        self,
        s3_client: boto3.client,
        bucket: str,
        key: str,
        data_store: Dict,
        result_queue: Queue,
        process_id: int,
        file_size: int
    ):
        """Download a single file without parallelization"""
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
        chunks_completed = {}  # Track chunks per file
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
                        if result.get('key') in chunks_completed:
                            del chunks_completed[result['key']]
                    elif result['status'] == 'error':
                        errors += 1
                        if result.get('key') in chunks_completed:
                            del chunks_completed[result['key']]
                    elif result['status'] == 'chunk_complete':
                        key = result['key']
                        if key not in chunks_completed:
                            chunks_completed[key] = {
                                'bytes': 0,
                                'chunks': 0,
                                'total_chunks': result.get('total_chunks', 0),
                                'total_size': result.get('total_size', 0),
                                'chunk_mb_per_sec': 0
                            }
                        chunks_completed[key]['bytes'] += result['chunk_size']
                        chunks_completed[key]['chunks'] += 1
                        chunks_completed[key]['chunk_mb_per_sec'] = result.get('chunk_mb_per_sec', 0)
                        total_bytes += result['chunk_size']

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

                    # Show chunk progress for files in progress
                    if chunks_completed:
                        print("\nFiles in progress:")
                        for key, info in chunks_completed.items():
                            total_chunks = info.get('total_chunks', 0)
                            current_chunks = info.get('chunks', 0)
                            downloaded_mb = info.get('bytes', 0) / (1024*1024)
                            total_mb = info.get('total_size', 0) / (1024*1024)
                            progress = (downloaded_mb / total_mb * 100) if total_mb > 0 else 0

                            chunk_speed = info.get('chunk_mb_per_sec', 0)
                            print(f"  {key}:")
                            print(f"    Progress: {progress:.1f}% ({current_chunks}/{total_chunks} chunks)")
                            print(f"    Downloaded: {downloaded_mb:.1f} MB / {total_mb:.1f} MB")
                            print(f"    Speed: {chunk_speed:.2f} MB/s")

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
    # Performance tuning parameters
    parser.add_argument('--processes', type=int, default=None,
                      help='Number of processes to use (defaults to CPU count - 4)')
    parser.add_argument('--concurrency', type=int, default=10,
                      help='Number of download threads per process (default: 10, recommended: 8-16)')
    parser.add_argument('--chunk-size-mb', type=int, default=128,
                      help='Size of each chunk in MB (default: 128). Larger chunks reduce overhead but require more memory')
    parser.add_argument('--chunk-threshold-mb', type=int, default=1024,
                      help='Files larger than this will use parallel downloading (default: 1024 MB)')
    parser.add_argument('--memory-limit-gb', type=float, default=1500,
                      help='Maximum RAM usage in GB (default: 1500)')
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
        multipart_size_mb=args.chunk_size_mb,
        chunk_threshold_mb=args.chunk_threshold_mb,
        memory_limit_gb=args.memory_limit_gb,
        verbose=args.verbose
    )

    # List objects in bucket using optimized client
    _, s3_client = downloader._create_session()

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
