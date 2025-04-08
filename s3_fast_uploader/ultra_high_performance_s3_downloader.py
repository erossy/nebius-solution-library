import os
import time
import asyncio
import aioboto3
import boto3
from botocore.config import Config
import concurrent.futures
import threading
import multiprocessing
import psutil
from multiprocessing import Process, Queue, cpu_count, Manager, shared_memory, Value
from typing import List, Optional, Dict, Any, Union, Tuple
import numpy as np
import traceback
import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

class DownloadStats:
    def __init__(self):
        self.total_bytes = Value('q', 0)
        self.completed_bytes = Value('q', 0)
        self.start_time = Value('d', 0.0)
        self.completed_files = Value('i', 0)
        self.failed_files = Value('i', 0)
        self.current_speed = Value('d', 0.0)

class UltraHighPerformanceS3Downloader:
    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str = None,
        processes: int = None,
        threads_per_process: int = None,
        chunk_size_mb: int = 256,
        max_attempts: int = 5,
        verbose: bool = True,
        memory_limit_gb: float = 1500,
        network_timeout: int = 300,
        tcp_keepalive: bool = True,
        max_pool_connections: int = 1000
    ):
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        # Optimize for 128 vCPU system
        self.processes = processes or min(cpu_count() - 2, 64)  # Reserve some CPUs for system
        self.threads_per_process = threads_per_process or 32    # Aggressive threading

        # Optimize chunk size for 2GB files and available memory
        self.chunk_size = chunk_size_mb * 1024 * 1024  # Convert to bytes
        self.max_attempts = max_attempts
        self.verbose = verbose

        # Memory management
        total_memory = psutil.virtual_memory().total
        self.memory_limit = min(memory_limit_gb * 1024 * 1024 * 1024, total_memory * 0.9)  # 90% of total RAM
        self.manager = Manager()
        self.current_memory_usage = self.manager.Value('q', 0)  # shared counter
        self._memory_lock = self.manager.Lock()  # shared lock

        # Configure boto3 client for maximum performance
        self.config = Config(
            max_pool_connections=max_pool_connections,
            connect_timeout=network_timeout,
            read_timeout=network_timeout,
            retries={'max_attempts': max_attempts},
            tcp_keepalive=tcp_keepalive,
            s3={
                'addressing_style': 'virtual'  # Use virtual-hosted style addressing for better performance
            }
        )

        # Initialize shared statistics with multiprocessing Values
        self.stats = DownloadStats()
        self.stats_lock = self.manager.Lock()

        # Dictionary to store downloaded data
        self.downloaded_data = self.manager.dict()

        # Configure logging
        logging.basicConfig(level=logging.INFO if verbose else logging.WARNING)
        self.logger = logging.getLogger(__name__)

    def _create_session(self):
        """Create optimized S3 session and client"""
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

        s3_client = session.client(
            's3',
            endpoint_url=self.endpoint_url,
            config=self.config,
            use_ssl=True
        )

        return session, s3_client

    async def _download_chunk_async(self, s3_client, bucket: str, key: str, start_byte: int, end_byte: int, 
                                  shared_mem_name: str, offset: int):
        """Asynchronously download a chunk of data directly into shared memory"""
        try:
            response = await s3_client.get_object(
                Bucket=bucket,
                Key=key,
                Range=f'bytes={start_byte}-{end_byte}'
            )

            chunk_data = await response['Body'].read()

            # Write directly to shared memory
            shm = shared_memory.SharedMemory(name=shared_mem_name)
            shm_array = np.ndarray((end_byte - start_byte + 1,), dtype=np.uint8, buffer=shm.buf[offset:])
            shm_array[:] = np.frombuffer(chunk_data, dtype=np.uint8)
            shm.close()

            return True
        except Exception as e:
            self.logger.error(f"Error downloading chunk {start_byte}-{end_byte} of {key}: {str(e)}")
            return False

    async def _download_file_async(self, s3_client, bucket: str, key: str, file_size: int):
        """Download a single file using async I/O directly into RAM"""
        # Check if we have enough memory
        with self._memory_lock:
            if self.current_memory_usage.value + file_size > self.memory_limit:
                raise Exception(f"Not enough memory to download {key} ({file_size} bytes)")
            self.current_memory_usage.value += file_size

        try:
            chunk_size = min(self.chunk_size, file_size // (self.threads_per_process * 2))
            num_chunks = (file_size + chunk_size - 1) // chunk_size

            # Create a buffer in RAM
            buffer = memoryview(bytearray(file_size))

            # Create download tasks
            tasks = []
            for i in range(num_chunks):
                start_byte = i * chunk_size
                end_byte = min(start_byte + chunk_size - 1, file_size - 1)
                task = self._download_chunk_async(s3_client, bucket, key, start_byte, end_byte, buffer)
                tasks.append(task)

            # Execute all chunks in parallel
            results = await asyncio.gather(*tasks)
            if all(results):
                # Store the data in shared dictionary
                data_bytes = bytes(buffer)
                self.downloaded_data[key] = data_bytes
                return data_bytes
            else:
                raise Exception(f"Failed to download all chunks for {key}")
        except Exception as e:
            # Release memory on error
            with self._memory_lock:
                self.current_memory_usage.value -= file_size
            raise Exception(f"Error downloading {key}: {str(e)}")
        finally:
            # Release the buffer
            del buffer

    async def _download_chunk_async(self, s3_client, bucket: str, key: str, start_byte: int, end_byte: int, buffer: memoryview):
        """Download a chunk of data into the buffer"""
        try:
            response = await s3_client.get_object(
                Bucket=bucket,
                Key=key,
                Range=f'bytes={start_byte}-{end_byte}'
            )

            chunk_data = await response['Body'].read()
            buffer[start_byte:end_byte + 1] = chunk_data
            return True
        except Exception as e:
            self.logger.error(f"Error downloading chunk {start_byte}-{end_byte} of {key}: {str(e)}")
            return False

    async def _process_download_async(self, bucket: str, keys: List[str], file_sizes: Dict[str, int]):
        """Process a batch of downloads asynchronously"""
        async with aioboto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
            config=self.config
        ) as s3_client:
            tasks = []
            for key in keys:
                if key in file_sizes:
                    task = self._download_file_async(s3_client, bucket, key, file_sizes[key])
                    tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            return dict(zip(keys, results))

    def _process_worker(self, bucket: str, keys: List[str], file_sizes: Dict[str, int]):
        """Worker function for each process"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(self._process_download_async(bucket, keys, file_sizes))
        except Exception as e:
            self.logger.error(f"Process worker error: {str(e)}")
            return {}
        finally:
            loop.close()

    def download_files(self, bucket: str, keys: List[str], report_interval_sec: int = 5) -> Dict[str, bytes]:
        """Download multiple files in parallel using processes and async I/O

        Returns:
            Dict[str, bytes]: Dictionary mapping file keys to their contents as bytes objects.
            All data is kept in RAM without touching disk.
        """
        total_size = 0
        file_sizes = {}

        # Get file sizes
        _, s3_client = self._create_session()
        for key in keys:
            try:
                response = s3_client.head_object(Bucket=bucket, Key=key)
                size = response['ContentLength']
                file_sizes[key] = size
                total_size += size
            except Exception as e:
                self.logger.error(f"Error getting size for {key}: {str(e)}")
                continue

        self.stats.total_bytes.value = total_size
        self.stats.start_time.value = time.time()

        # Distribute keys across processes
        chunks = np.array_split(keys, self.processes) if keys else []

        # Create process pool for parallel downloads
        with ProcessPoolExecutor(max_workers=self.processes) as executor:
            futures = []
            for chunk in chunks:
                future = executor.submit(
                    self._process_worker,
                    bucket,
                    chunk.tolist(),
                    file_sizes
                )
                futures.append(future)

            # Monitor progress
            completed_files = 0
            bytes_completed = 0
            start_time = time.time()
            last_report_time = start_time

            while completed_files < len(keys):
                time.sleep(0.1)  # Prevent busy waiting
                completed_files = 0
                bytes_completed = 0

                # Calculate progress
                for future in futures:
                    if future.done() and not future.exception():
                        result = future.result()
                        completed_files += sum(1 for k, v in result.items() if v is not None and not isinstance(v, Exception))
                        bytes_completed += sum(file_sizes[k] for k, v in result.items() if v is not None and not isinstance(v, Exception))

                current_time = time.time()
                if current_time - last_report_time >= report_interval_sec:
                    elapsed = current_time - start_time
                    speed = bytes_completed / elapsed if elapsed > 0 else 0

                    if self.verbose:
                        with self.stats_lock:
                            self.stats.completed_bytes.value = bytes_completed
                            self.stats.current_speed.value = speed
                            self.logger.info(
                                f"Progress: {completed_files}/{len(keys)} files, "
                                f"Speed: {speed/1e9:.2f} GB/s, "
                                f"Completed: {bytes_completed/1e9:.2f} GB"
                            )
                    last_report_time = current_time

            # Collect results from all processes
            for future in futures:
                try:
                    future.result()  # Wait for all processes to complete
                except Exception as e:
                    self.logger.error(f"Process error: {str(e)}")

            # Get final results from shared dictionary
            results = {}
            for key in keys:
                if key in self.downloaded_data:
                    results[key] = self.downloaded_data[key]

            # Update statistics
            with self.stats_lock:
                self.stats.completed_files.value = len(results)
                self.stats.failed_files.value = len(keys) - len(results)

            # Clean up memory
            with self._memory_lock:
                self.current_memory_usage.value = 0
            self.downloaded_data.clear()

        return results

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Ultra High Performance S3 Downloader')
    parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
    parser.add_argument('--access-key', required=True, help='AWS access key ID')
    parser.add_argument('--secret-key', required=True, help='AWS secret access key')
    parser.add_argument('--region', default=None, help='AWS region name')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
    parser.add_argument('--processes', type=int, default=None, help='Number of processes')
    parser.add_argument('--threads', type=int, default=None, help='Number of threads per process')
    parser.add_argument('--chunk-size-mb', type=int, default=256, help='Chunk size in MB')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')

    args = parser.parse_args()

    # Create downloader instance
    downloader = UltraHighPerformanceS3Downloader(
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
        processes=args.processes,
        threads_per_process=args.threads,
        chunk_size_mb=args.chunk_size_mb,
        verbose=args.verbose
    )

    # List objects in bucket
    _, s3_client = downloader._create_session()
    print(f"Listing objects in bucket '{args.bucket}' with prefix '{args.prefix}'...")

    keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
        if 'Contents' in page:
            keys.extend([obj['Key'] for obj in page['Contents']])

    print(f"Found {len(keys)} files to download")

    if not keys:
        print("No files found to download.")
        exit(0)

    # Start downloading
    result = downloader.download_files(
        bucket=args.bucket,
        keys=keys
    )

    print(f"Download completed. Successfully downloaded {downloader.stats.completed_files} files.")
    if downloader.stats.failed_files > 0:
        print(f"Failed to download {downloader.stats.failed_files} files.")
