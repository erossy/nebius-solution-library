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
from multiprocessing import Process, Queue, cpu_count, Manager, shared_memory
from typing import List, Optional, Dict, Any, Union, Tuple
import numpy as np
import traceback
import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

@dataclass
class DownloadStats:
    total_bytes: int = 0
    completed_bytes: int = 0
    start_time: float = 0
    completed_files: int = 0
    failed_files: int = 0
    current_speed: float = 0

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
        self.memory_limit = memory_limit_gb * 1024 * 1024 * 1024  # Convert to bytes

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

        # Initialize shared statistics
        self.manager = Manager()
        self.stats = DownloadStats()
        self.stats_lock = threading.Lock()

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

    async def _download_file_async(self, bucket: str, key: str, file_size: int):
        """Download a single file using async I/O and shared memory"""
        chunks = []
        chunk_size = min(self.chunk_size, file_size // (self.threads_per_process * 2))
        num_chunks = (file_size + chunk_size - 1) // chunk_size

        # Create shared memory for the entire file
        shm = shared_memory.SharedMemory(create=True, size=file_size)

        # Create async S3 client
        s3_client = aioboto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=self.config
        )

        # Create download tasks
        tasks = []
        for i in range(num_chunks):
            start_byte = i * chunk_size
            end_byte = min(start_byte + chunk_size - 1, file_size - 1)
            task = self._download_chunk_async(s3_client, bucket, key, start_byte, end_byte, 
                                            shm.name, start_byte)
            tasks.append(task)

        # Execute all chunks in parallel
        results = await asyncio.gather(*tasks)

        # Check if all chunks downloaded successfully
        if all(results):
            # Create a numpy array from shared memory
            data = np.ndarray(file_size, dtype=np.uint8, buffer=shm.buf)
            return data.tobytes()
        else:
            raise Exception(f"Failed to download all chunks for {key}")

        shm.close()
        shm.unlink()

    def download_files(self, bucket: str, keys: List[str], report_interval_sec: int = 5):
        """Download multiple files in parallel using processes and async I/O"""
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

        self.stats.total_bytes = total_size
        self.stats.start_time = time.time()

        # Create process pool for parallel downloads
        with ProcessPoolExecutor(max_workers=self.processes) as executor:
            futures = []
            for key in keys:
                if key in file_sizes:
                    future = executor.submit(
                        asyncio.run,
                        self._download_file_async(bucket, key, file_sizes[key])
                    )
                    futures.append((key, future))

            # Monitor progress
            completed = 0
            while completed < len(futures):
                completed = sum(1 for _, f in futures if f.done())
                bytes_completed = sum(
                    file_sizes[key] for key, f in futures 
                    if f.done() and not f.exception()
                )

                elapsed = time.time() - self.stats.start_time
                speed = bytes_completed / elapsed if elapsed > 0 else 0

                if self.verbose and elapsed >= report_interval_sec:
                    self.logger.info(
                        f"Progress: {completed}/{len(futures)} files, "
                        f"Speed: {speed/1e9:.2f} GB/s, "
                        f"Completed: {bytes_completed/1e9:.2f} GB"
                    )

            # Collect results
            results = {}
            for key, future in futures:
                try:
                    results[key] = future.result()
                    self.stats.completed_files += 1
                except Exception as e:
                    self.logger.error(f"Error downloading {key}: {str(e)}")
                    self.stats.failed_files += 1

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
