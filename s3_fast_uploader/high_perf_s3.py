"""
Chunk-Based Ultra-High-Performance S3 Downloader

Optimized for very high-end systems (128+ vCPUs, 1600GB RAM) to achieve 70+ Gbps download speeds.
Downloads directly to RAM with chunk-level parallelism across processes.

Key features:
- Distributes chunks (not files) across processes for maximum parallelism
- Each process handles chunks from multiple files concurrently
- Automatically reassembles chunks into complete files
- Optimized for large files (2GB+) with high throughput

Requirements:
- Python 3.8+
- aioboto3
- boto3

Usage:
python chunk_based_ultra_s3_downloader.py --bucket your-bucket --prefix your-prefix
"""

import os
import time
import asyncio
import aioboto3
import boto3
import io
import numpy as np
import multiprocessing
from multiprocessing import Process, Manager, cpu_count, current_process
import argparse
import logging
import traceback
import sys
import pickle
import json
from typing import List, Dict, Any, Optional, Tuple, Set


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(processName)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Set up logger
logger = logging.getLogger("ChunkDownloader")


class ChunkBasedUltraS3Downloader:
    """
    Ultra-high-performance S3 downloader that distributes chunks across processes.
    Optimized for high throughput on high-end systems with large files.
    """

    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: Optional[str] = None,
        processes: Optional[int] = None,
        concurrency_per_process: int = 50,
        chunk_size_mb: int = 64,
        max_attempts: int = 3,
        verbose: bool = False
    ):
        """
        Initialize the downloader with S3 credentials and performance parameters.

        Args:
            endpoint_url: S3 endpoint URL
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret access key
            region_name: AWS region name (optional)
            processes: Number of processes to use (defaults to CPU count)
            concurrency_per_process: Concurrent async operations per process
            chunk_size_mb: Size of object chunks in MB
            max_attempts: Maximum retry attempts for failed downloads
            verbose: Enable verbose logging
        """
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        # Default to number of available CPUs or specified value
        self.processes = processes if processes else max(1, cpu_count())

        self.concurrency_per_process = concurrency_per_process
        self.chunk_size_mb = chunk_size_mb
        self.max_attempts = max_attempts
        self.verbose = verbose

        # Configure logging
        if verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        logger.info(f"Initialized ChunkBasedUltraS3Downloader:")
        logger.info(f"  Processes: {self.processes}")
        logger.info(f"  Concurrency per process: {concurrency_per_process}")
        logger.info(f"  Chunk size: {chunk_size_mb} MB")
        logger.info(f"  Endpoint URL: {endpoint_url}")

    def _calculate_chunks(self, keys: List[str], bucket: str) -> Tuple[List[Dict], Dict]:
        """
        Calculate all chunks for all files.

        Args:
            keys: List of object keys
            bucket: S3 bucket name

        Returns:
            Tuple of (list of all chunks, dict of file sizes)
        """
        logger.info(f"Calculating chunks for {len(keys)} files...")

        # Create S3 client
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
        s3_client = session.client('s3', endpoint_url=self.endpoint_url)

        all_chunks = []
        file_sizes = {}
        chunk_size_bytes = self.chunk_size_mb * 1024 * 1024

        for key in keys:
            # Get object size
            try:
                response = s3_client.head_object(Bucket=bucket, Key=key)
                size = response['ContentLength']
                file_sizes[key] = size

                # Calculate chunks for this file
                num_chunks = (size + chunk_size_bytes - 1) // chunk_size_bytes  # Ceiling division

                logger.debug(f"File {key}: {size / (1024**2):.2f} MB, {num_chunks} chunks")

                for i in range(num_chunks):
                    start_byte = i * chunk_size_bytes
                    end_byte = min(start_byte + chunk_size_bytes - 1, size - 1)

                    all_chunks.append({
                        'key': key,
                        'start_byte': start_byte,
                        'end_byte': end_byte,
                        'chunk_index': i,
                        'total_chunks': num_chunks,
                        'size': end_byte - start_byte + 1
                    })
            except Exception as e:
                logger.error(f"Error getting size for {key}: {str(e)}")

        logger.info(f"Calculated {len(all_chunks)} chunks across {len(keys)} files")
        return all_chunks, file_sizes

    def _chunk_worker(
        self,
        bucket: str,
        chunks: List[Dict],
        chunk_data_dict: Dict,
        metrics_dict: Dict,
        process_id: int
    ):
        """
        Worker function to download assigned chunks.

        Args:
            bucket: S3 bucket name
            chunks: List of chunk specifications to download
            chunk_data_dict: Shared dictionary to store downloaded chunks
            metrics_dict: Shared dictionary to store performance metrics
            process_id: ID of the current process
        """
        process_name = f"Process-{process_id}"
        current_process().name = process_name

        logger.info(f"[{process_name}] Starting worker with {len(chunks)} chunks to download")

        # Configure process-local event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Run the async download function
        try:
            loop.run_until_complete(
                self._download_chunks_async(
                    bucket=bucket,
                    chunks=chunks,
                    chunk_data_dict=chunk_data_dict,
                    metrics_dict=metrics_dict,
                    process_id=process_id
                )
            )
        except Exception as e:
            logger.error(f"[{process_name}] Critical error: {str(e)}")
            traceback.print_exc()
        finally:
            loop.close()

    async def _download_chunks_async(
        self,
        bucket: str,
        chunks: List[Dict],
        chunk_data_dict: Dict,
        metrics_dict: Dict,
        process_id: int
    ):
        """
        Asynchronously download multiple chunks within a process.

        Args:
            bucket: S3 bucket name
            chunks: List of chunk specifications to download
            chunk_data_dict: Shared dictionary to store downloaded chunks
            metrics_dict: Shared dictionary to store performance metrics
            process_id: ID of the current process
        """
        process_name = f"Process-{process_id}"
        start_time = time.time()

        # Create aioboto3 session
        session = aioboto3.Session()

        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.concurrency_per_process)

        # Track progress
        completed_chunks = 0
        failed_chunks = 0
        total_bytes_downloaded = 0
        download_times = []

        async def download_chunk(chunk):
            nonlocal completed_chunks, failed_chunks, total_bytes_downloaded

            key = chunk['key']
            start_byte = chunk['start_byte']
            end_byte = chunk['end_byte']
            chunk_index = chunk['chunk_index']
            chunk_size = chunk['size']

            # Create a unique key for this chunk
            chunk_key = f"{key}_{chunk_index}"

            async with semaphore:
                for attempt in range(self.max_attempts):
                    try:
                        chunk_start_time = time.time()

                        # Create byte range string
                        byte_range = f'bytes={start_byte}-{end_byte}'

                        if self.verbose:
                            logger.debug(f"[{process_name}] Downloading {key} chunk {chunk_index+1}/{chunk['total_chunks']} ({byte_range})")

                        # Create S3 client
                        async with session.client(
                            's3',
                            endpoint_url=self.endpoint_url,
                            region_name=self.region_name,
                            aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key,
                            config=boto3.session.Config(
                                signature_version='s3v4',
                                max_pool_connections=self.concurrency_per_process * 2
                            )
                        ) as s3_client:
                            # Download chunk
                            response = await s3_client.get_object(
                                Bucket=bucket,
                                Key=key,
                                Range=byte_range
                            )

                            # Read chunk data
                            chunk_data = await response['Body'].read()

                            # Verify size
                            if len(chunk_data) != chunk_size:
                                logger.warning(
                                    f"[{process_name}] Size mismatch for {key} chunk {chunk_index}: "
                                    f"expected {chunk_size}, got {len(chunk_data)}"
                                )

                            # Store chunk in shared dictionary
                            chunk_data_dict[chunk_key] = chunk_data

                            # Update metrics
                            chunk_duration = time.time() - chunk_start_time
                            download_times.append(chunk_duration)
                            completed_chunks += 1
                            total_bytes_downloaded += len(chunk_data)

                            if self.verbose:
                                mb_per_sec = (len(chunk_data) / (1024 * 1024)) / chunk_duration
                                logger.debug(
                                    f"[{process_name}] Downloaded {key} chunk {chunk_index+1}/{chunk['total_chunks']} "
                                    f"in {chunk_duration:.2f}s ({mb_per_sec:.2f} MB/s)"
                                )

                            # Success - return from function
                            return

                    except Exception as e:
                        if attempt < self.max_attempts - 1:
                            # Log and retry
                            logger.warning(
                                f"[{process_name}] Error downloading {key} chunk {chunk_index} (attempt {attempt+1}/{self.max_attempts}): {str(e)}"
                            )
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            # Final attempt failed
                            logger.error(f"[{process_name}] Failed to download {key} chunk {chunk_index} after {self.max_attempts} attempts")
                            failed_chunks += 1
                            raise

        # Create tasks for all chunks
        tasks = []
        for chunk in chunks:
            task = asyncio.create_task(download_chunk(chunk))
            tasks.append(task)

        # Execute all tasks with progress tracking
        logger.info(f"[{process_name}] Downloading {len(chunks)} chunks with {self.concurrency_per_process} concurrency")

        # Wait for all tasks to complete (with error handling)
        completed_count = 0
        pending = set(tasks)
        while pending:
            # Wait for some tasks to complete
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=10  # Check status every 10 seconds
            )

            # Process completed tasks
            for task in done:
                try:
                    await task
                    completed_count += 1

                    # Periodically log progress
                    if completed_count % 20 == 0 or completed_count == len(chunks):
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            mb_downloaded = total_bytes_downloaded / (1024 * 1024)
                            mb_per_sec = mb_downloaded / elapsed
                            gbits_per_sec = (mb_per_sec * 8) / 1000

                            logger.info(
                                f"[{process_name}] Progress: {completed_count}/{len(chunks)} chunks "
                                f"({completed_count/len(chunks)*100:.1f}%) | "
                                f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)"
                            )

                except Exception as e:
                    logger.error(f"[{process_name}] Task error: {str(e)}")

            # If no tasks completed in this iteration, yield control to let other tasks make progress
            if not done:
                await asyncio.sleep(0.1)

        # Calculate process performance metrics
        elapsed = time.time() - start_time

        # Compute performance metrics
        if completed_chunks > 0:
            avg_download_time = sum(download_times) / len(download_times)
            std_download_time = np.std(download_times) if len(download_times) > 1 else 0
        else:
            avg_download_time = 0
            std_download_time = 0

        mb_total = total_bytes_downloaded / (1024 * 1024)
        mb_per_sec = mb_total / elapsed if elapsed > 0 else 0
        gbits_per_sec = (mb_per_sec * 8) / 1000

        # Store process metrics in shared dictionary
        metrics_dict[process_id] = {
            'completed_chunks': completed_chunks,
            'failed_chunks': failed_chunks,
            'total_chunks': len(chunks),
            'bytes_downloaded': total_bytes_downloaded,
            'elapsed_seconds': elapsed,
            'avg_chunk_time': avg_download_time,
            'std_chunk_time': std_download_time,
            'mb_per_second': mb_per_sec,
            'gbits_per_second': gbits_per_sec,
        }

        logger.info(
            f"[{process_name}] Completed: {completed_chunks}/{len(chunks)} chunks | "
            f"Failed: {failed_chunks} | "
            f"Total: {mb_total:.2f} MB | "
            f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)"
        )

    def download_files(
        self,
        bucket: str,
        keys: List[str],
        report_interval_sec: int = 5
    ) -> Dict[str, Any]:
        """
        Download files from S3 by distributing chunks across processes.

        Args:
            bucket: S3 bucket name
            keys: List of object keys to download
            report_interval_sec: Interval for progress reporting in seconds

        Returns:
            Dictionary with downloaded data and performance statistics
        """
        if not keys:
            logger.warning("No keys provided for download")
            return {'files_downloaded': 0, 'total_files': 0, 'data': {}}

        start_time = time.time()
        logger.info(f"Starting download of {len(keys)} files from bucket {bucket}")

        # Calculate all chunks for all files
        all_chunks, file_sizes = self._calculate_chunks(keys, bucket)

        if not all_chunks:
            logger.error("No valid chunks calculated. Check file sizes and permissions.")
            return {'files_downloaded': 0, 'total_files': len(keys), 'data': {}}

        # Create shared dictionaries for results and metrics
        manager = Manager()
        chunk_data_dict = manager.dict()  # Store downloaded chunks
        metrics_dict = manager.dict()     # Store process metrics

        # Distribute chunks evenly across processes
        chunks_per_process = [[] for _ in range(self.processes)]
        for i, chunk in enumerate(all_chunks):
            chunks_per_process[i % self.processes].append(chunk)

        logger.info(f"Distributing {len(all_chunks)} chunks across {self.processes} processes")
        active_processes = 0
        for i, process_chunks in enumerate(chunks_per_process):
            if process_chunks:
                active_processes += 1
                logger.debug(f"Process {i}: {len(process_chunks)} chunks")

        logger.info(f"Starting {active_processes} active processes")

        # Start download processes
        processes = []
        for i in range(self.processes):
            if not chunks_per_process[i]:
                continue

            p = Process(
                target=self._chunk_worker,
                args=(
                    bucket,
                    chunks_per_process[i],
                    chunk_data_dict,
                    metrics_dict,
                    i
                )
            )
            p.daemon = True
            p.start()
            processes.append(p)

        # Monitor progress
        last_report_time = time.time()
        try:
            # Wait for processes to complete, reporting progress
            while any(p.is_alive() for p in processes):
                current_time = time.time()

                # Report progress periodically
                if current_time - last_report_time >= report_interval_sec:
                    self._report_progress(
                        start_time=start_time,
                        metrics_dict=metrics_dict,
                        all_chunks=all_chunks,
                        chunk_data_dict=chunk_data_dict
                    )
                    last_report_time = current_time

                # Avoid busy-waiting
                time.sleep(0.1)

            # Final progress report
            self._report_progress(
                start_time=start_time,
                metrics_dict=metrics_dict,
                all_chunks=all_chunks,
                chunk_data_dict=chunk_data_dict
            )

        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
            for p in processes:
                p.terminate()

        # Wait for processes to finish
        logger.info("All processes completed, finalizing results...")
        for p in processes:
            logger.debug(f"Waiting for process {p.pid} to finish...")
            p.join(timeout=5)  # Wait up to 5 seconds
            if p.is_alive():
                logger.warning(f"Process {p.pid} still running after timeout, terminating")
                p.terminate()

        # Reassemble chunks into complete files
        logger.info("Reassembling chunks into complete files...")
        result_dict = {}
        successful_files = 0
        failed_files = 0
        reassembly_start_time = time.time()

        for key in keys:
            # Check if this file had any chunks
            if key not in file_sizes:
                logger.warning(f"File {key} was not processed (missing size information)")
                failed_files += 1
                continue

            # Get total expected chunks
            file_size = file_sizes[key]
            chunk_size_bytes = self.chunk_size_mb * 1024 * 1024
            expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes

            # Collect chunks for this file
            file_chunks = []
            for i in range(expected_chunks):
                chunk_key = f"{key}_{i}"
                if chunk_key in chunk_data_dict:
                    file_chunks.append((i, chunk_data_dict[chunk_key]))
                else:
                    logger.warning(f"Missing chunk {i} for file {key}")

            # Check if we have all chunks
            if len(file_chunks) == expected_chunks:
                # Sort chunks by index
                file_chunks.sort(key=lambda x: x[0])

                # Combine chunk data
                combined_data = b''.join(chunk[1] for chunk in file_chunks)

                # Verify size
                if len(combined_data) != file_size:
                    logger.warning(
                        f"Size mismatch for reassembled file {key}: "
                        f"expected {file_size}, got {len(combined_data)}"
                    )

                # Store in result
                result_dict[key] = combined_data
                successful_files += 1

                if self.verbose:
                    logger.debug(f"Reassembled {key} from {len(file_chunks)} chunks")
            else:
                logger.warning(
                    f"Incomplete file {key}: have {len(file_chunks)}/{expected_chunks} chunks"
                )
                failed_files += 1

        reassembly_time = time.time() - reassembly_start_time
        logger.info(f"Reassembly completed in {reassembly_time:.2f}s")

        # Calculate final statistics
        end_time = time.time()
        total_duration = end_time - start_time

        # Aggregate metrics from all processes
        completed_chunks = sum(m.get('completed_chunks', 0) for m in metrics_dict.values())
        failed_chunks = sum(m.get('failed_chunks', 0) for m in metrics_dict.values())
        total_bytes = sum(m.get('bytes_downloaded', 0) for m in metrics_dict.values())

        mb_downloaded = total_bytes / (1024 * 1024)
        mb_per_sec = mb_downloaded / total_duration if total_duration > 0 else 0
        gbits_per_sec = (mb_per_sec * 8) / 1000

        # Build results
        stats = {
            'files_downloaded': successful_files,
            'files_failed': failed_files,
            'total_files': len(keys),
            'bytes_downloaded': total_bytes,
            'chunks_completed': completed_chunks,
            'chunks_failed': failed_chunks,
            'total_chunks': len(all_chunks),
            'duration_seconds': total_duration,
            'reassembly_seconds': reassembly_time,
            'mb_per_second': mb_per_sec,
            'gbits_per_second': gbits_per_sec,
            'processes_used': len(processes),
            'concurrency_per_process': self.concurrency_per_process,
            'chunk_size_mb': self.chunk_size_mb,
            'data': result_dict
        }

        # Final summary
        logger.info("\nDownload Summary:")
        logger.info(f"  Files: {stats['files_downloaded']}/{stats['total_files']} ({stats['files_failed']} failed)")
        logger.info(f"  Chunks: {stats['chunks_completed']}/{stats['total_chunks']} ({stats['chunks_failed']} failed)")
        logger.info(f"  Total Size: {mb_downloaded:.2f} MB")
        logger.info(f"  Duration: {total_duration:.2f} seconds (incl. {reassembly_time:.2f}s reassembly)")
        logger.info(f"  Performance: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)")

        return stats

    def _report_progress(
        self,
        start_time: float,
        metrics_dict: Dict,
        all_chunks: List[Dict],
        chunk_data_dict: Dict
    ):
        """
        Report download progress.

        Args:
            start_time: Time when download started
            metrics_dict: Dictionary with metrics from all processes
            all_chunks: List of all chunks being downloaded
            chunk_data_dict: Dictionary containing downloaded chunks
        """
        current_time = time.time()
        elapsed = current_time - start_time

        # Calculate aggregate metrics
        completed_chunks = sum(m.get('completed_chunks', 0) for m in metrics_dict.values())
        failed_chunks = sum(m.get('failed_chunks', 0) for m in metrics_dict.values())
        total_chunks = len(all_chunks)

        # Calculate downloaded bytes based on actual chunks in dictionary
        total_bytes = sum(len(data) for data in chunk_data_dict.values())

        # Calculate performance metrics
        mb_downloaded = total_bytes / (1024 * 1024)
        mb_per_sec = mb_downloaded / elapsed if elapsed > 0 else 0
        gbits_per_sec = (mb_per_sec * 8) / 1000

        # Percent complete
        percent_complete = completed_chunks / total_chunks * 100 if total_chunks > 0 else 0

        # Log progress
        progress_message = (
            f"Progress: {completed_chunks}/{total_chunks} chunks "
            f"({percent_complete:.1f}%) | "
            f"Size: {mb_downloaded:.2f} MB | "
            f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)"
        )

        if failed_chunks > 0:
            progress_message += f" | Failed chunks: {failed_chunks}"

        logger.info(progress_message)

        # If verbose, also show per-process metrics
        if self.verbose and metrics_dict:
            logger.debug("Per-process metrics:")
            for pid, metrics in metrics_dict.items():
                if 'mb_per_second' in metrics:
                    logger.debug(
                        f"  Process {pid}: {metrics.get('completed_chunks', 0)}/{metrics.get('total_chunks', 0)} chunks, "
                        f"{metrics['mb_per_second']:.2f} MB/s ({metrics['gbits_per_second']:.2f} Gbit/s)"
                    )


def main():
    """Command-line entry point"""
    parser = argparse.ArgumentParser(description='Chunk-Based Ultra-High-Performance S3 Downloader (70+ Gbps)')

    # Required arguments
    parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
    parser.add_argument('--access-key', required=True, help='AWS access key ID')
    parser.add_argument('--secret-key', required=True, help='AWS secret access key')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')

    # Optional arguments
    parser.add_argument('--region', default=None, help='AWS region name')
    parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
    parser.add_argument('--processes', type=int, default=None, help='Number of processes to use (default: all CPU cores)')
    parser.add_argument('--concurrency', type=int, default=50, help='Concurrent downloads per process')
    parser.add_argument('--chunk-size-mb', type=int, default=64, help='Size of each chunk in MB')
    parser.add_argument('--report-interval', type=int, default=5, help='Progress reporting interval in seconds')
    parser.add_argument('--max-files', type=int, default=None, help='Maximum number of files to download')
    parser.add_argument('--save-to-file', help='Save downloaded data to a pickle file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')

    args = parser.parse_args()

    # Create downloader
    downloader = ChunkBasedUltraS3Downloader(
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
        processes=args.processes,
        concurrency_per_process=args.concurrency,
        chunk_size_mb=args.chunk_size_mb,
        verbose=args.verbose
    )

    # Set up boto3 session for listing objects
    session = boto3.Session(
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region
    )
    s3_client = session.client('s3', endpoint_url=args.endpoint_url)

    logger.info(f"Listing objects in bucket '{args.bucket}' with prefix '{args.prefix}'...")

    # List objects to download
    keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
        if 'Contents' in page:
            keys.extend([obj['Key'] for obj in page['Contents']])

    if args.max_files and len(keys) > args.max_files:
        logger.info(f"Limiting to {args.max_files} files (from {len(keys)} total)")
        keys = keys[:args.max_files]
    else:
        logger.info(f"Found {len(keys)} files to download")

    if not keys:
        logger.error("No files found to download.")
        return

    # Start downloading
    result = downloader.download_files(
        bucket=args.bucket,
        keys=keys,
        report_interval_sec=args.report_interval
    )

    # Save data to file if requested
    if args.save_to_file:
        logger.info(f"Saving downloaded data to {args.save_to_file}")
        with open(args.save_to_file, 'wb') as f:
            pickle.dump(result['data'], f)
        logger.info(f"Saved {len(result['data'])} files to {args.save_to_file}")

    # Save performance metrics to JSON
    metrics_file = 'download_metrics.json'
    metrics = {k: v for k, v in result.items() if k != 'data'}
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Saved performance metrics to {metrics_file}")

    logger.info("Download complete!")


if __name__ == "__main__":
    main()
