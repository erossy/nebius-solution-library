"""
Ultra-High-Performance S3 Downloader

Optimized for very high-end systems (128+ vCPUs, 1TB+ RAM) to achieve 70+ Gbps download speeds.
Downloads directly to RAM for maximum performance with large objects (2GB+).

Requirements:
- Python 3.8+
- aioboto3
- boto3

Usage:
python ultra_high_performance_s3_downloader.py --bucket your-bucket --prefix your-prefix
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
from concurrent.futures import ProcessPoolExecutor
import argparse
import logging
import traceback
import sys
from typing import List, Dict, Any, Optional, Tuple, Set

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(processName)s] [%(levelname)s] %(message)s',
  handlers=[
    logging.StreamHandler(sys.stdout)
  ]
)


class UltraHighPerformanceS3Downloader:
  """
  Ultra-high-performance S3 downloader optimized for 70+ Gbps throughput on high-end systems.
  Uses aioboto3 for async I/O and multiprocessing for maximum CPU utilization.
  """

  def __init__(
    self,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: Optional[str] = None,
    processes: Optional[int] = None,
    concurrency_per_process: int = 25,
    chunk_size_mb: int = 32,  # Increased chunk size for better throughput
    max_attempts: int = 3,
    chunk_processes: bool = True,  # Whether to download objects in chunks across processes
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
        chunk_size_mb: Size of object chunks in MB when using chunking
        max_attempts: Maximum retry attempts for failed downloads
        chunk_processes: If True, divide large objects into chunks across processes
        verbose: Enable verbose logging
    """
    self.endpoint_url = endpoint_url
    self.aws_access_key_id = aws_access_key_id
    self.aws_secret_access_key = aws_secret_access_key
    self.region_name = region_name

    # Default to 75% of available CPUs to leave some headroom for the system
    self.processes = processes if processes else max(1, int(cpu_count() * 0.75))

    self.concurrency_per_process = concurrency_per_process
    self.chunk_size_mb = chunk_size_mb
    self.max_attempts = max_attempts
    self.chunk_processes = chunk_processes
    self.verbose = verbose

    # Configure logging level
    self.logger = logging.getLogger(__name__)
    if verbose:
      self.logger.setLevel(logging.DEBUG)
    else:
      self.logger.setLevel(logging.INFO)

  async def _calculate_object_chunks(self, bucket: str, key: str, s3_client) -> List[Dict[str, Any]]:
    """
    Calculate chunk ranges for a large object.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        s3_client: aioboto3 S3 client

    Returns:
        List of chunk specifications including start_byte and end_byte
    """
    # Get object size
    response = await s3_client.head_object(Bucket=bucket, Key=key)
    size = response['ContentLength']

    chunk_size = self.chunk_size_mb * 1024 * 1024
    num_chunks = (size + chunk_size - 1) // chunk_size  # Ceiling division

    chunks = []
    for i in range(num_chunks):
      start_byte = i * chunk_size
      end_byte = min(start_byte + chunk_size - 1, size - 1)

      chunks.append({
        'key': key,
        'start_byte': start_byte,
        'end_byte': end_byte,
        'size': end_byte - start_byte + 1
      })

    return chunks

  async def _download_chunk(
    self,
    bucket: str,
    key: str,
    start_byte: int,
    end_byte: int,
    session,
    semaphore: asyncio.Semaphore
  ) -> bytes:
    """
    Download a specific byte range of an object.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        start_byte: Starting byte position
        end_byte: Ending byte position
        session: aioboto3 session
        semaphore: Semaphore to limit concurrent requests

    Returns:
        Bytes containing the chunk data
    """
    async with semaphore:
      for attempt in range(self.max_attempts):
        try:
          byte_range = f'bytes={start_byte}-{end_byte}'
          self.logger.debug(f"Downloading chunk {byte_range} from {key}")

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
            response = await s3_client.get_object(
              Bucket=bucket,
              Key=key,
              Range=byte_range
            )

            # Read all data
            chunk_data = await response['Body'].read()

            if len(chunk_data) != (end_byte - start_byte + 1):
              self.logger.warning(
                f"Chunk size mismatch for {key}: expected {end_byte - start_byte + 1}, "
                f"got {len(chunk_data)} bytes"
              )

            return chunk_data

        except Exception as e:
          self.logger.warning(f"Error downloading chunk {start_byte}-{end_byte} from {key}: {str(e)}")
          if attempt < self.max_attempts - 1:
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
          else:
            raise

  async def _download_object(
    self,
    bucket: str,
    key: str,
    session,
    semaphore: asyncio.Semaphore,
    result_dict: Dict
  ) -> None:
    """
    Download a complete object in one operation (no chunking).

    Args:
        bucket: S3 bucket name
        key: S3 object key
        session: aioboto3 session
        semaphore: Semaphore to limit concurrent requests
        result_dict: Shared dictionary to store results
    """
    async with semaphore:
      start_time = time.time()

      for attempt in range(self.max_attempts):
        try:
          self.logger.debug(f"Downloading complete object {key}")

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
            # Get object size first
            head_response = await s3_client.head_object(Bucket=bucket, Key=key)
            object_size = head_response['ContentLength']

            # Download the object
            response = await s3_client.get_object(Bucket=bucket, Key=key)
            buffer = io.BytesIO()

            # Stream the data in chunks to avoid loading everything into memory at once
            chunk = await response['Body'].read(8 * 1024 * 1024)  # 8MB chunks
            while chunk:
              buffer.write(chunk)
              chunk = await response['Body'].read(8 * 1024 * 1024)

            buffer.seek(0)
            data = buffer.getvalue()

            duration = time.time() - start_time
            mb_per_sec = (object_size / (1024 * 1024)) / duration if duration > 0 else 0

            # Store result in shared dictionary
            result_dict[key] = {
              'data': data,
              'size': object_size,
              'duration': duration,
              'speed_mbps': mb_per_sec
            }

            self.logger.debug(
              f"Downloaded {key} ({object_size / (1024 * 1024):.2f} MB) "
              f"in {duration:.2f}s at {mb_per_sec:.2f} MB/s"
            )

            return

        except Exception as e:
          self.logger.warning(f"Error downloading {key}: {str(e)}")
          if attempt < self.max_attempts - 1:
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
          else:
            raise

  async def _download_chunked_object(
    self,
    bucket: str,
    key: str,
    session,
    semaphore: asyncio.Semaphore,
    result_dict: Dict
  ) -> None:
    """
    Download a large object in chunks and reassemble.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        session: aioboto3 session
        semaphore: Semaphore to limit concurrent requests
        result_dict: Shared dictionary to store results
    """
    start_time = time.time()

    try:
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
        # Calculate chunk ranges
        chunks = await self._calculate_object_chunks(bucket, key, s3_client)

        self.logger.debug(f"Downloading {key} in {len(chunks)} chunks")

        # Create tasks for each chunk
        tasks = []
        for chunk in chunks:
          task = self._download_chunk(
            bucket=bucket,
            key=key,
            start_byte=chunk['start_byte'],
            end_byte=chunk['end_byte'],
            session=session,
            semaphore=semaphore
          )
          tasks.append(task)

        # Wait for all chunks to download
        chunk_data_list = await asyncio.gather(*tasks)

        # Combine chunks
        combined_data = b''.join(chunk_data_list)

        # Calculate metrics
        total_size = sum(len(data) for data in chunk_data_list)
        duration = time.time() - start_time
        mb_per_sec = (total_size / (1024 * 1024)) / duration if duration > 0 else 0

        # Store result
        result_dict[key] = {
          'data': combined_data,
          'size': total_size,
          'duration': duration,
          'speed_mbps': mb_per_sec
        }

        self.logger.debug(
          f"Downloaded {key} ({total_size / (1024 * 1024):.2f} MB) "
          f"in {duration:.2f}s at {mb_per_sec:.2f} MB/s"
        )

    except Exception as e:
      self.logger.error(f"Error in chunked download of {key}: {str(e)}")
      traceback.print_exc()

  async def _download_objects_in_process(
    self,
    bucket: str,
    keys: List[str],
    result_dict: Dict,
    metrics_dict: Dict,
    process_id: int
  ):
    """
    Download a set of objects within a single process.

    Args:
        bucket: S3 bucket name
        keys: List of object keys to download
        result_dict: Shared dictionary to store downloaded data
        metrics_dict: Shared dictionary to store performance metrics
        process_id: ID of the current process
    """
    process_name = f"Process-{process_id}"
    current_process().name = process_name

    self.logger.info(f"[{process_name}] Starting download of {len(keys)} objects")

    start_time = time.time()
    completed = 0
    errors = 0
    total_bytes = 0

    # Create aioboto3 session
    session = aioboto3.Session()

    # Create semaphore to limit concurrency
    semaphore = asyncio.Semaphore(self.concurrency_per_process)

    # Create tasks
    tasks = []
    for key in keys:
      if self.chunk_processes:
        # Use chunked download for large objects
        task = self._download_chunked_object(
          bucket=bucket,
          key=key,
          session=session,
          semaphore=semaphore,
          result_dict=result_dict
        )
      else:
        # Download whole objects
        task = self._download_object(
          bucket=bucket,
          key=key,
          session=session,
          semaphore=semaphore,
          result_dict=result_dict
        )

      tasks.append(task)

    # Execute all tasks
    try:
      # Use gather with return_exceptions to prevent one failure from stopping everything
      results = await asyncio.gather(*tasks, return_exceptions=True)

      # Process results
      for i, result in enumerate(results):
        if isinstance(result, Exception):
          self.logger.error(f"[{process_name}] Error downloading {keys[i]}: {result}")
          errors += 1
        else:
          completed += 1

      # Calculate metrics
      end_time = time.time()
      duration = end_time - start_time

      # Get total bytes from result_dict
      for key in keys:
        if key in result_dict:
          total_bytes += result_dict[key]['size']

      mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
      gbits_per_sec = (mb_per_sec * 8) / 1000

      # Store metrics
      metrics_dict[process_id] = {
        'completed': completed,
        'errors': errors,
        'total_files': len(keys),
        'total_bytes': total_bytes,
        'duration': duration,
        'mb_per_second': mb_per_sec,
        'gbits_per_second': gbits_per_sec
      }

      self.logger.info(
        f"[{process_name}] Completed: {completed}/{len(keys)} files, "
        f"Size: {total_bytes / (1024 ** 3):.2f} GB, "
        f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)"
      )

    except Exception as e:
      self.logger.error(f"[{process_name}] Critical error: {str(e)}")
      traceback.print_exc()

  def _process_worker(
    self,
    bucket: str,
    keys: List[str],
    result_dict: Dict,
    metrics_dict: Dict,
    process_id: int
  ):
    """
    Worker function that runs in each process.

    Args:
        bucket: S3 bucket name
        keys: List of object keys to download
        result_dict: Shared dictionary to store downloaded data
        metrics_dict: Shared dictionary to store performance metrics
        process_id: ID of the current process
    """
    # Set up asyncio event loop for this process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the async download function
    try:
      loop.run_until_complete(
        self._download_objects_in_process(
          bucket=bucket,
          keys=keys,
          result_dict=result_dict,
          metrics_dict=metrics_dict,
          process_id=process_id
        )
      )
    except Exception as e:
      self.logger.error(f"Error in process {process_id}: {str(e)}")
      traceback.print_exc()
    finally:
      loop.close()

  def download_files(
    self,
    bucket: str,
    keys: List[str],
    report_interval_sec: int = 5
  ) -> Dict[str, Any]:
    """
    Download multiple files from S3 using multiprocessing and async I/O for maximum performance.

    Args:
        bucket: S3 bucket name
        keys: List of S3 object keys to download
        report_interval_sec: Interval in seconds for progress reporting

    Returns:
        Dict containing downloaded data and performance statistics
    """
    if not keys:
      self.logger.warning("No keys provided for download")
      return {
        'files_downloaded': 0,
        'total_files': 0,
        'data': {}
      }

    self.logger.info(f"Starting download of {len(keys)} objects from bucket {bucket}")

    # Create manager for shared data
    manager = Manager()
    result_dict = manager.dict()  # Will store downloaded data
    metrics_dict = manager.dict()  # Will store performance metrics

    # Track overall performance
    start_time = time.time()

    # Distribute keys across processes
    keys_per_process = [[] for _ in range(self.processes)]
    for i, key in enumerate(keys):
      keys_per_process[i % self.processes].append(key)

    self.logger.info(f"Distributed {len(keys)} objects across {self.processes} processes")

    # Start download processes
    processes = []
    for i in range(self.processes):
      if not keys_per_process[i]:
        continue

      p = Process(
        target=self._process_worker,
        args=(
          bucket,
          keys_per_process[i],
          result_dict,
          metrics_dict,
          i
        )
      )
      p.daemon = True
      p.start()
      processes.append(p)

    # Setup progress reporting
    last_report_time = time.time()

    try:
      # Wait for processes to complete
      while any(p.is_alive() for p in processes):
        # Check if it's time for a progress report
        current_time = time.time()
        if current_time - last_report_time >= report_interval_sec:
          self._report_progress(
            start_time=start_time,
            metrics_dict=metrics_dict,
            total_files=len(keys)
          )
          last_report_time = current_time

        time.sleep(0.1)

      # Final progress report
      self._report_progress(
        start_time=start_time,
        metrics_dict=metrics_dict,
        total_files=len(keys)
      )

    except KeyboardInterrupt:
      self.logger.warning("Download interrupted by user")
      for p in processes:
        p.terminate()

    # Wait for all processes to finish
    for p in processes:
      p.join(timeout=1)
      if p.is_alive():
        p.terminate()

    # Calculate final statistics
    end_time = time.time()
    duration = end_time - start_time

    # Get metrics from all processes
    completed_files = sum(m.get('completed', 0) for m in metrics_dict.values())
    total_bytes = sum(m.get('total_bytes', 0) for m in metrics_dict.values())
    errors = sum(m.get('errors', 0) for m in metrics_dict.values())

    mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
    gbits_per_sec = (mb_per_sec * 8) / 1000

    # Convert manager.dict to regular dict for return
    data_dict = dict(result_dict)

    # Check for missing files
    missing_keys = set(keys) - set(data_dict.keys())
    if missing_keys:
      self.logger.warning(f"{len(missing_keys)} files failed to download")
      if self.verbose:
        for key in missing_keys:
          self.logger.warning(f"Missing: {key}")

    # Create final result
    stats = {
      'files_downloaded': completed_files,
      'total_files': len(keys),
      'bytes_downloaded': total_bytes,
      'duration_seconds': duration,
      'mb_per_second': mb_per_sec,
      'gbits_per_second': gbits_per_sec,
      'processes_used': self.processes,
      'concurrency_per_process': self.concurrency_per_process,
      'errors': errors,
      'data': data_dict
    }

    # Log final summary
    self.logger.info(f"\nDownload Summary:")
    self.logger.info(f"  Files: {stats['files_downloaded']}/{stats['total_files']}")
    self.logger.info(f"  Total Size: {total_bytes / (1024 ** 3):.2f} GB")
    self.logger.info(f"  Memory Used: {total_bytes / (1024 ** 2):.2f} MB")
    self.logger.info(f"  Duration: {duration:.2f} seconds")
    self.logger.info(f"  Performance: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)")
    if errors > 0:
      self.logger.info(f"  Errors: {errors}")

    return stats

  def _report_progress(
    self,
    start_time: float,
    metrics_dict: Dict,
    total_files: int
  ):
    """
    Report download progress.

    Args:
        start_time: Time when download started
        metrics_dict: Shared dictionary with metrics from all processes
        total_files: Total number of files to download
    """
    current_time = time.time()
    elapsed = current_time - start_time

    # Compute aggregate metrics
    completed_files = sum(m.get('completed', 0) for m in metrics_dict.values())
    total_bytes = sum(m.get('total_bytes', 0) for m in metrics_dict.values())
    errors = sum(m.get('errors', 0) for m in metrics_dict.values())

    if elapsed > 0 and total_bytes > 0:
      mb_downloaded = total_bytes / (1024 * 1024)
      mb_per_sec = mb_downloaded / elapsed
      gbits_per_sec = (mb_per_sec * 8) / 1000

      # Compute percentage complete
      percent_complete = (completed_files / total_files * 100) if total_files > 0 else 0

      # Print progress
      progress_msg = (
        f"Progress: {completed_files}/{total_files} files "
        f"({percent_complete:.1f}%) | "
        f"Size: {mb_downloaded:.2f} MB | "
        f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)"
      )

      if errors > 0:
        progress_msg += f" | Errors: {errors}"

      self.logger.info(progress_msg)


def main():
  """CLI entry point"""
  parser = argparse.ArgumentParser(description='Ultra-High-Performance S3 Downloader (70+ Gbps)')

  # Required arguments
  parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
  parser.add_argument('--access-key', required=True, help='AWS access key ID')
  parser.add_argument('--secret-key', required=True, help='AWS secret access key')
  parser.add_argument('--bucket', required=True, help='S3 bucket name')

  # Optional arguments
  parser.add_argument('--region', default=None, help='AWS region name')
  parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
  parser.add_argument('--processes', type=int, default=None,
                      help='Number of processes to use (default: 75% of CPU cores)')
  parser.add_argument('--concurrency', type=int, default=25, help='Concurrent operations per process')
  parser.add_argument('--chunk-size-mb', type=int, default=32, help='Chunk size for large objects (MB)')
  parser.add_argument('--report-interval', type=int, default=5, help='Progress reporting interval (seconds)')
  parser.add_argument('--max-files', type=int, default=None, help='Maximum number of files to download')
  parser.add_argument('--save-to-file', help='Save downloaded data to a pickle file')
  parser.add_argument('--no-chunking', action='store_true', help='Disable object chunking across processes')
  parser.add_argument('--verbose', action='store_true', help='Enable verbose output')

  args = parser.parse_args()

  # Create downloader
  downloader = UltraHighPerformanceS3Downloader(
    endpoint_url=args.endpoint_url,
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region,
    processes=args.processes,
    concurrency_per_process=args.concurrency,
    chunk_size_mb=args.chunk_size_mb,
    chunk_processes=not args.no_chunking,
    verbose=args.verbose
  )

  # Set up boto3 session for listing objects
  session = boto3.Session(
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region
  )
  s3_client = session.client('s3', endpoint_url=args.endpoint_url)

  logging.info(f"Listing objects in bucket '{args.bucket}' with prefix '{args.prefix}'...")

  # Get list of objects to download
  keys = []
  paginator = s3_client.get_paginator('list_objects_v2')
  for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
    if 'Contents' in page:
      keys.extend([obj['Key'] for obj in page['Contents']])

  if args.max_files and len(keys) > args.max_files:
    logging.info(f"Limiting to {args.max_files} files (from {len(keys)} total)")
    keys = keys[:args.max_files]
  else:
    logging.info(f"Found {len(keys)} files to download")

  if not keys:
    logging.error("No files found to download.")
    return

  # Execute download
  result = downloader.download_files(
    bucket=args.bucket,
    keys=keys,
    report_interval_sec=args.report_interval
  )

  # Save data to file if requested
  if args.save_to_file:
    import pickle
    logging.info(f"Saving downloaded data to {args.save_to_file}")
    with open(args.save_to_file, 'wb') as f:
      pickle.dump(result['data'], f)
    logging.info("Data saved successfully")

  logging.info("Download complete!")


if __name__ == "__main__":
  main()
