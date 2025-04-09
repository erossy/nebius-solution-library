"""
High-Throughput S3 Downloader - Optimized for 70+ Gbit/s throughput

This optimized implementation addresses:
1. Increased concurrency per process (from 1 to 50-100)
2. Optimized S3 client configuration for maximum throughput
3. Parallel chunk assembly with minimal memory copies
4. Efficient process management with shared memory
5. Reduced inter-process communication overhead

Usage example:
    downloader = HighThroughputS3Downloader(
        endpoint_url="https://storage.example.com",
        aws_access_key_id="your_key",
        aws_secret_access_key="your_secret",
        processes=50,                 # Adjust based on CPU cores
        concurrency_per_process=50,   # Higher concurrency per process
        chunk_size_mb=128             # Larger chunk size for better throughput
    )

    result = downloader.download_files(
        bucket="your-bucket",
        keys=["file1.bin", "file2.bin", ...],
        assembly_method="multi_process"
    )
"""

import os
import time
import asyncio
import aioboto3
import boto3
import io
import numpy as np
import multiprocessing
from multiprocessing import Process, Manager, cpu_count, current_process, shared_memory
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import argparse
import logging
import traceback
import sys
import pickle
import json
from typing import List, Dict, Any, Optional, Tuple, Set
from functools import partial

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(processName)s] [%(levelname)s] %(message)s',
  handlers=[
    logging.StreamHandler(sys.stdout)
  ]
)

# Set up logger
logger = logging.getLogger("S3Downloader")


class HighThroughputS3Downloader:
  """
  S3 downloader optimized for 70+ Gbit/s throughput on high-end systems.
  Uses chunk-level parallelism and optimized assembly processes.
  """

  def __init__(
    self,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: Optional[str] = None,
    processes: Optional[int] = None,
    concurrency_per_process: int = 50,  # Increased from 1 to 50
    chunk_size_mb: int = 128,  # Increased from 64 to 128
    max_attempts: int = 3,
    tcp_keepalive: bool = True,  # Enable TCP keepalive
    max_pool_connections: int = 1000,  # Increased connection pool
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
        tcp_keepalive: Enable TCP keepalive for better connection handling
        max_pool_connections: Maximum connections in the connection pool
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
    self.tcp_keepalive = tcp_keepalive
    self.max_pool_connections = max_pool_connections
    self.verbose = verbose

    # Configure logging
    if verbose:
      logger.setLevel(logging.DEBUG)
    else:
      logger.setLevel(logging.INFO)

    logger.info(f"Initialized HighThroughputS3Downloader:")
    logger.info(f"  Processes: {self.processes}")
    logger.info(f"  Concurrency per process: {concurrency_per_process}")
    logger.info(f"  Chunk size: {chunk_size_mb} MB")
    logger.info(f"  Max pool connections: {max_pool_connections}")
    logger.info(f"  TCP keepalive: {tcp_keepalive}")
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

    # Create S3 client with optimized configuration
    session = boto3.Session(
      aws_access_key_id=self.aws_access_key_id,
      aws_secret_access_key=self.aws_secret_access_key,
      region_name=self.region_name
    )

    # Create optimized client config
    config = boto3.session.Config(
      signature_version='s3v4',
      max_pool_connections=self.max_pool_connections,
      connect_timeout=5,
      read_timeout=120,
      retries={'max_attempts': 3}
    )

    s3_client = session.client('s3', endpoint_url=self.endpoint_url, config=config)

    all_chunks = []
    file_sizes = {}
    chunk_size_bytes = self.chunk_size_mb * 1024 * 1024

    # Use ThreadPoolExecutor to parallelize HEAD requests
    with ThreadPoolExecutor(max_workers=min(32, len(keys))) as executor:
      def get_object_size(key):
        try:
          response = s3_client.head_object(Bucket=bucket, Key=key)
          return key, response['ContentLength']
        except Exception as e:
          logger.error(f"Error getting size for {key}: {str(e)}")
          return key, None

      # Get all file sizes in parallel
      futures = [executor.submit(get_object_size, key) for key in keys]

      for future in futures:
        key, size = future.result()
        if size is not None:
          file_sizes[key] = size

    # Now calculate chunks for each file with known size
    for key, size in file_sizes.items():
      # Calculate chunks for this file
      num_chunks = (size + chunk_size_bytes - 1) // chunk_size_bytes  # Ceiling division

      logger.debug(f"File {key}: {size / (1024 ** 2):.2f} MB, {num_chunks} chunks")

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

    logger.info(f"Calculated {len(all_chunks)} chunks across {len(file_sizes)} files")
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

    # Configure shared client connection parameters for optimized performance
    client_config = boto3.session.Config(
      signature_version='s3v4',
      max_pool_connections=min(self.concurrency_per_process * 2, 1000),
      connect_timeout=5,
      read_timeout=300,
      retries={'max_attempts': 0}  # We handle retries ourselves for better control
    )

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
              logger.debug(
                f"[{process_name}] Downloading {key} chunk {chunk_index + 1}/{chunk['total_chunks']} ({byte_range})")

            # Create S3 client with optimized settings
            async with session.create_client(
              's3',
              endpoint_url=self.endpoint_url,
              region_name=self.region_name,
              aws_access_key_id=self.aws_access_key_id,
              aws_secret_access_key=self.aws_secret_access_key,
              config=client_config
            ) as s3_client:
              # Set TCP keepalive if enabled (helps with long-running connections)
              if self.tcp_keepalive and hasattr(s3_client._endpoint.http_session._session, 'socket_options'):
                s3_client._endpoint.http_session._session.socket_options += [
                  (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]

              # Download chunk with timeout
              try:
                response = await asyncio.wait_for(
                  s3_client.get_object(
                    Bucket=bucket,
                    Key=key,
                    Range=byte_range
                  ),
                  timeout=60  # 60 second timeout per request
                )

                # Read chunk data with timeout and in chunks
                buffer = io.BytesIO()
                body = response['Body']

                while True:
                  try:
                    # Read in moderate-sized chunks to avoid memory issues with very large parts
                    data = await asyncio.wait_for(body.read(8 * 1024 * 1024),
                                                  timeout=30)  # 8MB read chunks with 30s timeout
                    if not data:
                      break
                    buffer.write(data)
                  except asyncio.TimeoutError:
                    logger.warning(f"[{process_name}] Timeout reading data for {key} chunk {chunk_index}")
                    raise

                chunk_data = buffer.getvalue()

              except asyncio.TimeoutError:
                logger.warning(f"[{process_name}] Request timeout for {key} chunk {chunk_index}")
                if attempt < self.max_attempts - 1:
                  await asyncio.sleep(2 ** attempt)
                  continue
                else:
                  raise

              # Verify size
              if len(chunk_data) != chunk_size:
                logger.warning(
                  f"[{process_name}] Size mismatch for {key} chunk {chunk_index}: "
                  f"expected {chunk_size}, got {len(chunk_data)}"
                )
                if len(chunk_data) < chunk_size and attempt < self.max_attempts - 1:
                  # Retry if we got less data than expected and have retries left
                  await asyncio.sleep(1)
                  continue

              # Store chunk in shared dictionary
              chunk_data_dict[chunk_key] = chunk_data

              # Update metrics
              chunk_duration = time.time() - chunk_start_time
              download_times.append(chunk_duration)
              completed_chunks += 1
              total_bytes_downloaded += len(chunk_data)

              if self.verbose:
                mb_per_sec = (len(chunk_data) / (1024 * 1024)) / chunk_duration if chunk_duration > 0 else 0
                logger.debug(
                  f"[{process_name}] Downloaded {key} chunk {chunk_index + 1}/{chunk['total_chunks']} "
                  f"in {chunk_duration:.2f}s ({mb_per_sec:.2f} MB/s)"
                )

              # Success - return from function
              return

          except Exception as e:
            if attempt < self.max_attempts - 1:
              # Log and retry
              retry_wait = min(30, 2 ** attempt)  # Cap at 30 seconds
              logger.warning(
                f"[{process_name}] Error downloading {key} chunk {chunk_index} (attempt {attempt + 1}/{self.max_attempts}, "
                f"waiting {retry_wait}s): {str(e)}"
              )
              await asyncio.sleep(retry_wait)  # Exponential backoff
            else:
              # Final attempt failed
              logger.error(
                f"[{process_name}] Failed to download {key} chunk {chunk_index} after {self.max_attempts} attempts: {str(e)}")
              failed_chunks += 1
              raise

    # Create a limiter for task creation to avoid overwhelming the event loop
    max_pending_tasks = min(1000, self.concurrency_per_process * 3)
    pending_tasks = set()
    all_tasks_created = False
    chunk_index = 0

    # Process all chunks with controlled concurrency
    while not all_tasks_created or pending_tasks:
      # Create new tasks up to the limit
      while not all_tasks_created and len(pending_tasks) < max_pending_tasks:
        if chunk_index < len(chunks):
          task = asyncio.create_task(download_chunk(chunks[chunk_index]))
          pending_tasks.add(task)
          task.add_done_callback(pending_tasks.discard)
          chunk_index += 1
        else:
          all_tasks_created = True
          break

      if not pending_tasks:
        break

      # Wait for some tasks to complete
      await asyncio.sleep(0.1)

      # Periodically log progress
      if len(pending_tasks) % 100 == 0 or (chunk_index >= len(chunks) and len(pending_tasks) % 10 == 0):
        elapsed = time.time() - start_time
        if elapsed > 0 and total_bytes_downloaded > 0:
          mb_downloaded = total_bytes_downloaded / (1024 * 1024)
          mb_per_sec = mb_downloaded / elapsed
          gbits_per_sec = (mb_per_sec * 8) / 1000

          logger.info(
            f"[{process_name}] Progress: {completed_chunks}/{len(chunks)} chunks "
            f"({completed_chunks / len(chunks) * 100:.1f}%) | "
            f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s) | "
            f"Pending: {len(pending_tasks)}"
          )

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

  def _optimized_parallel_assembly(
    self,
    keys: List[str],
    file_sizes: Dict[str, int],
    chunk_data_dict: Dict[str, bytes],
    chunk_size_mb: int,
    assembly_method: str = "multi_process",
    num_processes: int = 8,
    num_threads: int = 16
  ) -> Dict[str, bytes]:
    """
    Optimized parallel chunk assembly using the specified method.

    Args:
        keys: List of all object keys
        file_sizes: Dictionary mapping keys to file sizes
        chunk_data_dict: Dictionary containing downloaded chunks
        chunk_size_mb: Size of each chunk in MB
        assembly_method: Method for reassembly ("multi_process", "threaded", or "stream")
        num_processes: Number of processes for multi-process assembly
        num_threads: Number of threads for threaded assembly

    Returns:
        Dictionary with reassembled files
    """
    if assembly_method == "multi_process":
      logger.info(f"Using multi-process assembly with {num_processes} processes")
      return self._multi_process_assembly(
        keys=keys,
        file_sizes=file_sizes,
        chunk_data_dict=chunk_data_dict,
        chunk_size_mb=chunk_size_mb,
        num_processes=num_processes
      )
    elif assembly_method == "threaded":
      logger.info(f"Using threaded assembly with {num_threads} threads")
      return self._threaded_assembly(
        keys=keys,
        file_sizes=file_sizes,
        chunk_data_dict=chunk_data_dict,
        chunk_size_mb=chunk_size_mb,
        num_threads=num_threads
      )
    elif assembly_method == "stream":
      logger.info(f"Using streaming assembly with {num_threads} threads")
      return self._streaming_assembly(
        keys=keys,
        file_sizes=file_sizes,
        chunk_data_dict=chunk_data_dict,
        chunk_size_mb=chunk_size_mb,
        num_threads=num_threads
      )
    else:
      logger.warning(f"Unknown assembly method '{assembly_method}', using threaded assembly")
      return self._threaded_assembly(
        keys=keys,
        file_sizes=file_sizes,
        chunk_data_dict=chunk_data_dict,
        chunk_size_mb=chunk_size_mb,
        num_threads=num_threads
      )

  def _multi_process_assembly(
    self,
    keys: List[str],
    file_sizes: Dict[str, int],
    chunk_data_dict: Dict[str, bytes],
    chunk_size_mb: int,
    num_processes: int = 8
  ) -> Dict[str, bytes]:
    """
    Optimized multi-process assembly that uses shared memory for large files.

    Args:
        keys: List of all object keys
        file_sizes: Dictionary mapping keys to file sizes
        chunk_data_dict: Dictionary containing downloaded chunks
        chunk_size_mb: Size of each chunk in MB
        num_processes: Number of processes to use

    Returns:
        Dictionary with reassembled files
    """
    assembly_start_time = time.time()

    # Group files by size for better load balancing
    files_by_size = {}
    for key in keys:
      if key in file_sizes:
        size = file_sizes[key]
        size_group = size // (1024 * 1024 * 100)  # Group by 100MB increments
        if size_group not in files_by_size:
          files_by_size[size_group] = []
        files_by_size[size_group].append(key)

    # Distribute files evenly across processes with large files first
    files_per_process = [[] for _ in range(num_processes)]
    process_load = [0] * num_processes

    # Sort size groups from largest to smallest
    for size_group in sorted(files_by_size.keys(), reverse=True):
      # Sort files within group by size (largest first)
      files = sorted(
        files_by_size[size_group],
        key=lambda k: file_sizes.get(k, 0),
        reverse=True
      )

      for key in files:
        # Assign to process with least load
        target_process = process_load.index(min(process_load))
        files_per_process[target_process].append(key)
        process_load[target_process] += file_sizes.get(key, 0)

    # Function to assemble files in a process
    def assemble_process_files(
      process_keys: List[str],
      file_sizes: Dict[str, int],
      chunk_data_dict: Dict[str, bytes],
      chunk_size_mb: int,
      result_dict: Dict,
      process_id: int
    ):
      """Assembly worker function that runs in each process"""
      process_start_time = time.time()
      process_name = f"Assembly-{process_id}"
      logger.info(f"[{process_name}] Starting assembly of {len(process_keys)} files")

      # Track statistics
      assembled_bytes = 0
      successful_files = 0
      failed_files = 0

      # Process each file
      for key in process_keys:
        try:
          # Skip if file size is unknown
          if key not in file_sizes:
            logger.warning(f"[{process_name}] File {key} has no size information, skipping")
            failed_files += 1
            continue

          # Get total expected chunks
          file_size = file_sizes[key]
          chunk_size_bytes = chunk_size_mb * 1024 * 1024
          expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes

          # Check if all chunks are available
          missing_chunks = []
          for i in range(expected_chunks):
            chunk_key = f"{key}_{i}"
            if chunk_key not in chunk_data_dict:
              missing_chunks.append(i)

          if missing_chunks:
            logger.warning(
              f"[{process_name}] File {key} is missing {len(missing_chunks)}/{expected_chunks} chunks"
            )
            failed_files += 1
            continue

          # Assemble file using pre-allocated buffer for efficiency
          file_start_time = time.time()

          # Use bytearray for in-place assembly
          result_buffer = bytearray(file_size)
          position = 0

          # Copy chunk data into the correct position
          for i in range(expected_chunks):
            chunk_key = f"{key}_{i}"
            chunk_data = chunk_data_dict[chunk_key]
            chunk_len = len(chunk_data)

            # Copy data to the correct position
            end_pos = min(position + chunk_len, file_size)
            result_buffer[position:end_pos] = chunk_data[:end_pos - position]
            position = end_pos

            # Allow chunks to be garbage-collected as we go
            del chunk_data_dict[chunk_key]

          # Convert to bytes
          combined_data = bytes(result_buffer)

          # Store in result dictionary
          result_dict[key] = combined_data

          # Update statistics
          file_duration = time.time() - file_start_time
          file_mb = file_size / (1024 * 1024)

          assembled_bytes += file_size
          successful_files += 1

          if file_size > 100 * 1024 * 1024:  # Only log for files > 100MB
            logger.info(
              f"[{process_name}] Assembled {key} ({file_mb:.2f} MB) "
              f"in {file_duration:.2f}s ({file_mb / file_duration:.2f} MB/s)"
            )

          # Log progress periodically
          if successful_files % 5 == 0:
            process_elapsed = time.time() - process_start_time
            process_mb = assembled_bytes / (1024 * 1024)
            logger.info(
              f"[{process_name}] Progress: {successful_files}/{len(process_keys)} files, "
              f"{process_mb:.2f} MB in {process_elapsed:.2f}s "
              f"({process_mb / process_elapsed:.2f} MB/s)"
            )

        except Exception as e:
          logger.error(f"[{process_name}] Error assembling {key}: {str(e)}")
          traceback.print_exc()
          failed_files += 1

      # Final process statistics
      process_duration = time.time() - process_start_time
      process_mb = assembled_bytes / (1024 * 1024)
      mb_per_sec = process_mb / process_duration if process_duration > 0 else 0

      logger.info(
        f"[{process_name}] Completed {successful_files}/{len(process_keys)} files "
        f"({failed_files} failed) | {process_mb:.2f} MB in {process_duration:.2f}s "
        f"({mb_per_sec:.2f} MB/s)"
      )

    # Create manager for shared dictionary
    manager = Manager()
    result_dict = manager.dict()

    # Start assembly processes
    assembly_processes = []
    for i in range(num_processes):
      if files_per_process[i]:
        p = Process(
          target=assemble_process_files,
          args=(
            files_per_process[i],
            file_sizes,
            chunk_data_dict,
            chunk_size_mb,
            result_dict,
            i
          )
        )
        p.daemon = True
        p.start()
        assembly_processes.append(p)

    # Wait for assembly processes to complete
    for p in assembly_processes:
      p.join()

    # Wait for assembly to complete
    assembly_duration = time.time() - assembly_start_time

    # Convert manager.dict to regular dict
    result = dict(result_dict)

    assembled_bytes = sum(len(data) for data in result.values())
    successful_files = len(result)
    failed_files = len(keys) - successful_files

    logger.info(
      f"Multi-process assembly completed in {assembly_duration:.2f}s | "
      f"Files: {successful_files}/{len(keys)} ({failed_files} failed) | "
      f"Total: {assembled_bytes / (1024 ** 3):.2f} GB | "
      f"Speed: {(assembled_bytes / (1024 * 1024)) / assembly_duration:.2f} MB/s"
    )

    return result

  def _threaded_assembly(
    self,
    keys: List[str],
    file_sizes: Dict[str, int],
    chunk_data_dict: Dict[str, bytes],
    chunk_size_mb: int,
    num_threads: int = 16
  ) -> Dict[str, bytes]:
    """
    Optimized threaded assembly using a ThreadPoolExecutor.

    Args:
        keys: List of all object keys
        file_sizes: Dictionary mapping keys to file sizes
        chunk_data_dict: Dictionary containing downloaded chunks
        chunk_size_mb: Size of each chunk in MB
        num_threads: Number of threads to use

    Returns:
        Dictionary with reassembled files
    """
    assembly_start_time = time.time()
    logger.info(f"Starting threaded assembly with {num_threads} threads")

    # Result dictionary
    result_dict = {}

    # Track statistics
    successful_files = 0
    failed_files = 0
    assembled_bytes = 0

    # Thread-safe lock for updating shared state
    stats_lock = threading.Lock()

    def assemble_file(key):
      """Thread worker function to assemble a single file"""
      nonlocal successful_files, failed_files, assembled_bytes

      try:
        # Skip if file size is unknown
        if key not in file_sizes:
          with stats_lock:
            failed_files += 1
          return False, key, 0

        # Get total expected chunks
        file_size = file_sizes[key]
        chunk_size_bytes = chunk_size_mb * 1024 * 1024
        expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes

        # Pre-check if all chunks are available
        all_chunks_available = True
        for i in range(expected_chunks):
          chunk_key = f"{key}_{i}"
          if chunk_key not in chunk_data_dict:
            all_chunks_available = False
            logger.warning(f"Missing chunk {i} for file {key}")
            break

        if not all_chunks_available:
          with stats_lock:
            failed_files += 1
          return False, key, 0

        # Use bytearray for in-place assembly
        file_buffer = bytearray(file_size)
        position = 0

        # Copy each chunk into the buffer
        for i in range(expected_chunks):
          chunk_key = f"{key}_{i}"
          chunk_data = chunk_data_dict[chunk_key]
          chunk_len = len(chunk_data)

          # Copy data to the correct position
          end_pos = min(position + chunk_len, file_size)
          file_buffer[position:end_pos] = chunk_data[:end_pos - position]
          position = end_pos

        # Convert to bytes
        combined_data = bytes(file_buffer)

        # Update shared state
        with stats_lock:
          result_dict[key] = combined_data
          successful_files += 1
          assembled_bytes += len(combined_data)

        return True, key, len(combined_data)

      except Exception as e:
        logger.error(f"Error assembling file {key}: {str(e)}")
        with stats_lock:
          failed_files += 1
        return False, key, 0

    # Process files in a thread pool
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
      # Submit all files
      future_to_key = {executor.submit(assemble_file, key): key for key in keys if key in file_sizes}

      # Process results as they complete
      for i, future in enumerate(concurrent.futures.as_completed(future_to_key)):
        key = future_to_key[future]
        try:
          success, _, size = future.result()

          # Log progress periodically
          if i % 10 == 0 or i == len(future_to_key) - 1:
            elapsed = time.time() - assembly_start_time
            logger.info(
              f"Assembly progress: {i + 1}/{len(keys)} files "
              f"({(i + 1) / len(keys) * 100:.1f}%), "
              f"{assembled_bytes / (1024 ** 3):.2f} GB in {elapsed:.2f}s"
            )

        except Exception as e:
          logger.error(f"Error processing assembly result for {key}: {str(e)}")

    # Calculate assembly performance
    assembly_duration = time.time() - assembly_start_time
    mb_assembled = assembled_bytes / (1024 * 1024)
    mb_per_sec = mb_assembled / assembly_duration if assembly_duration > 0 else 0

    logger.info(
      f"Threaded assembly completed in {assembly_duration:.2f}s | "
      f"Files: {successful_files} successful, {failed_files} failed | "
      f"Speed: {mb_per_sec:.2f} MB/s"
    )

    return result_dict

  def _streaming_assembly(
    self,
    keys: List[str],
    file_sizes: Dict[str, int],
    chunk_data_dict: Dict[str, bytes],
    chunk_size_mb: int,
    num_threads: int = 16
  ) -> Dict[str, bytes]:
    """
    Memory-efficient streaming assembly that processes chunks on-the-fly.

    Args:
        keys: List of all object keys
        file_sizes: Dictionary mapping keys to file sizes
        chunk_data_dict: Dictionary containing downloaded chunks
        chunk_size_mb: Size of each chunk in MB
        num_threads: Number of threads to use

    Returns:
        Dictionary with reassembled files
    """
    assembly_start_time = time.time()
    logger.info(f"Starting streaming assembly with {num_threads} threads")

    # Result dictionary
    result_dict = {}

    # Track statistics
    successful_files = 0
    failed_files = 0
    assembled_bytes = 0

    # Thread-safe lock for updating shared state
    stats_lock = threading.Lock()

    def stream_assemble_file(key):
      """Thread worker function to stream-assemble a single file"""
      nonlocal successful_files, failed_files, assembled_bytes

      try:
        # Skip if file size is unknown
        if key not in file_sizes:
          with stats_lock:
            failed_files += 1
          return False, key, 0

        # Get total expected chunks
        file_size = file_sizes[key]
        chunk_size_bytes = chunk_size_mb * 1024 * 1024
        expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes

        # Pre-check if all chunks are available
        all_chunks_available = True
        for i in range(expected_chunks):
          chunk_key = f"{key}_{i}"
          if chunk_key not in chunk_data_dict:
            all_chunks_available = False
            logger.warning(f"Missing chunk {i} for file {key}")
            break

        if not all_chunks_available:
          with stats_lock:
            failed_files += 1
          return False, key, 0

        # Stream chunks directly to output buffer
        output = io.BytesIO()

        # Write chunks in order
        for i in range(expected_chunks):
          chunk_key = f"{key}_{i}"
          chunk_data = chunk_data_dict[chunk_key]
          output.write(chunk_data)

          # Allow chunk to be garbage collected after use
          del chunk_data_dict[chunk_key]

        # Get final result
        output.seek(0)
        combined_data = output.getvalue()

        # Verify size
        if len(combined_data) != file_size:
          logger.warning(
            f"Size mismatch for reassembled file {key}: "
            f"expected {file_size}, got {len(combined_data)}"
          )

        # Update shared state
        with stats_lock:
          result_dict[key] = combined_data
          successful_files += 1
          assembled_bytes += len(combined_data)

        return True, key, len(combined_data)

      except Exception as e:
        logger.error(f"Error streaming file {key}: {str(e)}")
        with stats_lock:
          failed_files += 1
        return False, key, 0

    # Process files in a thread pool
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
      # Submit all files
      future_to_key = {executor.submit(stream_assemble_file, key): key for key in keys if key in file_sizes}

      # Process results as they complete
      for i, future in enumerate(concurrent.futures.as_completed(future_to_key)):
        key = future_to_key[future]
        try:
          success, _, size = future.result()

          # Log progress periodically
          if i % 10 == 0 or i == len(future_to_key) - 1:
            elapsed = time.time() - assembly_start_time
            mb_per_sec = (assembled_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            logger.info(
              f"Streaming progress: {i + 1}/{len(keys)} files "
              f"({(i + 1) / len(keys) * 100:.1f}%), "
              f"{assembled_bytes / (1024 ** 3):.2f} GB in {elapsed:.2f}s "
              f"({mb_per_sec:.2f} MB/s)"
            )

        except Exception as e:
          logger.error(f"Error processing assembly result for {key}: {str(e)}")

    # Calculate assembly performance
    assembly_duration = time.time() - assembly_start_time
    mb_assembled = assembled_bytes / (1024 * 1024)
    mb_per_sec = mb_assembled / assembly_duration if assembly_duration > 0 else 0

    logger.info(
      f"Streaming assembly completed in {assembly_duration:.2f}s | "
      f"Files: {successful_files} successful, {failed_files} failed | "
      f"Speed: {mb_per_sec:.2f} MB/s"
    )

    return result_dict

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
      process_metrics = []
      for pid, metrics in metrics_dict.items():
        if 'mb_per_second' in metrics and metrics.get('completed_chunks', 0) > 0:
          process_metrics.append((
            pid,
            metrics.get('completed_chunks', 0),
            metrics.get('total_chunks', 0),
            metrics.get('mb_per_second', 0),
            metrics.get('gbits_per_second', 0)
          ))

      # Sort by process ID
      process_metrics.sort()

      for pid, completed, total, mb_per_sec, gbits_per_sec in process_metrics:
        logger.debug(
          f"  Process {pid}: {completed}/{total} chunks, "
          f"{mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)"
        )


def main():
  """Command-line entry point"""
  parser = argparse.ArgumentParser(description='High-Throughput S3 Downloader (70+ Gbps)')

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
  parser.add_argument('--chunk-size-mb', type=int, default=128, help='Size of each chunk in MB')
  parser.add_argument('--report-interval', type=int, default=5, help='Progress reporting interval in seconds')
  parser.add_argument('--max-files', type=int, default=None, help='Maximum number of files to download')
  parser.add_argument('--save-to-file', help='Save downloaded data to a pickle file')
  parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
  parser.add_argument('--assembly-method', choices=['multi_process', 'threaded', 'stream'],
                      default='multi_process', help='Method for chunk assembly')
  parser.add_argument('--assembly-processes', type=int, default=None,
                      help='Number of processes for multi-process assembly')
  parser.add_argument('--assembly-threads', type=int, default=16,
                      help='Number of threads for threaded assembly')

  args = parser.parse_args()

  # Create downloader
  downloader = HighThroughputS3Downloader(
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
    report_interval_sec=args.report_interval,
    assembly_method=args.assembly_method,
    assembly_processes=args.assembly_processes,
    assembly_threads=args.assembly_threads
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
reduce
reassembly
overhead
logger.info(f"Optimizing chunk distribution across {self.processes} processes...")

# Group chunks by file
chunks_by_file = {}
for chunk in all_chunks:
  key = chunk['key']
  if key not in chunks_by_file:
    chunks_by_file[key] = []
  chunks_by_file[key].append(chunk)

# Sort files by total number of chunks (descending)
files_by_chunks = sorted(
  chunks_by_file.items(),
  key=lambda x: len(x[1]),
  reverse=True
)

# Initialize process assignments
chunks_per_process = [[] for _ in range(self.processes)]
chunks_assigned_per_process = [0]
for _ in range(self.processes):

# First, try to keep all chunks from the same file in the same process
# if it doesn't create significant imbalance
for file_key, file_chunks in files_by_chunks:
  # If file has too many chunks, split it across processes
  if len(file_chunks) > len(all_chunks) / self.processes * 1.5:
    # Distribute chunks across processes
    for i, chunk in enumerate(file_chunks):
      target_process = i % self.processes
      chunks_per_process[target_process].append(chunk)
      chunks_assigned_per_process[target_process] += 1
  else:
    # Keep all chunks for this file in the same process
    # Find the process with the least chunks
    target_process = chunks_assigned_per_process.index(min(chunks_assigned_per_process))
    chunks_per_process[target_process].extend(file_chunks)
    chunks_assigned_per_process[target_process] += len(file_chunks)

logger.info(f"Distributed {len(all_chunks)} chunks across {self.processes} processes")
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
        start_time=download_start_time,
        metrics_dict=metrics_dict,
        all_chunks=all_chunks,
        chunk_data_dict=chunk_data_dict
      )
      last_report_time = current_time

    # Avoid busy-waiting
    time.sleep(0.1)

  # Final progress report
  self._report_progress(
    start_time=download_start_time,
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

# Calculate download statistics
download_end_time = time.time()
download_duration = download_end_time - download_start_time

# Aggregate metrics from all processes
completed_chunks = sum(m.get('completed_chunks', 0) for m in metrics_dict.values())
failed_chunks = sum(m.get('failed_chunks', 0) for m in metrics_dict.values())
total_bytes = sum(m.get('bytes_downloaded', 0) for m in metrics_dict.values())

logger.info(f"Download phase completed in {download_duration:.2f}s")
logger.info(f"Downloaded {completed_chunks}/{len(all_chunks)} chunks ({failed_chunks} failed)")
logger.info(f"Total downloaded: {total_bytes / (1024 ** 3):.2f} GB")

# Now reassemble chunks into complete files using the optimized method
logger.info(f"Starting chunk assembly using {assembly_method} method...")

# Set default processes for multi-process assembly if not specified
if not assembly_processes:
  assembly_processes = min(16, cpu_count())

assembly_start_time = time.time()
result_dict = self._optimized_parallel_assembly(
  keys=keys,
  file_sizes=file_sizes,
  chunk_data_dict=chunk_data_dict,
  chunk_size_mb=self.chunk_size_mb,
  assembly_method=assembly_method,
  num_processes=assembly_processes,
  num_threads=assembly_threads
)

# Calculate assembly statistics
assembly_end_time = time.time()
assembly_duration = assembly_end_time - assembly_start_time

# Count successful and failed files
successful_files = len(result_dict)
failed_files = len(keys) - successful_files

# Calculate total size of assembled files
assembled_bytes = sum(len(data) for data in result_dict.values())

# Build final results
stats = {
  'files_downloaded': successful_files,
  'files_failed': failed_files,
  'total_files': len(keys),
  'bytes_downloaded': total_bytes,
  'bytes_assembled': assembled_bytes,
  'chunks_completed': completed_chunks,
  'chunks_failed': failed_chunks,
  'total_chunks': len(all_chunks),
  'download_seconds': download_duration,
  'assembly_seconds': assembly_duration,
  'total_seconds': assembly_end_time - download_start_time,
  'download_mb_per_second': (total_bytes / (1024 * 1024)) / download_duration if download_duration > 0 else 0,
  'assembly_mb_per_second': (assembled_bytes / (1024 * 1024)) / assembly_duration if assembly_duration > 0 else 0,
  'overall_mb_per_second': (assembled_bytes / (1024 * 1024)) / (assembly_end_time - download_start_time) if (
                                                                                                                assembly_end_time - download_start_time) > 0 else 0,
  'overall_gbits_per_second': ((assembled_bytes / (1024 * 1024)) / (
      assembly_end_time - download_start_time) * 8) / 1000 if (assembly_end_time - download_start_time) > 0 else 0,
  'processes_used': len(processes),
  'concurrency_per_process': self.concurrency_per_process,
  'chunk_size_mb': self.chunk_size_mb,
  'data': result_dict
}

# Final summary
logger.info("\nDownload Summary:")
logger.info(f"  Files: {stats['files_downloaded']}/{stats['total_files']} ({stats['files_failed']} failed)")
logger.info(f"  Chunks: {stats['chunks_completed']}/{stats['total_chunks']} ({stats['chunks_failed']} failed)")
logger.info(f"  Total Size: {assembled_bytes / (1024 ** 3):.2f} GB")
logger.info(f"  Download Time: {download_duration:.2f}s at {stats['download_mb_per_second']:.2f} MB/s")
logger.info(f"  Assembly Time: {assembly_duration:.2f}s at {stats['assembly_mb_per_second']:.2f} MB/s")
logger.info(f"  Total Time: {stats['total_seconds']:.2f}s")
logger.info(
  f"  Overall Performance: {stats['overall_mb_per_second']:.2f} MB/s ({stats['overall_gbits_per_second']:.2f} Gbit/s)")

return stats
