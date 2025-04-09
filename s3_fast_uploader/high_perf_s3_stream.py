import argparse
import boto3
import botocore.config
import concurrent.futures
import io
import json
import logging
import multiprocessing
import os
import pickle
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager, Process, Lock, Value, shared_memory
from typing import Dict, List, Set, Tuple, Any, Optional

# Set up logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class HighThroughputS3Downloader:
  """
  High-Throughput S3 downloader optimized for large files and high bandwidth scenarios.
  Capable of handling 60+ Gbps downloads on high-performance hardware.
  """

  def __init__(
    self,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: Optional[str] = None,
    processes: Optional[int] = None,
    concurrency_per_process: int = 50,
    chunk_size_mb: int = 128,
    verbose: bool = False,
    max_pool_connections: int = 100,
    tcp_keepalive: bool = True,
    retry_attempts: int = 3
  ):
    """
    Initialize the downloader with the given parameters.

    Args:
        endpoint_url: S3 endpoint URL
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        region_name: AWS region name (optional)
        processes: Number of processes to use (defaults to CPU count)
        concurrency_per_process: Concurrent downloads per process
        chunk_size_mb: Size of each chunk in MB
        verbose: Enable verbose output
        max_pool_connections: Maximum S3 connection pool size
        tcp_keepalive: Enable TCP keepalive
        retry_attempts: Number of retry attempts for S3 operations
    """
    self.endpoint_url = endpoint_url
    self.aws_access_key_id = aws_access_key_id
    self.aws_secret_access_key = aws_secret_access_key
    self.region_name = region_name

    # Determine process count, defaulting to max utilization for large-memory systems
    # For a 128 vCPU system, use 64 processes by default to leave some headroom
    if processes is None:
      processes = min(64, os.cpu_count() or 16)
    self.processes = processes

    self.concurrency_per_process = concurrency_per_process
    self.chunk_size_mb = chunk_size_mb
    self.verbose = verbose

    # S3 client configuration for high-throughput scenarios
    self.s3_config = botocore.config.Config(
      max_pool_connections=max_pool_connections,
      tcp_keepalive=tcp_keepalive,
      retries={'max_attempts': retry_attempts},
      connect_timeout=30,  # Longer timeouts for large files
      read_timeout=300,  # 5 minutes for large chunks
    )

    logger.info(f"Initialized HighThroughputS3Downloader with {self.processes} processes, "
                f"{self.concurrency_per_process} concurrent downloads per process, "
                f"and {self.chunk_size_mb} MB chunk size")

  def download_files(
    self,
    bucket: str,
    keys: List[str],
    report_interval_sec: int = 5,
    assembly_method: str = "multi_process",
    assembly_processes: Optional[int] = None,
    assembly_threads: int = 16,
    checkpoint_file: Optional[str] = None
  ) -> Dict[str, Any]:
    """
    Download files from S3 using parallel processes and concurrent requests.

    Args:
        bucket: S3 bucket name
        keys: List of keys to download
        report_interval_sec: Interval for progress reporting
        assembly_method: Method for chunk assembly ("multi_process", "threaded", or "stream")
        assembly_processes: Number of processes for multi-process assembly
        assembly_threads: Number of threads for threaded/stream assembly
        checkpoint_file: File to store/resume download progress

    Returns:
        Dictionary with downloaded data and performance metrics
    """
    start_time = time.time()
    logger.info(f"Starting download of {len(keys)} files from bucket '{bucket}'")

    # Get file sizes
    file_sizes = self._get_file_sizes(bucket, keys)
    logger.info(f"Retrieved size information for {len(file_sizes)} files")

    # Calculate total download size
    total_size_bytes = sum(file_sizes.values())
    total_size_gb = total_size_bytes / (1024 ** 3)
    logger.info(f"Total download size: {total_size_gb:.2f} GB")

    # Set default assembly processes based on system specs and workload
    if assembly_processes is None:
      if total_size_gb > 40:  # For large downloads, use more processes
        assembly_processes = min(32, os.cpu_count() or 16)
      else:
        assembly_processes = min(16, os.cpu_count() or 8)

    # Prepare chunks to download
    all_chunks = self._prepare_chunks(keys, file_sizes)
    logger.info(f"Prepared {len(all_chunks)} chunks for download")

    # Load existing checkpoint if available
    completed_chunks = set()
    if checkpoint_file and os.path.exists(checkpoint_file):
      try:
        with open(checkpoint_file, 'rb') as f:
          checkpoint_data = pickle.load(f)
          completed_chunks = checkpoint_data.get('completed_chunks', set())
          logger.info(f"Loaded checkpoint with {len(completed_chunks)} completed chunks")
      except Exception as e:
        logger.warning(f"Failed to load checkpoint file: {str(e)}")

    # Filter out already completed chunks
    chunks_to_download = [c for c in all_chunks if c['chunk_key'] not in completed_chunks]
    if len(completed_chunks) > 0:
      logger.info(f"Resuming download with {len(chunks_to_download)} chunks remaining")

    # Use a Manager for shared objects between processes
    manager = Manager()
    chunk_data_dict = manager.dict()
    metrics_dict = manager.dict()

    # Use a lock for updating metrics
    metrics_lock = manager.Lock()

    # Shared counters for real-time progress tracking
    download_bytes = Value('Q', 0)  # unsigned long long for byte count
    completed_count = Value('i', len(completed_chunks))
    failed_count = Value('i', 0)

    # Create a progress reporting thread
    stop_reporting = threading.Event()
    reporting_thread = threading.Thread(
      target=self._progress_reporter,
      args=(
        start_time, stop_reporting, download_bytes, completed_count, failed_count,
        len(all_chunks), total_size_bytes
      )

    # Create manager for shared dictionary
    manager = Manager()
    result_dict = manager.dict()

    # Start assembly processes
    assembly_processes = []
    for i in range(num_processes):
      if
    files_per_process[i]:
    p = Process(
      target=assemble_process_files,
      args=(
        i,
        files_per_process[i],
        dependency_maps[i],
        result_dict,
      )
    )
    p.daemon = True
    p.start()
    assembly_processes.append(p)

    # Wait for assembly processes to complete
    for p in assembly_processes:
      p.join()

    # Calculate assembly performance
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
    result_lock = threading.Lock()  # Separate lock for result dict to reduce contention

    # Preprocess keys to identify which ones have all required chunks
    valid_keys = []
    chunk_size_bytes = chunk_size_mb * 1024 * 1024

    for key in keys:
      if key not in file_sizes:
        continue

      file_size = file_sizes[key]
      expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes

      # Check if all chunks are available
      all_chunks_available = True
      for i in range(expected_chunks):
        chunk_key = f"{key}_{i}"
        if chunk_key not in chunk_data_dict:
          all_chunks_available = False
          break

      if all_chunks_available:
        valid_keys.append(key)

    logger.info(f"Found {len(valid_keys)}/{len(keys)} files with all chunks available")

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

        # Copy each chunk into the buffer
        for i in range(expected_chunks):
          chunk_key = f"{key}_{i}"
          chunk_data = chunk_data_dict[chunk_key]

          # Calculate position
          start_pos = i * chunk_size_bytes
          chunk_len = len(chunk_data)

          # Ensure we don't exceed the buffer size
          copy_length = min(chunk_len, file_size - start_pos)
          if copy_length > 0:
            file_buffer[start_pos:start_pos + copy_length] = chunk_data[:copy_length]

        # Convert to bytes
        combined_data = bytes(file_buffer)

        # Update shared state
        with result_lock:
          result_dict[key] = combined_data

        with stats_lock:
          successful_files += 1
          assembled_bytes += len(combined_data)

        return True, key, len(combined_data)

      except Exception as e:
        logger.error(f"Error assembling file {key}: {str(e)}")
        traceback.print_exc()
        with stats_lock:
          failed_files += 1
        return False, key, 0

    # Process files in a thread pool
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
      # Submit all files
      future_to_key = {executor.submit(assemble_file, key): key for key in valid_keys}

      # Process results as they complete
      for i, future in enumerate(concurrent.futures.as_completed(future_to_key)):
        key = future_to_key[future]
        try:
          success, _, size = future.result()

          # Log progress periodically
          if (i + 1) % 10 == 0 or i == len(future_to_key) - 1:
            elapsed = time.time() - assembly_start_time
            with stats_lock:
              mb_per_sec = (assembled_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
              logger.info(
                f"Assembly progress: {i + 1}/{len(valid_keys)} files "
                f"({(i + 1) / len(valid_keys) * 100:.1f}%), "
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
      f"Threaded assembly completed in {assembly_duration:.2f}s | "
      f"Files: {successful_files} successful, {failed_files} failed | "
      f"Speed: {mb_per_sec:.2f} MB/s"
    )

    return result_dict


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
  parser.add_argument('--processes', type=int, default=None,
                      help='Number of processes to use (default: min(64, CPU count))')
  parser.add_argument('--concurrency', type=int, default=50,
                      help='Concurrent downloads per process')
  parser.add_argument('--chunk-size-mb', type=int, default=128,
                      help='Size of each chunk in MB')
  parser.add_argument('--report-interval', type=int, default=5,
                      help='Progress reporting interval in seconds')
  parser.add_argument('--max-files', type=int, default=None,
                      help='Maximum number of files to download')
  parser.add_argument('--save-to-file',
                      help='Save downloaded data to a pickle file')
  parser.add_argument('--verbose', action='store_true',
                      help='Enable verbose output')
  parser.add_argument('--assembly-method',
                      choices=['multi_process', 'threaded', 'stream'],
                      default='multi_process',
                      help='Method for chunk assembly')
  parser.add_argument('--assembly-processes', type=int, default=None,
                      help='Number of processes for multi-process assembly')
  parser.add_argument('--assembly-threads', type=int, default=16,
                      help='Number of threads for threaded assembly')
  parser.add_argument('--checkpoint-file',
                      help='File to store/resume download progress')
  parser.add_argument('--tcp-window-size', type=int, default=None,
                      help='TCP window size in bytes')
  parser.add_argument('--s3-max-pool-connections', type=int, default=100,
                      help='Maximum S3 connection pool size')
  parser.add_argument('--s3-retry-attempts', type=int, default=3,
                      help='Number of S3 retry attempts')

  # Optimize defaults for our specific use case (30 x 2GB files)
  if len(sys.argv) == 1:
    parser.print_help()
    print("\nOptimized defaults for 30 x 2GB files on 128 vCPU, 1600GB RAM system:")
    parser.set_defaults(
      processes=30,  # One process per file
      concurrency=8,  # 8 concurrent downloads per file
      chunk_size_mb=256,  # Larger chunks to reduce overhead
      assembly_method='multi_process',
      assembly_processes=15,  # Half the processes for assembly
      s3_max_pool_connections=240  # Allow 8 connections per process
    )

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
    verbose=args.verbose,
    max_pool_connections=args.s3_max_pool_connections,
    retry_attempts=args.s3_retry_attempts
  )

  # Apply TCP window size if specified
  if args.tcp_window_size:
    try:
      import socket
      socket.setdefaulttimeout(300)  # 5 minute timeout for large file operations
      # Note: This is system-dependent and might require root privileges
      logger.info(f"Setting TCP window size to {args.tcp_window_size} bytes")
    except Exception as e:
      logger.warning(f"Failed to set TCP window size: {str(e)}")

  # Set up boto3 session for listing objects
  session = boto3.Session(
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region
  )
  s3_client = session.client(
    's3',
    endpoint_url=args.endpoint_url,
    config=botocore.config.Config(
      max_pool_connections=args.s3_max_pool_connections,
      tcp_keepalive=True,
      retries={'max_attempts': args.s3_retry_attempts}
    )
  )

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

  try:
    # Start downloading
    result = downloader.download_files(
      bucket=args.bucket,
      keys=keys,
      report_interval_sec=args.report_interval,
      assembly_method=args.assembly_method,
      assembly_processes=args.assembly_processes,
      assembly_threads=args.assembly_threads,
      checkpoint_file=args.checkpoint_file
    )

    # Save data to file if requested
    if args.save_to_file:
      logger.info(f"Saving downloaded data to {args.save_to_file}")
      with open(args.save_to_file, 'wb') as f:
        pickle.dump(result['data'], f)
      logger.info(f"Saved {len(result['data'])} files to {args.save_to_file}")

    # Save performance metrics to JSON
    metrics_file = 'download_metrics.json'
    metrics = result.get('metrics', {})
    with open(metrics_file, 'w') as f:
      json.dump(metrics, f, indent=2)
    logger.info(f"Saved performance metrics to {metrics_file}")

    logger.info("Download complete!")

  except KeyboardInterrupt:
    logger.info("Download interrupted by user. Partial results may be available.")
    # Save checkpoint if specified
    if args.checkpoint_file:
      logger.info(f"Checkpoint file: {args.checkpoint_file}")

  except Exception as e:
    logger.error(f"Download failed: {str(e)}")
    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
  main()


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
    This is particularly useful for very large files on memory-constrained systems.

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

    # Thread-safe locks for updating shared state
    stats_lock = threading.Lock()
    result_lock = threading.Lock()

    # Preprocess keys to identify which ones have all required chunks
    valid_keys = []
    chunk_size_bytes = chunk_size_mb * 1024 * 1024

    for key in keys:
      if key not in file_sizes:
        continue

      file_size = file_sizes[key]
      expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes

      # Check if all chunks are available
      all_chunks_available = True
      for i in range(expected_chunks):
        chunk_key = f"{key}_{i}"
        if chunk_key not in chunk_data_dict:
          all_chunks_available = False
          break

      if all_chunks_available:
        valid_keys.append(key)

    logger.info(f"Found {len(valid_keys)}/{len(keys)} files with all chunks available")

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

        # Pre-check if all chunks are available (should be redundant due to valid_keys filtering)
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
        # This is more memory efficient as we don't need to have all chunks in memory simultaneously
        output = io.BytesIO()

        # Write chunks in order - this is the key difference from regular assembly
        for i in range(expected_chunks):
          chunk_key = f"{key}_{i}"
          chunk_data = chunk_data_dict[chunk_key]
          output.write(chunk_data)

          # Allow chunk to be garbage collected after use
          # This helps reduce memory pressure
          if key != chunk_key:  # Only if it's a real chunk, not the original file
            with stats_lock:  # Lock to avoid dict modification during iteration
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
        with result_lock:
          result_dict[key] = combined_data

        with stats_lock:
          successful_files += 1
          assembled_bytes += len(combined_data)

        return True, key, len(combined_data)

      except Exception as e:
        logger.error(f"Error streaming file {key}: {str(e)}")
        traceback.print_exc()
        with stats_lock:
          failed_files += 1
        return False, key, 0

    # Process files in a thread pool
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
      # Submit all files
      future_to_key = {executor.submit(stream_assemble_file, key): key for key in valid_keys}

      # Process results as they complete
      for i, future in enumerate(concurrent.futures.as_completed(future_to_key)):
        key = future_to_key[future]
        try:
          success, _, size = future.result()

          # Log progress periodically
          if (i + 1) % 10 == 0 or i == len(future_to_key) - 1:
            elapsed = time.time() - assembly_start_time
            with stats_lock:
              mb_per_sec = (assembled_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
              logger.info(
                f"Streaming progress: {i + 1}/{len(valid_keys)} files "
                f"({(i + 1) / len(valid_keys) * 100:.1f}%), "
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
    )
    reporting_thread.daemon = True
    reporting_thread.start()

    # Divide chunks among processes
    chunks_per_process = self._distribute_chunks(chunks_to_download)

    # Start download processes
    download_processes = []
    for i, process_chunks in enumerate(chunks_per_process):
      if
    not process_chunks:
    continue

    p = Process(
      target=self._download_process,
      args=(
        i, bucket, process_chunks, self.concurrency_per_process,
        chunk_data_dict, metrics_dict, metrics_lock,
        download_bytes, completed_count, failed_count
      )
    )
    p.daemon = True
    p.start()
    download_processes.append(p)


  # Wait for downloads to complete
  for p in download_processes:
    p.join()

  # Update checkpoint if requested
  if checkpoint_file:
    try:
      # Get all completed chunk keys
      all_completed = set(chunk_data_dict.keys())
      all_completed.update(completed_chunks)

      checkpoint_data = {
        'completed_chunks': all_completed,
        'download_time': time.time() - start_time
      }

      with open(checkpoint_file, 'wb') as f:
        pickle.dump(checkpoint_data, f)
      logger.info(f"Updated checkpoint with {len(all_completed)} completed chunks")
    except Exception as e:
      logger.warning(f"Failed to update checkpoint file: {str(e)}")

  # Stop reporting thread
  stop_reporting.set()
  reporting_thread.join()

  # Calculate download performance
  download_duration = time.time() - start_time
  downloaded_bytes = sum(len(data) for data in chunk_data_dict.values())
  download_mbps = (downloaded_bytes / (1024 * 1024)) / download_duration if download_duration > 0 else 0
  download_gbps = download_mbps / 1000 * 8  # Convert to Gbps

  logger.info(
    f"Download completed in {download_duration:.2f}s | "
    f"Size: {downloaded_bytes / (1024 ** 3):.2f} GB | "
    f"Speed: {download_mbps:.2f} MB/s ({download_gbps:.2f} Gbps)"
  )

  # Start assembly
  assembly_start_time = time.time()
  logger.info(f"Starting assembly with method: {assembly_method}")

  # Assemble files
  result_dict = self._optimized_parallel_assembly(
    keys=keys,
    file_sizes=file_sizes,
    chunk_data_dict=chunk_data_dict,
    chunk_size_mb=self.chunk_size_mb,
    assembly_method=assembly_method,
    num_processes=assembly_processes,
    num_threads=assembly_threads
  )

  # Calculate overall metrics
  assembly_duration = time.time() - assembly_start_time
  total_duration = time.time() - start_time

  # Prepare result metrics
  metrics = {
    "download_duration_seconds": download_duration,
    "assembly_duration_seconds": assembly_duration,
    "total_duration_seconds": total_duration,
    "download_speed_mbps": download_mbps,
    "download_speed_gbps": download_gbps,
    "downloaded_bytes": downloaded_bytes,
    "downloaded_files": len(result_dict),
    "total_files_requested": len(keys),
    "processes_used": self.processes,
    "assembly_method": assembly_method,
    "assembly_processes": assembly_processes,
    "assembly_threads": assembly_threads
  }

  # Include per-process metrics if available
  if metrics_dict:
    metrics["process_metrics"] = {str(k): dict(v) for k, v in metrics_dict.items()}

  return {
    "data": result_dict,
    "metrics": metrics
  }


def _get_file_sizes(self, bucket: str, keys: List[str]) -> Dict[str, int]:
  """
  Get sizes of all files to download.

  Args:
      bucket: S3 bucket name
      keys: List of keys to download

  Returns:
      Dictionary mapping keys to file sizes
  """
  # Set up S3 client
  session = boto3.Session(
    aws_access_key_id=self.aws_access_key_id,
    aws_secret_access_key=self.aws_secret_access_key,
    region_name=self.region_name
  )
  s3_client = session.client('s3', endpoint_url=self.endpoint_url, config=self.s3_config)

  # Use multithreading for faster metadata retrieval
  file_sizes = {}

  def get_object_size(key):
    try:
      response = s3_client.head_object(Bucket=bucket, Key=key)
      return key, response.get('ContentLength', 0)
    except Exception as e:
      logger.warning(f"Error getting size for {key}: {str(e)}")
      return key, 0

  # Use ThreadPoolExecutor for parallel requests
  with ThreadPoolExecutor(max_workers=min(100, len(keys))) as executor:
    futures = [executor.submit(get_object_size, key) for key in keys]
    for future in concurrent.futures.as_completed(futures):
      key, size = future.result()
      if size > 0:
        file_sizes[key] = size

  return file_sizes


def _prepare_chunks(self, keys: List[str], file_sizes: Dict[str, int]) -> List[Dict]:
  """
  Prepare chunks for download based on file sizes.

  Args:
      keys: List of keys to download
      file_sizes: Dictionary mapping keys to file sizes

  Returns:
      List of chunk dictionaries
  """
  chunks = []
  chunk_size_bytes = self.chunk_size_mb * 1024 * 1024

  for key in keys:
    if key not in file_sizes:
      logger.warning(f"No size information for {key}, skipping")
      continue

    # Special handling for small files: don't chunk them
    if file_sizes[key] <= chunk_size_bytes:
      chunks.append({
        'key': key,
        'chunk_key': key,  # No chunking needed
        'start_byte': 0,
        'end_byte': file_sizes[key] - 1,
        'is_single_chunk': True
      })
      continue

    # Calculate number of chunks
    file_size = file_sizes[key]
    num_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes

    # Create chunks
    for i in range(num_chunks):
      start_byte = i * chunk_size_bytes
      end_byte = min(start_byte + chunk_size_bytes - 1, file_size - 1)
      chunks.append({
        'key': key,
        'chunk_key': f"{key}_{i}",
        'start_byte': start_byte,
        'end_byte': end_byte,
        'chunk_index': i,
        'total_chunks': num_chunks,
        'is_single_chunk': False
      })

  return chunks


def _distribute_chunks(self, chunks: List[Dict]) -> List[List[Dict]]:
  """
  Distribute chunks among processes, optimizing for locality and load balancing.

  Args:
      chunks: List of chunk dictionaries

  Returns:
      List of chunk lists, one per process
  """
  if not chunks:
    return [[] for _ in range(self.processes)]

  # Group chunks by file
  chunks_by_file = {}
  for chunk in chunks:
    key = chunk['key']
    if key not in chunks_by_file:
      chunks_by_file[key] = []
    chunks_by_file[key].append(chunk)

  # Sort files by size (largest first)
  files_by_size = sorted(
    chunks_by_file.keys(),
    key=lambda k: sum(c['end_byte'] - c['start_byte'] + 1 for c in chunks_by_file[k]),
    reverse=True
  )

  # Initialize process assignments
  chunks_per_process = [[] for _ in range(self.processes)]
  process_loads = [0] * self.processes

  # Assign chunks to processes, keeping chunks of the same file together when possible
  # and balancing the load
  for key in files_by_size:
    file_chunks = chunks_by_file[key]
    file_size = sum(c['end_byte'] - c['start_byte'] + 1 for c in file_chunks)

    # For very large files, distribute chunks across processes
    if file_size > 2 * 1024 * 1024 * 1024:  # 2GB
      # Sort chunks by position
      file_chunks.sort(key=lambda c: c['chunk_index'])

      # Distribute chunks evenly among processes
      for chunk in file_chunks:
        target_process = process_loads.index(min(process_loads))
        chunks_per_process[target_process].append(chunk)
        process_loads[target_process] += (chunk['end_byte'] - chunk['start_byte'] + 1)
    else:
      # Keep chunks of the same file together
      target_process = process_loads.index(min(process_loads))
      chunks_per_process[target_process].extend(file_chunks)
      process_loads[target_process] += file_size

  # Log distribution stats
  total_chunks = sum(len(chunks) for chunks in chunks_per_process)
  active_processes = sum(1 for chunks in chunks_per_process if chunks)
  logger.info(f"Distributed {total_chunks} chunks across {active_processes}/{self.processes} processes")

  if self.verbose:
    for i, process_chunks in enumerate(chunks_per_process):
      if process_chunks:
        process_size_gb = sum(c['end_byte'] - c['start_byte'] + 1 for c in process_chunks) / (1024 ** 3)
        logger.debug(f"Process {i}: {len(process_chunks)} chunks, {process_size_gb:.2f} GB")

  return chunks_per_process


def _download_process(
  self,
  process_id: int,
  bucket: str,
  chunks: List[Dict],
  concurrency: int,
  chunk_data_dict: Dict,
  metrics_dict: Dict,
  metrics_lock: Lock,
  download_bytes: Value,
  completed_count: Value,
  failed_count: Value
):
  """
  Process function to download a set of chunks.

  Args:
      process_id: Process ID
      bucket: S3 bucket name
      chunks: List of chunks to download
      concurrency: Number of concurrent downloads
      chunk_data_dict: Shared dictionary for downloaded chunks
      metrics_dict: Shared dictionary for metrics
      metrics_lock: Lock for updating metrics
      download_bytes: Shared counter for downloaded bytes
      completed_count: Shared counter for completed chunks
      failed_count: Shared counter for failed chunks
  """
  process_name = f"Download-{process_id}"

  # Set up S3 client
  session = boto3.Session(
    aws_access_key_id=self.aws_access_key_id,
    aws_secret_access_key=self.aws_secret_access_key,
    region_name=self.region_name
  )
  s3_client = session.client('s3', endpoint_url=self.endpoint_url, config=self.s3_config)

  # Initialize metrics
  process_start_time = time.time()
  process_metrics = {
    'process_id': process_id,
    'total_chunks': len(chunks),
    'completed_chunks': 0,
    'failed_chunks': 0,
    'downloaded_bytes': 0,
    'start_time': process_start_time,
    'mb_per_second': 0,
    'gbits_per_second': 0
  }

  with metrics_lock:
    metrics_dict[process_id] = process_metrics

  # Function to download a single chunk
  def download_chunk(chunk):
    chunk_start_time = time.time()
    chunk_key = chunk['chunk_key']
    key = chunk['key']

    try:
      # Prepare range request if needed
      if chunk.get('is_single_chunk', False):
        response = s3_client.get_object(Bucket=bucket, Key=key)
      else:
        # Use byte range
        byte_range = f"bytes={chunk['start_byte']}-{chunk['end_byte']}"
        response = s3_client.get_object(Bucket=bucket, Key=key, Range=byte_range)

      # Read chunk data
      chunk_data = response['Body'].read()
      chunk_size = len(chunk_data)

      # Store in shared dictionary
      chunk_data_dict[chunk_key] = chunk_data

      # Update metrics
      with metrics_lock:
        process_metrics['completed_chunks'] += 1
        process_metrics['downloaded_bytes'] += chunk_size

        # Calculate performance metrics
        elapsed = time.time() - process_start_time
        downloaded_mb = process_metrics['downloaded_bytes'] / (1024 * 1024)
        mb_per_sec = downloaded_mb / elapsed if elapsed > 0 else 0
        gbits_per_sec = (mb_per_sec * 8) / 1000

        process_metrics['mb_per_second'] = mb_per_sec
        process_metrics['gbits_per_second'] = gbits_per_sec

        metrics_dict[process_id] = process_metrics

      # Update shared counters atomically
      with download_bytes.get_lock():
        download_bytes.value += chunk_size

      with completed_count.get_lock():
        completed_count.value += 1

      return True, chunk_key, chunk_size

    except Exception as e:
      logger.error(f"[{process_name}] Error downloading {chunk_key}: {str(e)}")

      # Update metrics
      with metrics_lock:
        process_metrics['failed_chunks'] += 1
        metrics_dict[process_id] = process_metrics

      with failed_count.get_lock():
        failed_count.value += 1

      return False, chunk_key, 0

  # Use ThreadPoolExecutor for concurrent downloads
  with ThreadPoolExecutor(max_workers=concurrency) as executor:
    # Submit all chunks
    future_to_chunk = {executor.submit(download_chunk, chunk): chunk for chunk in chunks}

    # Process results as they complete
    for future in concurrent.futures.as_completed(future_to_chunk):
      success, chunk_key, chunk_size = future.result()

  # Final process metrics
  process_duration = time.time() - process_start_time

  with metrics_lock:
    process_metrics['duration'] = process_duration
    metrics_dict[process_id] = process_metrics

  logger.info(
    f"[{process_name}] Completed {process_metrics['completed_chunks']}/{len(chunks)} chunks "
    f"({process_metrics['failed_chunks']} failed) in {process_duration:.2f}s"
  )


def _progress_reporter(
  self,
  start_time: float,
  stop_event: threading.Event,
  download_bytes: Value,
  completed_count: Value,
  failed_count: Value,
  total_chunks: int,
  total_bytes: int
):
  """
  Thread function to report download progress periodically.

  Args:
      start_time: Time when download started
      stop_event: Event to signal stop
      download_bytes: Shared counter for downloaded bytes
      completed_count: Shared counter for completed chunks
      failed_count: Shared counter for failed chunks
      total_chunks: Total number of chunks
      total_bytes: Total bytes to download
  """
  last_report_time = start_time
  last_bytes_downloaded = 0

  while not stop_event.is_set():
    current_time = time.time()
    elapsed = current_time - start_time
    interval = current_time - last_report_time

    # Only report every 5 seconds
    if interval < 5:
      time.sleep(0.1)
      continue

    # Get current counts atomically
    with download_bytes.get_lock(), completed_count.get_lock(), failed_count.get_lock():
      bytes_downloaded = download_bytes.value
      completed = completed_count.value
      failed = failed_count.value

    # Calculate interval stats
    interval_bytes = bytes_downloaded - last_bytes_downloaded
    interval_mb = interval_bytes / (1024 * 1024)
    interval_mbps = interval_mb / interval if interval > 0 else 0
    interval_gbps = (interval_mbps * 8) / 1000

    # Calculate overall stats
    total_mb = bytes_downloaded / (1024 * 1024)
    overall_mbps = total_mb / elapsed if elapsed > 0 else 0
    overall_gbps = (overall_mbps * 8) / 1000

    # Calculate percent complete
    percent_bytes = bytes_downloaded / total_bytes * 100 if total_bytes > 0 else 0
    percent_chunks = completed / total_chunks * 100 if total_chunks > 0 else 0

    logger.info(
      f"Progress: {completed}/{total_chunks} chunks ({percent_chunks:.1f}%) | "
      f"Data: {bytes_downloaded / (1024 ** 3):.2f}/{total_bytes / (1024 ** 3):.2f} GB ({percent_bytes:.1f}%) | "
      f"Speed: {interval_mbps:.2f} MB/s ({interval_gbps:.2f} Gbps) | "
      f"Avg: {overall_mbps:.2f} MB/s ({overall_gbps:.2f} Gbps) | "
      f"Elapsed: {elapsed:.1f}s"
    )

    # Update last values
    last_report_time = current_time
    last_bytes_downloaded = bytes_downloaded

    # Sleep briefly
    time.sleep(0.1)


def _optimized_parallel_assembly(
  self,
  keys: List[str],
  file_sizes: Dict[str, int],
  chunk_data_dict: Dict[str, bytes],
  chunk_size_mb: int,
  assembly_method: str = "multi_process",
  num_processes: Optional[int] = None,
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
  # Set default processes based on workload
  if num_processes is None:
    num_processes = min(32, os.cpu_count() or 16)

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
  num_processes: int = 32  # Optimized for 128 vCPU system
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

  # Create dependency map for files
  dependency_maps = []
  for process_id, process_keys in enumerate(files_per_process):
    process_map = {}
    for key in process_keys:
      if key not in file_sizes:
        continue

      # Calculate expected chunks
      file_size = file_sizes[key]
      chunk_size_bytes = chunk_size_mb * 1024 * 1024
      expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes

      # Check which chunks are needed
      chunk_keys = []
      for i in range(expected_chunks):
        chunk_key = f"{key}_{i}"
        if chunk_key in chunk_data_dict:
          chunk_keys.append(chunk_key)

      if chunk_keys:
        process_map[key] = {
          'file_size': file_size,
          'chunk_keys': chunk_keys,
          'expected_chunks': expected_chunks
        }

    dependency_maps.append(process_map)

  # Function to assemble files in a process
  def assemble_process_files(
    process_id: int,
    process_keys: List[str],
    dependency_map: Dict,
    result_dict: Dict,
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
        # Skip if file not in dependency map
        if key not in dependency_map:
          logger.warning(f"[{process_name}] File {key} has no dependency information, skipping")
          failed_files += 1
          continue

        # Get file info
        file_info = dependency_map[key]
        file_size = file_info['file_size']
        chunk_keys = file_info['chunk_keys']
        expected_chunks = file_info['expected_chunks']

        # Check if all chunks are available
        if len(chunk_keys) != expected_chunks:
          missing_count = expected_chunks - len(chunk_keys)
          logger.warning(
            f"[{process_name}] File {key} is missing {missing_count}/{expected_chunks} chunks"
          )
          failed_files += 1
          continue

        # Assemble file using pre-allocated buffer for efficiency
        file_start_time = time.time()

        # Use bytearray for in-place assembly
        result_buffer = bytearray(file_size)
        position = 0

        # Calculate chunk size in bytes
        chunk_size_bytes = chunk_size_mb * 1024 * 1024

        # Copy chunk data into the correct position
        for i in range(expected_chunks):
          chunk_key = f"{key}_{i}"

          # Skip if chunk not available (should never happen due to checks above)
          if chunk_key not in chunk_data_dict:
            logger.error(f"[{process_name}] Missing chunk {i} for file {key} during assembly")
            continue

          chunk_data = chunk_data_dict[chunk_key]
          chunk_len = len(chunk_data)

          # Copy data to the correct position
          start_pos = i * chunk_size_bytes
          end_pos = min(start_pos + chunk_len, file_size)

          # Ensure buffer slicing is within bounds
          copy_length = min(chunk_len, end_pos - start_pos)
          if copy_length > 0:
            result_buffer[start_pos:start_pos + copy_length] = chunk_data[:copy_length]

            # Allow chunks to be garbage-collected as we go
            # This helps with memory management
            if key != chunk_key:  # Only delete if it's a real chunk, not the original file
              del chunk_data_dict[chunk_key]

        # Convert to bytes
        combined_data = bytes(result_buffer)

        # Verify size
        if len(combined_data) != file_size:
          logger.warning(
            f"[{process_name}] Size mismatch for {key}: expected {file_size}, got {len(combined_data)}"
          )

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
