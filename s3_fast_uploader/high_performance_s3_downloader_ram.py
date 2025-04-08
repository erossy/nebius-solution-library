"""
High-Performance S3 Downloader for RAM

This implementation achieves near-maximum S3 download performance by:
1. Using multiprocessing to bypass Python's GIL
2. Optimizing for parallel downloads with appropriate chunk sizes
3. Storing downloaded files in RAM instead of writing to disk
4. Providing a clean, importable interface for use in ML pipelines or other applications
"""

import os
import time
import boto3
import io
import concurrent.futures
import threading
import multiprocessing
from multiprocessing import Process, Queue, cpu_count, Manager
from typing import List, Optional, Dict, Any, Union, Tuple
import traceback


class HighPerformanceS3DownloaderRAM:
  """
  A high-performance S3 downloader that uses multiprocessing and threading
  to maximize throughput and stores data in RAM instead of disk.
  """

  def __init__(
    self,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = None,
    processes: int = None,
    concurrency_per_process: int = 25,
    multipart_size_mb: int = 5,
    max_attempts: int = 5,
    verbose: bool = True
  ):
    """
    Initialize the downloader with S3 credentials and performance parameters.

    Args:
        endpoint_url: S3 endpoint URL
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret access key
        region_name: AWS region name (optional)
        processes: Number of processes to use (defaults to CPU count)
        concurrency_per_process: Concurrent downloads per process
        multipart_size_mb: Size of multipart chunks in MB
        max_attempts: Maximum number of retry attempts for failed downloads
        verbose: Whether to print detailed debug information
    """
    self.endpoint_url = endpoint_url
    self.aws_access_key_id = aws_access_key_id
    self.aws_secret_access_key = aws_secret_access_key
    self.region_name = region_name
    self.processes = processes if processes else max(1, cpu_count())
    self.concurrency_per_process = concurrency_per_process
    self.multipart_size_mb = multipart_size_mb
    self.max_attempts = max_attempts
    self.verbose = verbose

    if self.verbose:
      print(f"Initialized HighPerformanceS3DownloaderRAM with:")
      print(f"  Endpoint URL: {endpoint_url}")
      print(f"  Region: {region_name or 'default'}")
      print(f"  Processes: {self.processes}")
      print(f"  Concurrency per process: {concurrency_per_process}")
      print(f"  Multipart size: {multipart_size_mb} MB")
      print(f"  Max retry attempts: {max_attempts}")

  def download_files(
    self,
    bucket: str,
    keys: List[str],
    report_interval_sec: int = 5
  ) -> Dict[str, Any]:
    """
    Download multiple files from S3 using multiprocessing for maximum performance,
    storing files in RAM instead of on disk.

    Args:
        bucket: S3 bucket name
        keys: List of S3 object keys to download
        report_interval_sec: Interval in seconds for progress reporting

    Returns:
        Dict containing performance statistics and the downloaded data
    """
    if self.verbose:
      print(f"Starting RAM download of {len(keys)} files")
      print(f"Bucket: {bucket}")

    start_time = time.time()

    # Create a manager to share data between processes
    manager = Manager()
    result_queue = manager.Queue()

    # This dictionary will store the downloaded data
    data_store = manager.dict()

    # Distribute keys across processes
    keys_per_process = [[] for _ in range(self.processes)]
    for i, key in enumerate(keys):
      keys_per_process[i % self.processes].append(key)

    if self.verbose:
      print(f"Distributed {len(keys)} files across {self.processes} processes:")
      for i, proc_keys in enumerate(keys_per_process):
        if proc_keys:
          print(f"  Process {i}: {len(proc_keys)} files")

    # Start download processes
    processes = []
    for i in range(self.processes):
      if not keys_per_process[i]:
        continue

      if self.verbose:
        print(f"Starting process {i} to download {len(keys_per_process[i])} files to RAM")

      p = Process(
        target=self._process_worker,
        args=(
          keys_per_process[i],
          bucket,
          data_store,
          result_queue,
          i,
          self.endpoint_url,
          self.aws_access_key_id,
          self.aws_secret_access_key,
          self.region_name,
          self.concurrency_per_process,
          self.multipart_size_mb,
          self.max_attempts,
          self.verbose
        )
      )
      p.daemon = True  # Make sure process exits if main process crashes
      p.start()
      processes.append(p)

    # Monitor and report progress
    total_bytes = 0
    completed_files = 0
    progress_data = {"start_time": start_time, "completed": 0, "total": len(keys)}

    try:
      while any(p.is_alive() for p in processes):
        self._report_progress(progress_data, result_queue, report_interval_sec)
        time.sleep(0.1)

      # Final check for any remaining results
      self._report_progress(progress_data, result_queue, 0)

    except KeyboardInterrupt:
      print("\nStopping downloads...")
      for p in processes:
        p.terminate()

    # Wait for processes to finish
    for p in processes:
      if self.verbose:
        print(f"Waiting for process {p.pid} to finish")
      p.join(timeout=1)  # Wait with timeout to avoid hanging
      if p.is_alive() and self.verbose:
        print(f"Process {p.pid} still running, forcing termination")
        p.terminate()

    # Calculate statistics
    end_time = time.time()
    duration = end_time - start_time
    bytes_downloaded = progress_data.get("bytes_downloaded", 0)
    mb_per_sec = (bytes_downloaded / (1024 * 1024)) / duration if duration > 0 else 0
    gbits_per_sec = (mb_per_sec * 8) / 1000

    # Convert the manager.dict to a regular dict for return
    downloaded_data = dict(data_store)

    stats = {
      "files_downloaded": progress_data.get("completed", 0),
      "total_files": len(keys),
      "bytes_downloaded": bytes_downloaded,
      "duration_seconds": duration,
      "mb_per_second": mb_per_sec,
      "gbits_per_second": gbits_per_sec,
      "processes_used": self.processes,
      "concurrency_per_process": self.concurrency_per_process,
      "data": downloaded_data  # Include the downloaded data
    }

    memory_used_mb = bytes_downloaded / (1024 * 1024)

    print(f"\nDownload Summary:")
    print(f"  Files: {stats['files_downloaded']}/{stats['total_files']}")
    print(f"  Total Size: {bytes_downloaded / (1024 ** 3):.2f} GB")
    print(f"  Memory Used: {memory_used_mb:.2f} MB")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"  Performance: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)")

    return stats

  @staticmethod
  def _process_worker(
    keys: List[str],
    bucket: str,
    data_store: Dict,
    result_queue: Queue,
    process_id: int,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str,
    concurrency: int,
    multipart_size_mb: int,
    max_attempts: int,
    verbose: bool = False
  ):
    """Worker function that runs in each process to download files to RAM"""
    worker_id = f"Process-{process_id}"
    process_start_time = time.time()

    if verbose:
      print(f"[{worker_id}] Starting worker with {len(keys)} files to download to RAM")
      print(f"[{worker_id}] Process ID: {os.getpid()}")

    try:
      # Create boto3 session and client
      if verbose:
        print(f"[{worker_id}] Creating boto3 session and client")

      session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
      )

      s3_client = session.client('s3', endpoint_url=endpoint_url)

      if verbose:
        print(f"[{worker_id}] Successfully created boto3 session and client")
        print(f"[{worker_id}] Getting file sizes")

      # Get file sizes for all keys
      file_sizes = {}
      for key in keys:
        try:
          if verbose:
            print(f"[{worker_id}] Getting size for {key}")

          response = s3_client.head_object(Bucket=bucket, Key=key)
          file_sizes[key] = response['ContentLength']

          if verbose:
            print(f"[{worker_id}] Size of {key}: {file_sizes[key] / (1024 * 1024):.2f} MB")

        except Exception as e:
          print(f"[{worker_id}] Error getting size for {key}: {e}")
          if verbose:
            traceback.print_exc()
          file_sizes[key] = 0  # Use 0 as placeholder

      downloaded_files = 0
      total_size = sum(file_sizes.values())

      if verbose:
        print(f"[{worker_id}] Total size to download: {total_size / (1024 * 1024):.2f} MB")
        print(f"[{worker_id}] Starting downloads to RAM")

      # Function to download a single file with retries
      def download_file(key):
        thread_id = f"{worker_id}-Thread-{threading.get_ident()}"
        file_size = file_sizes.get(key, 0)

        if verbose:
          print(f"[{thread_id}] Starting download of {key} to RAM")

        for attempt in range(max_attempts):
          try:
            if verbose:
              print(f"[{thread_id}] Attempt {attempt + 1} for {key}")

            start_time = time.time()

            # Use BytesIO to store data in memory
            buffer = io.BytesIO()

            # Configure the download with appropriate parameters
            config = boto3.s3.transfer.TransferConfig(
              multipart_threshold=multipart_size_mb * 1024 * 1024,
              multipart_chunksize=multipart_size_mb * 1024 * 1024,
              use_threads=True,
              max_concurrency=4  # Use limited concurrency within each thread
            )

            if verbose:
              print(f"[{thread_id}] Downloading {key} to memory")

            # Download the file to memory
            s3_client.download_fileobj(
              Bucket=bucket,
              Key=key,
              Fileobj=buffer,
              Config=config
            )

            # Reset buffer position to beginning
            buffer.seek(0)

            # Get the data as bytes
            data = buffer.getvalue()

            # Store data in the shared dictionary
            data_store[key] = data

            duration = time.time() - start_time
            mb_per_sec = (file_size / (1024 * 1024)) / duration if duration > 0 else 0

            if verbose:
              print(f"[{thread_id}] Successfully downloaded {key} to RAM in {duration:.2f}s ({mb_per_sec:.2f} MB/s)")
              print(f"[{thread_id}] Data size: {len(data)} bytes")

            # Report success
            result_queue.put({
              'status': 'success',
              'key': key,
              'size': file_size,
              'duration': duration,
              'mb_per_sec': mb_per_sec,
              'process_id': process_id
            })

            return True

          except Exception as e:
            error_msg = f"[{thread_id}] Error downloading {key}: {str(e)}"
            print(error_msg)

            if verbose:
              traceback.print_exc()

            if attempt < max_attempts - 1:
              wait_time = 2 ** attempt
              if verbose:
                print(f"[{thread_id}] Retrying in {wait_time}s...")
              time.sleep(wait_time)
            else:
              # Report error on final attempt
              result_queue.put({
                'status': 'error',
                'key': key,
                'error': str(e),
                'process_id': process_id
              })
              return False

      if verbose:
        print(f"[{worker_id}] Creating thread pool with max workers: {concurrency}")

      # Use ThreadPoolExecutor to download files concurrently
      with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        if verbose:
          print(f"[{worker_id}] Submitting {len(keys)} download tasks")

        futures = []
        for key in keys:
          future = executor.submit(download_file, key)
          futures.append(future)

        if verbose:
          print(f"[{worker_id}] Waiting for downloads to complete")

        # Wait for all futures to complete
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
          try:
            result = future.result()
            if result:
              downloaded_files += 1

            if verbose:
              print(f"[{worker_id}] Completed {i + 1}/{len(keys)} downloads")

          except Exception as e:
            print(f"[{worker_id}] Error in download task: {e}")
            if verbose:
              traceback.print_exc()

      process_duration = time.time() - process_start_time
      if verbose:
        print(f"[{worker_id}] Process completed in {process_duration:.2f}s")
        print(f"[{worker_id}] Downloaded {downloaded_files}/{len(keys)} files to RAM")

    except Exception as e:
      print(f"[{worker_id}] Critical error in worker process: {e}")
      if verbose:
        traceback.print_exc()
      # Make sure we don't silently fail
      result_queue.put({
        'status': 'process_error',
        'process_id': process_id,
        'error': str(e)
      })

  def _report_progress(
    self,
    progress_data: Dict[str, Any],
    result_queue: Queue,
    interval_sec: int
  ):
    """Report download progress at specified intervals"""
    current_time = time.time()

    # Initialize progress tracking variables if needed
    if "last_report_time" not in progress_data:
      progress_data["last_report_time"] = current_time
      progress_data["bytes_downloaded"] = 0
      progress_data["completed"] = 0
      progress_data["errors"] = 0
      progress_data["last_completed"] = 0
      progress_data["process_errors"] = 0

    # Process all available results from the queue
    results_processed = 0
    while not result_queue.empty():
      try:
        result = result_queue.get_nowait()
        results_processed += 1

        if result['status'] == 'success':
          progress_data["completed"] += 1
          progress_data["bytes_downloaded"] += result.get('size', 0)

          if self.verbose and progress_data["completed"] % 5 == 0:
            print(f"\nFile completed: {result.get('key', 'unknown')} "
                  f"({result.get('size', 0) / (1024 * 1024):.2f} MB in {result.get('duration', 0):.2f}s, "
                  f"{result.get('mb_per_sec', 0):.2f} MB/s)")

        elif result['status'] == 'error':
          progress_data["errors"] += 1
          print(f"\nError downloading {result.get('key', 'unknown')}: {result.get('error', 'unknown error')}")

        elif result['status'] == 'process_error':
          progress_data["process_errors"] += 1
          print(f"\nProcess {result.get('process_id', 'unknown')} error: {result.get('error', 'unknown error')}")

      except Exception as e:
        if self.verbose:
          print(f"Error processing result from queue: {e}")
        break

    if self.verbose and results_processed > 0:
      new_completed = progress_data["completed"] - progress_data["last_completed"]
      if new_completed > 0:
        print(f"\nProcessed {results_processed} results from queue, {new_completed} new completed files")
      progress_data["last_completed"] = progress_data["completed"]

    # Check if it's time to print a progress report
    if interval_sec > 0 and (current_time - progress_data["last_report_time"]) < interval_sec:
      return

    # Calculate and print progress
    elapsed = current_time - progress_data["start_time"]
    if elapsed > 0:
      mb_downloaded = progress_data["bytes_downloaded"] / (1024 * 1024)
      mb_per_sec = mb_downloaded / elapsed
      gbits_per_sec = (mb_per_sec * 8) / 1000

      status_line = (f"\rProgress: {progress_data['completed']}/{progress_data['total']} files "
                     f"({progress_data['completed'] / progress_data['total'] * 100:.1f}%) | "
                     f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s) | "
                     f"Errors: {progress_data['errors']}")

      if progress_data["process_errors"] > 0:
        status_line += f" | Process Errors: {progress_data['process_errors']}"

      print(status_line, end="", flush=True)

    progress_data["last_report_time"] = current_time


# Example usage
def download_and_use_data():
  """Example of how to download and use data with HighPerformanceS3DownloaderRAM"""
  # Create downloader
  downloader = HighPerformanceS3DownloaderRAM(
    endpoint_url="https://s3.example.com",
    aws_access_key_id="your_access_key",
    aws_secret_access_key="your_secret_key",
    processes=4,
    concurrency_per_process=10
  )

  # Download files to RAM
  result = downloader.download_files(
    bucket="your-bucket",
    keys=["file1.csv", "file2.json", "image.jpg"]
  )

  # Access the data (example for a CSV file)
  if "file1.csv" in result["data"]:
    import pandas as pd
    csv_data = result["data"]["file1.csv"]

    # Convert to pandas DataFrame
    df = pd.read_csv(io.BytesIO(csv_data))
    print(f"CSV data loaded with {len(df)} rows")

  # Example for JSON data
  if "file2.json" in result["data"]:
    import json
    json_data = result["data"]["file2.json"]

    # Parse JSON
    json_obj = json.loads(json_data.decode('utf-8'))
    print(f"JSON data loaded: {json_obj}")

  # Example for image data
  if "image.jpg" in result["data"]:
    from PIL import Image
    image_data = result["data"]["image.jpg"]

    # Load image
    img = Image.open(io.BytesIO(image_data))
    print(f"Image loaded: {img.size}")

  return result


# Example usage for direct script execution
if __name__ == "__main__":
  import argparse

  parser = argparse.ArgumentParser(description='High Performance S3 Downloader to RAM')
  parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
  parser.add_argument('--access-key', required=True, help='AWS access key ID')
  parser.add_argument('--secret-key', required=True, help='AWS secret access key')
  parser.add_argument('--region', default=None, help='AWS region name')
  parser.add_argument('--bucket', required=True, help='S3 bucket name')
  parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
  parser.add_argument('--processes', type=int, default=None, help='Number of processes to use')
  parser.add_argument('--concurrency', type=int, default=25, help='Concurrent downloads per process')
  parser.add_argument('--multipart-size-mb', type=int, default=5, help='Multipart chunk size in MB')
  parser.add_argument('--report-interval', type=int, default=5, help='Progress reporting interval in seconds')
  parser.add_argument('--max-files', type=int, default=None, help='Maximum number of files to download')
  parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
  parser.add_argument('--save-to-file', help='Save downloaded data to a file (pickle format)')

  args = parser.parse_args()

  # Create downloader
  downloader = HighPerformanceS3DownloaderRAM(
    endpoint_url=args.endpoint_url,
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region,
    processes=args.processes,
    concurrency_per_process=args.concurrency,
    multipart_size_mb=args.multipart_size_mb,
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
