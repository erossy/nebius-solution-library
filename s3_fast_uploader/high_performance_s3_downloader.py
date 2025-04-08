"""
High-Performance S3 Downloader for Python

This implementation achieves near-maximum S3 download performance by:
1. Using multiprocessing to bypass Python's GIL
2. Optimizing for parallel downloads with appropriate chunk sizes
3. Providing a clean, importable interface for use in ML pipelines or other applications
"""

import os
import time
import asyncio
import aioboto3
import boto3
import concurrent.futures
from multiprocessing import Process, Queue, cpu_count
from typing import List, Optional, Dict, Any, Union, Tuple


class HighPerformanceS3Downloader:
  """
  A high-performance S3 downloader that uses multiprocessing and asyncio
  to maximize throughput.
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
    max_attempts: int = 5
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
    """
    self.endpoint_url = endpoint_url
    self.aws_access_key_id = aws_access_key_id
    self.aws_secret_access_key = aws_secret_access_key
    self.region_name = region_name
    self.processes = processes if processes else max(1, cpu_count())
    self.concurrency_per_process = concurrency_per_process
    self.multipart_size_mb = multipart_size_mb
    self.max_attempts = max_attempts

  def download_files(
    self,
    bucket: str,
    keys: List[str],
    destination_dir: str,
    report_interval_sec: int = 5
  ) -> Dict[str, Any]:
    """
    Download multiple files from S3 using multiprocessing for maximum performance.

    Args:
        bucket: S3 bucket name
        keys: List of S3 object keys to download
        destination_dir: Local directory to save downloaded files
        report_interval_sec: Interval in seconds for progress reporting

    Returns:
        Dict containing performance statistics
    """
    os.makedirs(destination_dir, exist_ok=True)

    start_time = time.time()
    result_queue = Queue()

    # Distribute keys across processes
    keys_per_process = [[] for _ in range(self.processes)]
    for i, key in enumerate(keys):
      keys_per_process[i % self.processes].append(key)

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
          destination_dir,
          result_queue,
          i,
        )
      )
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
      p.join()

    # Calculate statistics
    end_time = time.time()
    duration = end_time - start_time
    bytes_downloaded = progress_data.get("bytes_downloaded", 0)
    mb_per_sec = (bytes_downloaded / (1024 * 1024)) / duration if duration > 0 else 0
    gbits_per_sec = (mb_per_sec * 8) / 1000

    stats = {
      "files_downloaded": progress_data.get("completed", 0),
      "total_files": len(keys),
      "bytes_downloaded": bytes_downloaded,
      "duration_seconds": duration,
      "mb_per_second": mb_per_sec,
      "gbits_per_second": gbits_per_sec,
      "processes_used": self.processes,
      "concurrency_per_process": self.concurrency_per_process,
    }

    print(f"\nDownload Summary:")
    print(f"  Files: {stats['files_downloaded']}/{stats['total_files']}")
    print(f"  Total Size: {bytes_downloaded / (1024 ** 3):.2f} GB")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"  Performance: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)")

    return stats

  def _process_worker(
    self,
    keys: List[str],
    bucket: str,
    destination_dir: str,
    result_queue: Queue,
    process_id: int
  ):
    """Worker function that runs in each process to download files"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
      self._download_files_async(
        keys=keys,
        bucket=bucket,
        destination_dir=destination_dir,
        result_queue=result_queue,
        process_id=process_id
      )
    )

  async def _download_files_async(
    self,
    keys: List[str],
    bucket: str,
    destination_dir: str,
    result_queue: Queue,
    process_id: int
  ):
    """Asynchronously download multiple files within a single process"""
    session = aioboto3.Session(
      aws_access_key_id=self.aws_access_key_id,
      aws_secret_access_key=self.aws_secret_access_key,
      region_name=self.region_name
    )

    # Create semaphore to limit concurrency
    semaphore = asyncio.Semaphore(self.concurrency_per_process)

    # Create download tasks
    tasks = []
    for key in keys:
      task = asyncio.create_task(
        self._download_file_with_retry(
          session=session,
          bucket=bucket,
          key=key,
          destination_dir=destination_dir,
          semaphore=semaphore,
          result_queue=result_queue,
          process_id=process_id
        )
      )
      tasks.append(task)

    # Wait for all downloads to complete
    await asyncio.gather(*tasks)

  async def _download_file_with_retry(
    self,
    session,
    bucket: str,
    key: str,
    destination_dir: str,
    semaphore: asyncio.Semaphore,
    result_queue: Queue,
    process_id: int
  ):
    """Download a single file with retry logic"""
    attempts = 0
    local_path = os.path.join(destination_dir, os.path.basename(key))

    while attempts < self.max_attempts:
      try:
        async with semaphore:
          start_time = time.time()

          # Get object size first to report progress
          async with session.resource(
            's3',
            endpoint_url=self.endpoint_url,
            config=boto3.session.Config(
              signature_version='s3v4',
              s3={'addressing_style': 'path'}
            )
          ) as s3:
            obj = await s3.Object(bucket, key)
            obj_info = await obj.get()
            file_size = obj_info['ContentLength']

          # Download the file
          async with session.client(
            's3',
            endpoint_url=self.endpoint_url,
            config=boto3.session.Config(
              signature_version='s3v4',
              s3={
                'addressing_style': 'path',
                'multipart_threshold': self.multipart_size_mb * 1024 * 1024,
                'multipart_chunksize': self.multipart_size_mb * 1024 * 1024,
                'use_accelerate_endpoint': False,
              }
            )
          ) as s3_client:
            with open(local_path, 'wb') as f:
              await s3_client.download_fileobj(bucket, key, f)

          duration = time.time() - start_time
          mb_per_sec = (file_size / (1024 * 1024)) / duration if duration > 0 else 0

          # Report success
          result_queue.put({
            'status': 'success',
            'key': key,
            'size': file_size,
            'duration': duration,
            'mb_per_sec': mb_per_sec,
            'process_id': process_id
          })

          return

      except Exception as e:
        attempts += 1
        if attempts >= self.max_attempts:
          result_queue.put({
            'status': 'error',
            'key': key,
            'error': str(e),
            'process_id': process_id
          })
        else:
          # Exponential backoff
          await asyncio.sleep(2 ** attempts)

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

    # Process all available results from the queue
    while not result_queue.empty():
      try:
        result = result_queue.get_nowait()
        if result['status'] == 'success':
          progress_data["completed"] += 1
          progress_data["bytes_downloaded"] += result.get('size', 0)
        else:
          progress_data["errors"] += 1
      except:
        break

    # Check if it's time to print a progress report
    if interval_sec > 0 and (current_time - progress_data["last_report_time"]) < interval_sec:
      return

    # Calculate and print progress
    elapsed = current_time - progress_data["start_time"]
    if elapsed > 0:
      mb_downloaded = progress_data["bytes_downloaded"] / (1024 * 1024)
      mb_per_sec = mb_downloaded / elapsed
      gbits_per_sec = (mb_per_sec * 8) / 1000

      print(f"\rProgress: {progress_data['completed']}/{progress_data['total']} files "
            f"({progress_data['completed'] / progress_data['total'] * 100:.1f}%) | "
            f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s) | "
            f"Errors: {progress_data['errors']}",
            end="", flush=True)

    progress_data["last_report_time"] = current_time


# Example usage for direct script execution
if __name__ == "__main__":
  import argparse

  parser = argparse.ArgumentParser(description='High Performance S3 Downloader')
  parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
  parser.add_argument('--access-key', required=True, help='AWS access key ID')
  parser.add_argument('--secret-key', required=True, help='AWS secret access key')
  parser.add_argument('--region', default=None, help='AWS region name')
  parser.add_argument('--bucket', required=True, help='S3 bucket name')
  parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
  parser.add_argument('--destination', required=True, help='Local directory for downloaded files')
  parser.add_argument('--processes', type=int, default=None, help='Number of processes to use')
  parser.add_argument('--concurrency', type=int, default=25, help='Concurrent downloads per process')
  parser.add_argument('--multipart-size-mb', type=int, default=5, help='Multipart chunk size in MB')
  parser.add_argument('--report-interval', type=int, default=5, help='Progress reporting interval in seconds')
  parser.add_argument('--max-files', type=int, default=None, help='Maximum number of files to download')

  args = parser.parse_args()

  # Create downloader
  downloader = HighPerformanceS3Downloader(
    endpoint_url=args.endpoint_url,
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region,
    processes=args.processes,
    concurrency_per_process=args.concurrency,
    multipart_size_mb=args.multipart_size_mb
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

  # Start downloading
  stats = downloader.download_files(
    bucket=args.bucket,
    keys=keys,
    destination_dir=args.destination,
    report_interval_sec=args.report_interval
  )

  print("\nDownload complete!")
