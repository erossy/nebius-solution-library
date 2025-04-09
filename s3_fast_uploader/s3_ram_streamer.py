#!/usr/bin/env python3
"""
S3 to RAM High-Performance Streamer

This script efficiently streams S3 bucket contents to RAM with maximum parallelism
using multiprocessing, async I/O, and direct memory management.

Requirements:
- Python 3.8+
- boto3
- aioboto3
- asyncio
- psutil
- numpy

Usage:
    python s3_ram_streamer.py --bucket <bucket_name> [--prefix <prefix>] [--processes <num>]

"""
import os
import sys
import time
import argparse
import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import logging
import signal
import numpy as np
import psutil
import boto3
import aioboto3
from typing import Dict, List, Set, Optional, Tuple, Any

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global storage for file contents
ram_storage = {}
ram_storage_lock = mp.Manager().Lock()
ram_stats = mp.Manager().dict()
ram_stats.update({
  'total_bytes': 0,
  'total_files': 0,
  'start_time': 0,
  'end_time': 0,
})


class S3RamStreamer:
  """Main class for streaming S3 objects to RAM"""

  def __init__(self, bucket_name: str, prefix: str = "", processes: int = None, endpoint_url: str = None,
               region_name: str = None, aws_access_key_id: str = None, aws_secret_access_key: str = None):
    """Initialize the streamer with bucket info and configuration"""
    self.bucket_name = bucket_name
    self.prefix = prefix
    # Default to 75% of available CPUs or manually specified number
    self.processes = processes or max(1, int(mp.cpu_count() * 0.75))

    # S3 connection parameters
    self.endpoint_url = endpoint_url
    self.region_name = region_name
    self.aws_access_key_id = aws_access_key_id
    self.aws_secret_access_key = aws_secret_access_key

    # Create S3 client with custom endpoint if provided
    self.s3_client = boto3.client(
      's3',
      endpoint_url=self.endpoint_url,
      region_name=self.region_name,
      aws_access_key_id=self.aws_access_key_id,
      aws_secret_access_key=self.aws_secret_access_key
    )

    # Limit number of processes to available CPUs
    if self.processes > mp.cpu_count():
      logger.warning(f"Requested {self.processes} processes but only {mp.cpu_count()} CPUs available")
      self.processes = mp.cpu_count()

    logger.info(f"Initializing with {self.processes} processes")
    if self.endpoint_url:
      logger.info(f"Using custom endpoint: {self.endpoint_url}")

    # Check available memory and warn if it might be insufficient
    self.available_memory = psutil.virtual_memory().available
    logger.info(f"Available memory: {self.available_memory / (1024 ** 3):.2f} GB")

  async def list_objects(self) -> List[Dict]:
    """List all objects in the bucket asynchronously"""
    logger.info(f"Listing objects in s3://{self.bucket_name}/{self.prefix}")

    # Create session with custom endpoint if provided
    session = aioboto3.Session()
    async with session.client(
      's3',
      endpoint_url=self.endpoint_url,
      region_name=self.region_name,
      aws_access_key_id=self.aws_access_key_id,
      aws_secret_access_key=self.aws_secret_access_key
    ) as s3:
      # Use pagination to handle large buckets efficiently
      paginator = s3.get_paginator('list_objects_v2')

      objects = []
      async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
        if 'Contents' in page:
          objects.extend(page['Contents'])

      logger.info(f"Found {len(objects)} objects in bucket")
      return objects

  async def download_object_async(self, obj: Dict) -> Tuple[str, bytes]:
    """Download a single object asynchronously"""
    key = obj['Key']
    size = obj['Size']

    if size == 0:
      logger.debug(f"Skipping empty object: {key}")
      return key, b''

    logger.debug(f"Downloading {key} ({size / (1024 ** 2):.2f} MB)")

    # Create session with custom endpoint if provided
    session = aioboto3.Session()
    async with session.client(
      's3',
      endpoint_url=self.endpoint_url,
      region_name=self.region_name,
      aws_access_key_id=self.aws_access_key_id,
      aws_secret_access_key=self.aws_secret_access_key
    ) as s3:
      response = await s3.get_object(Bucket=self.bucket_name, Key=key)
      async with response['Body'] as stream:
        data = await stream.read()

      logger.debug(f"Downloaded {key}: {len(data) / (1024 ** 2):.2f} MB")
      return key, data

  async def download_chunk_async(self, keys: List[str]) -> Dict[str, bytes]:
    """Download a chunk of objects asynchronously"""
    tasks = []
    result = {}

    # Create session with custom endpoint if provided
    session = aioboto3.Session()
    # Get object metadata for each key
    async with session.client(
      's3',
      endpoint_url=self.endpoint_url,
      region_name=self.region_name,
      aws_access_key_id=self.aws_access_key_id,
      aws_secret_access_key=self.aws_secret_access_key
    ) as s3:
      for key in keys:
        tasks.append(self.get_object_metadata(s3, key))

    # Wait for all metadata requests to complete
    objects = await asyncio.gather(*tasks)

    # Download each object
    tasks = []
    for obj in objects:
      if obj:  # Skip None results (failed metadata requests)
        tasks.append(self.download_object_async(obj))

    # Wait for all downloads to complete
    downloads = await asyncio.gather(*tasks)

    # Collect results
    for key, data in downloads:
      result[key] = data

    return result

  async def get_object_metadata(self, s3_client, key: str) -> Optional[Dict]:
    """Get metadata for a single object"""
    try:
      response = await s3_client.head_object(Bucket=self.bucket_name, Key=key)
      return {
        'Key': key,
        'Size': response['ContentLength'],
        'LastModified': response['LastModified']
      }
    except Exception as e:
      logger.error(f"Error getting metadata for {key}: {e}")
      return None

  def process_chunk(self, keys: List[str]) -> None:
    """Process a chunk of keys in a separate process"""
    process_id = os.getpid()
    logger.info(f"Process {process_id} starting with {len(keys)} objects")

    # Create and run an event loop in this process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
      chunk_result = loop.run_until_complete(self.download_chunk_async(keys))

      # Store results in shared memory
      with ram_storage_lock:
        for key, data in chunk_result.items():
          ram_storage[key] = data
          ram_stats['total_bytes'] += len(data)
          ram_stats['total_files'] += 1

      logger.info(f"Process {process_id} completed {len(chunk_result)} objects")

    except Exception as e:
      logger.error(f"Error in process {process_id}: {e}")
    finally:
      loop.close()

  def chunk_keys(self, keys: List[str], chunk_size: int) -> List[List[str]]:
    """Split keys into chunks for parallel processing"""
    return [keys[i:i + chunk_size] for i in range(0, len(keys), chunk_size)]

  async def stream_to_ram(self) -> None:
    """Stream all bucket objects to RAM with maximum parallelism"""
    ram_stats['start_time'] = time.time()

    # List all objects in the bucket
    objects = await self.list_objects()
    if not objects:
      logger.warning("No objects found in bucket")
      return

    # Extract keys
    keys = [obj['Key'] for obj in objects]
    total_size_estimate = sum(obj['Size'] for obj in objects)

    logger.info(f"Estimated total size: {total_size_estimate / (1024 ** 3):.2f} GB")
    if total_size_estimate > self.available_memory:
      logger.warning(f"Total size exceeds available memory! Some objects may not fit in RAM")

    # Calculate optimal chunk size based on number of objects and processes
    items_per_process = max(1, len(keys) // self.processes // 2)
    chunks = self.chunk_keys(keys, items_per_process)
    logger.info(f"Split {len(keys)} objects into {len(chunks)} chunks")

    # Use ProcessPoolExecutor for parallel processing
    with ProcessPoolExecutor(max_workers=self.processes) as executor:
      # Submit all chunks for processing
      futures = [executor.submit(self.process_chunk, chunk) for chunk in chunks]

      # Monitor progress
      while any(not future.done() for future in futures):
        completed = sum(future.done() for future in futures)
        logger.info(f"Progress: {completed}/{len(futures)} chunks completed | "
                    f"Objects loaded: {ram_stats['total_files']}/{len(keys)} | "
                    f"RAM used: {ram_stats['total_bytes'] / (1024 ** 3):.2f} GB")
        await asyncio.sleep(5)

    ram_stats['end_time'] = time.time()
    duration = ram_stats['end_time'] - ram_stats['start_time']

    # Report final statistics
    logger.info(f"=== Streaming Summary ===")
    logger.info(f"Total objects: {ram_stats['total_files']}")
    logger.info(f"Total data size: {ram_stats['total_bytes'] / (1024 ** 3):.2f} GB")
    logger.info(f"Time elapsed: {duration:.2f} seconds")
    logger.info(f"Average speed: {ram_stats['total_bytes'] / duration / (1024 ** 2):.2f} MB/s")

    # Memory usage report
    mem = psutil.virtual_memory()
    logger.info(f"RAM usage: {mem.percent}% | "
                f"Used: {mem.used / (1024 ** 3):.2f} GB | "
                f"Available: {mem.available / (1024 ** 3):.2f} GB")


def signal_handler(sig, frame):
  """Handle Ctrl+C gracefully"""
  logger.info("Interrupted by user, shutting down...")
  sys.exit(0)


async def main():
  """Main entry point"""
  parser = argparse.ArgumentParser(description="Stream S3 bucket contents to RAM with maximum parallelism")
  parser.add_argument("--bucket", required=True, help="S3 bucket name")
  parser.add_argument("--prefix", default="", help="S3 object prefix (folder)")
  parser.add_argument("--processes", type=int, default=None, help="Number of parallel processes")

  # S3 endpoint customization options
  parser.add_argument("--endpoint-url", help="Custom S3 endpoint URL (for MinIO, Ceph, etc.)")
  parser.add_argument("--region", help="AWS region name")
  parser.add_argument("--access-key", help="AWS access key ID")
  parser.add_argument("--secret-key", help="AWS secret access key")

  args = parser.parse_args()

  # Register signal handler for clean shutdown
  signal.signal(signal.SIGINT, signal_handler)

  # Create and run the streamer with optional custom endpoint
  streamer = S3RamStreamer(
    bucket_name=args.bucket,
    prefix=args.prefix,
    processes=args.processes,
    endpoint_url=args.endpoint_url,
    region_name=args.region,
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key
  )
  await streamer.stream_to_ram()

  # Keep the script running to maintain data in RAM
  logger.info("Data streaming complete. Press Ctrl+C to exit and free RAM.")
  try:
    # Keep the script running until interrupted
    while True:
      await asyncio.sleep(60)
      mem = psutil.virtual_memory()
      logger.info(f"RAM status: {mem.percent}% used | "
                  f"{mem.used / (1024 ** 3):.2f} GB used | "
                  f"{mem.available / (1024 ** 3):.2f} GB available")
  except KeyboardInterrupt:
    logger.info("Exiting and freeing RAM resources...")


if __name__ == "__main__":
  # Use uvloop if available for better performance
  try:
    import uvloop

    uvloop.install()
    logger.info("Using uvloop for improved async performance")
  except ImportError:
    logger.info("uvloop not available, using standard asyncio event loop")

  # Run the main async function
  asyncio.run(main())
