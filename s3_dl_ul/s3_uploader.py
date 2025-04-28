import argparse
import json
import multiprocessing
import time
import concurrent.futures
from datetime import datetime
import os
import io
import mmap

import boto3
import boto3.s3.transfer
import botocore.config
from termcolor import colored


def parse_args():
  parser = argparse.ArgumentParser()

  parser.add_argument("--access-key-aws-id", help="Access Key AWS ID", required=True)
  parser.add_argument("--secret-access-key", help="Secret Access Key", required=True)
  parser.add_argument("--region-name", help="Region Name", default="eu-north1")
  parser.add_argument("--endpoint-url", help="Endpoint URL", required=True)
  parser.add_argument("--bucket-name", help="Bucket Name", required=True)
  parser.add_argument("--filename-suffix", help="Filename suffix (can be without _ in the end)", required=False)
  parser.add_argument("--object-prefix", help="Prefix for S3 object keys", default="")
  parser.add_argument("--iteration-number", help="Iteration Number", type=int, required=True)
  parser.add_argument("--object-size-mb", help="Object Size in MB", type=int, required=True)
  parser.add_argument("--concurrency", help="Concurrency per file", type=int, required=True)
  parser.add_argument("--multipart-size-mb", help="Multipart part Size in MB", type=int, required=True)
  parser.add_argument("--file-parallelism", help="Number of files to upload in parallel", type=int, default=4)
  parser.add_argument("--max-pool-connections", help="Max boto3 connection pool size", type=int, default=100)
  parser.add_argument("--delete-previous", help="Delete previous files with the same prefix before starting",
                      action="store_true")
  # New flag to use cached data for uploads
  parser.add_argument("--use-cache-file", help="Use a cached file instead of random data", action="store_true")

  args = parser.parse_args()

  print(f"Arguments were parsed: {args}")
  return args


def create_boto3_client(access_key, secret_key, region, endpoint_url, max_pool_connections):
  """Create a boto3 S3 client with the given credentials"""
  session = boto3.session.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
  )

  botocore_config = botocore.config.Config(
    max_pool_connections=max_pool_connections,
    retries={'max_attempts': 10, 'mode': 'adaptive'}
  )

  return session.client("s3", endpoint_url=endpoint_url, config=botocore_config)


def create_or_get_cache_file(size_mb, cache_file_path="upload_cache.bin"):
  """Create or get a cache file of specified size"""
  size_bytes = size_mb * 1024 * 1024

  # Check if cache file exists with right size
  if os.path.exists(cache_file_path):
    file_size = os.path.getsize(cache_file_path)
    if file_size >= size_bytes:
      print(f"Using existing cache file: {cache_file_path}")
      return cache_file_path

  # Create a new cache file
  print(f"Creating new cache file of {size_mb} MB...")
  with open(cache_file_path, 'wb') as f:
    # Create file in chunks to avoid memory issues
    chunk_size = min(64 * 1024 * 1024, size_bytes)  # 64MB chunks or smaller
    remaining = size_bytes

    # Reuse the same data chunk for efficiency
    data_chunk = b'x' * chunk_size

    while remaining > 0:
      write_size = min(chunk_size, remaining)
      if write_size < chunk_size:
        f.write(data_chunk[:write_size])
      else:
        f.write(data_chunk)
      remaining -= write_size

  return cache_file_path


class StreamingBody:
  """Class to stream data from a file or memory buffer for S3 uploads"""

  def __init__(self, source, start, size):
    self.source = source
    self.start = start
    self.size = size
    self.position = 0

  def read(self, size=None):
    if self.position >= self.size:
      return b''

    if size is None or size < 0:
      size = self.size

    read_size = min(size, self.size - self.position)

    if isinstance(self.source, mmap.mmap):
      data = self.source[self.start + self.position:self.start + self.position + read_size]
    else:
      self.source.seek(self.start + self.position)
      data = self.source.read(read_size)

    self.position += len(data)
    return data


def upload_part_worker(args):
  """Worker function for uploading a part of a file"""
  bucket_name, object_key, upload_id, part_number, source, start, part_size, endpoint_url, access_key, secret_key, region = args

  # If source is None, create a simple data source for this part
  if source is None:
    # Create a simple pattern for this part
    pattern_chunk = b'x' * min(1024 * 1024, part_size)  # 1MB or smaller
    if len(pattern_chunk) == part_size:
      source = io.BytesIO(pattern_chunk)
    else:
      source = io.BytesIO()
      remaining = part_size
      while remaining > 0:
        write_size = min(len(pattern_chunk), remaining)
        source.write(pattern_chunk[:write_size])
        remaining -= write_size
      source.seek(0)

  # Create a fresh client for this worker
  s3_client = create_boto3_client(
    access_key=access_key,
    secret_key=secret_key,
    region=region,
    endpoint_url=endpoint_url,
    max_pool_connections=20  # Smaller pool for part workers
  )

  try:
    # Create a streaming body for this part
    body = StreamingBody(source, start, part_size)

    response = s3_client.upload_part(
      Bucket=bucket_name,
      Key=object_key,
      PartNumber=part_number,
      UploadId=upload_id,
      Body=body
    )
    return part_number, response['ETag']
  except Exception as e:
    print(f"Error uploading part {part_number} of {object_key}: {e}")
    raise


def process_single_file(args_dict, file_index, timestamp, cache_file_path=None):
  """Process a single file upload"""
  # Extract parameters from the dictionary
  bucket_name = args_dict['bucket_name']
  concurrency = args_dict['concurrency']
  multipart_size_mb = args_dict['multipart_size_mb']
  object_size_mb = args_dict['object_size_mb']
  endpoint_url = args_dict['endpoint_url']
  access_key = args_dict['access_key_aws_id']
  secret_key = args_dict['secret_access_key']
  region = args_dict['region_name']
  filename_suffix = args_dict.get('filename_suffix', '') or ''
  object_prefix = args_dict.get('object_prefix', '') or ''
  max_pool_connections = args_dict['max_pool_connections']
  use_cache_file = args_dict.get('use_cache_file', False)

  # Open data source within this process
  shared_source = None
  shared_file = None

  if use_cache_file and cache_file_path:
    # Open the cache file in this process
    shared_file = open(cache_file_path, 'rb')
    # Memory map the file for efficient access
    shared_source = mmap.mmap(shared_file.fileno(), 0, access=mmap.ACCESS_READ)
  else:
    # Create a single reusable buffer with pattern data
    buffer_size = min(object_size_mb * 1024 * 1024, 64 * 1024 * 1024)  # Max 64MB in memory
    pattern = b'x' * 1024 * 1024  # 1MB pattern
    buffer = io.BytesIO()

    # Fill buffer with repeating pattern
    remaining = buffer_size
    while remaining > 0:
      write_size = min(len(pattern), remaining)
      buffer.write(pattern[:write_size])
      remaining -= write_size

    buffer.seek(0)
    shared_source = buffer

  # Create a client for the main operations
  s3_client = create_boto3_client(
    access_key=access_key,
    secret_key=secret_key,
    region=region,
    endpoint_url=endpoint_url,
    max_pool_connections=max_pool_connections
  )

  # Calculate actual multipart size in bytes
  multipart_size = multipart_size_mb * 1024 * 1024
  total_size_bytes = object_size_mb * 1024 * 1024

  operation_start = time.time()
  retry_count = 0
  max_retries = 10
  file_display_name = f"file_{file_index}"

  # Main processing loop with retries
  while retry_count <= max_retries:
    try:
      # Format prefix to include trailing slash if needed
      formatted_prefix = ""
      if object_prefix:
        formatted_prefix = object_prefix if object_prefix.endswith('/') else f"{object_prefix}/"

      # Create a unique object key with the prefix
      object_key = f"{formatted_prefix}{filename_suffix}my_upload_{file_index}_{timestamp}"

      # Start a multipart upload
      multipart_upload = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=object_key
      )
      upload_id = multipart_upload['UploadId']

      # Prepare the parts for upload
      upload_parts = []
      part_number = 1
      position = 0

      while position < total_size_bytes:
        part_size = min(multipart_size, total_size_bytes - position)

        upload_parts.append((
          bucket_name,
          object_key,
          upload_id,
          part_number,
          shared_source,  # Pass the shared file or buffer
          position,  # Start position
          part_size,  # Size to read
          endpoint_url,
          access_key,
          secret_key,
          region
        ))

        position += part_size
        part_number += 1

      # Upload parts in parallel
      with multiprocessing.Pool(processes=concurrency) as pool:
        results = pool.map(upload_part_worker, upload_parts)

      # Complete the multipart upload
      s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=object_key,
        UploadId=upload_id,
        MultipartUpload={
          'Parts': [{'PartNumber': part_num, 'ETag': etag} for part_num, etag in results]
        }
      )

      # If we get here, the operation was successful
      break

    except Exception as e:
      retry_count += 1
      if retry_count > max_retries:
        print(f"Failed to process {file_display_name} after {max_retries} retries: {e}")
        raise
      print(f"Error processing {file_display_name} (attempt {retry_count}/{max_retries}): {e}")
      time.sleep(1)  # Wait before retrying

  # Calculate throughput
  operation_end = time.time()
  duration = operation_end - operation_start
  throughput_mb = object_size_mb / duration

  print(colored(
    f"{file_display_name}: size={object_size_mb:.2f}MB, duration={duration:.2f}s, throughput={throughput_mb:.2f} MiB/sec",
    "green"
  ))

  return throughput_mb


def delete_previous_files(bucket_name, prefix, s3_client):
  """Delete all files with the given prefix from the bucket"""
  if not prefix:
    print("Warning: No prefix specified for deletion. Skipping...")
    return

  print(f"Listing files with prefix '{prefix}' for deletion...")
  paginator = s3_client.get_paginator('list_objects_v2')
  pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

  delete_count = 0
  for page in pages:
    if 'Contents' in page:
      for obj in page['Contents']:
        key = obj['Key']
        try:
          s3_client.delete_object(Bucket=bucket_name, Key=key)
          delete_count += 1
        except Exception as e:
          print(f"Error deleting {key}: {e}")

  print(f"Deleted {delete_count} files with prefix '{prefix}'")


def main():
  # Parse command line arguments
  args = parse_args()

  # Initialize S3 client for the main process
  s3_client = create_boto3_client(
    access_key=args.access_key_aws_id,
    secret_key=args.secret_access_key,
    region=args.region_name,
    endpoint_url=args.endpoint_url,
    max_pool_connections=args.max_pool_connections
  )

  # Format filename suffix
  filename_suffix = args.filename_suffix or ""
  if filename_suffix and filename_suffix[-1] != "_":
    filename_suffix += "_"

  # Format object prefix
  object_prefix = args.object_prefix or ""

  # Delete previous files if requested
  if args.delete_previous:
    delete_prefix = object_prefix if object_prefix else filename_suffix
    if delete_prefix:
      delete_previous_files(args.bucket_name, delete_prefix, s3_client)
    else:
      print("Warning: Cannot delete previous files without a prefix or filename suffix")

  # Current timestamp for filenames
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  start_time = datetime.now()

  # Set up process-based parallelism for file operations
  max_workers = min(args.file_parallelism, args.iteration_number)
  print(f"Using {max_workers} parallel workers for file operations")

  # Create cache file if needed, but don't open it here
  cache_file_path = None
  if args.use_cache_file:
    cache_file_path = create_or_get_cache_file(args.object_size_mb)

  throughputs = []

  # Convert args to dictionary for pickling
  args_dict = vars(args)

  try:
    # Use ProcessPoolExecutor to handle multiple files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
      futures = []

      # Submit all file processing tasks
      for i in range(args.iteration_number):
        future = executor.submit(
          process_single_file,
          args_dict,
          i,
          timestamp,
          cache_file_path
        )
        futures.append(future)

      # Collect results as they complete
      for future in concurrent.futures.as_completed(futures):
        try:
          throughput = future.result()
          throughputs.append(throughput)
        except Exception as e:
          print(f"File processing failed: {e}")
  finally:
    # No shared resources to clean up in the main process anymore
    pass

  end_time = datetime.now()

  # Calculate statistics across all iterations
  if throughputs:
    import numpy as np  # Only import numpy here for statistics, not for data generation

    agg_throughputs = np.array(throughputs)
    percentiles = np.percentile(agg_throughputs, [0, 5, 50, 75, 95, 100])
    agg_stats = {
      "mean": float(f"{np.mean(agg_throughputs):.2f}"),
      "min": float(f"{np.min(agg_throughputs):.2f}"),
      "max": float(f"{np.max(agg_throughputs):.2f}"),
      "std": float(f"{np.std(agg_throughputs):.2f}"),
      "total_throughput": float(f"{np.sum(agg_throughputs):.2f}"),
    }

    # Add machine info for reference
    machine_info = {
      "cpu_count": os.cpu_count(),
      "requested_file_parallelism": args.file_parallelism,
      "actual_file_parallelism": max_workers,
      "concurrency_per_file": args.concurrency,
      "max_pool_connections": args.max_pool_connections,
      "using_cache_file": args.use_cache_file
    }

    summary = {
      "mode": "upload",
      "object_size_mb": args.object_size_mb,
      "num_iterations": args.iteration_number,
      "throughput_percentile": {
        "p0": float(f"{percentiles[0]:.2f}"),
        "p5": float(f"{percentiles[1]:.2f}"),
        "p50": float(f"{percentiles[2]:.2f}"),
        "p75": float(f"{percentiles[3]:.2f}"),
        "p95": float(f"{percentiles[4]:.2f}"),
        "p100": float(f"{percentiles[5]:.2f}"),
      },
      "throughput_aggregates": agg_stats,
      "machine_info": machine_info,
      "timestamp": timestamp,
      "start_time": start_time.isoformat(),
      "end_time": end_time.isoformat(),
      "total_duration_seconds": (end_time - start_time).total_seconds(),
      "delete_previous": args.delete_previous,
      "object_prefix": object_prefix,
    }

    print("\nJSON Summary:")
    print(json.dumps(summary, indent=2))

    # Print a simplified summary for quick reference
    total_throughput = agg_stats["total_throughput"]
    print(f"\nTotal Combined Throughput: {total_throughput:.2f} MiB/sec")
    print(f"Total Duration: {(end_time - start_time).total_seconds():.2f} seconds")
  else:
    print("No successful uploads to report.")


if __name__ == "__main__":
  # Set higher shared memory limit for better multiprocessing performance
  # Use spawn method for better compatibility
  multiprocessing.set_start_method('spawn', force=True)
  main()
