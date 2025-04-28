import argparse
import json
import multiprocessing
import time
import concurrent.futures
from datetime import datetime
import os
import io

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
  # No more cache file option as we'll use a completely different approach

  args = parser.parse_args()

  print(f"Arguments were parsed: {args}")
  return args


def create_boto3_client(access_key, secret_key, region, endpoint_url, max_pool_connections):
  """Create a boto3 S3 client with the given credentials"""
  session = boto3.session.Session(
    aws_access_key_id=access_key,
    aws_secret_access_secret=secret_key,
    region_name=region,
  )

  botocore_config = botocore.config.Config(
    max_pool_connections=max_pool_connections,
    retries={'max_attempts': 10, 'mode': 'adaptive'}
  )

  return session.client("s3", endpoint_url=endpoint_url, config=botocore_config)


class LazyFixedPatternBody:
  """A lazy body that generates fixed pattern data on demand without storing it all in memory"""

  def __init__(self, size, pattern=None):
    self.size = size
    self.position = 0
    # Default pattern is "x" repeated
    self.pattern = pattern or b'x' * min(1024 * 1024, size)  # 1MB pattern or smaller

  def read(self, size=None):
    if self.position >= self.size:
      return b''

    if size is None or size < 0:
      size = self.size - self.position

    read_size = min(size, self.size - self.position)
    self.position += read_size

    # Generate the data on-the-fly
    if read_size <= len(self.pattern):
      # If we need less than one pattern, return a slice
      return self.pattern[:read_size]
    else:
      # For larger reads, we'll need to repeat the pattern
      repeats = read_size // len(self.pattern)
      remainder = read_size % len(self.pattern)

      # Create and return the data without storing it all at once
      result = self.pattern * repeats
      if remainder:
        result += self.pattern[:remainder]

      return result


def upload_object_direct(args_dict, file_index, timestamp):
  """Upload a single object using boto3's upload_fileobj with fixed pattern data"""
  # Extract parameters
  bucket_name = args_dict['bucket_name']
  object_size_mb = args_dict['object_size_mb']
  endpoint_url = args_dict['endpoint_url']
  access_key = args_dict['access_key_aws_id']
  secret_key = args_dict['secret_access_key']
  region = args_dict['region_name']
  filename_suffix = args_dict.get('filename_suffix', '') or ''
  object_prefix = args_dict.get('object_prefix', '') or ''
  multipart_size_mb = args_dict['multipart_size_mb']
  max_pool_connections = args_dict['max_pool_connections']

  # Create S3 client
  s3_client = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
    endpoint_url=endpoint_url,
    config=botocore.config.Config(
      max_pool_connections=max_pool_connections,
      retries={'max_attempts': 10, 'mode': 'adaptive'}
    )
  )

  # Format prefix
  formatted_prefix = ""
  if object_prefix:
    formatted_prefix = object_prefix if object_prefix.endswith('/') else f"{object_prefix}/"

  # Create object key
  object_key = f"{formatted_prefix}{filename_suffix}my_upload_{file_index}_{timestamp}"

  # Calculate sizes
  total_size_bytes = object_size_mb * 1024 * 1024
  multipart_size_bytes = multipart_size_mb * 1024 * 1024

  # Create the data source that generates fixed pattern data on demand
  data_source = LazyFixedPatternBody(total_size_bytes)

  # Measure performance
  operation_start = time.time()
  retry_count = 0
  max_retries = 10
  file_display_name = f"file_{file_index}"

  # Upload with retries
  while retry_count <= max_retries:
    try:
      # Use the transfer manager for automatic multipart uploads
      s3_client.upload_fileobj(
        data_source,
        bucket_name,
        object_key,
        Config=boto3.s3.transfer.TransferConfig(
          multipart_threshold=multipart_size_bytes,
          max_concurrency=args_dict['concurrency'],
          multipart_chunksize=multipart_size_bytes,
          use_threads=True  # Use threads within this process
        )
      )
      break
    except Exception as e:
      retry_count += 1
      if retry_count > max_retries:
        print(f"Failed to process {file_display_name} after {max_retries} retries: {e}")
        raise
      print(f"Error processing {file_display_name} (attempt {retry_count}/{max_retries}): {e}")
      time.sleep(1)
      # Reset the position for next retry
      data_source.position = 0

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
  s3_client = boto3.client(
    "s3",
    aws_access_key_id=args.access_key_aws_id,
    aws_secret_access_key=args.secret_access_key,
    region_name=args.region_name,
    endpoint_url=args.endpoint_url,
    config=botocore.config.Config(
      max_pool_connections=args.max_pool_connections
    )
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

  throughputs = []

  # Convert args to dictionary for pickling (no complex objects like mmap)
  args_dict = vars(args)

  # Use ProcessPoolExecutor for file parallelism
  with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
    futures = []

    # Submit all file processing tasks
    for i in range(args.iteration_number):
      future = executor.submit(
        upload_object_direct,
        args_dict,
        i,
        timestamp
      )
      futures.append(future)

    # Collect results as they complete
    for future in concurrent.futures.as_completed(futures):
      try:
        throughput = future.result()
        throughputs.append(throughput)
      except Exception as e:
        print(f"File processing failed: {e}")

  end_time = datetime.now()

  # Calculate statistics across all iterations
  if throughputs:
    import numpy as np  # Only import numpy here for statistics

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
