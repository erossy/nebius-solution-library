import argparse
import json
import multiprocessing
import time
import concurrent.futures
from datetime import datetime
import os

import boto3
import boto3.s3.transfer
import botocore.config
import numpy as np
from termcolor import colored


def parse_args():
  parser = argparse.ArgumentParser()

  parser.add_argument("--access-key-aws-id", help="Access Key AWS ID", required=True)
  parser.add_argument("--secret-access-key", help="Secret Access Key", required=True)
  parser.add_argument("--region-name", help="Region Name", default="eu-north1")
  parser.add_argument("--endpoint-url", help="Endpoint URL", required=True)
  parser.add_argument("--bucket-name", help="Bucket Name", required=True)
  parser.add_argument("--filename-suffix", help="Filename suffix (can be without _ in the end)", required=False)
  parser.add_argument("--mode", help="Testing Mode", choices=["upload", "download"], required=True)
  parser.add_argument("--iteration-number", help="Iteration Number", type=int, required=True)
  parser.add_argument("--object-size-mb", help="Object Size in MB", type=int, required=True)
  parser.add_argument("--concurrency", help="Concurrency per file", type=int, required=True)
  parser.add_argument("--multipart-size-mb", help="Multipart part Size in MB", type=int, required=True)
  parser.add_argument("--file-parallelism", help="Number of files to download in parallel", type=int, default=4)
  parser.add_argument("--max-pool-connections", help="Max boto3 connection pool size", type=int, default=100)
  parser.add_argument("--prefix", help="Only download files with this prefix", type=str, default=None)

  args = parser.parse_args()

  print(f"Arguments were parsed: {args}")
  return args


def create_client(args):
  # Create a session with the credentials from environment variables
  session = boto3.session.Session(
    aws_access_key_id=args.access_key_aws_id,
    aws_secret_access_key=args.secret_access_key,
    region_name=args.region_name,
  )

  # Create an S3 client with increased connection pool for better parallelism
  botocore_config = botocore.config.Config(
    max_pool_connections=args.max_pool_connections,
    retries={'max_attempts': 10, 'mode': 'adaptive'}
  )
  s3_client = session.client("s3", endpoint_url=args.endpoint_url, config=botocore_config)
  return s3_client


class MultiprocessingS3:
  def __init__(self, s3_client, concurrency, multipart_size):
    self.__pool = multiprocessing.Pool(concurrency)
    self.s3_client = s3_client
    self.multipart_size = multipart_size
    time.sleep(0.5)  # Brief time for pool initialization

  def upload_object(self, bucket_name, object_key, data):
    datas = []

    mu = self.s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
    mu_id = mu['UploadId']
    i = 0
    j = 1
    while i < len(data):
      datas.append((bucket_name, object_key, mu_id, j, data[i:i + self.multipart_size], self.s3_client))
      i += self.multipart_size
      j += 1

    parts = self.__pool.map(self._upload_part, datas)

    part_info = {
      'Parts': [{'PartNumber': part[0], 'ETag': part[1]} for part in parts]
    }

    self.s3_client.complete_multipart_upload(
      Bucket=bucket_name,
      Key=object_key,
      UploadId=mu_id,
      MultipartUpload=part_info
    )

    return

  @staticmethod
  def _upload_part(arg):
    bucket_name, object_key, mu_id, index, data, s3_client = arg
    part = s3_client.upload_part(
      Bucket=bucket_name,
      Key=object_key,
      PartNumber=index,
      UploadId=mu_id,
      Body=data
    )
    return index, part['ETag']

  def download_object(self, bucket_name, object_key):
    head_res = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
    data_len = int(head_res['ContentLength'])

    part_range = []
    i = 0
    while i < data_len - 1:
      part_range.append((bucket_name, object_key, i, min(i + self.multipart_size, data_len) - 1, self.s3_client))
      i += self.multipart_size

    return self.__pool.map(self._download_range, part_range)

  @staticmethod
  def _download_range(arg):
    bucket_name, object_key, range_start, range_end, s3_client = arg
    result = s3_client.get_object(
      Bucket=bucket_name,
      Key=object_key,
      Range=f"bytes={range_start}-{range_end}"
    )
    data = result['Body'].read()
    return data


def process_file(args, file_index, timestamp, source_filename, s3_client, multipart_size):
  # Create a dedicated MultiprocessingS3 instance for this file
  pool_client = MultiprocessingS3(s3_client, args.concurrency, multipart_size)

  operation_start = time.time()
  retry_index = 0

  # For prefix mode, get the object size for throughput calculation
  object_size_mb = args.object_size_mb
  if args.mode == "download" and args.prefix:
    try:
      # Get the actual file size for existing files with prefix
      head_res = s3_client.head_object(Bucket=args.bucket_name, Key=source_filename)
      object_size_mb = int(head_res['ContentLength']) / (1024 * 1024)
      file_display_name = source_filename  # Use the original key name for display
    except Exception as e:
      print(f"Warning: Failed to get size for {source_filename}: {e}")
      file_display_name = f"file_{file_index}"
  else:
    file_display_name = f"file_{file_index}"

  while True:
    try:
      if args.mode == "upload":
        # Generate random data for upload mode
        data = np.random.bytes(args.object_size_mb * boto3.s3.transfer.MB)
        pool_client.upload_object(
          bucket_name=args.bucket_name,
          object_key=f"{args.filename_suffix or ''}my_upload_{file_index}_{timestamp}",
          data=data
        )
      elif args.mode == "download":
        pool_client.download_object(
          bucket_name=args.bucket_name,
          object_key=source_filename
        )
      else:
        raise ValueError(f"incorrect mode: {args.mode}")
      break
    except Exception as e:
      retry_index = retry_index + 1
      if retry_index > 10:
        print(f"Fail on {file_display_name}: {str(e)}, raising exception")
        raise e
      print(f"Fail on {file_display_name}: {str(e)}, retrying")
      time.sleep(1)  # Add a short delay before retrying
      continue

  operation_end = time.time()
  duration = operation_end - operation_start
  throughput_mb = object_size_mb / duration

  print(colored(
    f"{file_display_name}: size={object_size_mb:.2f}MB, duration={duration:.2f}s, throughput={throughput_mb:.2f} MiB/sec",
    "green"))

  return throughput_mb


def main():
  args = parse_args()

  # Initialize S3 client
  s3_client = create_client(args)

  # Calculate multipart size
  multipart_size = args.multipart_size_mb * boto3.s3.transfer.MB

  # Format filename suffix
  filename_suffix = args.filename_suffix or ""
  if filename_suffix and filename_suffix[-1] != "_":
    filename_suffix += "_"

  # Current timestamp for filenames
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  start_time = datetime.now()

  # For download mode with prefix, get files to download from bucket
  file_keys = []
  if args.mode == "download" and args.prefix:
    print(f"Listing files with prefix '{args.prefix}' from bucket {args.bucket_name}...")
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=args.bucket_name, Prefix=args.prefix)

    for page in pages:
      if 'Contents' in page:
        for obj in page['Contents']:
          file_keys.append(obj['Key'])

    if not file_keys:
      print(f"No files found with prefix '{args.prefix}'")
      return

    print(f"Found {len(file_keys)} files with prefix '{args.prefix}'")
    # Limit to iteration_number if specified
    if args.iteration_number < len(file_keys):
      print(f"Limiting to first {args.iteration_number} files as per iteration_number")
      file_keys = file_keys[:args.iteration_number]
    else:
      # Adjust iteration_number to match number of files found
      args.iteration_number = len(file_keys)
      print(f"Adjusting iteration_number to {args.iteration_number} to match files found")

  # For download benchmark without prefix, upload the file first and use it as the source
  source_filename = None
  if args.mode == "download" and not args.prefix:
    print("Preparing source file for download testing...")
    upload_client = MultiprocessingS3(s3_client, args.concurrency, multipart_size)
    source_filename = f"{filename_suffix}download_source_{timestamp}"
    # Generate random data for source file
    data = np.random.bytes(args.object_size_mb * boto3.s3.transfer.MB)
    upload_client.upload_object(
      bucket_name=args.bucket_name,
      object_key=source_filename,
      data=data
    )
    print(f"Source file uploaded as {source_filename}")

  # Set up process-based parallelism for file operations
  # Use the number of CPUs as guidance but don't exceed the requested file_parallelism
  max_workers = min(args.file_parallelism, args.iteration_number)
  print(f"Using {max_workers} parallel workers for file operations")

  throughputs = []

  # Use ProcessPoolExecutor to handle multiple files in parallel
  with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
    futures = []

    # Submit all file processing tasks
    if args.mode == "download" and args.prefix:
      # Submit tasks for downloading existing files with prefix
      for i, file_key in enumerate(file_keys):
        future = executor.submit(
          process_file,
          args,
          i,
          timestamp,
          file_key,  # Use the actual file key instead of source_filename
          s3_client,
          multipart_size
        )
        futures.append(future)
    else:
      # Standard behavior for upload or download without prefix
      for i in range(args.iteration_number):
        future = executor.submit(
          process_file,
          args,
          i,
          timestamp,
          source_filename,
          s3_client,
          multipart_size
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
      "mode": args.mode,
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
    }

    print("\nJSON Summary:")
    print(json.dumps(summary, indent=2))

    # Print a simplified summary for quick reference
    total_throughput = agg_stats["total_throughput"]
    print(f"\nTotal Combined Throughput: {total_throughput:.2f} MiB/sec")
    print(f"Total Duration: {(end_time - start_time).total_seconds():.2f} seconds")
  else:
    print("No successful downloads to report.")


if __name__ == "__main__":
  # Set higher shared memory limit for better multiprocessing performance
  # This helps with sharing memory between processes
  multiprocessing.set_start_method('spawn')  # More stable on high-core count systems
  main()
