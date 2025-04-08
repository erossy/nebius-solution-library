"""
S3 Download Performance Testing Guide
====================================

This guide outlines a structured approach to benchmark the high-performance S3 downloader
on a single node and compare its performance against standard methods.
"""

import os
import time
import boto3
import argparse
import concurrent.futures
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import aiobotocore
import asyncio
from tqdm import tqdm
import json
import multiprocessing

# Import the high-performance downloader
from high_performance_s3_downloader import HighPerformanceS3Downloader


def prepare_test_files(
  s3_client,
  bucket_name,
  prefix,
  file_sizes_mb=[10, 100, 512],
  num_files_per_size=10
):
  """
  Generate and upload test files of specific sizes to S3 bucket.

  Args:
      s3_client: Boto3 S3 client
      bucket_name: Target S3 bucket name
      prefix: Key prefix for test files
      file_sizes_mb: List of file sizes in MB to generate
      num_files_per_size: Number of files to generate for each size

  Returns:
      Dictionary mapping file size to list of keys
  """
  import io
  import random

  print("Preparing test files...")
  file_keys = {size: [] for size in file_sizes_mb}

  for size_mb in file_sizes_mb:
    for i in range(num_files_per_size):
      # Generate a random binary file of the specified size
      # Using random data to prevent compression benefits
      random_bytes = random.randbytes(size_mb * 1024 * 1024)
      data = io.BytesIO(random_bytes)

      # Create a key with size in the name
      key = f"{prefix}/test_{size_mb}mb_{i}.bin"

      print(f"Uploading {key} ({size_mb} MB)...")
      s3_client.upload_fileobj(data, bucket_name, key)
      file_keys[size_mb].append(key)

  return file_keys


def standard_boto3_download(
  endpoint_url,
  aws_access_key_id,
  aws_secret_access_key,
  region_name,
  bucket,
  keys,
  destination_dir
):
  """
  Download files using standard boto3 client (sequential).

  Returns:
      Dictionary with performance statistics
  """
  os.makedirs(destination_dir, exist_ok=True)

  session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
  )
  s3_client = session.client('s3', endpoint_url=endpoint_url)

  start_time = time.time()
  total_bytes = 0

  for key in tqdm(keys, desc="Downloading files"):
    filename = os.path.basename(key)
    local_path = os.path.join(destination_dir, filename)

    # Get object size
    response = s3_client.head_object(Bucket=bucket, Key=key)
    file_size = response['ContentLength']
    total_bytes += file_size

    # Download file
    s3_client.download_file(bucket, key, local_path)

  end_time = time.time()
  duration = end_time - start_time
  mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
  gbits_per_sec = (mb_per_sec * 8) / 1000

  return {
    'method': 'boto3_sequential',
    'files_downloaded': len(keys),
    'total_bytes': total_bytes,
    'duration_seconds': duration,
    'mb_per_second': mb_per_sec,
    'gbits_per_second': gbits_per_sec
  }


def boto3_threaded_download(
  endpoint_url,
  aws_access_key_id,
  aws_secret_access_key,
  region_name,
  bucket,
  keys,
  destination_dir,
  max_workers=20
):
  """
  Download files using boto3 with ThreadPoolExecutor for parallelism.

  Returns:
      Dictionary with performance statistics
  """
  os.makedirs(destination_dir, exist_ok=True)

  # Create session and client
  session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
  )
  s3_client = session.client('s3', endpoint_url=endpoint_url)

  # Get sizes of all files first
  total_bytes = 0
  for key in keys:
    response = s3_client.head_object(Bucket=bucket, Key=key)
    total_bytes += response['ContentLength']

  def download_file(key):
    filename = os.path.basename(key)
    local_path = os.path.join(destination_dir, filename)
    s3_client.download_file(bucket, key, local_path)
    return key

  start_time = time.time()

  # Download files in parallel
  with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_key = {executor.submit(download_file, key): key for key in keys}

    # Show progress
    with tqdm(total=len(keys), desc=f"Downloading files (threads={max_workers})") as pbar:
      for future in concurrent.futures.as_completed(future_to_key):
        pbar.update(1)

  end_time = time.time()
  duration = end_time - start_time
  mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
  gbits_per_sec = (mb_per_sec * 8) / 1000

  return {
    'method': f'boto3_threaded(workers={max_workers})',
    'files_downloaded': len(keys),
    'total_bytes': total_bytes,
    'duration_seconds': duration,
    'mb_per_second': mb_per_sec,
    'gbits_per_second': gbits_per_sec
  }


async def aioboto3_download_single(
  endpoint_url,
  aws_access_key_id,
  aws_secret_access_key,
  region_name,
  bucket,
  keys,
  destination_dir,
  max_concurrency=25
):
  """
  Download files using aioboto3 for async I/O.

  Returns:
      Dictionary with performance statistics
  """
  os.makedirs(destination_dir, exist_ok=True)

  # Create session
  session = aiobotocore.session.AioSession()

  # First get total size
  total_bytes = 0
  async with session.create_client(
    's3',
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
  ) as s3_client:
    for key in keys:
      response = await s3_client.head_object(Bucket=bucket, Key=key)
      total_bytes += response['ContentLength']

  # Create semaphore to limit concurrency
  semaphore = asyncio.Semaphore(max_concurrency)

  async def download_file(key):
    async with semaphore:
      filename = os.path.basename(key)
      local_path = os.path.join(destination_dir, filename)

      async with session.create_client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
      ) as s3_client:
        with open(local_path, 'wb') as f:
          await s3_client.download_fileobj(bucket, key, f)

  # Create download tasks
  tasks = [download_file(key) for key in keys]

  # Track progress
  progress = tqdm(total=len(keys), desc=f"Downloading files (async={max_concurrency})")

  # Helper to update progress
  async def update_progress(task):
    await task
    progress.update(1)

  # Start downloading
  start_time = time.time()
  await asyncio.gather(*(update_progress(task) for task in tasks))
  end_time = time.time()

  progress.close()

  duration = end_time - start_time
  mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
  gbits_per_sec = (mb_per_sec * 8) / 1000

  return {
    'method': f'aioboto3(concurrency={max_concurrency})',
    'files_downloaded': len(keys),
    'total_bytes': total_bytes,
    'duration_seconds': duration,
    'mb_per_second': mb_per_sec,
    'gbits_per_second': gbits_per_sec
  }


def multiprocess_download_helper(args):
  """Helper function for multiprocessing download method"""
  key, bucket, endpoint_url, aws_access_key_id, aws_secret_access_key, region_name, destination_dir = args

  session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
  )
  s3_client = session.client('s3', endpoint_url=endpoint_url)

  filename = os.path.basename(key)
  local_path = os.path.join(destination_dir, filename)

  s3_client.download_file(bucket, key, local_path)
  return key


def boto3_multiprocess_download(
  endpoint_url,
  aws_access_key_id,
  aws_secret_access_key,
  region_name,
  bucket,
  keys,
  destination_dir,
  processes=4
):
  """
  Download files using multiprocessing.

  Returns:
      Dictionary with performance statistics
  """
  os.makedirs(destination_dir, exist_ok=True)

  # Create session and client
  session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
  )
  s3_client = session.client('s3', endpoint_url=endpoint_url)

  # Get sizes of all files first
  total_bytes = 0
  for key in keys:
    response = s3_client.head_object(Bucket=bucket, Key=key)
    total_bytes += response['ContentLength']

  # Prepare arguments for multiprocessing
  args = [(key, bucket, endpoint_url, aws_access_key_id, aws_secret_access_key, region_name, destination_dir) for key in
          keys]

  start_time = time.time()

  # Download files using multiprocessing
  with multiprocessing.Pool(processes=processes) as pool:
    # Use imap to get results as they complete
    for _ in tqdm(pool.imap_unordered(multiprocess_download_helper, args),
                  total=len(keys),
                  desc=f"Downloading files (processes={processes})"):
      pass

  end_time = time.time()
  duration = end_time - start_time
  mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
  gbits_per_sec = (mb_per_sec * 8) / 1000

  return {
    'method': f'boto3_multiprocess(processes={processes})',
    'files_downloaded': len(keys),
    'total_bytes': total_bytes,
    'duration_seconds': duration,
    'mb_per_second': mb_per_sec,
    'gbits_per_second': gbits_per_sec
  }


def high_performance_download(
  endpoint_url,
  aws_access_key_id,
  aws_secret_access_key,
  region_name,
  bucket,
  keys,
  destination_dir,
  processes=4,
  concurrency_per_process=25,
  multipart_size_mb=5
):
  """
  Download files using our high-performance S3 downloader.

  Returns:
      Dictionary with performance statistics
  """
  downloader = HighPerformanceS3Downloader(
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name,
    processes=processes,
    concurrency_per_process=concurrency_per_process,
    multipart_size_mb=multipart_size_mb
  )

  stats = downloader.download_files(
    bucket=bucket,
    keys=keys,
    destination_dir=destination_dir
  )

  # Add method name to stats
  stats['method'] = f'high_performance(p={processes},c={concurrency_per_process})'

  return stats


def run_performance_tests(
  endpoint_url,
  aws_access_key_id,
  aws_secret_access_key,
  region_name,
  bucket,
  keys,
  file_sizes,
  test_dir,
  include_baseline=True,
  processes_list=[1, 2, 4, 8],
  concurrency_list=[10, 25, 50]
):
  """
  Run a series of performance tests with different methods.

  Args:
      endpoint_url: S3 endpoint URL
      aws_access_key_id: AWS access key
      aws_secret_access_key: AWS secret key
      region_name: AWS region
      bucket: S3 bucket name
      keys: Dictionary mapping file size to list of keys
      file_sizes: List of file sizes to test
      test_dir: Base directory for test files
      include_baseline: Whether to include slower baseline methods
      processes_list: List of process counts to test
      concurrency_list: List of concurrency values to test

  Returns:
      List of dictionaries with test results
  """
  results = []

  # For each file size category
  for size_mb in file_sizes:
    size_keys = keys[size_mb]
    print(f"\n\n===== Testing with {len(size_keys)} files, {size_mb} MB each =====")

    # Create a specific directory for this size
    size_dir = os.path.join(test_dir, f"{size_mb}mb")

    # Baseline tests
    if include_baseline:
      # Standard sequential boto3
      print("\nTesting standard boto3 (sequential)...")
      boto3_sequential_stats = standard_boto3_download(
        endpoint_url, aws_access_key_id, aws_secret_access_key, region_name,
        bucket, size_keys, os.path.join(size_dir, "boto3_sequential")
      )
      boto3_sequential_stats['file_size_mb'] = size_mb
      results.append(boto3_sequential_stats)

      # Threaded boto3
      for workers in [5, 20, 50]:
        print(f"\nTesting boto3 with ThreadPoolExecutor (workers={workers})...")
        boto3_threaded_stats = boto3_threaded_download(
          endpoint_url, aws_access_key_id, aws_secret_access_key, region_name,
          bucket, size_keys, os.path.join(size_dir, f"boto3_threaded_{workers}"),
          max_workers=workers
        )
        boto3_threaded_stats['file_size_mb'] = size_mb
        results.append(boto3_threaded_stats)

      # aioboto3
      for concurrency in [10, 25, 50]:
        print(f"\nTesting aioboto3 (concurrency={concurrency})...")
        aioboto3_stats = asyncio.run(aioboto3_download_single(
          endpoint_url, aws_access_key_id, aws_secret_access_key, region_name,
          bucket, size_keys, os.path.join(size_dir, f"aioboto3_{concurrency}"),
          max_concurrency=concurrency
        ))
        aioboto3_stats['file_size_mb'] = size_mb
        results.append(aioboto3_stats)

      # Multiprocessing boto3
      for processes in [2, 4, 8]:
        print(f"\nTesting boto3 with multiprocessing (processes={processes})...")
        boto3_mp_stats = boto3_multiprocess_download(
          endpoint_url, aws_access_key_id, aws_secret_access_key, region_name,
          bucket, size_keys, os.path.join(size_dir, f"boto3_mp_{processes}"),
          processes=processes
        )
        boto3_mp_stats['file_size_mb'] = size_mb
        results.append(boto3_mp_stats)

    # High-performance downloader tests
    for processes in processes_list:
      for concurrency in concurrency_list:
        print(f"\nTesting high-performance downloader (processes={processes}, concurrency={concurrency})...")
        hp_stats = high_performance_download(
          endpoint_url, aws_access_key_id, aws_secret_access_key, region_name,
          bucket, size_keys, os.path.join(size_dir, f"high_perf_p{processes}_c{concurrency}"),
          processes=processes,
          concurrency_per_process=concurrency
        )
        hp_stats['file_size_mb'] = size_mb
        results.append(hp_stats)

  return results


def visualize_results(results, output_dir):
  """
  Create visualizations of test results.

  Args:
      results: List of dictionaries with test results
      output_dir: Directory to save visualizations
  """
  os.makedirs(output_dir, exist_ok=True)

  # Convert results to DataFrame
  df = pd.DataFrame(results)

  # Group results by file size
  size_groups = df.groupby('file_size_mb')

  plt.figure(figsize=(12, 8))

  # Plot performance by method for each file size
  for size_mb, group in size_groups:
    plt.figure(figsize=(14, 8))

    # Sort by performance
    group = group.sort_values('gbits_per_second')

    # Create bar chart
    plt.barh(group['method'], group['gbits_per_second'], color='skyblue')

    plt.xlabel('Performance (Gbit/s)')
    plt.ylabel('Method')
    plt.title(f'S3 Download Performance Comparison - {size_mb} MB Files')
    plt.grid(axis='x', linestyle='--', alpha=0.7)

    # Add performance values
    for i, v in enumerate(group['gbits_per_second']):
      plt.text(v + 0.1, i, f"{v:.2f} Gbit/s", va='center')

    # Save figure
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'performance_comparison_{size_mb}mb.png'))
    plt.close()

  # Create summary table
  summary = df.pivot_table(
    index='method',
    columns='file_size_mb',
    values='gbits_per_second',
    aggfunc='mean'
  )

  # Plot summary heatmap
  plt.figure(figsize=(10, 8))
  import seaborn as sns
  sns.heatmap(summary, annot=True, fmt='.2f', cmap='YlGnBu')
  plt.title('Performance by Method and File Size (Gbit/s)')
  plt.tight_layout()
  plt.savefig(os.path.join(output_dir, 'performance_heatmap.png'))

  # Save results as CSV
  df.to_csv(os.path.join(output_dir, 'performance_results.csv'), index=False)

  # Save summary table
  summary.to_csv(os.path.join(output_dir, 'performance_summary.csv'))

  # Also save as JSON for potential programmatic use
  df.to_json(os.path.join(output_dir, 'performance_results.json'), orient='records', indent=2)

  print(f"\nResults saved to {output_dir}")

  return df


def main():
  parser = argparse.ArgumentParser(description='S3 Download Performance Testing')
  parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
  parser.add_argument('--access-key', required=True, help='AWS access key ID')
  parser.add_argument('--secret-key', required=True, help='AWS secret access key')
  parser.add_argument('--region', default=None, help='AWS region')
  parser.add_argument('--bucket', required=True, help='S3 bucket name')
  parser.add_argument('--prefix', default='test', help='Key prefix for test files')
  parser.add_argument('--output-dir', default='./s3_performance_tests', help='Output directory for test results')
  parser.add_argument('--prepare-files', action='store_true', help='Generate and upload test files')
  parser.add_argument('--file-sizes', type=int, nargs='+', default=[10, 100, 512], help='File sizes to test in MB')
  parser.add_argument('--files-per-size', type=int, default=10, help='Number of files per size category')
  parser.add_argument('--skip-baseline', action='store_true', help='Skip baseline (slower) methods')
  parser.add_argument('--processes', type=int, nargs='+', default=[1, 2, 4, 8], help='Process counts to test')
  parser.add_argument('--concurrency', type=int, nargs='+', default=[10, 25, 50], help='Concurrency values to test')

  args = parser.parse_args()

  # Create session
  session = boto3.Session(
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region
  )
  s3_client = session.client('s3', endpoint_url=args.endpoint_url)

  # Prepare test files if requested
  if args.prepare_files:
    file_keys = prepare_test_files(
      s3_client,
      args.bucket,
      args.prefix,
      file_sizes_mb=args.file_sizes,
      num_files_per_size=args.files_per_size
    )
  else:
    # Otherwise, list existing files matching the prefix
    print(f"Listing existing files in bucket '{args.bucket}' with prefix '{args.prefix}'...")
    file_keys = {size: [] for size in args.file_sizes}

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
      if 'Contents' in page:
        for obj in page['Contents']:
          for size in args.file_sizes:
            if f"test_{size}mb_" in obj['Key']:
              file_keys[size].append(obj['Key'])

    # Check if we have enough files
    for size, keys in file_keys.items():
      print(f"Found {len(keys)} files of size {size} MB")
      if len(keys) == 0:
        print(f"Error: No files found for size {size} MB. Use --prepare-files to generate test files.")
        return
      if len(keys) > args.files_per_size:
        file_keys[size] = keys[:args.files_per_size]

  # Create output directory
  os.makedirs(args.output_dir, exist_ok=True)

  # Run performance tests
  results = run_performance_tests(
    args.endpoint_url,
    args.access_key,
    args.secret_key,
    args.region,
    args.bucket,
    file_keys,
    args.file_sizes,
    args.output_dir,
    include_baseline=not args.skip_baseline,
    processes_list=args.processes,
    concurrency_list=args.concurrency
  )

  # Visualize results
  visualize_results(results, args.output_dir)


if __name__ == "__main__":
  main()
