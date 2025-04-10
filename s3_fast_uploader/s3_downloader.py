import argparse
import json
import multiprocessing
import time
import concurrent.futures
from datetime import datetime
import os
import sys
import tempfile
import fcntl
import io
from contextlib import closing

import boto3
import boto3.s3.transfer
import botocore.config
import numpy as np
from termcolor import colored

try:
  # Optional imports for better performance
  import psutil
  import asyncio

  HAS_ASYNCIO = True
except ImportError:
  HAS_ASYNCIO = False


def parse_args():
  parser = argparse.ArgumentParser()

  parser.add_argument("--access-key-aws-id", help="Access Key AWS ID", required=True)
  parser.add_argument("--secret-access-key", help="Secret Access Key", required=True)
  parser.add_argument("--region-name", help="Region Name", default="eu-north1")
  parser.add_argument("--endpoint-url", help="Endpoint URL", required=True)
  parser.add_argument("--bucket-name", help="Bucket Name", required=True)
  parser.add_argument("--iteration-number", help="Iteration Number", type=int, required=True)
  parser.add_argument("--object-size-mb", help="Object Size in MB", type=int, required=True)
  parser.add_argument("--concurrency", help="Concurrency per file", type=int, required=True)
  parser.add_argument("--multipart-size-mb", help="Multipart part Size in MB", type=int, required=True)
  parser.add_argument("--file-parallelism", help="Number of files to download in parallel", type=int, default=4)
  parser.add_argument("--max-pool-connections", help="Max boto3 connection pool size", type=int, default=100)
  parser.add_argument("--prefix", help="Only download files with this prefix", type=str, default=None)
  parser.add_argument("--save-to-disk", help="Path where to save downloaded files", type=str, default=None)
  parser.add_argument("--streaming-mode", help="Use direct streaming to disk", action="store_true", default=True)
  parser.add_argument("--skip-validation", help="Skip post-download validation", action="store_true", default=False)
  parser.add_argument("--io-threads", help="Number of I/O threads per worker", type=int, default=8)
  parser.add_argument("--chunk-size-kb", help="Chunk size in KB for streaming", type=int, default=1024)
  parser.add_argument("--auto-optimize", help="Automatically optimize parameters for CPU", action="store_true",
                      default=False)

  args = parser.parse_args()

  print(f"Arguments were parsed: {args}")
  return args


def ensure_directory_exists(directory_path):
  """Ensure that the specified directory exists, creating it if necessary"""
  if not os.path.exists(directory_path):
    try:
      os.makedirs(directory_path, exist_ok=True)
      print(f"Created directory at {directory_path}")
      return True
    except Exception as e:
      print(colored(f"Error creating directory: {e}", "red"))
      return False
  return True


def create_boto3_client(access_key, secret_key, region, endpoint_url, max_pool_connections):
  """Create a boto3 S3 client with optimized settings for high-throughput transfers"""
  session = boto3.session.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
  )

  # Enhanced configuration for better performance
  botocore_config = botocore.config.Config(
    max_pool_connections=max_pool_connections,
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    connect_timeout=10,  # Longer connection timeout
    read_timeout=30,  # Longer read timeout
    tcp_keepalive=True,  # Keep connections alive
    use_dualstack_endpoint=False,  # Disable dual stack for better performance if not needed
  )

  # Create the client with optimized settings
  client = session.client("s3", endpoint_url=endpoint_url, config=botocore_config)

  # Set socket options for client if psutil is available
  try:
    import socket
    # Attempt to set aggressive TCP settings on the boto3 internal HTTP client
    for http_client in client._endpoint.http_session._manager.connection_pool_kw.get('connections', []):
      sock = getattr(http_client, 'sock', None)
      if sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # Disable Nagle's algorithm
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # Enable keepalive
  except (ImportError, AttributeError, TypeError) as e:
    # Not critical if it fails
    pass

  return client


def download_part_to_memory(args):
  """Worker function for downloading a part of a file to memory"""
  bucket_name, object_key, start_byte, end_byte, endpoint_url, access_key, secret_key, region = args

  # Create a fresh client for this worker
  s3_client = create_boto3_client(
    access_key=access_key,
    secret_key=secret_key,
    region=region,
    endpoint_url=endpoint_url,
    max_pool_connections=20  # Smaller pool for part workers
  )

  try:
    response = s3_client.get_object(
      Bucket=bucket_name,
      Key=object_key,
      Range=f"bytes={start_byte}-{end_byte}"
    )
    # Read data and return size
    chunk = response['Body'].read()
    return len(chunk)
  except Exception as e:
    print(f"Error downloading part of {object_key} ({start_byte}-{end_byte}): {e}")
    raise


def download_part_to_file(args):
  """Worker function for downloading a part of a file directly to disk using multiple I/O threads"""
  bucket_name, object_key, start_byte, end_byte, endpoint_url, access_key, secret_key, region, output_file, io_threads, chunk_size_kb = args

  # Create a fresh client for this worker with optimized settings
  s3_client = create_boto3_client(
    access_key=access_key,
    secret_key=secret_key,
    region=region,
    endpoint_url=endpoint_url,
    max_pool_connections=io_threads * 2  # More connections per worker
  )

  try:
    # Calculate chunk boundaries for parallel I/O within this part
    total_bytes = end_byte - start_byte + 1
    bytes_per_io_thread = total_bytes // io_threads

    if bytes_per_io_thread <= 0:
      # If the part is too small for multiple threads, use a single thread
      io_threads = 1
      bytes_per_io_thread = total_bytes

    # Function for each I/O thread to handle a sub-range
    def download_and_write_subrange(thread_idx):
      thread_start = start_byte + (thread_idx * bytes_per_io_thread)
      thread_end = min(thread_start + bytes_per_io_thread - 1, end_byte)

      if thread_idx == io_threads - 1:  # Make sure the last thread gets all remaining bytes
        thread_end = end_byte

      try:
        # Download this thread's sub-range
        thread_response = s3_client.get_object(
          Bucket=bucket_name,
          Key=object_key,
          Range=f"bytes={thread_start}-{thread_end}"
        )

        # Read data in optimized chunks
        thread_body = thread_response['Body']
        thread_bytes_written = 0
        chunk_size = chunk_size_kb * 1024  # Convert KB to bytes

        # Open the file and acquire an exclusive lock for this process
        with open(output_file, 'r+b') as f:
          fcntl.flock(f, fcntl.LOCK_EX)  # Exclusive lock
          try:
            # Seek to the correct position for this thread
            f.seek(thread_start)

            # Use buffered I/O for better performance
            buf = bytearray(chunk_size)
            bytes_read = thread_body.read(chunk_size)
            while bytes_read:
              f.write(bytes_read)
              thread_bytes_written += len(bytes_read)
              bytes_read = thread_body.read(chunk_size)
          finally:
            fcntl.flock(f, fcntl.LOCK_UN)  # Release lock

        return thread_bytes_written
      except Exception as e:
        print(f"Error in I/O thread {thread_idx} downloading part of {object_key} ({thread_start}-{thread_end}): {e}")
        raise

    # Create and start I/O threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=io_threads) as executor:
      futures = [executor.submit(download_and_write_subrange, i) for i in range(io_threads)]
      results = [future.result() for future in concurrent.futures.as_completed(futures)]

    total_bytes_written = sum(results)
    return total_bytes_written

  except Exception as e:
    print(f"Error downloading part of {object_key} ({start_byte}-{end_byte}): {e}")
    raise


def process_single_file(args_dict, file_index, timestamp, file_key):
  """Process a single file download and return information needed for validation"""
  # Extract parameters from the dictionary
  bucket_name = args_dict['bucket_name']
  concurrency = args_dict['concurrency']
  multipart_size_mb = args_dict['multipart_size_mb']
  object_size_mb = args_dict['object_size_mb']
  endpoint_url = args_dict['endpoint_url']
  access_key = args_dict['access_key_aws_id']
  secret_key = args_dict['secret_access_key']
  region = args_dict['region_name']
  max_pool_connections = args_dict['max_pool_connections']
  prefix = args_dict.get('prefix')
  save_to_disk = args_dict.get('save_to_disk')
  streaming_mode = args_dict.get('streaming_mode', True)
  io_threads = args_dict.get('io_threads', 8)
  chunk_size_kb = args_dict.get('chunk_size_kb', 1024)
  auto_optimize = args_dict.get('auto_optimize', True)

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

  operation_start = time.time()
  retry_count = 0
  max_retries = 10

  # For download with prefix, get the actual file size
  if prefix:
    try:
      # Get the actual file size for existing files with prefix
      head_res = s3_client.head_object(Bucket=bucket_name, Key=file_key)
      object_size_mb = int(head_res['ContentLength']) / (1024 * 1024)
      file_display_name = file_key  # Use the original key name for display
    except Exception as e:
      print(f"Warning: Failed to get size for {file_key}: {e}")
      file_display_name = f"file_{file_index}"
  else:
    file_display_name = f"file_{file_index}"

  # Prepare output path if saving to disk
  output_path = None
  if save_to_disk:
    # Create a clean filename based on the file_key
    safe_filename = os.path.basename(file_key).replace('/', '_')
    output_path = os.path.join(save_to_disk, safe_filename)

  # Main processing loop with retries
  while retry_count <= max_retries:
    try:
      # Get the object size
      head_res = s3_client.head_object(Bucket=bucket_name, Key=file_key)
      content_length = int(head_res['ContentLength'])

      # If we're not saving to disk or not using streaming mode, use the memory method
      if not save_to_disk or not streaming_mode:
        # Calculate parts for download
        download_parts = []
        position = 0

        while position < content_length:
          end_position = min(position + multipart_size - 1, content_length - 1)

          download_parts.append((
            bucket_name,
            file_key,
            position,
            end_position,
            endpoint_url,
            access_key,
            secret_key,
            region
          ))

          position = end_position + 1

        # Download parts in parallel
        with multiprocessing.Pool(processes=concurrency) as pool:
          results = pool.map(download_part_to_memory, download_parts)

        # Calculate total downloaded size
        total_downloaded = sum(results)

      else:
        # For direct streaming to disk:
        # 1. Create the file with the right size first
        with open(output_path, 'wb') as f:
          # Pre-allocate the file size for better performance
          f.seek(content_length - 1)
          f.write(b'\0')

        # 2. Calculate parts for download
        download_parts = []
        position = 0

        while position < content_length:
          end_position = min(position + multipart_size - 1, content_length - 1)

          download_parts.append((
            bucket_name,
            file_key,
            position,
            end_position,
            endpoint_url,
            access_key,
            secret_key,
            region,
            output_path,
            io_threads,
            chunk_size_kb
          ))

          position = end_position + 1

        # 3. Download parts in parallel directly to the file
        with multiprocessing.Pool(processes=concurrency) as pool:
          results = pool.map(download_part_to_file, download_parts)

        # 4. Calculate total downloaded size
        total_downloaded = sum(results)

      # Calculate throughput
      operation_end = time.time()
      duration = operation_end - operation_start
      throughput_mb = object_size_mb / duration

      print(colored(
        f"{file_display_name}: size={object_size_mb:.2f}MB, duration={duration:.2f}s, throughput={throughput_mb:.2f} MiB/sec",
        "green"
      ))

      # Return information
      return {
        'file_key': file_key,
        'file_display_name': file_display_name,
        'download_success': True,
        'content_length': content_length,
        'download_size': total_downloaded,
        'output_path': output_path,
        'throughput_mb': throughput_mb
      }

    except Exception as e:
      retry_count += 1
      if retry_count > max_retries:
        print(f"Failed to process {file_display_name} after {max_retries} retries: {e}")
        return {
          'file_key': file_key,
          'file_display_name': file_display_name,
          'download_success': False,
          'content_length': 0,
          'download_size': 0,
          'output_path': output_path,
          'error': str(e),
          'throughput_mb': 0
        }
      print(f"Error processing {file_display_name} (attempt {retry_count}/{max_retries}): {e}")
      time.sleep(1)  # Wait before retrying


def validate_downloaded_files(download_results, args):
  """Validate all downloaded files after downloads are complete"""
  if args.skip_validation:
    print("\nValidation skipped as requested.")
    return download_results

  print("\nValidating downloaded files...")

  if not args.save_to_disk:
    print("No files saved to disk, skipping validation.")
    return download_results

  validation_results = []
  validation_failures = []

  for result in download_results:
    if not result['download_success']:
      # Skip files that failed during download
      validation_failures.append(result)
      validation_results.append(result)
      continue

    file_path = result['output_path']
    expected_size = result['content_length']

    try:
      # Get actual file size on disk
      actual_size = os.path.getsize(file_path)

      # Compare sizes
      if actual_size == expected_size:
        print(colored(f"✓ {result['file_display_name']}: Validation successful ({actual_size} bytes)", "green"))
        result['validation_success'] = True
      else:
        error_msg = f"Size mismatch: expected {expected_size} bytes, got {actual_size} bytes"
        print(colored(f"✗ {result['file_display_name']}: Validation failed - {error_msg}", "red"))
        result['validation_success'] = False
        result['validation_error'] = error_msg
        validation_failures.append(result)
    except Exception as e:
      error_msg = f"Validation error: {str(e)}"
      print(colored(f"✗ {result['file_display_name']}: {error_msg}", "red"))
      result['validation_success'] = False
      result['validation_error'] = error_msg
      validation_failures.append(result)

    validation_results.append(result)

  # Print validation summary
  total = len(validation_results)
  failed = len(validation_failures)

  if failed > 0:
    print(colored(f"\nValidation completed with issues: {failed} out of {total} files failed validation", "yellow"))
  else:
    print(colored(f"\nValidation completed successfully: All {total} files passed validation", "green"))

  return validation_results


def upload_test_file(bucket_name, object_key, object_size_mb, concurrency, multipart_size_mb,
                     endpoint_url, access_key, secret_key, region):
  """Upload a test file for download benchmarking"""
  print(f"Uploading test file {object_key} of size {object_size_mb}MB...")

  # Create a client
  s3_client = create_boto3_client(
    access_key=access_key,
    secret_key=secret_key,
    region=region,
    endpoint_url=endpoint_url,
    max_pool_connections=concurrency
  )

  # Generate random data
  data = np.random.bytes(object_size_mb * 1024 * 1024)

  # Calculate multipart size
  multipart_size = multipart_size_mb * 1024 * 1024

  # Start multipart upload
  multipart_upload = s3_client.create_multipart_upload(
    Bucket=bucket_name,
    Key=object_key
  )
  upload_id = multipart_upload['UploadId']

  # Prepare parts
  upload_parts = []
  part_number = 1
  position = 0

  # Helper function for part upload
  def upload_part_worker(args):
    """Worker function for uploading a part of a file"""
    bucket_name, object_key, upload_id, part_number, data, endpoint_url, access_key, secret_key, region = args

    # Create a fresh client for this worker
    s3_client = create_boto3_client(
      access_key=access_key,
      secret_key=secret_key,
      region=region,
      endpoint_url=endpoint_url,
      max_pool_connections=20  # Smaller pool for part workers
    )

    try:
      response = s3_client.upload_part(
        Bucket=bucket_name,
        Key=object_key,
        PartNumber=part_number,
        UploadId=upload_id,
        Body=data
      )
      return part_number, response['ETag']
    except Exception as e:
      print(f"Error uploading part {part_number} of {object_key}: {e}")
      raise

  while position < len(data):
    end_position = min(position + multipart_size, len(data))
    part_data = data[position:end_position]

    upload_parts.append((
      bucket_name,
      object_key,
      upload_id,
      part_number,
      part_data,
      endpoint_url,
      access_key,
      secret_key,
      region
    ))

    position = end_position
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

  print(f"Test file uploaded as {object_key}")
  return object_key


def auto_optimize_parameters(args):
  """Automatically optimize parameters based on CPU count and system info"""
  # Skip optimization if auto_optimize is False
  if not args.auto_optimize:
    print("Auto-optimization disabled. Using provided parameters.")
    return args

  import psutil

  # Get system information
  cpu_count = os.cpu_count()
  memory_info = psutil.virtual_memory()
  total_memory_gb = memory_info.total / (1024 ** 3)
  disk_io = psutil.disk_io_counters()

  print(f"Detected {cpu_count} CPU cores and {total_memory_gb:.1f} GB memory")

  # Optimize file parallelism based on CPU count
  if args.auto_optimize:
    # Set file_parallelism to use 75% of available cores
    optimal_file_parallelism = max(1, int(cpu_count * 0.75))
    if args.file_parallelism != optimal_file_parallelism:
      print(f"Auto-optimizing: Changing file_parallelism from {args.file_parallelism} to {optimal_file_parallelism}")
      args.file_parallelism = optimal_file_parallelism

    # Calculate optimal part concurrency
    # Aim to keep total threads at 4x CPU count for optimal IO-bound performance
    total_desired_threads = cpu_count * 4
    threads_per_file = max(2, total_desired_threads // args.file_parallelism)
    if args.concurrency != threads_per_file:
      print(f"Auto-optimizing: Changing concurrency from {args.concurrency} to {threads_per_file}")
      args.concurrency = threads_per_file

    # Set io_threads based on remaining CPU capacity
    # We want to fully saturate the CPU without oversubscribing too much
    optimal_io_threads = max(2, cpu_count // args.file_parallelism)
    if args.io_threads != optimal_io_threads:
      print(f"Auto-optimizing: Changing io_threads from {args.io_threads} to {optimal_io_threads}")
      args.io_threads = optimal_io_threads

    # Set optimal connection pools
    optimal_connections = args.file_parallelism * args.concurrency * 2
    if args.max_pool_connections != optimal_connections:
      print(f"Auto-optimizing: Changing max_pool_connections from {args.max_pool_connections} to {optimal_connections}")
      args.max_pool_connections = optimal_connections

    # Adjust chunk size based on memory
    # For systems with more memory, larger chunks improve throughput
    if total_memory_gb > 16:
      optimal_chunk_size = 4096  # 4MB for high-memory systems
    elif total_memory_gb > 8:
      optimal_chunk_size = 2048  # 2MB for medium-memory systems
    else:
      optimal_chunk_size = 1024  # 1MB for low-memory systems

    if args.chunk_size_kb != optimal_chunk_size:
      print(f"Auto-optimizing: Changing chunk_size_kb from {args.chunk_size_kb} to {optimal_chunk_size}")
      args.chunk_size_kb = optimal_chunk_size

  return args


def main():
  # Parse command line arguments
  args = parse_args()

  try:
    # Try to import psutil for better auto-optimization
    import psutil
    # Auto-optimize parameters based on system capabilities
    if args.auto_optimize:
      args = auto_optimize_parameters(args)
  except ImportError:
    print("Warning: psutil not available. Auto-optimization will be limited.")
    # Basic optimization without psutil
    if args.auto_optimize:
      cpu_count = os.cpu_count()
      args.file_parallelism = max(1, int(cpu_count * 0.75))
      args.concurrency = max(2, (cpu_count * 4) // args.file_parallelism)
      args.io_threads = max(2, cpu_count // args.file_parallelism)
      args.max_pool_connections = args.file_parallelism * args.concurrency * 2
    else:
      print("Auto-optimization disabled. Using provided parameters.")

  # Make sure save directory exists if specified
  if args.save_to_disk:
    directory_success = ensure_directory_exists(args.save_to_disk)
    if not directory_success:
      print(colored("Warning: Failed to create or access directory. Files will not be saved to disk.", "yellow"))
      args.save_to_disk = None

  # Initialize S3 client for the main process
  s3_client = create_boto3_client(
    access_key=args.access_key_aws_id,
    secret_key=args.secret_access_key,
    region=args.region_name,
    endpoint_url=args.endpoint_url,
    max_pool_connections=args.max_pool_connections
  )

  # Current timestamp for filenames
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  start_time = datetime.now()

  # Get files to download from bucket
  file_keys = []
  if args.prefix:
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
  else:
    # For download benchmark without prefix, upload the file first and use it as the source
    print("Preparing source file for download testing...")
    source_filename = f"download_source_{timestamp}"
    upload_test_file(
      bucket_name=args.bucket_name,
      object_key=source_filename,
      object_size_mb=args.object_size_mb,
      concurrency=args.concurrency,
      multipart_size_mb=args.multipart_size_mb,
      endpoint_url=args.endpoint_url,
      access_key=args.access_key_aws_id,
      secret_key=args.secret_access_key,
      region=args.region_name
    )
    # Use the uploaded file for all download iterations
    file_keys = [source_filename] * args.iteration_number

  # Set up process-based parallelism for file operations
  # Use the number of CPUs as guidance but don't exceed the requested file_parallelism
  max_workers = min(args.file_parallelism, args.iteration_number)
  print(f"Using {max_workers} parallel workers for file operations")

  throughputs = []

  # Convert args to dictionary for pickling
  args_dict = vars(args)

  # Use ProcessPoolExecutor to handle multiple files in parallel
  with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
    futures = []

    # Submit all file processing tasks
    for i, file_key in enumerate(file_keys):
      future = executor.submit(
        process_single_file,
        args_dict,
        i,
        timestamp,
        file_key  # Use the actual file key
      )
      futures.append(future)

    # Collect results as they complete
    download_results = []
    for future in concurrent.futures.as_completed(futures):
      try:
        result = future.result()
        download_results.append(result)
        if result['download_success']:
          throughputs.append(result['throughput_mb'])
      except Exception as e:
        print(f"File processing failed: {e}")

  # Validate all downloaded files after all downloads are complete
  validation_results = validate_downloaded_files(download_results, args)

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
      "save_to_disk": args.save_to_disk,
      "streaming_mode": args.streaming_mode,
    }

    summary = {
      "mode": "download",
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
      "save_to_disk": args.save_to_disk,
      "validation_summary": {
        "total_files": len(validation_results),
        "successful_downloads": sum(1 for r in validation_results if r['download_success']),
        "validation_failures": sum(1 for r in validation_results if r.get('validation_success') is False)
      }
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
  # Use spawn method for better compatibility
  multiprocessing.set_start_method('spawn', force=True)
  main()
