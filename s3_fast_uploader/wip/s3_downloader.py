import argparse
import json
import multiprocessing
import time
import concurrent.futures
from datetime import datetime
import os
import sys

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
  parser.add_argument("--iteration-number", help="Iteration Number", type=int, required=True)
  parser.add_argument("--object-size-mb", help="Object Size in MB", type=int, required=True)
  parser.add_argument("--concurrency", help="Concurrency per file", type=int, required=True)
  parser.add_argument("--multipart-size-mb", help="Multipart part Size in MB", type=int, required=True)
  parser.add_argument("--file-parallelism", help="Number of files to download in parallel", type=int, default=4)
  parser.add_argument("--max-pool-connections", help="Max boto3 connection pool size", type=int, default=100)
  parser.add_argument("--prefix", help="Only download files with this prefix", type=str, default=None)
  parser.add_argument("--save-to-disk", help="Path where to save downloaded files", type=str, default=None)

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


def download_part_worker(args):
  """Worker function for downloading a part of a file"""
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
    # Read data
    chunk = response['Body'].read()
    return start_byte, chunk  # Always return position and data for reassembly
  except Exception as e:
    print(f"Error downloading part of {object_key} ({start_byte}-{end_byte}): {e}")
    raise


def save_file_to_disk_worker(args):
  """Worker function for saving a file to disk"""
  file_data, output_path = args
  try:
    with open(output_path, 'wb') as f:
      f.write(file_data)
    return True, output_path
  except Exception as e:
    return False, f"Error saving {output_path}: {e}"


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
  save_to_disk = args_dict.get('save_to_disk', None)

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
        results = pool.map(download_part_worker, download_parts)

      # Sort results by start position and concat data
      sorted_results = sorted(results, key=lambda x: x[0])
      complete_file = b''.join([chunk for _, chunk in sorted_results])

      # Calculate throughput
      operation_end = time.time()
      duration = operation_end - operation_start
      throughput_mb = object_size_mb / duration

      print(colored(
        f"{file_display_name}: size={object_size_mb:.2f}MB, duration={duration:.2f}s, throughput={throughput_mb:.2f} MiB/sec",
        "green"
      ))

      # Return information - we'll handle disk saving later in a separate phase
      return {
        'file_key': file_key,
        'file_display_name': file_display_name,
        'download_success': True,
        'content_length': content_length,
        'download_size': len(complete_file),
        'output_path': output_path,
        'throughput_mb': throughput_mb,
        'file_data': complete_file  # Always keep the data in memory
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
          'throughput_mb': 0,
          'file_data': None
        }
      print(f"Error processing {file_display_name} (attempt {retry_count}/{max_retries}): {e}")
      time.sleep(1)  # Wait before retrying


def upload_test_file(bucket_name, object_key, object_size_mb, concurrency, multipart_size_mb,
                     endpoint_url, access_key, secret_key, region):
  """Upload a terraform file for download benchmarking"""
  print(f"Uploading terraform file {object_key} of size {object_size_mb}MB...")

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


def save_files_to_disk(download_results, max_workers):
  """Save all successfully downloaded files to disk in parallel"""
  if not any(result.get('output_path') for result in download_results):
    print("No files to save to disk.")
    return download_results

  print("\nSaving files to disk in parallel...")
  save_tasks = []

  # Prepare tasks for files that were successfully downloaded
  for result in download_results:
    if result['download_success'] and result.get('file_data') and result.get('output_path'):
      save_tasks.append((result['file_data'], result['output_path']))

  save_results = []

  # Use ProcessPoolExecutor to save files in parallel
  with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(save_file_to_disk_worker, task) for task in save_tasks]

    for future, result in zip(futures, download_results):
      try:
        save_success, message = future.result()
        result['save_success'] = save_success
        if not save_success:
          result['save_error'] = message
          print(colored(f"Error saving {result['file_display_name']}: {message}", "red"))
        else:
          print(colored(f"Saved {result['file_display_name']} to {result['output_path']}", "green"))
      except Exception as e:
        result['save_success'] = False
        result['save_error'] = str(e)
        print(colored(f"Error saving {result['file_display_name']}: {e}", "red"))

      # Remove the file data from the result to free memory
      if 'file_data' in result:
        del result['file_data']

      save_results.append(result)

  # Print summary
  total_files = len(save_tasks)
  successful = sum(1 for r in save_results if r.get('save_success') == True)
  print(f"\nSaved {successful} out of {total_files} files to disk")

  return save_results


def validate_downloaded_files(download_results, args):
  """Validate all downloaded files after downloads are complete"""
  print("\nValidating downloaded files...")

  if not args.save_to_disk:
    print("No files saved to disk, skipping validation.")
    return download_results

  validation_results = []
  validation_failures = []

  for result in download_results:
    if not result.get('save_success', False):
      # Skip files that failed during save
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

  # PHASE 2: Save all files to disk in parallel (if save_to_disk is enabled)
  if args.save_to_disk:
    download_results = save_files_to_disk(download_results, max_workers)

    # Validate all downloaded files after all saves are complete
    validation_results = validate_downloaded_files(download_results, args)
  else:
    validation_results = download_results

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
        "successful_saves": sum(
          1 for r in validation_results if r.get('save_success', False)) if args.save_to_disk else 0,
        "validation_failures": sum(
          1 for r in validation_results if r.get('validation_success') is False) if args.save_to_disk else 0
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
