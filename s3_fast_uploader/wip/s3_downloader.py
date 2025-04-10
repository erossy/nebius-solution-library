import argparse
import json
import multiprocessing
import time
import concurrent.futures
from datetime import datetime
import os
import sys
import threading
import queue
from collections import defaultdict

import boto3
import boto3.s3.transfer
import botocore.config
import numpy as np
from termcolor import colored

# Global shared resources - initialized in main process
manager = None
file_parts = None
complete_files = None
file_metadata = None
file_lock = None


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
  parser.add_argument("--save-interval", help="Interval in seconds to check for completed files", type=float,
                      default=0.5)

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


def register_file(file_key, total_parts, content_length):
  """Register a new file in the tracking system"""
  with file_lock:
    file_parts[file_key] = manager.dict()
    file_metadata[file_key] = {
      'total_parts': total_parts,
      'content_length': content_length,
      'download_start': time.time(),
      'parts_completed': 0
    }


def add_part(file_key, part_number, start_byte, part_file):
  """Add a downloaded part reference to the tracking system"""
  with file_lock:
    if file_key in file_parts:
      file_parts[file_key][part_number] = (start_byte, part_file)

      # Update metadata
      if file_key in file_metadata:
        file_metadata[file_key]['parts_completed'] += 1

        # Check if file is complete
        if file_metadata[file_key]['parts_completed'] >= file_metadata[file_key]['total_parts']:
          complete_files[file_key] = True

      return True
    return False


def get_complete_files():
  """Get a list of files that are complete and ready to be written"""
  with file_lock:
    return list(complete_files.keys())


def get_file_parts(file_key):
  """Get all parts for a specific file"""
  if file_key in file_parts:
    return dict(file_parts[file_key])
  return {}


def remove_file(file_key):
  """Remove a file from tracking after it's been written to disk"""
  with file_lock:
    metadata = None
    if file_key in file_metadata:
      metadata = dict(file_metadata[file_key])
      del file_metadata[file_key]

    if file_key in file_parts:
      del file_parts[file_key]

    if file_key in complete_files:
      del complete_files[file_key]

    return metadata


def get_progress():
  """Get current download progress information"""
  with file_lock:
    progress = {}
    for file_key in file_metadata:
      if file_key in file_metadata:
        metadata = dict(file_metadata[file_key])
        progress[file_key] = {
          'total_parts': metadata['total_parts'],
          'parts_completed': metadata['parts_completed'],
          'percentage': (metadata['parts_completed'] / metadata['total_parts']) * 100 if metadata[
                                                                                           'total_parts'] > 0 else 0,
          'complete': file_key in complete_files
        }
    return progress


def download_part_worker(args):
  """Worker function for downloading a part of a file"""
  bucket_name, object_key, start_byte, end_byte, part_number, save_to_disk, endpoint_url, access_key, secret_key, region = args

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

    # Add to download tracker if we're saving to disk
    if save_to_disk:
      # Create directory for temp parts if it doesn't exist
      safe_key = object_key.replace('/', '_')
      part_dir = os.path.join(save_to_disk, f"{safe_key}_parts")
      os.makedirs(part_dir, exist_ok=True)

      # Write the part to a temporary file instead of storing in memory
      part_file = os.path.join(part_dir, f"part_{part_number}")
      with open(part_file, 'wb') as f:
        f.write(chunk)

      # Add reference to the part file instead of the actual binary data
      add_part(object_key, part_number, start_byte, part_file)

    return len(chunk)  # Return the size we processed
  except Exception as e:
    print(f"Error downloading part {part_number} of {object_key} ({start_byte}-{end_byte}): {e}")
    raise


def file_writer_process(save_to_disk, check_interval=0.5):
  """Process that checks for completed files and writes them to disk"""
  print(f"Starting file writer process with check interval of {check_interval} seconds")

  while True:
    try:
      # Check for completed files
      complete_file_keys = get_complete_files()

      for file_key in complete_file_keys:
        try:
          # Get all parts for this file
          parts = get_file_parts(file_key)

          if not parts:
            continue

          # Sort parts by start byte
          sorted_parts = sorted(parts.items(), key=lambda x: parts[x[0]][0])

          # Open all part files and concatenate
          complete_file = bytearray()
          for _, (_, part_file) in sorted_parts:
            try:
              with open(part_file, 'rb') as f:
                complete_file.extend(f.read())
            except Exception as e:
              print(f"Error reading part file {part_file}: {e}")
              raise

          # Create a clean filename based on the file_key
          safe_filename = os.path.basename(file_key).replace('/', '_')
          output_path = os.path.join(save_to_disk, safe_filename)

          # Save to disk
          with open(output_path, 'wb') as f:
            f.write(complete_file)

          # Clean up temp part files
          safe_key = file_key.replace('/', '_')
          part_dir = os.path.join(save_to_disk, f"{safe_key}_parts")
          if os.path.exists(part_dir):
            for _, (_, part_file) in sorted_parts:
              try:
                os.remove(part_file)
              except Exception as e:
                print(f"Warning: Failed to remove temp file {part_file}: {e}")
            try:
              os.rmdir(part_dir)
            except Exception as e:
              print(f"Warning: Failed to remove temp directory {part_dir}: {e}")

          # Get metadata for reporting
          metadata = remove_file(file_key)
          if metadata:
            download_duration = time.time() - metadata['download_start']
            content_length = metadata['content_length']
            object_size_mb = content_length / (1024 * 1024)
            throughput_mb = object_size_mb / download_duration if download_duration > 0 else 0

            print(colored(
              f"Wrote {file_key} to {output_path}: size={object_size_mb:.2f}MB, duration={download_duration:.2f}s, throughput={throughput_mb:.2f} MiB/sec",
              "green"
            ))
          else:
            print(f"Wrote {file_key} to {output_path}")

        except Exception as e:
          print(colored(f"Error writing file {file_key} to disk: {e}", "red"))
          # Remove from tracking to avoid retrying
          remove_file(file_key)

      # No need to run continuously at full CPU
      time.sleep(check_interval)

    except Exception as e:
      print(f"Error in file writer process: {e}")
      time.sleep(check_interval)


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
  save_to_disk = args_dict.get('save_to_disk', None) is not None

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
  if save_to_disk:
    # Create a clean filename based on the file_key
    safe_filename = os.path.basename(file_key).replace('/', '_')
    output_path = os.path.join(args_dict.get('save_to_disk'), safe_filename)
  else:
    output_path = None

  # Main processing loop with retries
  while retry_count <= max_retries:
    try:
      # Get the object size
      head_res = s3_client.head_object(Bucket=bucket_name, Key=file_key)
      content_length = int(head_res['ContentLength'])

      # Calculate parts for download
      download_parts = []
      position = 0
      part_number = 1

      while position < content_length:
        end_position = min(position + multipart_size - 1, content_length - 1)

        download_parts.append((
          bucket_name,
          file_key,
          position,
          end_position,
          part_number,
          save_to_disk,
          endpoint_url,
          access_key,
          secret_key,
          region
        ))

        position = end_position + 1
        part_number += 1

      # Register the file with the download tracker if we're saving to disk
      if save_to_disk:
        register_file(file_key, len(download_parts), content_length)

      # Download parts in parallel
      with multiprocessing.Pool(processes=concurrency) as pool:
        results = pool.map(download_part_worker, download_parts)

      # Calculate total downloaded
      total_downloaded = sum(results)

      # Calculate throughput
      operation_end = time.time()
      duration = operation_end - operation_start
      throughput_mb = object_size_mb / duration

      print(colored(
        f"{file_display_name}: size={object_size_mb:.2f}MB, duration={duration:.2f}s, throughput={throughput_mb:.2f} MiB/sec",
        "green"
      ))

      # Return information without validation
      return {
        'file_key': file_key,
        'file_display_name': file_display_name,
        'download_success': True,
        'content_length': content_length,
        'download_size': total_downloaded,
        'output_path': output_path if save_to_disk else None,
        'throughput_mb': throughput_mb
      }

      # If we get here, the operation was successful
      break

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
          'output_path': output_path if save_to_disk else None,
          'error': str(e),
          'throughput_mb': 0
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


def validate_downloaded_files(bucket_name, validation_files, s3_client, save_to_disk):
  """Validate all downloaded files after downloads are complete"""
  print("\nValidating downloaded files...")

  if not save_to_disk:
    print("No files saved to disk, skipping validation.")
    return []

  validation_results = []
  validation_failures = []

  for file_key in validation_files:
    try:
      # Create safe filename
      safe_filename = os.path.basename(file_key).replace('/', '_')
      file_path = os.path.join(save_to_disk, safe_filename)

      # Get metadata from S3
      head_res = s3_client.head_object(Bucket=bucket_name, Key=file_key)
      expected_size = head_res['ContentLength']

      # Get actual file size on disk
      if os.path.exists(file_path):
        actual_size = os.path.getsize(file_path)

        # Compare sizes
        if actual_size == expected_size:
          print(colored(f"✓ {file_key}: Validation successful ({actual_size} bytes)", "green"))
          result = {
            'file_key': file_key,
            'validation_success': True,
            'expected_size': expected_size,
            'actual_size': actual_size
          }
        else:
          error_msg = f"Size mismatch: expected {expected_size} bytes, got {actual_size} bytes"
          print(colored(f"✗ {file_key}: Validation failed - {error_msg}", "red"))
          result = {
            'file_key': file_key,
            'validation_success': False,
            'validation_error': error_msg,
            'expected_size': expected_size,
            'actual_size': actual_size
          }
          validation_failures.append(result)
      else:
        error_msg = f"File not found: {file_path}"
        print(colored(f"✗ {file_key}: Validation failed - {error_msg}", "red"))
        result = {
          'file_key': file_key,
          'validation_success': False,
          'validation_error': error_msg
        }
        validation_failures.append(result)

      validation_results.append(result)
    except Exception as e:
      error_msg = f"Validation error: {str(e)}"
      print(colored(f"✗ {file_key}: {error_msg}", "red"))
      result = {
        'file_key': file_key,
        'validation_success': False,
        'validation_error': error_msg
      }
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


def progress_reporter(interval=2.0):
  """Report overall download progress periodically"""
  while True:
    try:
      progress = get_progress()
      if progress:
        # Print a simple progress overview
        complete_files_count = sum(1 for f in progress.values() if f['complete'])
        total_files = len(progress)
        total_parts = sum(f['total_parts'] for f in progress.values())
        completed_parts = sum(f['parts_completed'] for f in progress.values())

        if total_parts > 0:
          overall_percentage = (completed_parts / total_parts) * 100
        else:
          overall_percentage = 0

        # Clear line and print progress
        sys.stdout.write("\r" + " " * 80 + "\r")  # Clear line
        sys.stdout.write(
          f"Progress: {complete_files_count}/{total_files} files complete | " +
          f"{completed_parts}/{total_parts} parts ({overall_percentage:.1f}%)"
        )
        sys.stdout.flush()

      time.sleep(interval)
    except Exception as e:
      print(f"\nError in progress reporter: {e}")
      time.sleep(interval)


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

  # Initialize the shared resources for download tracking
  global manager, file_parts, complete_files, file_metadata, file_lock

  # Initialize shared resources if saving to disk
  file_writer_process_obj = None
  progress_reporter_thread = None

  if args.save_to_disk:
    # Initialize shared resources
    manager = multiprocessing.Manager()
    file_parts = manager.dict()
    complete_files = manager.dict()
    file_metadata = manager.dict()
    file_lock = manager.Lock()

    # Start the file writer process
    file_writer_process_obj = multiprocessing.Process(
      target=file_writer_process,
      args=(args.save_to_disk, args.save_interval)
    )
    file_writer_process_obj.daemon = True
    file_writer_process_obj.start()

    # Start the progress reporter thread
    progress_reporter_thread = threading.Thread(
      target=progress_reporter,
      args=(2.0,)  # Report every 2 seconds
    )
    progress_reporter_thread.daemon = True
    progress_reporter_thread.start()

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

  # Wait for the file writer to finish processing any remaining files
  if file_writer_process_obj:
    print("\nWaiting for file writer to finish processing remaining files...")
    time.sleep(5)  # Give some time for the last files to be processed

    # Validate all downloaded files
    validation_results = validate_downloaded_files(
      args.bucket_name,
      [result['file_key'] for result in download_results if result['download_success']],
      s3_client,
      args.save_to_disk
    )

    # Stop the file writer process
    file_writer_process_obj.terminate()
    file_writer_process_obj.join(timeout=2)

    # Clean up any remaining temporary directories
    if args.save_to_disk:
      for file_key in file_keys:
        safe_key = file_key.replace('/', '_')
        part_dir = os.path.join(args.save_to_disk, f"{safe_key}_parts")
        if os.path.exists(part_dir):
          try:
            import shutil
            shutil.rmtree(part_dir, ignore_errors=True)
            print(f"Cleaned up temporary directory: {part_dir}")
          except Exception as e:
            print(f"Warning: Failed to clean up temporary directory {part_dir}: {e}")

  # Stop the progress reporter thread
  if progress_reporter_thread and progress_reporter_thread.is_alive():
    # No need to explicitly stop - daemon threads will exit when main thread exits
    print("\n")  # Just add a newline after the progress output

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
      "save_to_disk": args.save_to_disk is not None,
      "save_interval": args.save_interval if args.save_to_disk else None,
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
        "total_files": len(download_results),
        "successful_downloads": sum(1 for r in download_results if r['download_success']),
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
