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
  parser.add_argument("--deferred-assembly",
                      help="When to assemble and save files (only applies when save-to-disk is enabled): 'per-file' (default) or 'all-files'",
                      choices=['per-file', 'all-files'], default='per-file')

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
  # Check the number of arguments to determine which format we're using
  if len(args) == 11:  # Extended format with file_index and part_index
    bucket_name, object_key, start_byte, end_byte, endpoint_url, access_key, secret_key, region, save_to_disk, file_index, part_index = args
    include_indices = True
  elif len(args) == 9:  # Original format for compatibility with process_single_file
    bucket_name, object_key, start_byte, end_byte, endpoint_url, access_key, secret_key, region, save_to_disk = args
    include_indices = False
    file_index = part_index = 0
  else:
    raise ValueError(f"Unexpected number of arguments: {len(args)}")

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
    if save_to_disk:
      if include_indices:
        # Return file_index, part_index, position and data for later reassembly
        return file_index, part_index, start_byte, chunk
      else:
        # Original format for backward compatibility
        return start_byte, chunk
    else:
      return len(chunk)  # Just return the size we processed
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
  if save_to_disk:
    # Create a clean filename based on the file_key
    safe_filename = os.path.basename(file_key).replace('/', '_')
    output_path = os.path.join(save_to_disk, safe_filename)
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

      while position < content_length:
        end_position = min(position + multipart_size - 1, content_length - 1)

        # Note: We're using the original parameter format (9 parameters)
        # This is compatible with the updated download_part_worker function
        download_parts.append((
          bucket_name,
          file_key,
          position,
          end_position,
          endpoint_url,
          access_key,
          secret_key,
          region,
          save_to_disk is not None  # Need to keep data if we're saving to disk
        ))

        position = end_position + 1

      # Download parts in parallel
      with multiprocessing.Pool(processes=concurrency) as pool:
        results = pool.map(download_part_worker, download_parts)

      # If saving to disk, reassemble the file
      if save_to_disk:
        # Sort results by start position
        sorted_results = sorted(results, key=lambda x: x[0])

        # Concatenate all parts
        complete_file = b''.join([chunk for _, chunk in sorted_results])

        # Save to disk without immediate validation
        if output_path:
          try:
            with open(output_path, 'wb') as f:
              f.write(complete_file)
            print(f"Downloaded {file_display_name} to {output_path}")
          except Exception as e:
            print(colored(f"Error saving file to disk: {e}", "red"))
            # Return error info for validation phase
            return {
              'file_key': file_key,
              'file_display_name': file_display_name,
              'download_success': False,
              'content_length': content_length,
              'download_size': 0,
              'output_path': output_path,
              'error': str(e),
              'throughput_mb': 0
            }

        # If we get here, the download was successful
        # Calculate throughput
        operation_end = time.time()
        duration = operation_end - operation_start
        throughput_mb = object_size_mb / duration

        print(colored(
          f"{file_display_name}: size={object_size_mb:.2f}MB, duration={duration:.2f}s, throughput={throughput_mb:.2f} MiB/sec",
          "green"
        ))

        # Return information needed for later validation
        return {
          'file_key': file_key,
          'file_display_name': file_display_name,
          'download_success': True,
          'content_length': content_length,
          'download_size': len(complete_file) if save_to_disk else sum(results),
          'output_path': output_path,
          'throughput_mb': throughput_mb
        }
      else:
        # If not saving to disk, just calculate the size from the results
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
          'output_path': None,
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


def download_file_parts(args_dict, file_index, file_key):
  """Download all parts of a file and return them for later reassembly"""
  # Extract parameters from the dictionary
  bucket_name = args_dict['bucket_name']
  concurrency = args_dict['concurrency']
  multipart_size_mb = args_dict['multipart_size_mb']
  endpoint_url = args_dict['endpoint_url']
  access_key = args_dict['access_key_aws_id']
  secret_key = args_dict['secret_access_key']
  region = args_dict['region_name']
  max_pool_connections = args_dict['max_pool_connections']
  save_to_disk = args_dict.get('save_to_disk', None)

  # Create a client for the main operations
  s3_client = create_boto3_client(
    access_key=access_key,
    secret_key=secret_key,
    region=region,
    endpoint_url=endpoint_url,
    max_pool_connections=max_pool_connections
  )

  # Calculate multipart size in bytes
  multipart_size = multipart_size_mb * 1024 * 1024

  operation_start = time.time()
  retry_count = 0
  max_retries = 10

  # Get file display name
  if '/' in file_key:
    file_display_name = file_key  # Use the original key name for display
  else:
    file_display_name = f"file_{file_index}"

  # Prepare output path if saving to disk
  if save_to_disk:
    # Create a clean filename based on the file_key
    safe_filename = os.path.basename(file_key).replace('/', '_')
    output_path = os.path.join(save_to_disk, safe_filename)
  else:
    output_path = None

  # Main processing loop with retries
  while retry_count <= max_retries:
    try:
      # Get the object size
      head_res = s3_client.head_object(Bucket=bucket_name, Key=file_key)
      content_length = int(head_res['ContentLength'])
      object_size_mb = content_length / (1024 * 1024)

      # Calculate parts for download
      download_parts = []
      position = 0
      part_index = 0

      while position < content_length:
        end_position = min(position + multipart_size - 1, content_length - 1)

        # Add file_index and part_index to the tuple
        download_parts.append((
          bucket_name,
          file_key,
          position,
          end_position,
          endpoint_url,
          access_key,
          secret_key,
          region,
          save_to_disk is not None,  # Need to keep data if we're saving to disk
          file_index,
          part_index
        ))

        position = end_position + 1
        part_index += 1

      # Download parts in parallel
      with multiprocessing.Pool(processes=concurrency) as pool:
        results = pool.map(download_part_worker, download_parts)

      # Calculate throughput
      operation_end = time.time()
      duration = operation_end - operation_start
      throughput_mb = object_size_mb / duration

      print(colored(
        f"{file_display_name}: size={object_size_mb:.2f}MB, duration={duration:.2f}s, throughput={throughput_mb:.2f} MiB/sec",
        "green"
      ))

      # Return information for later processing
      return {
        'file_key': file_key,
        'file_index': file_index,
        'file_display_name': file_display_name,
        'download_success': True,
        'content_length': content_length,
        'output_path': output_path,
        'results': results,
        'throughput_mb': throughput_mb,
        'save_to_disk': save_to_disk is not None
      }

    except Exception as e:
      retry_count += 1
      if retry_count > max_retries:
        print(f"Failed to process {file_display_name} after {max_retries} retries: {e}")
        return {
          'file_key': file_key,
          'file_index': file_index,
          'file_display_name': file_display_name,
          'download_success': False,
          'content_length': 0,
          'output_path': output_path if save_to_disk else None,
          'error': str(e),
          'throughput_mb': 0,
          'save_to_disk': save_to_disk is not None
        }
      print(f"Error processing {file_display_name} (attempt {retry_count}/{max_retries}): {e}")
      time.sleep(1)  # Wait before retrying


def reassemble_and_save_files(download_results):
  """Reassemble and save all successfully downloaded files"""
  print("\nReassembling and saving downloaded files...")

  # Filter for successful downloads that need to be saved
  save_tasks = [result for result in download_results if result['download_success'] and result['save_to_disk']]

  # Group results by file_index to organize parts
  files_to_save = {}
  for task in save_tasks:
    file_index = task['file_index']
    if file_index not in files_to_save:
      files_to_save[file_index] = task

  save_results = []

  for file_index, file_info in files_to_save.items():
    try:
      # Get all parts for this file
      file_parts = []
      for result in file_info['results']:
        if isinstance(result, tuple) and len(result) >= 4:
          # Extract the correct tuple elements (file_index, part_index, start_byte, chunk)
          if result[0] == file_index:  # Ensure this part belongs to the current file
            file_parts.append((result[2], result[3]))  # (start_byte, chunk)

      # Sort by start position
      sorted_parts = sorted(file_parts, key=lambda x: x[0])

      # Concatenate all parts
      complete_file = b''.join([chunk for _, chunk in sorted_parts])

      # Save to disk
      output_path = file_info['output_path']
      if output_path:
        with open(output_path, 'wb') as f:
          f.write(complete_file)
        print(colored(f"✓ {file_info['file_display_name']}: Saved to {output_path}", "green"))

        # Add validation info
        file_info['validation_success'] = len(complete_file) == file_info['content_length']
        if not file_info['validation_success']:
          file_info[
            'validation_error'] = f"Size mismatch: expected {file_info['content_length']} bytes, got {len(complete_file)} bytes"
          print(colored(f"✗ Validation failed for {file_info['file_display_name']}: {file_info['validation_error']}",
                        "red"))

        # Update download_size for statistics
        file_info['download_size'] = len(complete_file)

      save_results.append(file_info)
    except Exception as e:
      print(colored(f"Error saving {file_info['file_display_name']}: {e}", "red"))
      file_info['validation_success'] = False
      file_info['validation_error'] = str(e)
      save_results.append(file_info)

  return save_results


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


def validate_downloaded_files(save_results, args):
  """Validate all downloaded files after saves are complete"""
  print("\nValidating downloaded files...")

  if not args.save_to_disk:
    print("No files saved to disk, skipping validation.")
    return save_results

  validation_failures = []

  # Print validation summary
  total = len(save_results)
  failed = sum(1 for r in save_results if r.get('validation_success') is False)

  if failed > 0:
    print(colored(f"\nValidation completed with issues: {failed} out of {total} files failed validation", "yellow"))
  else:
    print(colored(f"\nValidation completed successfully: All {total} files passed validation", "green"))

  return save_results


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
  max_workers = min(args.file_parallelism, args.iteration_number)
  print(f"Using {max_workers} parallel workers for file operations")

  throughputs = []
  download_results = []
  save_results = []
  download_duration = 0
  save_duration = 0

  # Convert args to dictionary for pickling
  args_dict = vars(args)

  # Determine if we should use deferred assembly
  # Only consider deferred assembly if save-to-disk is enabled
  use_deferred_assembly = args.save_to_disk and args.deferred_assembly == 'all-files'

  if use_deferred_assembly:
    # ==== TWO-PHASE APPROACH: DOWNLOAD EVERYTHING FIRST, THEN REASSEMBLE ====
    print("\n===== PHASE 1: DOWNLOADING ALL FILE PARTS =====")
    print(f"Using deferred assembly mode: download all files first, then save to disk")
    download_start = datetime.now()

    # Use ProcessPoolExecutor to handle multiple files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
      futures = []

      # Submit all file downloading tasks
      for i, file_key in enumerate(file_keys):
        future = executor.submit(
          download_file_parts,
          args_dict,
          i,
          file_key
        )
        futures.append(future)

      # Collect results as they complete
      for future in concurrent.futures.as_completed(futures):
        try:
          result = future.result()
          download_results.append(result)
          if result['download_success']:
            throughputs.append(result['throughput_mb'])
        except Exception as e:
          print(f"File processing failed: {e}")

    download_end = datetime.now()
    download_duration = (download_end - download_start).total_seconds()
    print(f"\nDownload phase completed in {download_duration:.2f} seconds")

    # Reassembly and save phase
    if args.save_to_disk:
      print("\n===== PHASE 2: REASSEMBLING AND SAVING FILES =====")
      save_start = datetime.now()

      # Reassemble and save all files
      save_results = reassemble_and_save_files(download_results)

      save_end = datetime.now()
      save_duration = (save_end - save_start).total_seconds()
      print(f"\nSave phase completed in {save_duration:.2f} seconds")

      # Validate saved files
      validation_results = validate_downloaded_files(save_results, args)
    else:
      validation_results = download_results

  else:
    # ==== ORIGINAL APPROACH: DOWNLOAD AND PROCESS EACH FILE INDEPENDENTLY ====
    print("\n===== DOWNLOADING AND PROCESSING FILES INDIVIDUALLY =====")
    if args.save_to_disk:
      print(f"Using standard assembly mode: files will be saved as they are downloaded")
    else:
      print(f"No save-to-disk specified: files will be downloaded to memory only")
    download_start = datetime.now()

    # Use ProcessPoolExecutor to handle multiple files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
      futures = []

      # Submit all file processing tasks
      for i, file_key in enumerate(file_keys):
        future = executor.submit(
          process_single_file,  # Use the original function that downloads and saves per file
          args_dict,
          i,
          timestamp,
          file_key
        )
        futures.append(future)

      # Collect results as they complete
      for future in concurrent.futures.as_completed(futures):
        try:
          result = future.result()
          download_results.append(result)
          if result['download_success']:
            throughputs.append(result['throughput_mb'])
        except Exception as e:
          print(f"File processing failed: {e}")

    download_end = datetime.now()
    download_duration = (download_end - download_start).total_seconds()
    print(f"\nAll files processed in {download_duration:.2f} seconds")

    # For per-file approach, validation happens inside process_single_file
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
      "assembly_mode": args.deferred_assembly,
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
      "phases": {
        "download": {
          "duration_seconds": download_duration
        },
        "save": {
          "duration_seconds": save_duration if args.save_to_disk else 0
        }
      },
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

    if args.save_to_disk:
      print(f"Assembly Mode: {args.deferred_assembly}")
      if use_deferred_assembly:
        print(f"  - Download phase: {download_duration:.2f} seconds")
        print(f"  - Save phase: {save_duration:.2f} seconds")
  else:
    print("No successful downloads to report.")


if __name__ == "__main__":
  # Set higher shared memory limit for better multiprocessing performance
  # Use spawn method for better compatibility
  multiprocessing.set_start_method('spawn', force=True)
  main()
