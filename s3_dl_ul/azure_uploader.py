import argparse
import json
import multiprocessing
import time
import concurrent.futures
from datetime import datetime
import os
import numpy as np
from termcolor import colored

from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.storage.blob import BlobClient, ContentSettings
from azure.core.exceptions import ResourceExistsError


def parse_args():
  parser = argparse.ArgumentParser()

  parser.add_argument("--connection-string", help="Azure Storage Connection String", required=True)
  parser.add_argument("--container-name", help="Container Name", required=True)
  parser.add_argument("--filename-suffix", help="Filename suffix (can be without _ in the end)", required=False)
  parser.add_argument("--blob-prefix", help="Prefix for Azure blob names", default="")
  parser.add_argument("--iteration-number", help="Iteration Number", type=int, required=True)
  parser.add_argument("--object-size-mb", help="Object Size in MB", type=int, required=True)
  parser.add_argument("--concurrency", help="Concurrency per file", type=int, required=True)
  parser.add_argument("--multipart-size-mb", help="Multipart part Size in MB", type=int, required=True)
  parser.add_argument("--file-parallelism", help="Number of files to upload in parallel", type=int, default=4)
  parser.add_argument("--max-connections", help="Max Azure connection pool size", type=int, default=100)
  parser.add_argument("--delete-previous", help="Delete previous files with the same prefix before starting",
                      action="store_true")

  args = parser.parse_args()

  print(f"Arguments were parsed: {args}")
  return args


def create_azure_client(connection_string, max_connections):
  """Create an Azure Blob Storage client with the given connection string"""
  # Create the BlobServiceClient object
  return BlobServiceClient.from_connection_string(
    connection_string,
    max_connections=max_connections
  )


def upload_part_worker(args):
  """Worker function for uploading a part of a file"""
  container_name, blob_name, part_number, data, connection_string = args

  # Create a fresh client for this worker
  blob_service_client = BlobServiceClient.from_connection_string(
    connection_string,
    max_connections=10  # Smaller pool for part workers
  )

  container_client = blob_service_client.get_container_client(container_name)

  # In Azure, we append to a blob instead of doing multipart uploads like in S3
  append_blob_client = container_client.get_blob_client(f"{blob_name}_part_{part_number}")

  try:
    # Create the append blob if it doesn't exist
    if part_number == 1:
      append_blob_client.create_append_blob()

    # Append the data to the blob
    append_blob_client.append_block(data)
    return part_number, append_blob_client.url
  except Exception as e:
    print(f"Error uploading part {part_number} of {blob_name}: {e}")
    raise


def process_single_file(args_dict, file_index, timestamp):
  """Process a single file upload"""
  # Extract parameters from the dictionary
  container_name = args_dict['container_name']
  concurrency = args_dict['concurrency']
  multipart_size_mb = args_dict['multipart_size_mb']
  object_size_mb = args_dict['object_size_mb']
  connection_string = args_dict['connection_string']
  filename_suffix = args_dict.get('filename_suffix', '') or ''
  blob_prefix = args_dict.get('blob_prefix', '') or ''
  max_connections = args_dict['max_connections']

  # Create a client for the main operations
  blob_service_client = create_azure_client(
    connection_string=connection_string,
    max_connections=max_connections
  )

  container_client = blob_service_client.get_container_client(container_name)

  # Ensure container exists
  try:
    container_client.create_container()
    print(f"Container '{container_name}' created.")
  except ResourceExistsError:
    print(f"Container '{container_name}' already exists.")

  # Calculate actual multipart size in bytes
  multipart_size = multipart_size_mb * 1024 * 1024

  operation_start = time.time()
  retry_count = 0
  max_retries = 10
  file_display_name = f"file_{file_index}"

  # Main processing loop with retries
  while retry_count <= max_retries:
    try:
      # Generate random data for upload
      data = np.random.bytes(object_size_mb * 1024 * 1024)

      # Format prefix to include trailing slash if needed
      formatted_prefix = ""
      if blob_prefix:
        formatted_prefix = blob_prefix if blob_prefix.endswith('/') else f"{blob_prefix}/"

      # Create a unique blob name with the prefix
      blob_name = f"{formatted_prefix}{filename_suffix}my_upload_{file_index}_{timestamp}"

      # For Azure, we'll use the simpler block blob upload for larger files
      blob_client = container_client.get_blob_client(blob_name)

      # For larger files, we break it into chunks and upload them in parallel
      if object_size_mb > 256:  # 256MB is a good threshold for block blobs
        # Prepare the parts for upload
        upload_parts = []
        part_number = 1
        position = 0

        while position < len(data):
          end_position = min(position + multipart_size, len(data))
          part_data = data[position:end_position]

          upload_parts.append((
            container_name,
            blob_name,
            part_number,
            part_data,
            connection_string
          ))

          position = end_position
          part_number += 1

        # Upload parts in parallel
        with multiprocessing.Pool(processes=concurrency) as pool:
          results = pool.map(upload_part_worker, upload_parts)

        # In Azure, we need to create a block list and commit it
        block_list = [f"part_{part_num}" for part_num, _ in results]
        blob_client.commit_block_list(block_list)
      else:
        # For smaller files, direct upload is more efficient
        blob_client.upload_blob(data, overwrite=True)

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


def delete_previous_files(container_name, prefix, container_client):
  """Delete all blobs with the given prefix from the container"""
  if not prefix:
    print("Warning: No prefix specified for deletion. Skipping...")
    return

  print(f"Listing blobs with prefix '{prefix}' for deletion...")
  delete_count = 0

  # List all blobs with the prefix
  blobs = container_client.list_blobs(name_starts_with=prefix)

  for blob in blobs:
    try:
      container_client.delete_blob(blob.name)
      delete_count += 1
    except Exception as e:
      print(f"Error deleting {blob.name}: {e}")

  print(f"Deleted {delete_count} blobs with prefix '{prefix}'")


def main():
  # Parse command line arguments
  args = parse_args()

  # Initialize Azure client for the main process
  blob_service_client = create_azure_client(
    connection_string=args.connection_string,
    max_connections=args.max_connections
  )

  container_client = blob_service_client.get_container_client(args.container_name)

  # Ensure container exists
  try:
    container_client.create_container()
    print(f"Container '{args.container_name}' created.")
  except ResourceExistsError:
    print(f"Container '{args.container_name}' already exists.")

  # Format filename suffix
  filename_suffix = args.filename_suffix or ""
  if filename_suffix and filename_suffix[-1] != "_":
    filename_suffix += "_"

  # Format blob prefix
  blob_prefix = args.blob_prefix or ""

  # Delete previous files if requested
  if args.delete_previous:
    # Use the blob prefix for deletion if provided
    delete_prefix = blob_prefix if blob_prefix else filename_suffix
    if delete_prefix:
      delete_previous_files(args.container_name, delete_prefix, container_client)
    else:
      print("Warning: Cannot delete previous files without a prefix or filename suffix")

  # Current timestamp for filenames
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  start_time = datetime.now()

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
    for i in range(args.iteration_number):
      future = executor.submit(
        process_single_file,
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
      "max_connections": args.max_connections,
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
      "blob_prefix": blob_prefix,
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
