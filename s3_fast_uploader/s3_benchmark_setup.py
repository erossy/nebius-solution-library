"""
S3 Download Performance Testing Guide (Revised)
==============================================

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
import shutil

# Import the high-performance downloader
# from high_performance_s3_downloader import HighPerformanceS3Downloader

def prepare_test_files(
    s3_client,
    bucket_name,
    prefix,
    file_sizes_mb=[10, 100, 512],
    num_files_per_size=10
):
    """
    Generate and upload test files of specific sizes to S3 bucket.
    Files are uploaded using multipart upload to handle large files efficiently.

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
            # Create a key with size in the name
            key = f"{prefix}/test_{size_mb}mb_{i}.bin"
            print(f"Uploading {key} ({size_mb} MB)...")

            # Create file in chunks to avoid memory issues
            chunk_size_mb = 10  # Use smaller chunks (10MB)
            total_chunks = size_mb // chunk_size_mb
            if size_mb % chunk_size_mb != 0:
                total_chunks += 1

            # Upload in chunks using multipart upload
            mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=key)
            parts = []

            try:
                for chunk_idx in range(total_chunks):
                    # Last chunk might be smaller
                    current_chunk_size = min(chunk_size_mb, size_mb - (chunk_idx * chunk_size_mb))

                    # Generate random data for this chunk
                    chunk_bytes = os.urandom(current_chunk_size * 1024 * 1024)

                    # Upload part
                    part_number = chunk_idx + 1
                    response = s3_client.upload_part(
                        Bucket=bucket_name,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=mpu['UploadId'],
                        Body=chunk_bytes
                    )

                    # Save ETag for completion
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': response['ETag']
                    })

                    print(f"  Uploaded part {part_number}/{total_chunks} for {key}")

                # Complete multipart upload
                s3_client.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=mpu['UploadId'],
                    MultipartUpload={'Parts': parts}
                )

                file_keys[size_mb].append(key)
                print(f"  Completed upload of {key}")

            except Exception as e:
                # Abort upload if something goes wrong
                s3_client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=mpu['UploadId']
                )
                print(f"  Error uploading {key}: {str(e)}")
                raise

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
    completed_bytes = 0
    last_update_time = time.time()
    update_interval = 0.5  # Update rate info every 0.5 seconds

    # Get file sizes first
    file_sizes = {}
    for key in keys:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        size = response['ContentLength']
        file_sizes[key] = size
        total_bytes += size

    with tqdm(total=len(keys), desc="Downloading files") as pbar:
        for i, key in enumerate(keys):
            filename = os.path.basename(key)
            local_path = os.path.join(destination_dir, filename)

            # Download file
            s3_client.download_file(bucket, key, local_path)

            # Update progress
            completed_bytes += file_sizes[key]

            # Update rate information
            current_time = time.time()
            elapsed = current_time - last_update_time

            if elapsed >= update_interval or i == len(keys) - 1:
                # Calculate transfer rate
                total_elapsed = current_time - start_time
                if total_elapsed > 0:
                    mb_per_sec = (completed_bytes / (1024 * 1024)) / total_elapsed
                    gbit_per_sec = (mb_per_sec * 8) / 1000
                    pbar.set_postfix({"MB/s": f"{mb_per_sec:.2f}", "Gbit/s": f"{gbit_per_sec:.2f}"})
                    last_update_time = current_time

            pbar.update(1)

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
    file_sizes = {}
    for key in keys:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        size = response['ContentLength']
        file_sizes[key] = size
        total_bytes += size

    def download_file(key):
        filename = os.path.basename(key)
        local_path = os.path.join(destination_dir, filename)
        s3_client.download_file(bucket, key, local_path)
        return key

    start_time = time.time()

    # For progress tracking with transfer rate
    completed_bytes = 0
    completed_files = 0
    last_update_time = time.time()
    update_interval = 0.5  # Update rate display every 0.5 seconds

    # Download files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {executor.submit(download_file, key): key for key in keys}

        # Show progress with transfer rate
        with tqdm(total=len(keys), desc=f"Downloading files (threads={max_workers})") as pbar:
            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                completed_files += 1
                completed_bytes += file_sizes[key]

                # Update rate information
                current_time = time.time()
                elapsed = current_time - last_update_time

                if elapsed >= update_interval or completed_files == len(keys):
                    # Calculate transfer rate
                    total_elapsed = current_time - start_time
                    if total_elapsed > 0:
                        mb_per_sec = (completed_bytes / (1024 * 1024)) / total_elapsed
                        gbit_per_sec = (mb_per_sec * 8) / 1000
                        pbar.set_postfix({"MB/s": f"{mb_per_sec:.2f}", "Gbit/s": f"{gbit_per_sec:.2f}"})
                        last_update_time = current_time

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
    Download files using aiobotocore for async I/O.

    Returns:
        Dictionary with performance statistics
    """
    os.makedirs(destination_dir, exist_ok=True)

    # Create session using the correct API
    # Try different methods to create the session based on the aiobotocore version
    try:
        # First try the Session class directly
        from aiobotocore.session import AioSession
        session = AioSession()
    except (ImportError, AttributeError):
        try:
            # Then try the create_client function directly
            import aiobotocore
            # We'll create clients directly without session in the rest of the code
            session = None
        except:
            # Last resort, use boto3 session and warn
            print("Warning: Could not initialize aiobotocore properly. Using boto3 instead.")
            import boto3
            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            return standard_boto3_download(
                endpoint_url, aws_access_key_id, aws_secret_access_key, region_name,
                bucket, keys, destination_dir
            )

    # First get total size
    total_bytes = 0
    file_sizes = {}

    # Create client based on what's available
    if session is None:
        # Direct client creation
        s3_client = await aiobotocore.create_client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        client_context = None  # No context manager
    else:
        # Session-based client creation
        s3_client = await session.create_client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        ).__aenter__()
        client_context = s3_client  # For later cleanup

    try:
        # Get file sizes
        for key in keys:
            try:
                response = await s3_client.head_object(Bucket=bucket, Key=key)
                size = response['ContentLength']
                file_sizes[key] = size
                total_bytes += size
            except Exception as e:
                print(f"Error getting size for {key}: {e}")
                # Use a placeholder size
                file_sizes[key] = 1024 * 1024  # 1MB placeholder
                total_bytes += file_sizes[key]

        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(max_concurrency)

        async def download_file(key):
            async with semaphore:
                filename = os.path.basename(key)
                local_path = os.path.join(destination_dir, filename)

                # Create a new client for each download to avoid issues
                if session is None:
                    # Direct client creation
                    async with aiobotocore.create_client(
                        's3',
                        endpoint_url=endpoint_url,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=region_name
                    ) as download_client:
                        # Stream the response
                        response = await download_client.get_object(Bucket=bucket, Key=key)
                        with open(local_path, 'wb') as f:
                            # Read in chunks to handle large files
                            chunk = await response['Body'].read(8192)  # 8KB chunks
                            while chunk:
                                f.write(chunk)
                                chunk = await response['Body'].read(8192)
                else:
                    # Session-based client creation
                    async with session.create_client(
                        's3',
                        endpoint_url=endpoint_url,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=region_name
                    ) as download_client:
                        # Stream the response
                        response = await download_client.get_object(Bucket=bucket, Key=key)
                        with open(local_path, 'wb') as f:
                            # Read in chunks to handle large files
                            chunk = await response['Body'].read(8192)  # 8KB chunks
                            while chunk:
                                f.write(chunk)
                                chunk = await response['Body'].read(8192)

                return key

        # Record start time
        start_time = time.time()

        # Create download tasks
        tasks = [asyncio.create_task(download_file(key)) for key in keys]

        # Track progress with transfer rate display
        progress = tqdm(total=len(keys), desc=f"Downloading files (async={max_concurrency})")
        completed_bytes = 0
        completed_count = 0
        last_update_time = time.time()
        update_interval = 0.5  # Update rate info every 0.5 seconds

        # Process tasks as they complete
        pending = set(tasks)
        while pending:
            # Wait for some task to complete
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED, timeout=0.1
            )

            # Process completed tasks
            for task in done:
                try:
                    key = task.result()
                    completed_count += 1
                    completed_bytes += file_sizes.get(key, 0)

                    # Update rate information periodically
                    current_time = time.time()
                    elapsed = current_time - last_update_time

                    if elapsed >= update_interval or completed_count == len(keys):
                        # Calculate transfer rate
                        total_elapsed = current_time - start_time
                        if total_elapsed > 0:
                            mb_per_sec = (completed_bytes / (1024 * 1024)) / total_elapsed
                            gbit_per_sec = (mb_per_sec * 8) / 1000
                            progress.set_postfix({"MB/s": f"{mb_per_sec:.2f}", "Gbit/s": f"{gbit_per_sec:.2f}"})
                            last_update_time = current_time

                    progress.update(1)
                except Exception as e:
                    print(f"\nError during async download: {str(e)}")

        progress.close()

        # Calculate final statistics
        end_time = time.time()
        duration = end_time - start_time
        mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
        gbits_per_sec = (mb_per_sec * 8) / 1000

        return {
            'method': f'aioboto3(concurrency={max_concurrency})',
            'files_downloaded': completed_count,
            'total_bytes': total_bytes,
            'duration_seconds': duration,
            'mb_per_second': mb_per_sec,
            'gbits_per_second': gbits_per_sec
        }

    finally:
        # Clean up the client if needed
        if client_context:
            await client_context.__aexit__(None, None, None)

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
    file_sizes = {}
    for key in keys:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        size = response['ContentLength']
        file_sizes[key] = size
        total_bytes += size

    # Distribute files among processes
    keys_per_process = [[] for _ in range(processes)]
    for i, key in enumerate(keys):
        keys_per_process[i % processes].append(key)

    # Define a worker function that each process will run
    def worker_process(process_keys, result_queue):
        # Create a new session in this process
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        s3_client = session.client('s3', endpoint_url=endpoint_url)

        for key in process_keys:
            try:
                filename = os.path.basename(key)
                local_path = os.path.join(destination_dir, filename)
                s3_client.download_file(bucket, key, local_path)
                result_queue.put(('success', key))
            except Exception as e:
                result_queue.put(('error', key, str(e)))

    # Create a queue for results
    manager = multiprocessing.Manager()
    result_queue = manager.Queue()

    # Start the worker processes
    start_time = time.time()
    workers = []
    for i in range(processes):
        if keys_per_process[i]:  # Only start processes that have work to do
            p = multiprocessing.Process(
                target=worker_process,
                args=(keys_per_process[i], result_queue)
            )
            p.daemon = True  # Make sure processes exit if main process crashes
            p.start()
            workers.append(p)

    # Monitor progress
    completed_files = 0
    completed_bytes = 0
    last_update_time = time.time()
    update_interval = 0.5

    with tqdm(total=len(keys), desc=f"Downloading files (processes={processes})") as pbar:
        # Continue until all files are processed or all processes are dead
        while completed_files < len(keys) and any(p.is_alive() for p in workers):
            # Check for results
            while not result_queue.empty():
                try:
                    result = result_queue.get_nowait()
                    if result[0] == 'success':
                        key = result[1]
                        completed_files += 1
                        completed_bytes += file_sizes.get(key, 0)
                    else:  # Error case
                        key, error = result[1], result[2]
                        print(f"\nError downloading {key}: {error}")
                except Exception:
                    # Just continue if there's an issue with the queue
                    pass

            # Update progress display
            current_time = time.time()
            elapsed = current_time - last_update_time

            if elapsed >= update_interval:
                # Calculate transfer rate
                total_elapsed = current_time - start_time
                if total_elapsed > 0 and completed_bytes > 0:
                    mb_per_sec = (completed_bytes / (1024 * 1024)) / total_elapsed
                    gbit_per_sec = (mb_per_sec * 8) / 1000
                    pbar.set_postfix({"MB/s": f"{mb_per_sec:.2f}", "Gbit/s": f"{gbit_per_sec:.2f}"})
                    last_update_time = current_time

            # Update the progress bar
            pbar.n = completed_files
            pbar.refresh()

            # Short sleep to prevent CPU spinning
            time.sleep(0.1)

    # Check for any remaining results
    while not result_queue.empty():
        try:
            result = result_queue.get_nowait()
            if result[0] == 'success':
                completed_files += 1
        except:
            pass

    # Ensure all processes are terminated
    for p in workers:
        if p.is_alive():
            p.terminate()

    end_time = time.time()
    duration = end_time - start_time
    mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
    gbits_per_sec = (mb_per_sec * 8) / 1000

    return {
        'method': f'boto3_multiprocess(processes={processes})',
        'files_downloaded': completed_files,
        'total_bytes': total_bytes,
        'duration_seconds': duration,
        'mb_per_second': mb_per_sec,
        'gbits_per_second': gbits_per_sec
    }

    return {
        'method': f'boto3_multiprocess(processes={processes})',
        'files_downloaded': len(keys),
        'total_bytes': total_bytes,
        'duration_seconds': duration,
        'mb_per_second': mb_per_sec,
        'gbits_per_second': gbits_per_sec
    }

# Mock implementation for testing without the actual high-performance downloader
def high_performance_download_mock(
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
    Mock implementation of high-performance downloader for testing purposes.
    Combines multiprocessing with async download within each process.

    Returns:
        Dictionary with performance statistics
    """
    print(f"Using mock high-performance downloader with {processes} processes and {concurrency_per_process} concurrent downloads per process")

    os.makedirs(destination_dir, exist_ok=True)

    # Create session and client for size calculation
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    s3_client = session.client('s3', endpoint_url=endpoint_url)

    # Get total size
    total_bytes = 0
    for key in keys:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        total_bytes += response['ContentLength']

    # Distribute keys across processes
    keys_per_process = [[] for _ in range(processes)]
    for i, key in enumerate(keys):
        keys_per_process[i % processes].append(key)

    def process_worker(process_keys, queue):
        """Worker function to download files within a process"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def download_files_async():
            # Create semaphore to limit concurrency
            semaphore = asyncio.Semaphore(concurrency_per_process)

            async def download_file(key):
                async with semaphore:
                    filename = os.path.basename(key)
                    local_path = os.path.join(destination_dir, filename)

                    # Use aiobotocore directly instead of aioboto3
                    session = aiobotocore.get_session()
                    async with session.create_client(
                        's3',
                        endpoint_url=endpoint_url,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=region_name
                    ) as s3_client:
                        # Stream the response
                        response = await s3_client.get_object(Bucket=bucket, Key=key)
                        with open(local_path, 'wb') as f:
                            # Read in chunks to handle large files
                            chunk = await response['Body'].read(8192)  # 8KB chunks
                            while chunk:
                                f.write(chunk)
                                chunk = await response['Body'].read(8192)

                        # Report success
                        queue.put({'status': 'success', 'key': key})

            # Create download tasks
            tasks = [download_file(key) for key in process_keys]

            # Wait for all downloads to complete
            await asyncio.gather(*tasks)

        loop.run_until_complete(download_files_async())

    # Create a queue for process communication
    manager = multiprocessing.Manager()
    result_queue = manager.Queue()

    # Record start time
    start_time = time.time()

    # Start download processes
    processes_list = []
    for i in range(processes):
        if keys_per_process[i]:
            p = multiprocessing.Process(
                target=process_worker,
                args=(keys_per_process[i], result_queue)
            )
            p.start()
            processes_list.append(p)

    # Track progress
    completed = 0
    completed_bytes = 0
    file_sizes = {}  # Cache for file sizes
    last_update_time = time.time()
    update_interval = 0.5  # Update rate info every 0.5 seconds

    # Get file sizes first
    for key in keys:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        file_sizes[key] = response['ContentLength']

    with tqdm(total=len(keys), desc=f"High-perf download (p={processes}, c={concurrency_per_process})") as progress:
        while completed < len(keys) and any(p.is_alive() for p in processes_list):
            try:
                # Check for completed downloads
                results_processed = 0
                bytes_in_this_batch = 0

                while not result_queue.empty():
                    result = result_queue.get_nowait()
                    if result['status'] == 'success':
                        key = result['key']
                        completed += 1
                        bytes_in_this_batch += file_sizes[key]
                        completed_bytes += file_sizes[key]
                        results_processed += 1

                # Update progress bar
                if results_processed > 0:
                    current_time = time.time()
                    elapsed = current_time - last_update_time

                    if elapsed >= update_interval:
                        # Calculate current transfer rate
                        mb_per_sec = (completed_bytes / (1024 * 1024)) / (current_time - start_time)
                        gbit_per_sec = (mb_per_sec * 8) / 1000

                        # Update progress bar with transfer rate
                        progress.set_postfix({"MB/s": f"{mb_per_sec:.2f}", "Gbit/s": f"{gbit_per_sec:.2f}"})
                        last_update_time = current_time

                    # Update completed count
                    progress.update(results_processed)
            except Exception as e:
                print(f"Error processing results: {str(e)}")

            time.sleep(0.1)

    # Wait for processes to finish
    for p in processes_list:
        p.join()

    # Final check for any remaining results
    while not result_queue.empty():
        try:
            result = result_queue.get_nowait()
            if result['status'] == 'success':
                completed += 1
        except:
            pass

    # Calculate statistics
    end_time = time.time()
    duration = end_time - start_time
    mb_per_sec = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0
    gbits_per_sec = (mb_per_sec * 8) / 1000

    return {
        'method': f'high_performance(p={processes},c={concurrency_per_process})',
        'files_downloaded': completed,
        'total_bytes': total_bytes,
        'duration_seconds': duration,
        'mb_per_second': mb_per_sec,
        'gbits_per_second': gbits_per_sec
    }

    return {
        'method': f'high_performance(p={processes},c={concurrency_per_process})',
        'files_downloaded': completed,
        'total_bytes': total_bytes,
        'duration_seconds': duration,
        'mb_per_second': mb_per_sec,
        'gbits_per_second': gbits_per_sec
    }

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
                try:
                    aioboto3_stats = asyncio.run(aioboto3_download_single(
                        endpoint_url, aws_access_key_id, aws_secret_access_key, region_name,
                        bucket, size_keys, os.path.join(size_dir, f"aioboto3_{concurrency}"),
                        max_concurrency=concurrency
                    ))
                    aioboto3_stats['file_size_mb'] = size_mb
                    results.append(aioboto3_stats)
                except Exception as e:
                    print(f"Error running aioboto3 test: {str(e)}")

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

        # High-performance downloader tests (using mock implementation for this example)
        for processes in processes_list:
            for concurrency in concurrency_list:
                print(f"\nTesting high-performance downloader (processes={processes}, concurrency={concurrency})...")
                hp_stats = high_performance_download_mock(
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
    parser.add_argument('--mock-only', action='store_true', help='Use mock implementation only (no actual high-performance downloader)')

    args = parser.parse_args()

    # Create session
    session = boto3.Session(
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region
    )
    s3_client = session.client('s3', endpoint_url=args.endpoint_url)

    # Clean up if needed
    try:
        if os.path.exists(args.output_dir):
            shutil.rmtree(args.output_dir)
        os.makedirs(args.output_dir, exist_ok=True)
    except Exception as e:
        print(f"Warning: Could not clean output directory: {str(e)}")

    start_time = time.time()

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

    total_duration = time.time() - start_time
    print(f"\nTotal benchmark duration: {total_duration:.2f} seconds")

if __name__ == "__main__":
    main()
