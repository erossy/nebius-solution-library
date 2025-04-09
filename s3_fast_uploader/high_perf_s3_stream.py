def _optimized_parallel_assembly(
        self,
        keys: List[str],
        file_sizes: Dict[str, int],
        chunk_data_dict: Dict[str, bytes],
        chunk_size_mb: int,
        assembly_method: str = "multi_process",
        num_processes: int = 8,
        num_threads: int = 16
    ) -> Dict[str, bytes]:
        """
        Optimized parallel chunk assembly using the specified method.
        
        Args:
            keys: List of all object keys
            file_sizes: Dictionary mapping keys to file sizes
            chunk_data_dict: Dictionary containing downloaded chunks
            chunk_size_mb: Size of each chunk in MB
            assembly_method: Method for reassembly ("multi_process", "threaded", or "stream")
            num_processes: Number of processes for multi-process assembly
            num_threads: Number of threads for threaded assembly
            
        Returns:
            Dictionary with reassembled files
        """
        if assembly_method == "multi_process":
            logger.info(f"Using multi-process assembly with {num_processes} processes")
            return self._multi_process_assembly(
                keys=keys,
                file_sizes=file_sizes,
                chunk_data_dict=chunk_data_dict,
                chunk_size_mb=chunk_size_mb,
                num_processes=num_processes
            )
        elif assembly_method == "threaded":
            logger.info(f"Using threaded assembly with {num_threads} threads")
            return self._threaded_assembly(
                keys=keys,
                file_sizes=file_sizes,
                chunk_data_dict=chunk_data_dict,
                chunk_size_mb=chunk_size_mb,
                num_threads=num_threads
            )
        elif assembly_method == "stream":
            logger.info(f"Using streaming assembly with {num_threads} threads")
            return self._streaming_assembly(
                keys=keys,
                file_sizes=file_sizes,
                chunk_data_dict=chunk_data_dict,
                chunk_size_mb=chunk_size_mb,
                num_threads=num_threads
            )
        else:
            logger.warning(f"Unknown assembly method '{assembly_method}', using threaded assembly")
            return self._threaded_assembly(
                keys=keys,
                file_sizes=file_sizes,
                chunk_data_dict=chunk_data_dict,
                chunk_size_mb=chunk_size_mb,
                num_threads=num_threads
            )
    
    def _multi_process_assembly(
        self,
        keys: List[str],
        file_sizes: Dict[str, int],
        chunk_data_dict: Dict[str, bytes],
        chunk_size_mb: int,
        num_processes: int = 8
    ) -> Dict[str, bytes]:
        """
        Optimized multi-process assembly that uses shared memory for large files.
        
        Args:
            keys: List of all object keys
            file_sizes: Dictionary mapping keys to file sizes
            chunk_data_dict: Dictionary containing downloaded chunks
            chunk_size_mb: Size of each chunk in MB
            num_processes: Number of processes to use
            
        Returns:
            Dictionary with reassembled files
        """
        assembly_start_time = time.time()
        
        # Group files by size for better load balancing
        files_by_size = {}
        for key in keys:
            if key in file_sizes:
                size = file_sizes[key]
                size_group = size // (1024 * 1024 * 100)  # Group by 100MB increments
                if size_group not in files_by_size:
                    files_by_size[size_group] = []
                files_by_size[size_group].append(key)
        
        # Distribute files evenly across processes with large files first
        files_per_process = [[] for _ in range(num_processes)]
        process_load = [0] * num_processes
        
        # Sort size groups from largest to smallest
        for size_group in sorted(files_by_size.keys(), reverse=True):
            # Sort files within group by size (largest first)
            files = sorted(
                files_by_size[size_group],
                key=lambda k: file_sizes.get(k, 0),
                reverse=True
            )
            
            for key in files:
                # Assign to process with least load
                target_process = process_load.index(min(process_load))
                files_per_process[target_process].append(key)
                process_load[target_process] += file_sizes.get(key, 0)
        
        # Function to assemble files in a process
        def assemble_process_files(
            process_keys: List[str],
            file_sizes: Dict[str, int],
            chunk_data_dict: Dict[str, bytes],
            chunk_size_mb: int,
            result_dict: Dict,
            process_id: int
        ):
            """Assembly worker function that runs in each process"""
            process_start_time = time.time()
            process_name = f"Assembly-{process_id}"
            logger.info(f"[{process_name}] Starting assembly of {len(process_keys)} files")
            
            # Track statistics
            assembled_bytes = 0
            successful_files = 0
            failed_files = 0
            
            # Process each file
            for key in process_keys:
                try:
                    # Skip if file size is unknown
                    if key not in file_sizes:
                        logger.warning(f"[{process_name}] File {key} has no size information, skipping")
                        failed_files += 1
                        continue
                    
                    # Get total expected chunks
                    file_size = file_sizes[key]
                    chunk_size_bytes = chunk_size_mb * 1024 * 1024
                    expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes
                    
                    # Check if all chunks are available
                    missing_chunks = []
                    for i in range(expected_chunks):
                        chunk_key = f"{key}_{i}"
                        if chunk_key not in chunk_data_dict:
                            missing_chunks.append(i)
                    
                    if missing_chunks:
                        logger.warning(
                            f"[{process_name}] File {key} is missing {len(missing_chunks)}/{expected_chunks} chunks"
                        )
                        failed_files += 1
                        continue
                    
                    # Assemble file using pre-allocated buffer for efficiency
                    file_start_time = time.time()
                    
                    # Use bytearray for in-place assembly
                    result_buffer = bytearray(file_size)
                    position = 0
                    
                    # Copy chunk data into the correct position
                    for i in range(expected_chunks):
                        chunk_key = f"{key}_{i}"
                        chunk_data = chunk_data_dict[chunk_key]
                        chunk_len = len(chunk_data)
                        
                        # Copy data to the correct position
                        end_pos = min(position + chunk_len, file_size)
                        result_buffer[position:end_pos] = chunk_data[:end_pos-position]
                        position = end_pos
                        
                        # Allow chunks to be garbage-collected as we go
                        del chunk_data_dict[chunk_key]
                    
                    # Convert to bytes
                    combined_data = bytes(result_buffer)
                    
                    # Store in result dictionary
                    result_dict[key] = combined_data
                    
                    # Update statistics
                    file_duration = time.time() - file_start_time
                    file_mb = file_size / (1024 * 1024)
                    
                    assembled_bytes += file_size
                    successful_files += 1
                    
                    if file_size > 100 * 1024 * 1024:  # Only log for files > 100MB
                        logger.info(
                            f"[{process_name}] Assembled {key} ({file_mb:.2f} MB) "
                            f"in {file_duration:.2f}s ({file_mb/file_duration:.2f} MB/s)"
                        )
                    
                    # Log progress periodically
                    if successful_files % 5 == 0:
                        process_elapsed = time.time() - process_start_time
                        process_mb = assembled_bytes / (1024 * 1024)
                        logger.info(
                            f"[{process_name}] Progress: {successful_files}/{len(process_keys)} files, "
                            f"{process_mb:.2f} MB in {process_elapsed:.2f}s "
                            f"({process_mb/process_elapsed:.2f} MB/s)"
                        )
                        
                except Exception as e:
                    logger.error(f"[{process_name}] Error assembling {key}: {str(e)}")
                    traceback.print_exc()
                    failed_files += 1
            
            # Final process statistics
            process_duration = time.time() - process_start_time
            process_mb = assembled_bytes / (1024 * 1024)
            mb_per_sec = process_mb / process_duration if process_duration > 0 else 0
            
            logger.info(
                f"[{process_name}] Completed {successful_files}/{len(process_keys)} files "
                f"({failed_files} failed) | {process_mb:.2f} MB in {process_duration:.2f}s "
                f"({mb_per_sec:.2f} MB/s)"
            )
        
        # Create manager for shared dictionary
        manager = Manager()
        result_dict = manager.dict()
        
        # Start assembly processes
        assembly_processes = []
        for i in range(num_processes):
            if files_per_process[i]:
                p = Process(
                    target=assemble_process_files,
                    args=(
                        files_per_process[i],
                        file_sizes,
                        chunk_data_dict,
                        chunk_size_mb,
                        result_dict,
                        i
                    )
                )
                p.daemon = True
                p.start()
                assembly_processes.append(p)
        
        # Wait for assembly processes to complete
        for p in assembly_processes:
            p.join()
            
        # Wait for assembly to complete
        assembly_duration = time.time() - assembly_start_time
        
        # Convert manager.dict to regular dict
        result = dict(result_dict)
        
        assembled_bytes = sum(len(data) for data in result.values())
        successful_files = len(result)
        failed_files = len(keys) - successful_files
        
        logger.info(
            f"Multi-process assembly completed in {assembly_duration:.2f}s | "
            f"Files: {successful_files}/{len(keys)} ({failed_files} failed) | "
            f"Total: {assembled_bytes / (1024**3):.2f} GB | "
            f"Speed: {(assembled_bytes / (1024 * 1024)) / assembly_duration:.2f} MB/s"
        )
        
        return result
    
    def _threaded_assembly(
        self,
        keys: List[str],
        file_sizes: Dict[str, int],
        chunk_data_dict: Dict[str, bytes],
        chunk_size_mb: int,
        num_threads: int = 16
    ) -> Dict[str, bytes]:
        """
        Optimized threaded assembly using a ThreadPoolExecutor.
        
        Args:
            keys: List of all object keys
            file_sizes: Dictionary mapping keys to file sizes
            chunk_data_dict: Dictionary containing downloaded chunks
            chunk_size_mb: Size of each chunk in MB
            num_threads: Number of threads to use
            
        Returns:
            Dictionary with reassembled files
        """
        assembly_start_time = time.time()
        logger.info(f"Starting threaded assembly with {num_threads} threads")
        
        # Result dictionary
        result_dict = {}
        
        # Track statistics
        successful_files = 0
        failed_files = 0
        assembled_bytes = 0
        
        # Thread-safe lock for updating shared state
        stats_lock = threading.Lock()
        
        def assemble_file(key):
            """Thread worker function to assemble a single file"""
            nonlocal successful_files, failed_files, assembled_bytes
            
            try:
                # Skip if file size is unknown
                if key not in file_sizes:
                    with stats_lock:
                        failed_files += 1
                    return False, key, 0
                
                # Get total expected chunks
                file_size = file_sizes[key]
                chunk_size_bytes = chunk_size_mb * 1024 * 1024
                expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes
                
                # Pre-check if all chunks are available
                all_chunks_available = True
                for i in range(expected_chunks):
                    chunk_key = f"{key}_{i}"
                    if chunk_key not in chunk_data_dict:
                        all_chunks_available = False
                        logger.warning(f"Missing chunk {i} for file {key}")
                        break
                
                if not all_chunks_available:
                    with stats_lock:
                        failed_files += 1
                    return False, key, 0
                
                # Use bytearray for in-place assembly
                file_buffer = bytearray(file_size)
                position = 0
                
                # Copy each chunk into the buffer
                for i in range(expected_chunks):
                    chunk_key = f"{key}_{i}"
                    chunk_data = chunk_data_dict[chunk_key]
                    chunk_len = len(chunk_data)
                    
                    # Copy data to the correct position
                    end_pos = min(position + chunk_len, file_size)
                    file_buffer[position:end_pos] = chunk_data[:end_pos-position]
                    position = end_pos
                
                # Convert to bytes
                combined_data = bytes(file_buffer)
                
                # Update shared state
                with stats_lock:
                    result_dict[key] = combined_data
                    successful_files += 1
                    assembled_bytes += len(combined_data)
                
                return True, key, len(combined_data)
                
            except Exception as e:
                logger.error(f"Error assembling file {key}: {str(e)}")
                with stats_lock:
                    failed_files += 1
                return False, key, 0
        
        # Process files in a thread pool
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit all files
            future_to_key = {executor.submit(assemble_file, key): key for key in keys if key in file_sizes}
            
            # Process results as they complete
            for i, future in enumerate(concurrent.futures.as_completed(future_to_key)):
                key = future_to_key[future]
                try:
                    success, _, size = future.result()
                    
                    # Log progress periodically
                    if i % 10 == 0 or i == len(future_to_key) - 1:
                        elapsed = time.time() - assembly_start_time
                        logger.info(
                            f"Assembly progress: {i+1}/{len(keys)} files "
                            f"({(i+1)/len(keys)*100:.1f}%), "
                            f"{assembled_bytes/(1024**3):.2f} GB in {elapsed:.2f}s"
                        )
                        
                except Exception as e:
                    logger.error(f"Error processing assembly result for {key}: {str(e)}")
        
        # Calculate assembly performance
        assembly_duration = time.time() - assembly_start_time
        mb_assembled = assembled_bytes / (1024 * 1024)
        mb_per_sec = mb_assembled / assembly_duration if assembly_duration > 0 else 0
        
        logger.info(
            f"Threaded assembly completed in {assembly_duration:.2f}s | "
            f"Files: {successful_files} successful, {failed_files} failed | "
            f"Speed: {mb_per_sec:.2f} MB/s"
        )
        
        return result_dict
    
    def _streaming_assembly(
        self,
        keys: List[str],
        file_sizes: Dict[str, int],
        chunk_data_dict: Dict[str, bytes],
        chunk_size_mb: int,
        num_threads: int = 16
    ) -> Dict[str, bytes]:
        """
        Memory-efficient streaming assembly that processes chunks on-the-fly.
        
        Args:
            keys: List of all object keys
            file_sizes: Dictionary mapping keys to file sizes
            chunk_data_dict: Dictionary containing downloaded chunks
            chunk_size_mb: Size of each chunk in MB
            num_threads: Number of threads to use
            
        Returns:
            Dictionary with reassembled files
        """
        assembly_start_time = time.time()
        logger.info(f"Starting streaming assembly with {num_threads} threads")
        
        # Result dictionary
        result_dict = {}
        
        # Track statistics
        successful_files = 0
        failed_files = 0
        assembled_bytes = 0
        
        # Thread-safe lock for updating shared state
        stats_lock = threading.Lock()
        
        def stream_assemble_file(key):
            """Thread worker function to stream-assemble a single file"""
            nonlocal successful_files, failed_files, assembled_bytes
            
            try:
                # Skip if file size is unknown
                if key not in file_sizes:
                    with stats_lock:
                        failed_files += 1
                    return False, key, 0
                
                # Get total expected chunks
                file_size = file_sizes[key]
                chunk_size_bytes = chunk_size_mb * 1024 * 1024
                expected_chunks = (file_size + chunk_size_bytes - 1) // chunk_size_bytes
                
                # Pre-check if all chunks are available
                all_chunks_available = True
                for i in range(expected_chunks):
                    chunk_key = f"{key}_{i}"
                    if chunk_key not in chunk_data_dict:
                        all_chunks_available = False
                        logger.warning(f"Missing chunk {i} for file {key}")
                        break
                
                if not all_chunks_available:
                    with stats_lock:
                        failed_files += 1
                    return False, key, 0
                
                # Stream chunks directly to output buffer
                output = io.BytesIO()
                
                # Write chunks in order
                for i in range(expected_chunks):
                    chunk_key = f"{key}_{i}"
                    chunk_data = chunk_data_dict[chunk_key]
                    output.write(chunk_data)
                    
                    # Allow chunk to be garbage collected after use
                    del chunk_data_dict[chunk_key]
                
                # Get final result
                output.seek(0)
                combined_data = output.getvalue()
                
                # Verify size
                if len(combined_data) != file_size:
                    logger.warning(
                        f"Size mismatch for reassembled file {key}: "
                        f"expected {file_size}, got {len(combined_data)}"
                    )
                
                # Update shared state
                with stats_lock:
                    result_dict[key] = combined_data
                    successful_files += 1
                    assembled_bytes += len(combined_data)
                
                return True, key, len(combined_data)
                
            except Exception as e:
                logger.error(f"Error streaming file {key}: {str(e)}")
                with stats_lock:
                    failed_files += 1
                return False, key, 0
        
        # Process files in a thread pool
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit all files
            future_to_key = {executor.submit(stream_assemble_file, key): key for key in keys if key in file_sizes}
            
            # Process results as they complete
            for i, future in enumerate(concurrent.futures.as_completed(future_to_key)):
                key = future_to_key[future]
                try:
                    success, _, size = future.result()
                    
                    # Log progress periodically
                    if i % 10 == 0 or i == len(future_to_key) - 1:
                        elapsed = time.time() - assembly_start_time
                        mb_per_sec = (assembled_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"Streaming progress: {i+1}/{len(keys)} files "
                            f"({(i+1)/len(keys)*100:.1f}%), "
                            f"{assembled_bytes/(1024**3):.2f} GB in {elapsed:.2f}s "
                            f"({mb_per_sec:.2f} MB/s)"
                        )
                        
                except Exception as e:
                    logger.error(f"Error processing assembly result for {key}: {str(e)}")
        
        # Calculate assembly performance
        assembly_duration = time.time() - assembly_start_time
        mb_assembled = assembled_bytes / (1024 * 1024)
        mb_per_sec = mb_assembled / assembly_duration if assembly_duration > 0 else 0
        
        logger.info(
            f"Streaming assembly completed in {assembly_duration:.2f}s | "
            f"Files: {successful_files} successful, {failed_files} failed | "
            f"Speed: {mb_per_sec:.2f} MB/s"
        )
        
        return result_dict
    
    def _report_progress(
        self,
        start_time: float,
        metrics_dict: Dict,
        all_chunks: List[Dict],
        chunk_data_dict: Dict
    ):
        """
        Report download progress.
        
        Args:
            start_time: Time when download started
            metrics_dict: Dictionary with metrics from all processes
            all_chunks: List of all chunks being downloaded
            chunk_data_dict: Dictionary containing downloaded chunks
        """
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Calculate aggregate metrics
        completed_chunks = sum(m.get('completed_chunks', 0) for m in metrics_dict.values())
        failed_chunks = sum(m.get('failed_chunks', 0) for m in metrics_dict.values())
        total_chunks = len(all_chunks)
        
        # Calculate downloaded bytes based on actual chunks in dictionary
        total_bytes = sum(len(data) for data in chunk_data_dict.values())
        
        # Calculate performance metrics
        mb_downloaded = total_bytes / (1024 * 1024)
        mb_per_sec = mb_downloaded / elapsed if elapsed > 0 else 0
        gbits_per_sec = (mb_per_sec * 8) / 1000
        
        # Percent complete
        percent_complete = completed_chunks / total_chunks * 100 if total_chunks > 0 else 0
        
        # Log progress
        progress_message = (
            f"Progress: {completed_chunks}/{total_chunks} chunks "
            f"({percent_complete:.1f}%) | "
            f"Size: {mb_downloaded:.2f} MB | "
            f"Speed: {mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)"
        )
        
        if failed_chunks > 0:
            progress_message += f" | Failed chunks: {failed_chunks}"
            
        logger.info(progress_message)
        
        # If verbose, also show per-process metrics
        if self.verbose and metrics_dict:
            process_metrics = []
            for pid, metrics in metrics_dict.items():
                if 'mb_per_second' in metrics and metrics.get('completed_chunks', 0) > 0:
                    process_metrics.append((
                        pid,
                        metrics.get('completed_chunks', 0),
                        metrics.get('total_chunks', 0),
                        metrics.get('mb_per_second', 0),
                        metrics.get('gbits_per_second', 0)
                    ))
            
            # Sort by process ID
            process_metrics.sort()
            
            for pid, completed, total, mb_per_sec, gbits_per_sec in process_metrics:
                logger.debug(
                    f"  Process {pid}: {completed}/{total} chunks, "
                    f"{mb_per_sec:.2f} MB/s ({gbits_per_sec:.2f} Gbit/s)"
                )


def main():
    """Command-line entry point"""
    parser = argparse.ArgumentParser(description='High-Throughput S3 Downloader (70+ Gbps)')
    
    # Required arguments
    parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
    parser.add_argument('--access-key', required=True, help='AWS access key ID')
    parser.add_argument('--secret-key', required=True, help='AWS secret access key')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    
    # Optional arguments
    parser.add_argument('--region', default=None, help='AWS region name')
    parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
    parser.add_argument('--processes', type=int, default=None, help='Number of processes to use (default: all CPU cores)')
    parser.add_argument('--concurrency', type=int, default=50, help='Concurrent downloads per process')
    parser.add_argument('--chunk-size-mb', type=int, default=128, help='Size of each chunk in MB')
    parser.add_argument('--report-interval', type=int, default=5, help='Progress reporting interval in seconds')
    parser.add_argument('--max-files', type=int, default=None, help='Maximum number of files to download')
    parser.add_argument('--save-to-file', help='Save downloaded data to a pickle file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('--assembly-method', choices=['multi_process', 'threaded', 'stream'], 
                        default='multi_process', help='Method for chunk assembly')
    parser.add_argument('--assembly-processes', type=int, default=None, 
                        help='Number of processes for multi-process assembly')
    parser.add_argument('--assembly-threads', type=int, default=16,
                        help='Number of threads for threaded assembly')
    
    args = parser.parse_args()
    
    # Create downloader
    downloader = HighThroughputS3Downloader(
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
        processes=args.processes,
        concurrency_per_process=args.concurrency,
        chunk_size_mb=args.chunk_size_mb,
        verbose=args.verbose
    )
    
    # Set up boto3 session for listing objects
    session = boto3.Session(
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region
    )
    s3_client = session.client('s3', endpoint_url=args.endpoint_url)
    
    logger.info(f"Listing objects in bucket '{args.bucket}' with prefix '{args.prefix}'...")
    
    # List objects to download
    keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
        if 'Contents' in page:
            keys.extend([obj['Key'] for obj in page['Contents']])
    
    if args.max_files and len(keys) > args.max_files:
        logger.info(f"Limiting to {args.max_files} files (from {len(keys)} total)")
        keys = keys[:args.max_files]
    else:
        logger.info(f"Found {len(keys)} files to download")
    
    if not keys:
        logger.error("No files found to download.")
        return
    
    # Start downloading
    result = downloader.download_files(
        bucket=args.bucket,
        keys=keys,
        report_interval_sec=args.report_interval,
        assembly_method=args.assembly_method,
        assembly_processes=args.assembly_processes,
        assembly_threads=args.assembly_threads
    )
    
    # Save data to file if requested
    if args.save_to_file:
        logger.info(f"Saving downloaded data to {args.save_to_file}")
        with open(args.save_to_file, 'wb') as f:
            pickle.dump(result['data'], f)
        logger.info(f"Saved {len(result['data'])} files to {args.save_to_file}")
    
    # Save performance metrics to JSON
    metrics_file = 'download_metrics.json'
    metrics = {k: v for k, v in result.items() if k != 'data'}
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Saved performance metrics to {metrics_file}")
    
    logger.info("Download complete!")


if __name__ == "__main__":
    main()
