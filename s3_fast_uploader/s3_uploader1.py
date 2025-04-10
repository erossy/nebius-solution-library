def auto_optimize_parameters(args):
  """Automatically optimize parameters based on CPU count and download requirements"""
  # Skip optimization if auto_optimize is False
  if not args.auto_optimize:
    print("Auto-optimization disabled. Using provided parameters.")
    return args

  import math
  import psutil

  # Get system information
  cpu_count = os.cpu_count()
  print(f"Detected {cpu_count} CPU cores")

  # Get files to download from bucket (to determine file count)
  s3_client = create_boto3_client(
    access_key=args.access_key_aws_id,
    secret_key=args.secret_access_key,
    region=args.region_name,
    endpoint_url=args.endpoint_url,
    max_pool_connections=args.max_pool_connections
  )

  # Determine the number of files to download
  num_files = args.iteration_number
  total_volume_mb = 0

  if args.prefix:
    # Count actual files with prefix
    file_keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=args.bucket_name, Prefix=args.prefix)

    for page in pages:
      if 'Contents' in page:
        for obj in page['Contents']:
          file_keys.append(obj['Key'])
          # Get actual file size
          try:
            head_res = s3_client.head_object(Bucket=args.bucket_name, Key=obj['Key'])
            file_size_mb = int(head_res['ContentLength']) / (1024 * 1024)
            total_volume_mb += file_size_mb
          except Exception as e:
            print(f"Warning: Failed to get size for {obj['Key']}: {e}")
            # Use provided object size as fallback
            total_volume_mb += args.object_size_mb

    num_files = len(file_keys)
    if num_files == 0:
      print("No files found with prefix. Using default iteration number.")
      num_files = args.iteration_number
      total_volume_mb = args.object_size_mb * num_files
  else:
    # For benchmark mode, use the specified parameters
    total_volume_mb = args.object_size_mb * args.iteration_number

  print(f"Files to download: {num_files}, Total volume: {total_volume_mb:.2f} MB")

  # Apply the new auto-optimize logic
  # 1. file-parallelism = smallest of amount of files to download vs 0.75*cpu_cores
  optimal_file_parallelism = min(num_files, int(cpu_count * 0.75))
  optimal_file_parallelism = max(1, optimal_file_parallelism)  # Ensure at least 1

  # 2. concurrency = smallest between 16 and roundup of amount of files to download/file-parallelism
  if optimal_file_parallelism > 0:  # Protect against division by zero
    optimal_concurrency = min(16, math.ceil(num_files / optimal_file_parallelism))
  else:
    optimal_concurrency = min(16, num_files)
  optimal_concurrency = max(1, optimal_concurrency)  # Ensure at least 1

  # 3. multipart-size-mb = total volume to download / (file-parallelism * concurrency)
  # Ensure we don't get too small parts
  if total_volume_mb > 0 and optimal_file_parallelism > 0 and optimal_concurrency > 0:
    optimal_multipart_size = total_volume_mb / (optimal_file_parallelism * optimal_concurrency)
    # Ensure minimum part size is at least 5MB (S3 minimum) and round to nearest MB
    optimal_multipart_size = max(5, round(optimal_multipart_size))
  else:
    # Fallback if calculation fails
    optimal_multipart_size = args.multipart_size_mb

  # Update the parameters if they're different from current values
  if args.file_parallelism != optimal_file_parallelism:
    print(f"Auto-optimizing: Changing file_parallelism from {args.file_parallelism} to {optimal_file_parallelism}")
    args.file_parallelism = optimal_file_parallelism

  if args.concurrency != optimal_concurrency:
    print(f"Auto-optimizing: Changing concurrency from {args.concurrency} to {optimal_concurrency}")
    args.concurrency = optimal_concurrency

  if args.multipart_size_mb != optimal_multipart_size:
    print(f"Auto-optimizing: Changing multipart_size_mb from {args.multipart_size_mb} to {optimal_multipart_size}")
    args.multipart_size_mb = optimal_multipart_size

  print("Auto-optimization completed for file_parallelism, concurrency, and multipart_size_mb only.")
  return args
