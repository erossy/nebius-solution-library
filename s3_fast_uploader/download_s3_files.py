"""
Example scripts showing how to use the HighPerformanceS3Downloader in real-world scenarios
"""

#
# Example 1: Simple Command Line Downloader
# -----------------------------------------
# This example shows how to use the downloader directly from the command line
#

# Save this as download_s3_files.py
# Usage: python download_s3_files.py --bucket your-bucket --prefix data/ --destination ./downloads/ --endpoint-url https://s3.example.com

import os
import argparse
from high_performance_s3_downloader import HighPerformanceS3Downloader
import boto3


def main():
  parser = argparse.ArgumentParser(description='High Performance S3 Downloader')
  parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
  parser.add_argument('--access-key', required=True, help='AWS access key ID')
  parser.add_argument('--secret-key', required=True, help='AWS secret access key')
  parser.add_argument('--region', default=None, help='AWS region name')
  parser.add_argument('--bucket', required=True, help='S3 bucket name')
  parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
  parser.add_argument('--destination', required=True, help='Local directory for downloaded files')
  parser.add_argument('--processes', type=int, default=None, help='Number of processes to use')
  parser.add_argument('--concurrency', type=int, default=25, help='Concurrent downloads per process')
  parser.add_argument('--multipart-size-mb', type=int, default=5, help='Multipart chunk size in MB')

  args = parser.parse_args()

  # Create downloader
  downloader = HighPerformanceS3Downloader(
    endpoint_url=args.endpoint_url,
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region,
    processes=args.processes,
    concurrency_per_process=args.concurrency,
    multipart_size_mb=args.multipart_size_mb
  )

  # List objects in bucket
  session = boto3.Session(
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region
  )
  s3_client = session.client('s3', endpoint_url=args.endpoint_url)

  print(f"Listing objects in bucket '{args.bucket}' with prefix '{args.prefix}'...")

  keys = []
  paginator = s3_client.get_paginator('list_objects_v2')
  for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
    if 'Contents' in page:
      keys.extend([obj['Key'] for obj in page['Contents']])

  print(f"Found {len(keys)} files to download")

  if not keys:
    print("No files found to download.")
    return

  # Start downloading
  stats = downloader.download_files(
    bucket=args.bucket,
    keys=keys,
    destination_dir=args.destination
  )

  print("\nDownload complete!")
  print(f"Performance: {stats['mb_per_second']:.2f} MB/s ({stats['gbits_per_second']:.2f} Gbit/s)")


if __name__ == "__main__":
  main()

#
# Example 2: Integration with ML Training Pipeline
# -----------------------------------------------
# This example shows how to use the downloader for an ML training pipeline with PyTorch
#

# Save this as train_model.py
import os
import argparse
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from torchvision import transforms, models
import boto3

from high_performance_s3_downloader import HighPerformanceS3Downloader

# Import the S3ImageDataset from integration code
from dataloader_integration import S3ImageDataset


def parse_args():
  parser = argparse.ArgumentParser(description='Train a model with data from S3')
  parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
  parser.add_argument('--access-key', required=True, help='AWS access key ID')
  parser.add_argument('--secret-key', required=True, help='AWS secret access key')
  parser.add_argument('--region', default=None, help='AWS region name')
  parser.add_argument('--bucket', required=True, help='S3 bucket name')
  parser.add_argument('--prefix', default='images/', help='S3 key prefix to filter objects')
  parser.add_argument('--cache-dir', default='/tmp/model_data', help='Local directory to cache files')
  parser.add_argument('--batch-size', type=int, default=32, help='Training batch size')
  parser.add_argument('--epochs', type=int, default=10, help='Number of epochs')
  parser.add_argument('--lr', type=float, default=0.001, help='Learning rate')
  parser.add_argument('--processes', type=int, default=4, help='Number of download processes')
  parser.add_argument('--concurrency', type=int, default=25, help='Concurrent downloads per process')

  return parser.parse_args()


def main():
  args = parse_args()

  # S3 configuration
  s3_config = {
    'endpoint_url': args.endpoint_url,
    'aws_access_key_id': args.access_key,
    'aws_secret_access_key': args.secret_key,
    'region_name': args.region
  }

  # List objects in bucket
  session = boto3.Session(
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region
  )
  s3_client = session.client('s3', endpoint_url=args.endpoint_url)

  print(f"Listing objects in bucket '{args.bucket}' with prefix '{args.prefix}'...")

  # Get list of image files from S3
  keys = []
  paginator = s3_client.get_paginator('list_objects_v2')
  for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
    if 'Contents' in page:
      for obj in page['Contents']:
        key = obj['Key']
        if key.lower().endswith(('.jpg', '.jpeg', '.png')):
          keys.append(key)

  print(f"Found {len(keys)} image files")

  # Define transformations
  transform = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
  ])

  # Create dataset
  dataset = S3ImageDataset(
    bucket=args.bucket,
    keys=keys,
    cache_dir=args.cache_dir,
    transform=transform,
    s3_config=s3_config,
    processes=args.processes,
    concurrency_per_process=args.concurrency
  )

  # Create data loader
  dataloader = DataLoader(
    dataset,
    batch_size=args.batch_size,
    shuffle=True,
    num_workers=4,
    pin_memory=True
  )

  # Initialize model
  model = models.resnet18(pretrained=True)
  num_classes = 10  # Adjust based on your dataset
  model.fc = nn.Linear(model.fc.in_features, num_classes)

  # Move model to GPU if available
  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
  model = model.to(device)

  # Define loss function and optimizer
  criterion = nn.CrossEntropyLoss()
  optimizer = optim.Adam(model.parameters(), lr=args.lr)

  # Training loop
  for epoch in range(args.epochs):
    model.train()
    running_loss = 0.0

    for i, data in enumerate(dataloader):
      inputs, labels = data
      inputs, labels = inputs.to(device), labels.to(device)

      optimizer.zero_grad()
      outputs = model(inputs)
      loss = criterion(outputs, labels)
      loss.backward()
      optimizer.step()

      running_loss += loss.item()

      if i % 10 == 9:
        print(f'Epoch: {epoch + 1}, Batch: {i + 1}, Loss: {running_loss / 10:.4f}')
        running_loss = 0.0

    print(f'Completed epoch {epoch + 1}/{args.epochs}')

  print('Training complete!')

  # Save the model
  torch.save(model.state_dict(), 'trained_model.pth')


if __name__ == "__main__":
  main()

#
# Example 3: Benchmark S3 Performance
# ----------------------------------
# This script benchmarks S3 download performance with different settings
#

# Save this as benchmark_s3.py
import os
import time
import argparse
import numpy as np
from high_performance_s3_downloader import HighPerformanceS3Downloader
import boto3
import shutil


def parse_args():
  parser = argparse.ArgumentParser(description='Benchmark S3 Download Performance')
  parser.add_argument('--endpoint-url', required=True, help='S3 endpoint URL')
  parser.add_argument('--access-key', required=True, help='AWS access key ID')
  parser.add_argument('--secret-key', required=True, help='AWS secret access key')
  parser.add_argument('--region', default=None, help='AWS region name')
  parser.add_argument('--bucket', required=True, help='S3 bucket name')
  parser.add_argument('--prefix', default='', help='S3 key prefix to filter objects')
  parser.add_argument('--destination', default='/tmp/s3_benchmark', help='Local directory for downloaded files')
  parser.add_argument('--min-processes', type=int, default=1, help='Minimum number of processes to test')
  parser.add_argument('--max-processes', type=int, default=8, help='Maximum number of processes to test')
  parser.add_argument('--min-concurrency', type=int, default=5, help='Minimum concurrency per process to test')
  parser.add_argument('--max-concurrency', type=int, default=50, help='Maximum concurrency per process to test')
  parser.add_argument('--concurrency-step', type=int, default=5, help='Step size for concurrency testing')
  parser.add_argument('--runs', type=int, default=3, help='Number of runs per configuration')
  parser.add_argument('--max-files', type=int, default=100, help='Maximum number of files to use for benchmark')

  return parser.parse_args()


def main():
  args = parse_args()

  # List objects in bucket
  session = boto3.Session(
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    region_name=args.region
  )
  s3_client = session.client('s3', endpoint_url=args.endpoint_url)

  print(f"Listing objects in bucket '{args.bucket}' with prefix '{args.prefix}'...")

  keys = []
  paginator = s3_client.get_paginator('list_objects_v2')
  for page in paginator.paginate(Bucket=args.bucket, Prefix=args.prefix):
    if 'Contents' in page:
      keys.extend([obj['Key'] for obj in page['Contents']])

  # Limit the number of files for benchmarking
  if args.max_files and len(keys) > args.max_files:
    print(f"Limiting to {args.max_files} files (from {len(keys)} total)")
    keys = keys[:args.max_files]
  else:
    print(f"Using {len(keys)} files for benchmarking")

  if not keys:
    print("No files found for benchmarking.")
    return

  # Create results directory
  os.makedirs(args.destination, exist_ok=True)

  # Collect results
  results = []

  # Test different process and concurrency configurations
  for processes in range(args.min_processes, args.max_processes + 1):
    for concurrency in range(args.min_concurrency, args.max_concurrency + 1, args.concurrency_step):
      # Create downloader
      downloader = HighPerformanceS3Downloader(
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
        processes=processes,
        concurrency_per_process=concurrency,
        multipart_size_mb=5
      )

      # Create temporary directory for this configuration
      temp_dir = os.path.join(args.destination, f"p{processes}_c{concurrency}")
      if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
      os.makedirs(temp_dir, exist_ok=True)

      # Run multiple times for this configuration
      config_results = []
      for run in range(args.runs):
        print(f"Testing with {processes} processes, {concurrency} concurrency (Run {run + 1}/{args.runs})...")

        start_time = time.time()
        stats = downloader.download_files(
          bucket=args.bucket,
          keys=keys,
          destination_dir=temp_dir,
          report_interval_sec=10
        )
        duration = time.time() - start_time

        config_results.append({
          'processes': processes,
          'concurrency': concurrency,
          'run': run + 1,
          'duration_seconds': duration,
          'mb_per_second': stats['mb_per_second'],
          'gbits_per_second': stats['gbits_per_second'],
          'files_downloaded': stats['files_downloaded'],
          'bytes_downloaded': stats['bytes_downloaded']
        })

        # Clean up after each run
        shutil.rmtree(temp_dir)
        os.makedirs(temp_dir, exist_ok=True)

      # Calculate average performance for this configuration
      avg_performance = {
        'processes': processes,
        'concurrency': concurrency,
        'avg_duration': np.mean([r['duration_seconds'] for r in config_results]),
        'avg_mb_per_sec': np.mean([r['mb_per_second'] for r in config_results]),
        'avg_gbits_per_sec': np.mean([r['gbits_per_second'] for r in config_results]),
        'std_gbits_per_sec': np.std([r['gbits_per_second'] for r in config_results])
      }

      results.append(avg_performance)

      print(f"Average performance with {processes} processes, {concurrency} concurrency: "
            f"{avg_performance['avg_gbits_per_sec']:.2f} Gbit/s "
            f"(± {avg_performance['std_gbits_per_sec']:.2f})")

      # Clean up this configuration directory
      shutil.rmtree(temp_dir)

  # Sort results by performance
  results.sort(key=lambda x: x['avg_gbits_per_sec'], reverse=True)

  # Print top configurations
  print("\nTop Performing Configurations:")
  print("-----------------------------")
  for i, result in enumerate(results[:5]):
    print(f"{i + 1}. Processes: {result['processes']}, Concurrency: {result['concurrency']} - "
          f"{result['avg_gbits_per_sec']:.2f} Gbit/s (± {result['std_gbits_per_sec']:.2f})")

  # Save results to a CSV file
  import csv
  result_file = os.path.join(args.destination, "benchmark_results.csv")
  with open(result_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

  print(f"\nBenchmark complete! Results saved to {result_file}")

  # Clean up destination directory
  if os.path.exists(args.destination):
    shutil.rmtree(args.destination)


if __name__ == "__main__":
  main()
