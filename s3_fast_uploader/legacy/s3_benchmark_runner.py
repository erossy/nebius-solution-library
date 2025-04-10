#!/usr/bin/env python3
import argparse
import subprocess
import json
import os
import time
import csv
from datetime import datetime
import itertools
import re


def parse_args():
  parser = argparse.ArgumentParser(description='Find optimal S3 benchmark parameters')

  # Required parameters from the original script
  parser.add_argument("--access-key-aws-id", help="Access Key AWS ID", required=True)
  parser.add_argument("--secret-access-key", help="Secret Access Key", required=True)
  parser.add_argument("--region-name", help="Region Name", default="eu-north1")
  parser.add_argument("--endpoint-url", help="Endpoint URL", required=True)
  parser.add_argument("--bucket-name", help="Bucket Name", required=True)
  parser.add_argument("--filename-suffix", help="Filename suffix", required=False)
  parser.add_argument("--object-size-mb", help="Object Size in MB", type=int, required=True)
  parser.add_argument("--prefix", help="Only download files with this prefix", type=str, default=None)

  # Parameter ranges for optimization
  parser.add_argument("--file-parallelism-range", help="Range for file_parallelism (min,max,step)",
                      default="16,128,16")
  parser.add_argument("--concurrency-range", help="Range for concurrency (min,max,step)",
                      default="4,16,2")
  parser.add_argument("--multipart-size-mb-range", help="Range for multipart_size_mb (min,max,step)",
                      default="16,64,16")
  parser.add_argument("--max-pool-connections-range", help="Range for max_pool_connections (min,max,step)",
                      default="128,1024,128")

  # Test configuration
  parser.add_argument("--iterations", help="Number of iterations for each parameter combination",
                      type=int, default=2)
  parser.add_argument("--mode", help="Testing Mode", choices=["upload", "download"],
                      default="download")
  parser.add_argument("--cleanup-memory", help="Enable memory cleanup", action="store_true")
  parser.add_argument("--output-dir", help="Directory to store results", default="benchmark_results")
  parser.add_argument("--quick-terraform_to_spawn_env", help="Run a quick terraform_to_spawn_env with fewer combinations", action="store_true")
  parser.add_argument("--iteration-number", help="Number of files to process", type=int, default=150)
  parser.add_argument("--s3-benchmark-path", help="Path to the S3 benchmark script",
                      default="./s3_downloader_v2.py")

  args = parser.parse_args()
  return args


def parse_range(range_str):
  """Parse a range string like '16,128,16' into a list of values"""
  min_val, max_val, step = map(int, range_str.split(','))
  return list(range(min_val, max_val + 1, step))


def run_benchmark(args, params, run_id):
  """Run a single benchmark with the given parameters"""
  cmd = [
    "python3", args.s3_benchmark_path,
    "--access-key-aws-id", args.access_key_aws_id,
    "--secret-access-key", args.secret_access_key,
    "--region-name", args.region_name,
    "--endpoint-url", args.endpoint_url,
    "--bucket-name", args.bucket_name,
    "--mode", args.mode,
    "--iteration-number", str(args.iteration_number),
    "--object-size-mb", str(args.object_size_mb),
    "--file-parallelism", str(params["file_parallelism"]),
    "--concurrency", str(params["concurrency"]),
    "--multipart-size-mb", str(params["multipart_size_mb"]),
    "--max-pool-connections", str(params["max_pool_connections"]),
  ]

  if args.filename_suffix:
    cmd.extend(["--filename-suffix", args.filename_suffix])

  if args.prefix:
    cmd.extend(["--prefix", args.prefix])

  if args.cleanup_memory:
    cmd.append("--cleanup-memory")

  print(f"\n[{run_id}] Running benchmark with parameters:")
  print(f"  file_parallelism: {params['file_parallelism']}")
  print(f"  concurrency: {params['concurrency']}")
  print(f"  multipart_size_mb: {params['multipart_size_mb']}")
  print(f"  max_pool_connections: {params['max_pool_connections']}")

  try:
    # Run the benchmark and capture output
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True)

    # Parse the JSON summary from the output
    json_summary = None
    json_pattern = re.compile(r'JSON Summary:\s+(\{.*\})', re.DOTALL)
    match = json_pattern.search(output)

    if match:
      json_str = match.group(1)
      try:
        json_summary = json.loads(json_str)
      except json.JSONDecodeError:
        print(f"Failed to parse JSON summary")

    # Parse the total throughput from the output
    throughput = None
    throughput_pattern = re.compile(r'Total Combined Throughput: (\d+\.\d+) MiB/sec')
    match = throughput_pattern.search(output)

    if match:
      throughput = float(match.group(1))

    return {
      "success": True,
      "throughput": throughput,
      "json_summary": json_summary,
      "output": output
    }

  except subprocess.CalledProcessError as e:
    print(f"Benchmark failed with exit code {e.returncode}")
    print(e.output)
    return {
      "success": False,
      "error": str(e),
      "output": e.output if hasattr(e, 'output') else None
    }
  except Exception as e:
    print(f"Error running benchmark: {e}")
    return {
      "success": False,
      "error": str(e),
      "output": None
    }


def select_parameter_combinations(args, quick_test=False):
  """Generate parameter combinations to terraform_to_spawn_env"""
  file_parallelism_values = parse_range(args.file_parallelism_range)
  concurrency_values = parse_range(args.concurrency_range)
  multipart_size_mb_values = parse_range(args.multipart_size_mb_range)
  max_pool_connections_values = parse_range(args.max_pool_connections_range)

  # For quick terraform_to_spawn_env, take a subset of values
  if quick_test:
    file_parallelism_values = file_parallelism_values[::2] or [file_parallelism_values[0]]
    concurrency_values = concurrency_values[::2] or [concurrency_values[0]]
    multipart_size_mb_values = multipart_size_mb_values[::2] or [multipart_size_mb_values[0]]
    max_pool_connections_values = [max(file_parallelism * concurrency * 2, 128)
                                   for file_parallelism in file_parallelism_values
                                   for concurrency in concurrency_values]
    max_pool_connections_values = sorted(list(set(max_pool_connections_values)))

  # Generate all combinations
  param_combinations = []
  for fp in file_parallelism_values:
    for cc in concurrency_values:
      for mp in multipart_size_mb_values:
        # Only terraform_to_spawn_env reasonable max_pool_connections values
        min_pool = max(fp * cc * 2, 128)  # At least 2x the concurrent operations
        suitable_pools = [p for p in max_pool_connections_values if p >= min_pool]

        if not suitable_pools:
          # If no suitable value exists, use the minimum calculated value
          pool_values = [min_pool]
        elif quick_test:
          # For quick terraform_to_spawn_env, just use the smallest suitable value
          pool_values = [suitable_pools[0]]
        else:
          # Use all suitable values
          pool_values = suitable_pools

        for pc in pool_values:
          param_combinations.append({
            "file_parallelism": fp,
            "concurrency": cc,
            "multipart_size_mb": mp,
            "max_pool_connections": pc
          })

  return param_combinations


def main():
  args = parse_args()

  # Create output directory
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  output_dir = f"{args.output_dir}_{timestamp}"
  os.makedirs(output_dir, exist_ok=True)

  # Generate parameter combinations
  param_combinations = select_parameter_combinations(args, args.quick_test)
  total_combinations = len(param_combinations)

  print(f"Testing {total_combinations} parameter combinations with {args.iterations} iterations each")
  print(f"Results will be saved to {output_dir}/")

  # Prepare results CSV
  csv_file = f"{output_dir}/benchmark_results.csv"
  with open(csv_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
      "run_id",
      "file_parallelism",
      "concurrency",
      "multipart_size_mb",
      "max_pool_connections",
      "iteration",
      "success",
      "throughput",
      "p50_throughput",
      "p95_throughput",
      "mean_throughput",
      "total_duration",
      "timestamp"
    ])

  # Run benchmarks
  results = []
  run_count = 0
  start_time = time.time()

  for combination_idx, params in enumerate(param_combinations):
    combination_results = []

    for iteration in range(args.iterations):
      run_count += 1
      run_id = f"run_{run_count:03d}"
      current_time = time.time()
      elapsed = current_time - start_time

      # Estimate remaining time
      avg_time_per_run = elapsed / run_count if run_count > 0 else 0
      total_runs = total_combinations * args.iterations
      remaining_runs = total_runs - run_count
      remaining_time = avg_time_per_run * remaining_runs

      print(f"\nCombination {combination_idx + 1}/{total_combinations}, "
            f"Iteration {iteration + 1}/{args.iterations}")
      print(f"Progress: {run_count}/{total_runs} runs "
            f"({run_count / total_runs * 100:.1f}%)")

      if avg_time_per_run > 0:
        print(f"Elapsed: {elapsed / 60:.1f} minutes, "
              f"Remaining: {remaining_time / 60:.1f} minutes")

      # Run the benchmark
      result = run_benchmark(args, params, run_id)
      combination_results.append(result)

      # Save individual run details
      with open(f"{output_dir}/{run_id}_details.json", 'w') as f:
        json.dump({
          "params": params,
          "result": result
        }, f, indent=2)

      # Update CSV with results
      with open(csv_file, 'a', newline='') as f:
        writer = csv.writer(f)

        if result["success"] and result["throughput"] and result["json_summary"]:
          json_summary = result["json_summary"]
          writer.writerow([
            run_id,
            params["file_parallelism"],
            params["concurrency"],
            params["multipart_size_mb"],
            params["max_pool_connections"],
            iteration,
            result["success"],
            result["throughput"],
            json_summary["throughput_percentile"]["p50"] if "throughput_percentile" in json_summary else None,
            json_summary["throughput_percentile"]["p95"] if "throughput_percentile" in json_summary else None,
            json_summary["throughput_aggregates"]["mean"] if "throughput_aggregates" in json_summary else None,
            json_summary["total_duration_seconds"] if "total_duration_seconds" in json_summary else None,
            json_summary["timestamp"] if "timestamp" in json_summary else None
          ])
        else:
          writer.writerow([
            run_id,
            params["file_parallelism"],
            params["concurrency"],
            params["multipart_size_mb"],
            params["max_pool_connections"],
            iteration,
            result["success"],
            None, None, None, None, None, None
          ])

      # Save full output
      with open(f"{output_dir}/{run_id}_output.txt", 'w') as f:
        f.write(result["output"] if result["output"] else "No output captured")

    # Track combination results
    results.append({
      "params": params,
      "results": combination_results
    })

  # Analyze and find the best combination
  successful_combinations = []

  for res in results:
    params = res["params"]
    successful_runs = [r for r in res["results"] if r["success"] and r["throughput"]]

    if successful_runs:
      avg_throughput = sum(r["throughput"] for r in successful_runs) / len(successful_runs)
      successful_combinations.append({
        "params": params,
        "avg_throughput": avg_throughput,
        "success_rate": len(successful_runs) / len(res["results"]),
        "runs": len(successful_runs)
      })

  # Sort by average throughput
  successful_combinations.sort(key=lambda x: x["avg_throughput"], reverse=True)

  # Save summary results
  with open(f"{output_dir}/summary_results.json", 'w') as f:
    json.dump({
      "top_combinations": successful_combinations[:10],
      "all_combinations": successful_combinations,
      "test_parameters": vars(args)
    }, f, indent=2)

  # Generate readable summary
  with open(f"{output_dir}/summary_report.txt", 'w') as f:
    f.write("S3 BENCHMARK PARAMETER OPTIMIZATION RESULTS\n")
    f.write("=========================================\n\n")
    f.write(f"Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Total combinations tested: {total_combinations}\n")
    f.write(f"Iterations per combination: {args.iterations}\n\n")

    f.write("TOP 10 PARAMETER COMBINATIONS\n")
    f.write("-----------------------------\n\n")

    for i, combo in enumerate(successful_combinations[:10]):
      f.write(f"Rank {i + 1}: {combo['avg_throughput']:.2f} MiB/sec\n")
      f.write(f"  file_parallelism: {combo['params']['file_parallelism']}\n")
      f.write(f"  concurrency: {combo['params']['concurrency']}\n")
      f.write(f"  multipart_size_mb: {combo['params']['multipart_size_mb']}\n")
      f.write(f"  max_pool_connections: {combo['params']['max_pool_connections']}\n")
      f.write(f"  Success rate: {combo['success_rate'] * 100:.1f}%\n\n")

  # Print results to console
  if successful_combinations:
    best = successful_combinations[0]
    print("\n\n========================================")
    print("BENCHMARK PARAMETER OPTIMIZATION COMPLETE")
    print("========================================")
    print(f"\nBest parameter combination found:")
    print(f"  file_parallelism: {best['params']['file_parallelism']}")
    print(f"  concurrency: {best['params']['concurrency']}")
    print(f"  multipart_size_mb: {best['params']['multipart_size_mb']}")
    print(f"  max_pool_connections: {best['params']['max_pool_connections']}")
    print(f"\nAverage throughput: {best['avg_throughput']:.2f} MiB/sec")
    print(f"Success rate: {best['success_rate'] * 100:.1f}%")
    print(f"\nComplete results saved to: {output_dir}/")
    print(f"Top 10 combinations: {output_dir}/summary_report.txt")
  else:
    print("\nNo successful benchmark runs were found.")

  # Generate one-liner command with the best parameters
  if successful_combinations:
    best = successful_combinations[0]
    cmd = f"python3 {args.s3_benchmark_path} "
    cmd += f"--access-key-aws-id {args.access_key_aws_id} "
    cmd += f"--secret-access-key {args.secret_access_key} "
    cmd += f"--region-name {args.region_name} "
    cmd += f"--endpoint-url {args.endpoint_url} "
    cmd += f"--bucket-name {args.bucket_name} "
    cmd += f"--mode {args.mode} "
    cmd += f"--iteration-number {args.iteration_number} "
    cmd += f"--object-size-mb {args.object_size_mb} "
    cmd += f"--file-parallelism {best['params']['file_parallelism']} "
    cmd += f"--concurrency {best['params']['concurrency']} "
    cmd += f"--multipart-size-mb {best['params']['multipart_size_mb']} "
    cmd += f"--max-pool-connections {best['params']['max_pool_connections']} "

    if args.filename_suffix:
      cmd += f"--filename-suffix {args.filename_suffix} "

    if args.prefix:
      cmd += f"--prefix {args.prefix} "

    if args.cleanup_memory:
      cmd += "--cleanup-memory "

    with open(f"{output_dir}/best_command.txt", 'w') as f:
      f.write(cmd.strip())

    print(f"\nOne-liner command with best parameters: {output_dir}/best_command.txt")


if __name__ == "__main__":
  main()
