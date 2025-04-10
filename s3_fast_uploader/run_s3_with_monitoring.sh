#!/bin/bash

# Set variables
OUTPUT_FILE="nethogs_data.csv"
INTERVAL=2  # Sample every 2 seconds
PYTHON_PROCESS="python3"  # Process name to track (using python3)

# Create CSV header
echo "timestamp,process,sent_KB,received_KB" > $OUTPUT_FILE

echo "Starting Nethogs monitoring for $PYTHON_PROCESS processes..."

# Start the monitoring process
(
while true; do
  timestamp=$(date +"%Y-%m-%d %H:%M:%S")
  # Run nethogs in batch mode (-t) for one sample, filter for Python processes
  sudo nethogs -t -c 1 | grep "$PYTHON_PROCESS" | while read line; do
    process=$(echo $line | awk '{print $2}')
    sent=$(echo $line | awk '{print $3}')
    received=$(echo $line | awk '{print $4}')
    echo "$timestamp,$process,$sent,$received" >> $OUTPUT_FILE
  done
  sleep $INTERVAL
done
) &

MONITOR_PID=$!
echo "Monitoring started with PID $MONITOR_PID"

# Run the S3 downloader script with the specific parameters
echo "Starting S3 downloader script with provided parameters..."
python3 s3_downloader.py \
  --access-key-aws-id NAKI9QZTO6NB86Q6ZF37 \
  --secret-access-key VJYzUhdf9t+spsKpNCvujjspIUSnhjM/HoMKSigW \
  --endpoint-url https://storage.eu-north1.nebius.cloud:443 \
  --bucket-name renes-bucket \
  --iteration-number 172 \
  --prefix tempfile_ \
  --concurrency 2 \
  --multipart-size-mb 1024 \
  --file-parallelism 96 \
  --max-pool-connections 4096 \
  --object-size-mb 2048

# After the script completes, stop monitoring
kill $MONITOR_PID
echo "Monitoring stopped at $(date +'%Y-%m-%d %H:%M:%S')"

# Generate visualization
echo "Generating bandwidth visualization..."
python3 visualize_bandwidth.py

echo "Done! Visualization saved as s3_bandwidth_usage.png"
