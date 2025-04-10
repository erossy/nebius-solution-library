#!/bin/bash

# Set variables
OUTPUT_FILE="nethogs_total_bandwidth.csv"
INTERVAL=2  # Sample every 2 seconds
INTERFACE="enp134s0"  # Your network interface

# Create CSV header
echo "timestamp,sent_KB,received_KB" > $OUTPUT_FILE

echo "Starting Nethogs total bandwidth monitoring on interface $INTERFACE..."

# Define a function to extract numbers from nethogs output
extract_nethogs_total() {
    # Run nethogs and capture its output
    # We use 'script' command to capture the output since nethogs updates the screen in-place
    script -q -c "sudo nethogs $INTERFACE -d 1 -t -c 2" /dev/null | grep -i "TOTAL" | tail -1 | \
    awk '{print $(NF-1), $NF}' | sed 's/KB\/s//'
}

# Start the monitoring process
(
while true; do
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")

    # Get the TOTAL line from nethogs
    nethogs_values=$(extract_nethogs_total)

    if [ ! -z "$nethogs_values" ]; then
        # Extract sent and received values
        sent=$(echo "$nethogs_values" | awk '{print $1}')
        received=$(echo "$nethogs_values" | awk '{print $2}')

        # Save to file
        echo "$timestamp,$sent,$received" >> $OUTPUT_FILE

        # Print current values
        echo "$timestamp: Sent=$sent KB/s, Received=$received KB/s"
    else
        echo "Warning: Could not extract TOTAL from nethogs output"
    fi

    sleep $INTERVAL
done
) &

MONITOR_PID=$!
echo "Monitoring started with PID $MONITOR_PID"

# Run your S3 downloader script
echo "Starting S3 downloader script..."
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

# Create a simple visualization script
cat > plot_total_bandwidth.py << 'EOL'
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# Read the data
try:
    df = pd.read_csv('nethogs_total_bandwidth.csv')

    if df.empty:
        print("No data collected! CSV file is empty.")
        exit(1)

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Make sure values are numeric
    df['sent_KB'] = pd.to_numeric(df['sent_KB'], errors='coerce')
    df['received_KB'] = pd.to_numeric(df['received_KB'], errors='coerce')

    # Remove any invalid rows
    df = df.dropna()

    if df.empty:
        print("No valid data after cleaning!")
        exit(1)

    # Create the plot
    plt.figure(figsize=(15, 8))

    # Plot sent and received bandwidth
    plt.plot(df['timestamp'], df['sent_KB'], label='Upload (KB/s)', color='blue', linewidth=2)
    plt.plot(df['timestamp'], df['received_KB'], label='Download (KB/s)', color='green', linewidth=2)

    # Format the plot
    plt.title('Total Network Bandwidth Usage Over Time', fontsize=16)
    plt.xlabel('Time', fontsize=14)
    plt.ylabel('Bandwidth (KB/s)', fontsize=14)
    plt.grid(True)
    plt.legend(fontsize=12)

    # Format x-axis
    plt.gcf().autofmt_xdate()
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))

    # Add summary statistics
    download_max = df['received_KB'].max()
    download_avg = df['received_KB'].mean()
    upload_max = df['sent_KB'].max()
    upload_avg = df['sent_KB'].mean()

    stats_text = (f"Max Download: {download_max:.2f} KB/s\n"
                 f"Avg Download: {download_avg:.2f} KB/s\n"
                 f"Max Upload: {upload_max:.2f} KB/s\n"
                 f"Avg Upload: {upload_avg:.2f} KB/s")

    # Add text box with stats
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    plt.gca().text(0.02, 0.95, stats_text, transform=plt.gca().transAxes,
            fontsize=12, verticalalignment='top', bbox=props)

    plt.tight_layout()
    plt.savefig('total_bandwidth_usage.png', dpi=300)

    print(f"Summary statistics:")
    print(f"- Download: Max={download_max:.2f} KB/s, Avg={download_avg:.2f} KB/s")
    print(f"- Upload: Max={upload_max:.2f} KB/s, Avg={upload_avg:.2f} KB/s")

    plt.show()
except Exception as e:
    print(f"Error: {e}")
EOL

# Generate visualization
echo "Generating bandwidth visualization..."
python3 plot_total_bandwidth.py

echo "Done! Visualization saved as total_bandwidth_usage.png"
