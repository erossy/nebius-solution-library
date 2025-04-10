#!/bin/bash

# Set variables
OUTPUT_FILE="nethogs_total_bandwidth.csv"
INTERVAL=2  # Sample every 2 seconds

# Create CSV header
echo "timestamp,sent_KB,received_KB" > $OUTPUT_FILE

echo "Starting Nethogs total bandwidth monitoring..."

# Start the monitoring process
(
while true; do
  timestamp=$(date +"%Y-%m-%d %H:%M:%S")

  # Run nethogs in batch mode (-t) and grab only the TOTAL line
  total_line=$(sudo nethogs -t -c 1 | grep "TOTAL" | tail -1)

  if [ ! -z "$total_line" ]; then
    # Extract sent and received values
    sent=$(echo "$total_line" | awk '{print $2}')
    received=$(echo "$total_line" | awk '{print $3}')

    # Save to file
    echo "$timestamp,$sent,$received" >> $OUTPUT_FILE

    # Print current values
    echo "$timestamp: Sent=$sent KB/s, Received=$received KB/s"
  fi

  sleep $INTERVAL
done
) &

MONITOR_PID=$!
echo "Monitoring started with PID $MONITOR_PID"

# Run the S3 downloader script with the specific parameters
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
import seaborn as sns

# Set the style
sns.set(style="darkgrid")

# Read the data
df = pd.read_csv('nethogs_total_bandwidth.csv')

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

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
plt.show()

print(f"Summary statistics:")
print(f"- Download: Max={download_max:.2f} KB/s, Avg={download_avg:.2f} KB/s")
print(f"- Upload: Max={upload_max:.2f} KB/s, Avg={upload_avg:.2f} KB/s")
EOL

# Generate visualization
echo "Generating bandwidth visualization..."
python3 plot_total_bandwidth.py

echo "Done! Visualization saved as total_bandwidth_usage.png"
