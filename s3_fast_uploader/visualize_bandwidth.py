import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime

# Read the data
df = pd.read_csv('nethogs_data.csv')

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Group by timestamp and process, sum the bandwidth
grouped = df.groupby(['timestamp', 'process']).agg({
    'sent_KB': 'sum',
    'received_KB': 'sum'
}).reset_index()

# Create a pivot table for easier plotting
pivot_sent = pd.pivot_table(grouped, values='sent_KB', index='timestamp', columns='process', fill_value=0)
pivot_received = pd.pivot_table(grouped, values='received_KB', index='timestamp', columns='process', fill_value=0)

# Calculate total bandwidth per timestamp
total_sent = pivot_sent.sum(axis=1).to_frame('Total Upload')
total_received = pivot_received.sum(axis=1).to_frame('Total Download')
total_bandwidth = pd.concat([total_sent, total_received], axis=1)

# Set up the plots - Create a 3-panel plot
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 15), sharex=True)

# Plot 1: Total bandwidth over time
total_bandwidth.plot(ax=ax1, linewidth=2)
ax1.set_title('Total Bandwidth Usage Over Time', fontsize=16)
ax1.set_ylabel('KB/s', fontsize=14)
ax1.grid(True)
ax1.legend(fontsize=12)

# Plot 2: Upload bandwidth by process
pivot_sent.plot(ax=ax2, kind='area', stacked=True, colormap='viridis')
ax2.set_title('Upload Bandwidth by Process', fontsize=16)
ax2.set_ylabel('KB/s', fontsize=14)
ax2.grid(True)

# Plot 3: Download bandwidth by process
pivot_received.plot(ax=ax3, kind='area', stacked=True, colormap='plasma')
ax3.set_title('Download Bandwidth by Process', fontsize=16)
ax3.set_ylabel('KB/s', fontsize=14)
ax3.grid(True)

# Format x-axis
plt.gcf().autofmt_xdate()
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
ax3.set_xlabel('Time', fontsize=14)

# Add summary statistics
download_max = total_received['Total Download'].max()
download_avg = total_received['Total Download'].mean()
upload_max = total_sent['Total Upload'].max()
upload_avg = total_sent['Total Upload'].mean()

stats_text = (f"Max Download: {download_max:.2f} KB/s\n"
             f"Avg Download: {download_avg:.2f} KB/s\n"
             f"Max Upload: {upload_max:.2f} KB/s\n"
             f"Avg Upload: {upload_avg:.2f} KB/s")

# Add text box with stats
props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
ax1.text(0.02, 0.95, stats_text, transform=ax1.transAxes,
        fontsize=12, verticalalignment='top', bbox=props)

# Adjust layout and save
plt.tight_layout()
plt.savefig('s3_bandwidth_usage.png', dpi=300)
plt.show()

print(f"Summary statistics:")
print(f"- Download: Max={download_max:.2f} KB/s, Avg={download_avg:.2f} KB/s")
print(f"- Upload: Max={upload_max:.2f} KB/s, Avg={upload_avg:.2f} KB/s")
