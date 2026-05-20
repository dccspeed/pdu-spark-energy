"""
Command-line tool to process raw PDU power metrics from InfluxDB JSON format.

Usage:
    python generate_processed_power_metrics_file.py <input_file.csv.gz>

Input:
    GZIP-compressed JSON file exported from InfluxDB query

Output:
    Creates 'all_power_processed.csv.gz' with processed power metrics
    Format: time,active-power-outlet,value
"""

import sys
import pduutil as pu

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python generate_processed_power_metrics_file.py <power_metrics_file.csv.gz>")
        sys.exit(1)
    
    power_metrics_file_path_gz = sys.argv[1]
    pu.generate_processed_power_metrics_file(power_metrics_file_path_gz)
    print("Processing complete. Output saved to: all_power_processed.csv.gz")
