"""
File Processing Utilities Module

Helper functions for processing data files including:
- Iterating through Spark History Server event logs
- Converting raw PDU metrics from InfluxDB JSON format to CSV
"""

import json
import gzip


def datafiles_iterator():
    """
    Iterate through Spark History Server event log files using directory structure.
    
    Expects a 'history-server-tree.json' file describing the directory structure:
    base_dir/app_name/vm_config/num_machines/event_log_file
    
    Yields:
        tuple: (datafile_full_path, config_dict)
            datafile_full_path (str): Full path to event log file
            config_dict (dict): Contains 'app', 'vm_config', 'num_machines', 'app_id'
    """
    with open("history-server-tree.json", "r") as f:
        directory_tree = json.load(f)

    directory_tree = directory_tree[0]
    base_dir = directory_tree["name"]
    
    # Navigate the nested directory structure
    for app_tree in directory_tree["contents"]:
        app_dir = app_tree["name"]
        for vmconfig_tree in app_tree["contents"]:
            vmconfig_dir = vmconfig_tree["name"]
            for num_machines_tree in vmconfig_tree["contents"]:
                num_machines_dir = num_machines_tree["name"]
                for datafile_tree in num_machines_tree["contents"]:
                    datafile = datafile_tree["name"]
                    datafile_full_path = f"{base_dir}/{app_dir}/{vmconfig_dir}/{num_machines_dir}/{datafile}"
                    config = {
                        "app": app_dir,
                        "vm_config": vmconfig_dir,
                        "num_machines": num_machines_dir,
                        "app_id": datafile,
                    }
                    yield datafile_full_path, config


def generate_processed_power_metrics_file(power_metrics_file_path):
    """
    Convert raw PDU metrics from InfluxDB JSON format to processed CSV format.
    
    Input format: GZIP-compressed JSON from InfluxDB query with structure:
        {"results": [{"series": [{"values": [...]}]}]}
    
    Output format: GZIP-compressed CSV with columns:
        time,active-power-outlet,value
    
    The function filters for "power" measurements with "active-power-outlet" tags
    and extracts the outlet number from the tag.
    
    Args:
        power_metrics_file_path (str): Path to input GZIP JSON file
        
    Output:
        Creates 'all_power_processed.csv.gz' in current directory
    """
    with gzip.open(power_metrics_file_path, 'r') as pmfile, \
            gzip.open("all_power_processed.csv.gz", 'w') as pmfile_processed:
        
        # Write CSV header
        header = b"time,active-power-outlet,value\n"
        pmfile_processed.write(header)
        
        # Parse InfluxDB JSON structure
        values = json.load(pmfile)["results"][0]["series"][0]["values"]
        
        # Process each measurement
        for v in values:
            # Filter for power measurements with active-power-outlet tag
            if v[2] == "power" and "active-power-outlet" in v[3]:
                # Extract outlet number from tag (format: "xxx-xxx-xxx-N")
                outlet = int(v[3].split("-")[3])
                power = int(v[4])
                
                # Write CSV row: timestamp, outlet, power_value
                encoded = "%s,%d,%d\n" % (v[0], outlet, power)
                encoded = str.encode(encoded)
                pmfile_processed.write(encoded)

