"""
Spark History Server Event Log Parser Module

Parses and analyzes Apache Spark History Server event log files to extract:
- Application execution timeline
- Stage-level performance metrics
- Task execution profiles
- Resource utilization (CPU, memory, disk I/O, network)
- Active task timelines per worker/host

Event logs are JSON-based files where each line represents a Spark event
(e.g., SparkListenerApplicationStart, SparkListenerStageCompleted, etc.)
"""

import json
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn import preprocessing

import pduoutletmap as om



def format_date(d):
    """
    Format datetime object to ISO 8601 string.
    
    Args:
        d (datetime): Datetime object
        
    Returns:
        str: ISO 8601 formatted string (YYYY-MM-DDTHH:MM:SSZ)
    """
    d = d.strftime('%Y-%m-%dT%H:%M:%SZ')
    return d


def to_date(unix_timestamp):
    """
    Convert Unix timestamp (milliseconds) to UTC datetime object.
    
    Args:
        unix_timestamp (int): Unix timestamp in milliseconds
        
    Returns:
        datetime: UTC datetime object
    """
    d = datetime.fromtimestamp(unix_timestamp / 1000, timezone.utc)
    return d


def get_lines_by_event(datafile):
    """
    Parse Spark event log file and group events by type.
    
    Each line in the event log is a JSON object with an "Event" field.
    
    Args:
        datafile (str): Path to Spark History Server event log file
        
    Returns:
        dict: Maps event type (str) to list of event dictionaries
    """
    lines_by_event = {}
    with open(datafile, "r") as f:
        for line in f:
            try:
                d = json.loads(line)
            except:
                print(f"ERROR_LINE({datafile}): {line}")
            events = lines_by_event.get(d["Event"], [])
            events.append(d)
            lines_by_event[d["Event"]] = events
    return lines_by_event


def active_tasks_timeline_per_worker(datafile):
    cores_per_worker = get_cores_per_worker(datafile)
    task_end_events = get_lines_by_event(datafile)["SparkListenerTaskEnd"]
    tasks_per_worker = {}
    for event in task_end_events:
        launch_time = to_date(event["Task Info"]["Launch Time"])
        finish_time = to_date(event["Task Info"]["Finish Time"])
        attempt = event["Task Info"]["Attempt"]
        if attempt > 0:
            print(f"MoreThanOneAttemptTask: {datafile}", file=sys.stderr)
        host = event["Task Info"]["Host"]
        worker = om.get_worker_from_host(host)
        worker_tasks = tasks_per_worker.get(worker, [])
        worker_tasks.append([launch_time, finish_time])
        tasks_per_worker[worker] = worker_tasks

    timeline_per_worker = {}
    for worker in tasks_per_worker:
        max_cores = cores_per_worker[worker]
        worker_tasks = tasks_per_worker[worker]
        timestamps = sorted(list(set([t for ts in worker_tasks for t in ts])))
        task_counts = [0] * len(timestamps)
        timestamp_to_idx = {}
        for i in range(len(timestamps)):
            t = timestamps[i]
            timestamp_to_idx[t] = i

        for task in worker_tasks:
            launch_time = task[0]
            finish_time = task[1]
            idx = timestamp_to_idx[launch_time]
            while idx + 1 < len(timestamps) and timestamps[idx + 1] <= finish_time:
                idx += 1
                task_counts[idx] += 1
                task_counts[idx] = min(task_counts[idx], max_cores)

        timeline_per_worker[worker] = (timestamps, task_counts)

    return timeline_per_worker


def active_tasks_timeline_per_host(datafile):
    cores_per_host = get_cores_per_host(datafile)
    task_end_events = get_lines_by_event(datafile)["SparkListenerTaskEnd"]
    tasks_per_host = {}
    for event in task_end_events:
        launch_time = to_date(event["Task Info"]["Launch Time"])
        finish_time = to_date(event["Task Info"]["Finish Time"])
        attempt = event["Task Info"]["Attempt"]
        if attempt > 0:
            print(f"MoreThanOneAttemptTask: {event}")
        host = event["Task Info"]["Host"]
        host_tasks = tasks_per_host.get(host, [])
        host_tasks.append([launch_time, finish_time])
        tasks_per_host[host] = host_tasks

    timeline_per_host = {}
    for host in tasks_per_host:
        max_cores = cores_per_host[host]
        host_tasks = tasks_per_host[host]
        timestamps = sorted(list(set([t for ts in host_tasks for t in ts])))
        task_counts = [0] * len(timestamps)
        timestamp_to_idx = {}
        for i in range(len(timestamps)):
            t = timestamps[i]
            timestamp_to_idx[t] = i

        for task in host_tasks:
            launch_time = task[0]
            finish_time = task[1]
            idx = timestamp_to_idx[launch_time]
            while idx + 1 < len(timestamps) and timestamps[idx + 1] <= finish_time:
                idx += 1
                task_counts[idx] += 1
                task_counts[idx] = min(task_counts[idx], max_cores)

        timeline_per_host[host] = (timestamps, task_counts)

    return timeline_per_host


def get_app_name(datafile):
    """
    Extract Spark application name from event log.
    
    Args:
        datafile (str): Path to Spark History Server event log file
        
    Returns:
        str: Application name
    """
    events = get_lines_by_event(datafile)
    app_name = events["SparkListenerApplicationStart"][0]["App Name"]
    return app_name


def get_time_interval(datafile):
    """
    Get application start and end timestamps.
    
    Args:
        datafile (str): Path to Spark History Server event log file
        
    Returns:
        tuple: (start_datetime, end_datetime)
    """
    events = get_lines_by_event(datafile)
    start_timestamp = to_date(events["SparkListenerApplicationStart"][0]["Timestamp"])
    end_timestamp = to_date(events["SparkListenerApplicationEnd"][0]["Timestamp"])
    return start_timestamp, end_timestamp


def get_hosts(datafile):
    """
    Get list of all executor hostnames used in the application.
    
    Args:
        datafile (str): Path to Spark History Server event log file
        
    Returns:
        list: List of hostnames (e.g., ['slave401', 'slave402'])
    """
    executor_added_events = get_lines_by_event(datafile)["SparkListenerExecutorAdded"]
    hosts = []
    for event in executor_added_events:
        hosts.append(event["Executor Info"]["Host"])
    return hosts


def get_cores_per_host(datafile):
    """
    Get total CPU cores allocated to each host.
    
    Sums cores across all executors on each host.
    
    Args:
        datafile (str): Path to Spark History Server event log file
        
    Returns:
        dict: Maps hostname to total core count
    """
    executor_added_events = get_lines_by_event(datafile)["SparkListenerExecutorAdded"]
    cores_per_host = {}
    for event in executor_added_events:
        host = event["Executor Info"]["Host"]
        total_cores = event["Executor Info"]["Total Cores"]
        cores_per_host[host] = cores_per_host.get(host, 0) + total_cores
    return cores_per_host


def get_cores_per_worker(datafile):
    """
    Get total CPU cores allocated to each physical worker.
    
    Maps hosts to workers and aggregates core counts.
    
    Args:
        datafile (str): Path to Spark History Server event log file
        
    Returns:
        dict: Maps worker ID to total core count
    """
    executor_added_events = get_lines_by_event(datafile)["SparkListenerExecutorAdded"]
    cores_per_worker = {}
    for event in executor_added_events:
        host = event["Executor Info"]["Host"]
        worker = om.get_worker_from_host(host)
        total_cores = event["Executor Info"]["Total Cores"]
        cores_per_worker[worker] = cores_per_worker.get(worker, 0) + total_cores
    return cores_per_worker


def get_stage_names(datafile):
    """
    Extract stage names from the event log.
    
    Args:
        datafile (str): Path to Spark History Server event log file
        
    Returns:
        dict: Maps stage ID to stage name
    """
    stage_completed_events = get_lines_by_event(datafile)["SparkListenerStageCompleted"]
    stage_names = {event["Stage Info"]["Stage ID"]: event["Stage Info"]["Stage Name"]
                   for event in stage_completed_events}
    return stage_names


def get_cpu_metrics():
    """
    Return list of CPU-related metric names tracked by Spark.
    
    Returns:
        list: Metric names for CPU time measurements
    """
    return ['internal.metrics.executorCpuTime', 'internal.metrics.executorDeserializeCpuTime']


def get_memory_metrics():
    """
    Return list of memory-related metric names tracked by Spark.
    
    Returns:
        list: Metric names for memory usage measurements
    """
    return ['internal.metrics.peakExecutionMemory']


def get_disk_write_metrics():
    """
    Return list of disk write metric names tracked by Spark.
    
    Includes both regular output and shuffle writes.
    
    Returns:
        list: Metric names for disk write measurements
    """
    return ['internal.metrics.output.bytesWritten', 'internal.metrics.shuffle.write.bytesWritten']


def get_disk_read_metrics():
    """
    Return list of local disk read metric names tracked by Spark.
    
    Includes both regular input and local shuffle reads.
    
    Returns:
        list: Metric names for local disk read measurements
    """
    return ['internal.metrics.input.bytesRead', 'internal.metrics.shuffle.read.localBytesRead']


def get_net_read_metrics():
    """
    Return list of network read metric names tracked by Spark.
    
    Covers remote shuffle reads over the network.
    
    Returns:
        list: Metric names for network read measurements
    """
    return ['internal.metrics.shuffle.read.remoteBytesRead']


def get_metric_names():
    """
    Get all standard Spark metric names.
    
    Combines CPU, memory, disk I/O, and network metrics.
    
    Returns:
        list: Complete list of metric names
    """
    return (get_cpu_metrics() + get_memory_metrics() + get_disk_read_metrics() +
            get_disk_write_metrics() + get_net_read_metrics())


def get_metric_names_from_datafiles(datafiles):
    """
    Extract all unique metric names actually present in event log files.
    
    This is useful when metric names vary across Spark versions or
    when only a subset of metrics are collected.
    
    Args:
        datafiles (list): List of paths to Spark History Server event log files
        
    Returns:
        list: Sorted list of unique metric names found in the files
    """
    metric_names = set()
    for datafile in datafiles:
        with open(datafile, "r") as f:
            for line in f:
                try:
                    d = json.loads(line)
                except:
                    print(f"ERROR_LINE({datafile}): {line}")

                if d["Event"] == "SparkListenerStageCompleted":
                    metrics = d["Stage Info"]["Accumulables"]
                    metrics = [a["Name"] for a in metrics if a["Name"].startswith("internal.metric")]
                    for m in metrics:
                        metric_names.add(m)
    metric_names = sorted(list(metric_names))

    return metric_names


def get_stage_profiles(datafile, metric_names):
    """
    Extract stage-level performance profiles from event log.
    
    Each stage profile includes submission/completion times and metric values
    (CPU time, memory, disk I/O, network, etc.).
    
    Args:
        datafile (str): Path to Spark History Server event log file
        metric_names (list): List of metric names to extract for each stage
        
    Returns:
        dict or None: Maps stage ID to [submission_time, completion_time, metrics_array]
                      Returns None if any stage has multiple attempts (failures/retries)
    """
    lines_by_event = {}
    with open(datafile, "r") as f:
        for line in f:
            try:
                d = json.loads(line)
            except:
                print(f"ERROR_LINE({datafile}): {line}")
            events = lines_by_event.get(d["Event"], [])
            events.append(d)
            lines_by_event[d["Event"]] = events

    stage_profiles = dict()
    for s in lines_by_event["SparkListenerStageCompleted"]:
        # Skip if stage had retries (multiple attempts indicate failures)
        if s["Stage Info"]["Stage Attempt ID"] > 0:
            return None
        
        s_info = s["Stage Info"]
        submission_time = to_date(s_info["Submission Time"])
        s_id = s_info["Stage ID"]
        completion_time = to_date(s_info["Completion Time"])
        
        # Extract metrics from Accumulables
        metrics_dict = {}
        for a in s_info["Accumulables"]:
            metrics_dict[a["Name"]] = a["Value"]
        
        # Build metrics array in the order specified by metric_names
        metrics_list = []
        for metric_name in metric_names:
            v = metrics_dict.get(metric_name, 0)
            metrics_list.append(v)
        metrics_list = np.array(metrics_list)
        stage_profiles[s_id] = [submission_time, completion_time, metrics_list]
    
    return stage_profiles


def get_vecs_per_app(appkey_to_datafile, metric_names):
    """
    Extract metric vectors for all stages across multiple applications.
    
    Each application's stages are represented as a matrix where:
    - Rows represent stages (sorted by stage ID)
    - Columns represent different metrics
    
    Args:
        appkey_to_datafile (dict): Maps application key to event log file path
        metric_names (list): List of metric names to extract
        
    Returns:
        dict: Maps application key to numpy array of shape (num_stages, num_metrics)
    """
    vecs_per_app = dict()
    for appkey in appkey_to_datafile:
        datafile = appkey_to_datafile[appkey]
        stage_profiles = get_stage_profiles(datafile, metric_names)
        sorted_stage_ids = sorted(list(stage_profiles))
        vecs = [stage_profiles[sid][2] for sid in sorted_stage_ids]
        vecs = np.array(vecs, dtype=float)
        vecs_per_app[appkey] = vecs
    return vecs_per_app


def get_active_tasks_score(datafile):
    """
    Calculate a utilization score based on active task timeline.
    
    This computes the area under the active tasks curve relative to maximum
    possible utilization, providing a measure of cluster efficiency.
    
    Args:
        datafile (str): Path to Spark History Server event log file
        
    Note:
        Currently incomplete implementation - calculates but doesn't return score.
    """
    timeline_per_worker = active_tasks_timeline_per_worker(datafile)
    start, end = get_time_interval(datafile)
    
    for worker in sorted(timeline_per_worker):
        timestamps, task_count = timeline_per_worker[worker]
        df = pd.DataFrame()
        df["timestamp"] = timestamps
        mind = df["timestamp"].min()

        def normalize(d1):
            return (d1 - start).total_seconds()

        df["timestamp"] = df["timestamp"].apply(normalize)
        df["task_count"] = task_count

        task_counts = list(df["task_count"])
        timestamps = list(df["timestamp"])
        widths = []
        bar_pos = []
        y = []
        app_area = 0
        
        # Calculate area under task count curve
        for i in range(1, len(timestamps)):
            width = (timestamps[i] - timestamps[i - 1])
            widths.append(width)
            bar_pos.append((timestamps[i] + timestamps[i - 1]) / 2)
            y.append(task_counts[i])
            app_area += width * task_counts[i]

        total_area = (end - start).total_seconds() * max(task_counts)


def get_stages_dataframe(datafile_to_config, metric_names, power_metrics):
    """
    Build comprehensive DataFrame with stage-level performance and energy data.
    
    Combines Spark stage metrics with PDU power measurements to create a unified
    dataset for analysis. Each row represents a stage with:
    - Application metadata (name, ID, config)
    - Application-level metrics (runtime, energy, power)
    - Stage-level metrics (runtime, energy, power, resource usage)
    
    Args:
        datafile_to_config (dict): Maps event log file path to config dict
            Config should contain: 'app', 'app_id', 'num_machines', 'vm_config'
        metric_names (list): List of Spark metric names to include
        power_metrics (PowerMetrics): PowerMetrics object with PDU data
        
    Returns:
        DataFrame: Pandas DataFrame with columns for all metrics and power data
            Columns include: app, app_id, stage_id, stage_name, runtime, 
            energy, power_avg, plus all specified metric_names
    """
    columns = ["app", "app_id", "app_start", "app_end", "num_machines", "vm_config", "app_runtime", "app_energy",
               "app_power_avg", "app_active_tasks_score", "stage_id", "stage_name", "stage_submission_time", "stage_completion_time",
               "stage_runtime", "energy", "power_avg"]
    columns = columns + metric_names
    rows = []
    
    for datafile in datafile_to_config:
        # Extract stage profiles from event log
        stage_profiles = get_stage_profiles(datafile, metric_names)
        if stage_profiles is None:
            print(f"StageWithMultipleAttempts: {datafile}")
            continue
        
        # Get cluster configuration
        hosts = get_hosts(datafile)
        outlets = om.get_outlets_from_hosts(hosts)
        app_start, app_end = get_time_interval(datafile)
        app_runtime = (app_end - app_start).total_seconds()
        
        # Calculate application-level energy consumption
        app_energy = 0
        app_power_avg = 0
        for outlet in outlets:
            e, p = power_metrics.get_energy_watt_second_and_power_avg_watt(outlet, app_start, app_end)
            app_energy += e
            app_power_avg += p
        
        # Get application metadata
        config = datafile_to_config[datafile]
        app_name = config["app"]
        app_id = config["app_id"]
        num_machines = config["num_machines"]
        vm_config = config["vm_config"]
        stage_names = get_stage_names(datafile)
        sorted_stage_ids = sorted(list(stage_profiles))
        stage_rows = []
        
        # Process each stage
        for stage_id in sorted_stage_ids:
            submission_time = stage_profiles[stage_id][0]
            completion_time = stage_profiles[stage_id][1]
            stage_runtime = (completion_time - submission_time).total_seconds()
            
            # Build row with application-level data
            stage_row = [app_name, app_id, app_start, app_end, num_machines, vm_config, app_runtime, app_energy,
                         app_power_avg, stage_id, stage_names[stage_id], submission_time, completion_time,
                         stage_runtime]
            
            # Calculate stage-level energy consumption
            energy = 0
            power = 0
            for outlet in outlets:
                e, p = power_metrics.get_energy_watt_second_and_power_avg_watt(outlet, submission_time, completion_time)
                energy += e
                power += p

            stage_row.append(energy)
            stage_row.append(power)
            
            # Append Spark metrics
            stage_row = stage_row + list(stage_profiles[stage_id][2])
            stage_rows.append(stage_row)
        
        rows = rows + stage_rows
    
    return pd.DataFrame(rows, columns=columns)


def get_normalized_vecs_per_app(vecs_per_app):
    """
    Normalize metric vectors across all applications for machine learning.
    
    Applies two-stage normalization:
    1. MaxAbsScaler: Scale each metric to [-1, 1] range
    2. L2 normalization: Normalize each stage vector to unit length
    
    This makes metrics comparable across different scales and applications.
    
    Args:
        vecs_per_app (dict): Maps application key to metric matrix (from get_vecs_per_app)
        
    Returns:
        dict: Maps application key to normalized metric matrix
    """
    vecs_normalized_per_app = dict()
    min_max_scaler = preprocessing.MaxAbsScaler()
    
    # Fit scaler on all data combined
    min_max_transform = min_max_scaler.fit(np.concatenate([vecs_per_app[k] for k in vecs_per_app]))
    
    # Apply normalization to each application
    for k in vecs_per_app:
        vecs = vecs_per_app[k]
        vecs_minmax = min_max_transform.transform(vecs)
        vecs_normalized = np.array(vecs_minmax)
        vecs_normalized = preprocessing.normalize(vecs_normalized, norm="l2")
        vecs_normalized_per_app[k] = vecs_normalized

    return vecs_normalized_per_app
