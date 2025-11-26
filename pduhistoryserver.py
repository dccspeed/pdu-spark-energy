import json
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn import preprocessing

import pduoutletmap as om


def format_date(d):
    d = d.strftime('%Y-%m-%dT%H:%M:%SZ')
    return d


def to_date(unix_timestamp):
    d = datetime.fromtimestamp(unix_timestamp / 1000, timezone.utc)
    return d


def get_lines_by_event(datafile):
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
    events = get_lines_by_event(datafile)
    app_name = events["SparkListenerApplicationStart"][0]["App Name"]
    return app_name


def get_time_interval(datafile):
    events = get_lines_by_event(datafile)
    start_timestamp = to_date(events["SparkListenerApplicationStart"][0]["Timestamp"])
    end_timestamp = to_date(events["SparkListenerApplicationEnd"][0]["Timestamp"])
    return start_timestamp, end_timestamp


def get_hosts(datafile):
    executor_added_events = get_lines_by_event(datafile)["SparkListenerExecutorAdded"]
    hosts = []
    for event in executor_added_events:
        hosts.append(event["Executor Info"]["Host"])
    return hosts


def get_cores_per_host(datafile):
    executor_added_events = get_lines_by_event(datafile)["SparkListenerExecutorAdded"]
    cores_per_host = {}
    for event in executor_added_events:
        host = event["Executor Info"]["Host"]
        total_cores = event["Executor Info"]["Total Cores"]
        cores_per_host[host] = cores_per_host.get(host, 0) + total_cores
    return cores_per_host

def get_cores_per_worker(datafile):
    executor_added_events = get_lines_by_event(datafile)["SparkListenerExecutorAdded"]
    cores_per_worker = {}
    for event in executor_added_events:
        host = event["Executor Info"]["Host"]
        worker = om.get_worker_from_host(host)
        total_cores = event["Executor Info"]["Total Cores"]
        cores_per_worker[worker] = cores_per_worker.get(worker, 0) + total_cores
    return cores_per_worker


def get_stage_names(datafile):
    stage_completed_events = get_lines_by_event(datafile)["SparkListenerStageCompleted"]
    stage_names = {event["Stage Info"]["Stage ID"]: event["Stage Info"]["Stage Name"]
                   for event in stage_completed_events}
    return stage_names


def get_cpu_metrics():
    return ['internal.metrics.executorCpuTime', 'internal.metrics.executorDeserializeCpuTime']


def get_memory_metrics():
    return ['internal.metrics.peakExecutionMemory']


def get_disk_write_metrics():
    return ['internal.metrics.output.bytesWritten', 'internal.metrics.shuffle.write.bytesWritten']


def get_disk_read_metrics():
    return ['internal.metrics.input.bytesRead', 'internal.metrics.shuffle.read.localBytesRead']


def get_net_read_metrics():
    return ['internal.metrics.shuffle.read.remoteBytesRead']


def get_metric_names():
    return (get_cpu_metrics() + get_memory_metrics() + get_disk_read_metrics() +
            get_disk_write_metrics() + get_net_read_metrics())


def get_metric_names_from_datafiles(datafiles):
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
        if s["Stage Info"]["Stage Attempt ID"] > 0:
            return None
        s_info = s["Stage Info"]
        submission_time = to_date(s_info["Submission Time"])
        s_id = s_info["Stage ID"]
        completion_time = to_date(s_info["Completion Time"])
        metrics_dict = {}
        for a in s_info["Accumulables"]:
            metrics_dict[a["Name"]] = a["Value"]
        metrics_list = []
        for metric_name in metric_names:
            v = metrics_dict.get(metric_name, 0)
            metrics_list.append(v)
        metrics_list = np.array(metrics_list)
        stage_profiles[s_id] = [submission_time, completion_time, metrics_list]
    return stage_profiles


def get_vecs_per_app(appkey_to_datafile, metric_names):
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
        for i in range(1, len(timestamps)):
            width = (timestamps[i] - timestamps[i - 1])
            widths.append(width)
            bar_pos.append((timestamps[i] + timestamps[i - 1]) / 2)
            y.append(task_counts[i])
            app_area += width * task_counts[i]

        total_area = (end - start).total_seconds() * max(task_counts)


def get_stages_dataframe(datafile_to_config, metric_names, power_metrics):
    columns = ["app", "app_id", "app_start", "app_end", "num_machines", "vm_config", "app_runtime", "app_energy",
               "app_power_avg", "app_active_tasks_score", "stage_id", "stage_name", "stage_submission_time", "stage_completion_time",
               "stage_runtime", "energy", "power_avg"]
    columns = columns + metric_names
    rows = []
    for datafile in datafile_to_config:
        stage_profiles = get_stage_profiles(datafile, metric_names)
        if stage_profiles is None:
            print(f"StageWithMultipleAttempts: {datafile}")
            continue
        hosts = get_hosts(datafile)
        outlets = om.get_outlets_from_hosts(hosts)
        app_start, app_end = get_time_interval(datafile)
        app_runtime = (app_end - app_start).total_seconds()
        app_energy = 0
        app_power_avg = 0
        for outlet in outlets:
            e, p = power_metrics.get_energy_watt_second_and_power_avg_watt(outlet, app_start, app_end)
            app_energy += e
            app_power_avg += p
        config = datafile_to_config[datafile]
        app_name = config["app"]
        app_id = config["app_id"]
        num_machines = config["num_machines"]
        vm_config = config["vm_config"]
        stage_names = get_stage_names(datafile)
        sorted_stage_ids = sorted(list(stage_profiles))
        stage_rows = []
        for stage_id in sorted_stage_ids:
            submission_time = stage_profiles[stage_id][0]
            completion_time = stage_profiles[stage_id][1]
            stage_runtime = (completion_time - submission_time).total_seconds()
            stage_row = [app_name, app_id, app_start, app_end, num_machines, vm_config, app_runtime, app_energy,
                         app_power_avg, stage_id, stage_names[stage_id], submission_time, completion_time,
                         stage_runtime]
            energy = 0
            power = 0
            for outlet in outlets:
                e, p = power_metrics.get_energy_watt_second_and_power_avg_watt(outlet, submission_time, completion_time)
                energy += e
                power += p

            stage_row.append(energy)
            stage_row.append(power)
            stage_row = stage_row + list(stage_profiles[stage_id][2])
            stage_rows.append(stage_row)
        rows = rows + stage_rows
    return pd.DataFrame(rows, columns=columns)


def get_normalized_vecs_per_app(vecs_per_app):
    vecs_normalized_per_app = dict()
    min_max_scaler = preprocessing.MaxAbsScaler()
    min_max_transform = min_max_scaler.fit(np.concatenate([vecs_per_app[k] for k in vecs_per_app]))
    for k in vecs_per_app:
        vecs = vecs_per_app[k]
        vecs_minmax = min_max_transform.transform(vecs)
        vecs_normalized = np.array(vecs_minmax)
        vecs_normalized = preprocessing.normalize(vecs_normalized, norm="l2")
        vecs_normalized_per_app[k] = vecs_normalized

    return vecs_normalized_per_app
