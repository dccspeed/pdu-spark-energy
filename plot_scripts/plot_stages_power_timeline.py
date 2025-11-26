import json
import sys

import pduplots as pp
import pduhistoryserver as hs
from pdumetrics import PowerMetrics

import numpy as np
import pandas as pd


def datafiles_iterator(output_dir):
    with open("history-server-tree.json", "r") as f:
        directory_tree = json.load(f)

    directory_tree = directory_tree[0]
    base_dir = directory_tree["name"]
    for app_tree in directory_tree["contents"]:
        app_dir = app_tree["name"]
        if app_dir != "terasort": continue
        for vmconfig_tree in app_tree["contents"][:1]:
            vmconfig_dir = vmconfig_tree["name"]
            for num_machines_tree in vmconfig_tree["contents"][:1]:
                num_machines_dir = num_machines_tree["name"]
                for datafile_tree in num_machines_tree["contents"][:1]:
                    datafile = datafile_tree["name"]
                    datafile_full_path = f"{base_dir}/{app_dir}/{vmconfig_dir}/{num_machines_dir}/{datafile}"
                    plot_title = f"{output_dir}/{app_dir}-{vmconfig_dir}-{num_machines_dir}-{datafile}"
                    yield datafile_full_path, plot_title


output_dir = sys.argv[1]

#pm = PowerMetrics("all_power_processed.csv.gz")
for datafile, plot_title in datafiles_iterator(output_dir):
    print(datafile)
    timeline_per_host = hs.active_tasks_timeline_per_host(datafile)
    for host in timeline_per_host:
        timestamps, task_count = timeline_per_host[host]
        df = pd.DataFrame()
        df["timestamp"] = timestamps
        df["task_count"] = task_count
        print(host, df["timestamp"].min(), df["timestamp"].max())
        df = df.groupby(["task_count"])["timestamp"].size()
        print(df.reset_index())

    # pp.plot_timeline_stages_power(datafile, pm, plot_title)
