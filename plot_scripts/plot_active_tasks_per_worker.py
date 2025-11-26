import matplotlib.pyplot as plt
import pandas as pd

import pduhistoryserver as hs
import pduutil as pu
import sys

output_dir = sys.argv[1]

for datafile, config in pu.datafiles_iterator():
    app = config["app"]
    timeline_per_worker = hs.active_tasks_timeline_per_worker(datafile)
    start, end = hs.get_time_interval(datafile)
    vm_config = config["vm_config"]
    num_machines = int(config["num_machines"])
    app_id = config["app_id"]
    fig, axs = plt.subplots(nrows=1, ncols=num_machines, figsize=(20,5))
    fig.suptitle(f"Aplicação={app} Config={vm_config}\n", fontsize=18)
    axs = iter(axs)
    for worker in sorted(timeline_per_worker):
        timestamps, task_count = timeline_per_worker[worker]
        df = pd.DataFrame()
        df["timestamp"] = timestamps
        mind = df["timestamp"].min()
        def normalize(d1):
            return (d1-start).total_seconds()
        df["timestamp"] = df["timestamp"].apply(normalize)
        df["task_count"] = task_count

        task_counts = list(df["task_count"])
        timestamps = list(df["timestamp"])
        widths = []
        bar_pos = []
        y = []
        app_area = 0
        for i in range(1,len(timestamps)):
            width = (timestamps[i] - timestamps[i-1])
            widths.append(width)
            bar_pos.append((timestamps[i] + timestamps[i-1]) / 2)
            y.append(task_counts[i])
            app_area += width * task_counts[i]

        total_area = (end - start).total_seconds() * max(task_counts)
        ax = next(axs)
        ax.set_xlim(0, (end-start).total_seconds())
        ax.set_title(f"Trab.={worker} Score={app_area / total_area:.2f}")
        ax.set_xlabel("Linha do tempo (segundos)")
        ax.set_ylabel("Tarefas ativas por trabalhador")
        ax.bar(bar_pos, y, width=widths)
    fig_filename = f"{output_dir}/{app}-{vm_config}-{num_machines}-{app_id}.pdf"
    fig.savefig(fig_filename, bbox_inches='tight')
    fig.clear()
    plt.close(fig)
    print(f"Figure saved: {fig_filename}")
