import matplotlib.pyplot as plt
import numpy as np

import pduhistoryserver as hs
import pduoutletmap as om


def _intervals2layers(si):
    layers = [[si[0]]]
    for p in si[1:]:
        for lay in layers:
            if lay[-1][-1] < p[0]:
                lay.append(p)
                break
        else:
            layers.append([p])

    return layers


def plot_timeline_stages_power(datafile, power_metrics, plot_title):
    start, end = hs.get_time_interval(datafile)
    metric_names = hs.get_metric_names_from_datafiles([datafile])
    stages_data = hs.get_stage_profiles(datafile, metric_names)
    intervals = sorted([[k] + stages_data[k][:2] for k in stages_data], key=lambda t: t[1])
    intervals = [d[1:] for d in intervals]
    intervals = [[(d[0] - start).total_seconds(), (d[1] - start).total_seconds()] for d in intervals]

    plt.figure()
    plt.title(plot_title)

    # get power data
    hosts = hs.get_hosts(datafile)
    workers = om.get_workers_from_hosts(hosts)
    ymax = -1
    xmax = -1
    xtotal = None
    ytotal = None
    for worker in workers:
        x, y = power_metrics.worker_timeline(worker, start, end)
        x = x - x.min()
        plt.plot(x, y, label=f"Trabalhador {worker}")
        xtotal = x
        if ytotal is None:
            ytotal = y
        else:
            ytotal = np.sum([ytotal, y], axis=0)
        ymax = max(ymax, ytotal.max())
        xmax = max(xmax, x.max())


    layers = _intervals2layers(intervals)
    for i, lay in enumerate(layers):
        x1, x2 = zip(*lay)
        plt.hlines([i * 50 + (ymax / 2)] * len(x1), x1, x2, lw=10)

    plt.plot(xtotal, ytotal, label="Todos os trabalhadores")
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    plt.xlim(0, xmax)
    plt.ylim(0, ymax + 10)
    plt.xlabel("Linha do tempo (segundos)")
    plt.ylabel("Potência (Watt)")

    plt.savefig(f"{plot_title}.pdf", bbox_inches='tight')
