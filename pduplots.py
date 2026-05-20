"""
Power Timeline Plotting Module

Utilities for visualizing power consumption timelines alongside Spark stage execution.

This module creates plots that show:
- Power consumption per worker over time
- Total cluster power consumption
- Spark stage execution intervals overlaid on the timeline
"""

import matplotlib.pyplot as plt
import numpy as np

import pduhistoryserver as hs
import pduoutletmap as om


def _intervals2layers(si):
    """
    Organize overlapping time intervals into non-overlapping layers for visualization.
    
    This is used to display multiple concurrent Spark stages without overlap in the plot.
    Each layer contains stages that don't temporally overlap with each other.
    
    Args:
        si (list): List of [start_time, end_time] intervals (sorted by start time)
        
    Returns:
        list: List of layers, where each layer is a list of non-overlapping intervals
    """
    layers = [[si[0]]]
    for p in si[1:]:
        for lay in layers:
            # If this interval doesn't overlap with the last in this layer, add it
            if lay[-1][-1] < p[0]:
                lay.append(p)
                break
        else:
            # No suitable layer found, create a new one
            layers.append([p])

    return layers


def plot_timeline_stages_power(datafile, power_metrics, plot_title):
    """
    Create a comprehensive plot showing power consumption and Spark stages over time.
    
    The plot includes:
    - Individual worker power consumption lines
    - Total cluster power consumption
    - Horizontal bars representing stage execution intervals
    
    Args:
        datafile (str): Path to Spark History Server event log file
        power_metrics (PowerMetrics): PowerMetrics object with loaded PDU data
        plot_title (str): Title for the plot (also used as output filename)
        
    Output:
        Saves plot as '{plot_title}.pdf' in current directory
    """
    # Get application time interval and stage information
    start, end = hs.get_time_interval(datafile)
    metric_names = hs.get_metric_names_from_datafiles([datafile])
    stages_data = hs.get_stage_profiles(datafile, metric_names)
    
    # Convert stage intervals to relative time (seconds from start)
    intervals = sorted([[k] + stages_data[k][:2] for k in stages_data], key=lambda t: t[1])
    intervals = [d[1:] for d in intervals]
    intervals = [[(d[0] - start).total_seconds(), (d[1] - start).total_seconds()] for d in intervals]

    plt.figure()
    plt.title(plot_title)

    # Get power data for each worker
    hosts = hs.get_hosts(datafile)
    workers = om.get_workers_from_hosts(hosts)
    ymax = -1  # Maximum power value for y-axis scaling
    xmax = -1  # Maximum time value for x-axis scaling
    xtotal = None
    ytotal = None
    
    # Plot power timeline for each worker
    for worker in workers:
        x, y = power_metrics.worker_timeline(worker, start, end)
        x = x - x.min()  # Normalize to start at 0
        plt.plot(x, y, label=f"Trabalhador {worker}")
        xtotal = x
        
        # Accumulate total power across all workers
        if ytotal is None:
            ytotal = y
        else:
            ytotal = np.sum([ytotal, y], axis=0)
        ymax = max(ymax, ytotal.max())
        xmax = max(xmax, x.max())

    # Organize stages into non-overlapping layers for visualization
    layers = _intervals2layers(intervals)
    
    # Draw horizontal bars for each stage in each layer
    for i, lay in enumerate(layers):
        x1, x2 = zip(*lay)
        # Offset each layer vertically for visibility
        plt.hlines([i * 50 + (ymax / 2)] * len(x1), x1, x2, lw=10)

    # Plot total cluster power consumption
    plt.plot(xtotal, ytotal, label="Todos os trabalhadores")
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    plt.xlim(0, xmax)
    plt.ylim(0, ymax + 10)
    plt.xlabel("Linha do tempo (segundos)")
    plt.ylabel("Potência (Watt)")

    plt.savefig(f"{plot_title}.pdf", bbox_inches='tight')
