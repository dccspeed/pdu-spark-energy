"""
PDU Power Metrics Processing Module

This module handles loading and processing of power consumption data from PDU outlets.
It provides functionality to:
- Load time-series power metrics from CSV files
- Calculate energy consumption (watt-seconds) for specific time intervals
- Aggregate power data across multiple outlets/workers
"""

from datetime import datetime
from sklearn.metrics import auc
import numpy as np
import pduoutletmap as om

import pandas as pd


def to_date(iso):
    """
    Convert ISO 8601 datetime string to Python datetime object.
    
    Args:
        iso (str): ISO 8601 formatted datetime string
        
    Returns:
        datetime: Python datetime object
    """
    d = datetime.fromisoformat(iso)
    return d


class PowerMetrics:
    """
    Manages power consumption time-series data from PDU outlets.
    
    This class loads power metrics from a CSV file and provides methods to:
    - Query energy consumption over specific time intervals
    - Retrieve power timelines for individual workers
    - Calculate average power consumption
    
    Attributes:
        timeline_by_outlet (dict): Maps outlet IDs to (time_delta, power_value) tuples
        min_date (datetime): Earliest timestamp in the dataset
        max_date (datetime): Latest timestamp in the dataset
        data (DataFrame): Raw power metrics data
    """
    
    def __init__(self, metrics_file_name):
        """
        Initialize PowerMetrics by loading data from CSV file.
        
        Args:
            metrics_file_name (str): Path to CSV file with columns: time, active-power-outlet, value
        """
        df = pd.read_csv(metrics_file_name)
        print(df.columns)
        
        # Convert ISO timestamps to datetime objects
        df["date"] = df["time"].apply(to_date)
        min_date = min(df["date"])
        max_date = max(df["date"])
        
        # Calculate time delta in seconds from the start
        df["delta"] = df["date"].apply(lambda d: (d - min_date).total_seconds())

        # Create timeline for each outlet (x=time_delta, y=power_value)
        unique_outlets = df["active-power-outlet"].unique()
        timeline_by_outlet = dict()
        for outlet in unique_outlets:
            df_outlet = df[df["active-power-outlet"] == outlet]
            x = df_outlet["delta"]
            y = df_outlet["value"]

            timeline_by_outlet[outlet] = (x, y)

        self.timeline_by_outlet = timeline_by_outlet
        self.min_date = min_date
        self.max_date = max_date
        self.data = df

    def get_energy_watt_second_and_power_avg_watt(self, outlet, start_date, end_date):
        """
        Calculate energy consumption and average power for a specific outlet and time range.
        
        Uses Area Under Curve (AUC) integration to calculate energy in watt-seconds.
        
        Args:
            outlet (int): PDU outlet ID
            start_date (datetime): Start of time interval
            end_date (datetime): End of time interval
            
        Returns:
            tuple: (energy_watt_seconds, average_power_watts)
            
        Raises:
            AssertionError: If end_date <= start_date
        """
        assert end_date > start_date
        timeline = self.timeline_by_outlet[outlet]
        
        # Convert dates to time deltas relative to min_date
        from_delta = (start_date - self.min_date).total_seconds()
        from_delta = max(from_delta, 0)
        to_delta = (end_date - self.min_date).total_seconds()
        
        # Find indices in timeline that bound the requested interval
        idx1 = np.searchsorted(timeline[0], from_delta, side="right") - 1
        idx1 = max(idx1, 0)
        idx2 = np.searchsorted(timeline[0], to_delta, side="left")
        
        # Extract time and power values for the interval
        x = timeline[0][idx1: (idx2 + 1)]
        y = timeline[1][idx1: (idx2 + 1)]
        
        try:
            # Calculate energy as area under the power curve
            area = auc(x, y)
        except Exception as e:
            print(from_delta, to_delta, idx1, idx2, list(x), list(y), len(timeline[0]))
            raise e

        return area, sum(y) / len(y)

    def worker_timeline(self, worker, start_date, end_date):
        """
        Get aggregated power timeline for a worker (which may have multiple outlets).
        
        Args:
            worker (int): Worker ID (1-6)
            start_date (datetime): Start of time interval
            end_date (datetime): End of time interval
            
        Returns:
            tuple: (time_array, power_array) where power is summed across all worker outlets
        """
        outlets = om.get_worker_outlets(worker)
        from_delta = (start_date - self.min_date).total_seconds()
        to_delta = (end_date - self.min_date).total_seconds()

        x = None
        y = None
        
        # Aggregate power values across all outlets for this worker
        for outlet in outlets:
            timeline = self.timeline_by_outlet[outlet]
            idxs = np.searchsorted(timeline[0], [from_delta, to_delta])
            outlet_x = timeline[0][idxs[0]: (idxs[1] + 1)]
            outlet_y = timeline[1][idxs[0]: (idxs[1] + 1)]

            x = np.array(outlet_x)
            if y is None:
                y = np.array(outlet_y)
            else:
                y = np.sum([y, np.array(outlet_y)], axis=0)

        return x, y
