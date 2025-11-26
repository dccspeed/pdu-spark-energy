from datetime import datetime
from sklearn.metrics import auc
import numpy as np
import pduoutletmap as om

import pandas as pd


def to_date(iso):
    d = datetime.fromisoformat(iso)
    return d


class PowerMetrics:
    def __init__(self, metrics_file_name):
        df = pd.read_csv(metrics_file_name)
        print(df.columns)
        df["date"] = df["time"].apply(to_date)
        min_date = min(df["date"])
        max_date = max(df["date"])
        df["delta"] = df["date"].apply(lambda d: (d - min_date).total_seconds())

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
        assert end_date > start_date
        timeline = self.timeline_by_outlet[outlet]
        from_delta = (start_date - self.min_date).total_seconds()
        from_delta = max(from_delta, 0)
        to_delta = (end_date - self.min_date).total_seconds()
        idx1 = np.searchsorted(timeline[0], from_delta, side="right") - 1
        idx1 = max(idx1, 0)
        idx2 = np.searchsorted(timeline[0], to_delta, side="left")
        x = timeline[0][idx1: (idx2 + 1)]
        y = timeline[1][idx1: (idx2 + 1)]
        try:
            area = auc(x, y)
        except Exception as e:
            print(from_delta, to_delta, idx1, idx2, list(x), list(y), len(timeline[0]))
            raise e

        return area, sum(y) / len(y)

    def worker_timeline(self, worker, start_date, end_date):
        outlets = om.get_worker_outlets(worker)
        from_delta = (start_date - self.min_date).total_seconds()
        to_delta = (end_date - self.min_date).total_seconds()

        x = None
        y = None
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
