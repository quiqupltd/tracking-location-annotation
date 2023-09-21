"""
module to define CSVConsumer class
"""
from typing import Tuple

import numpy as np
import pandas as pd

from tracking_location_annotation.data.data_provider import DataProvider


class CSVConsumer(DataProvider):
    """
    data prodivder from csv files
    """

    def __init__(self, start_date: np.datetime64, batch_size_in_days: int, data_path: str) -> None:
        self.start_date = start_date
        self.end_date = start_date + np.timedelta64(batch_size_in_days + 1)
        self.end_datetime = self.end_date - np.timedelta64(21, "h")
        self.data_path = data_path

    def fetch_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        df_missions = pd.read_csv(self.data_path + "/missions_data.csv")
        df_waypoints = pd.read_csv(self.data_path + "/waypoints_data.csv")
        df_jobs = pd.read_csv(self.data_path + "/jobs_data.csv")
        df_tl = pd.read_csv(self.data_path + "/tl_data.csv")

        return df_missions, df_waypoints, df_jobs, df_tl
