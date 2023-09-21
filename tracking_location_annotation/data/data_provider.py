"""
abstract class created to be inherited from
defined data providers
"""
from abc import ABC, abstractmethod
from typing import Tuple

import numpy as np
import pandas as pd


class DataProvider(ABC):
    """
    DataProvider class provides the capability
    to read data from providers into the app
    """

    def __init__(self, start_date: np.datetime64, batch_size_in_days: int) -> None:
        self.start_date = start_date
        self.end_date = start_date + np.timedelta64(batch_size_in_days + 1)
        self.end_datetime = self.end_date - np.timedelta64(21, "h")

    @abstractmethod
    def fetch_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:  # pragma: no cover
        """
        to fetch the data given a start time
        and a data source
        """
