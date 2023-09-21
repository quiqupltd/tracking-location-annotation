"""
Define class to output in diffrent sinks
"""
from abc import ABC, abstractmethod
from typing import List

import pandas as pd

from tracking_location_annotation.models import TrackingLocation


class Sink(ABC):
    """
    parent class for sinks to output result's data
    """

    def __init__(self) -> None:
        self.tls: List[TrackingLocation] = []
        self.name: str = "sink"

    @abstractmethod
    def connect(self) -> "Sink":
        """
        connect to the sink
        -> create file, connect to kafka topic, etc...
        """

    def append(self, tl: TrackingLocation) -> None:
        """
        function that takes the annotated tracking
        location object and adds it to the result list
        """
        self.tls.append(tl)

    @abstractmethod
    def flush(self) -> None:
        """
        push the annotated tls to sink
        """

    def close(self) -> None:
        """
        releases resources acquired by the sink
        """

    def get_dataframe(self) -> pd.DataFrame:
        """
        returns result in a dataframe
        """
        df = pd.DataFrame([tl.__dict__ for tl in self.tls])
        df = df[["uuid", "waypoint_id"]].reset_index(drop=True)
        return df
