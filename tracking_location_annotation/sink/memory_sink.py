"""
Define class to output to memory
"""
from typing import List

from tracking_location_annotation.common.log import get_logger
from tracking_location_annotation.models import TrackingLocation
from tracking_location_annotation.sink.sink import Sink

logger = get_logger(__name__)


class MemorySink(Sink):
    """
    saves the results to memory only
    """

    def __init__(self) -> None:
        logger.info("initilizing output sink to memory")
        self.tls: List[TrackingLocation] = []
        self.name: str = "memory_sink"

    def connect(self) -> "MemorySink":
        return self

    def flush(self) -> None:
        self.tls.clear()
