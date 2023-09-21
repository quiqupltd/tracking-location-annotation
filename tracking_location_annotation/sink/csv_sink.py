"""
Define class to output in a csv file
"""
import csv
import typing
from typing import List

from tracking_location_annotation.common.log import get_logger
from tracking_location_annotation.models import TrackingLocation
from tracking_location_annotation.sink.sink import Sink

logger = get_logger(__name__)

HEADER = [
    "uuid",
    "user_id",
    "recorded_at",
    "is_moving",
    "timestamp",
    "battery_level",
    "altitude",
    "altitude_accuracy",
    "longitude",
    "latitude",
    "speed",
    "heading",
    "coords_accuracy",
    "activity_type",
    "activity_confidence",
    "waypoint_id",
]


def _tls_to_rows(tls: List[TrackingLocation]) -> List[List]:
    return [
        [
            tl.uuid,
            tl.user_id,
            tl.recorded_at,
            tl.is_moving,
            tl.timestamp,
            tl.battery_level,
            tl.altitude,
            tl.altitude_accuracy,
            tl.longitude,
            tl.latitude,
            tl.speed,
            tl.heading,
            tl.coords_accuracy,
            tl.activity_type,
            tl.activity_confidence,
            tl.waypoint_id,
        ]
        for tl in tls
    ]


# pylint: disable=consider-using-with
class CSVSink(Sink):
    """
    create csv file and append data to it
    """

    def __init__(self, filename: str = "output.csv") -> None:
        logger.info("initilizing output sink as csv file")
        self.filename: str = filename
        self.csvwriter = None
        self.fd = None
        self.tls: List[TrackingLocation] = []
        self.name: str = "csv_sink"

    def __str__(self):
        return f" filesink - filname: {self.filename}"

    @typing.no_type_check
    def connect(self) -> "CSVSink":
        """create the csv file and initilize the header"""
        logger.info("creating filesink as %s for output", self.filename)
        self.fd = open(self.filename, "w", encoding="utf-8")  # pylint: disable
        self.csvwriter = csv.writer(self.fd)
        self.csvwriter.writerow(HEADER)  # type: ignore
        return self

    @typing.no_type_check
    def flush(self) -> None:
        """
        functions that flushes the output to a csv file
        creates row that maps tl attributes => waypoint_id
        """
        self.csvwriter.writerows(_tls_to_rows(self.tls))
        self.fd.flush()
        self.tls.clear()

    @typing.no_type_check
    def close(self) -> None:
        """
        write results and close file
        """
        self.flush()
        self.fd.close()
