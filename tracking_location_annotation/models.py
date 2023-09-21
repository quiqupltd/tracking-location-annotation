"""
file to define the models that will represent the records with some helper functions
"""
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from numpy import datetime64

from tracking_location_annotation.common.utils import maybe_int


# pylint: disable=too-many-arguments, redefined-builtin
class Mission:
    """
    class represting mission object, and its jobs
    """

    def __init__(
        self,
        id: int,
        courier_id: Optional[Any],
        state: str,
        created_at: datetime64,
        updated_at: datetime64,
        timestamp: datetime64,
        record_type: str,
    ) -> None:

        self.id = int(id)
        self.courier_id = maybe_int(courier_id)
        self.state = state
        self.created_at = created_at
        self.updated_at = updated_at
        self.timestamp = timestamp
        self.record_type = record_type
        self.jobs: Dict[int, Job] = {}
        self.tls_bucket: List[TrackingLocation] = []
        self.waypoints_processing_order: List[int] = []
        self.jobs_from_other_missions: Set[int] = set()
        self.intermediate_tls_bucket: List[TrackingLocation] = []

    @property
    def is_done(self) -> bool:
        """returns true if the mission is done"""
        return self.state in ["complete", "cancelled"]

    def __str__(self) -> str:  # pragma: no cover
        return f"[{self.timestamp}] Mission#:{self.id}, state:{self.state}, courier_id:{self.courier_id}"

    def __repr__(self) -> str:  # pragma: no cover
        return f"[{self.timestamp}] Mission#:{self.id}, state:{self.state}, courier_id:{self.courier_id}"

    def add_job(self, new_job: "Job") -> None:
        """adds job to mission"""
        self.jobs[new_job.id] = new_job

    def remove_job(self, job: "Job") -> None:
        """removes job from mission"""
        self.jobs.pop(job.id, None)


class Job:
    """
    class represting job object, and its waypoints
    """

    def __init__(
        self,
        id: int,
        state: str,
        created_at: datetime64,
        updated_at: datetime64,
        mission_id: Optional[Any],
        timestamp: datetime64,
        record_type: str,
    ) -> None:

        self.id = int(id)
        self.state = state
        self.created_at = created_at
        self.updated_at = updated_at
        self.mission_id = maybe_int(mission_id)
        self.timestamp = timestamp
        self.record_type = record_type
        self.waypoints: Dict[int, Waypoint] = {}

    def __str__(self) -> str:  # pragma: no cover
        return f"[{self.timestamp}] Job#:{self.id}, state:{self.state}, mission_id:{self.mission_id}"

    def __repr__(self) -> str:  # pragma: no cover
        return f"[{self.timestamp}] Job#:{self.id}, state:{self.state}, mission_id:{self.mission_id}"

    def mission(self) -> Optional[Mission]:
        """returns the mission of the job or none"""
        if not self.mission_id:
            return None
        return MISSIONS.get(self.mission_id)

    def add_waypoint(self, new_waypoint: "Waypoint") -> None:
        """replaces the old waypoint with new waypoint or adds new one"""
        self.waypoints[new_waypoint.id] = new_waypoint

    @property
    def is_done(self) -> bool:
        """returns true if the job is done"""
        return self.state in ["complete", "cancelled"]


class Waypoint:
    """
    model to represent the waypoints table records with some helper functions
    """

    def __init__(
        self,
        id: float,
        job_id: Optional[Any],
        courier_id: Optional[Any],
        state: str,
        created_at: datetime64,
        updated_at: datetime64,
        timestamp: datetime64,
        record_type: str,
    ) -> None:

        self.id = int(id)
        self.job_id = maybe_int(job_id)
        self.courier_id = maybe_int(courier_id)
        self.state = state
        self.created_at = created_at
        self.updated_at = updated_at
        self.timestamp = timestamp
        self.record_type = record_type

    def __str__(self) -> str:  # pragma: no cover
        return f"[{self.timestamp}] Waypoint#:{self.id}, state:{self.state}, job_id:{self.job_id}"

    def __repr__(self) -> str:  # pragma: no cover
        return f"[{self.timestamp}] Waypoint#:{self.id}, state:{self.state}, job_id:{self.job_id}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Waypoint):
            return False
        return self.id == other.id

    def mission(self) -> Optional[Mission]:
        """returns the associated mission or null"""
        if job := self.job():
            return job.mission()
        return None

    def job(self) -> Optional[Job]:
        """returns the associated job or null or none"""
        if not self.job_id:
            return None
        return JOBS.get(self.job_id)


class TrackingLocation:
    """
    model to represent the tracking locations table records with some helper functions
    """

    def __init__(self, row: pd.Series) -> None:

        self.user_id = row.user_id
        self.recorded_at = row.recorded_at
        self.is_moving = row.is_moving
        self.uuid = row.uuid
        self.timestamp = row.timestamp
        self.odometer = row.odometer
        self.battery_level = row.battery_level
        self.altitude = row.altitude
        self.longitude = row.longitude
        self.altitude_accuracy = row.altitude_accuracy
        self.latitude = row.latitude
        self.speed = row.speed
        self.heading = row.heading
        self.coords_accuracy = row.coords_accuracy
        self.activity_type = row.activity_type
        self.activity_confidence = row.activity_confidence
        self.record_type = row.record_type
        self.mission_state = None
        self.waypoint_id = None

    def __str__(self) -> str:  # pragma: no cover
        return f"[{self.timestamp}] TL#:{self.uuid}, courier_id:{self.user_id}"

    def __repr__(self) -> str:  # pragma: no cover
        return f"[{self.timestamp}] TL#:{self.uuid}, courier_id:{self.user_id}"


from tracking_location_annotation.db import (  # pylint: disable=wrong-import-position, cyclic-import
    JOBS,
    MISSIONS,
)
