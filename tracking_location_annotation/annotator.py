"""
module to define annotator class
"""
from tracking_location_annotation.common.log import get_logger
from tracking_location_annotation.models import Waypoint
from tracking_location_annotation.sink.sink import Sink

logger = get_logger(__name__)
TIME_LIMIT_MINUTES = 10


class Annotator:
    """
    annotator to check business scenarios
    before annotating tracking location records
    """

    def __init__(self, sink: Sink) -> None:
        self.sink = sink

    def annotate(self, *, new_waypoint: Waypoint, old_waypoint: Waypoint) -> None:
        """
        implements business scenarios
        """
        if not old_waypoint and new_waypoint:
            raise ValueError("both old and new waypoints should be passed for annotation")
        if new_waypoint.id != old_waypoint.id:
            raise ValueError(f"waypoints must have same id (got #{new_waypoint.id} and #{old_waypoint.id})")
        if new_waypoint.state == old_waypoint.state:
            logger.warning("new and old have same state (%s), waypoint_id = #%d", new_waypoint.state, new_waypoint.id)
            return

        # waypoint arrived
        # mission cancelled or finished
        # timedetlta <= 10 mins or not
        if mission := new_waypoint.mission():
            if mission.is_done:
                time_diff = abs(new_waypoint.timestamp - mission.timestamp).total_seconds()  # type: ignore
                waypoint_arrived_and_misison_done_within_10_mins = (
                    time_diff / 60 <= TIME_LIMIT_MINUTES and new_waypoint.state in "arrived"
                )
                if waypoint_arrived_and_misison_done_within_10_mins:
                    self.write_annotation(new_waypoint)
                else:
                    mission.tls_bucket.clear()
                    logger.debug("Tracking locations bucket cleared for mission#%d", mission.id)
                return

        # pending -> arrived
        # pending -> finished
        # arrived -> finished
        allowed_changes = [("pending", "arrived"), ("pending", "finished"), ("arrived", "finished")]

        if (old_waypoint.state, new_waypoint.state) in allowed_changes:
            self.write_annotation(new_waypoint)

    def write_annotation(self, waypoint: Waypoint):
        """
        annotates tl record based on business scenarios
        """
        mission = waypoint.mission()
        if not mission:
            logger.warning(
                "annotation skipped for waypoint #%d, mission not found, (job_id = %d)", waypoint.id, waypoint.job_id
            )
            return

        for tl in mission.tls_bucket:
            tl.waypoint_id = waypoint.id  # type: ignore
            logger.debug("tracking location %d => waypoint %d", tl.user_id, tl.waypoint_id)
            self.sink.append(tl)

        mission.tls_bucket.clear()
