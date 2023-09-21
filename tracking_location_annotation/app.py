"""
main module defining application algorithm
"""
from numpy import datetime64

from tracking_location_annotation.annotator import Annotator
from tracking_location_annotation.common.benchmark import measure
from tracking_location_annotation.common.log import get_logger
from tracking_location_annotation.common.utils import add_if_not_on_top, maybe_int
from tracking_location_annotation.data.data_provider import DataProvider
from tracking_location_annotation.data.get_data_util import get_data

# global dicts
from tracking_location_annotation.db import (
    JOBS,
    MISSIONS,
    courier_id_to_mission_id,
    unmapped_jobs,
    unmapped_waypoints,
)

# models
from tracking_location_annotation.models import Job, Mission, TrackingLocation, Waypoint
from tracking_location_annotation.sink.sink import Sink

# initilizing logger
logger = get_logger(__name__)


@measure("process_mission")
def process_mission(mission: Mission, datetime_upper_limit: datetime64) -> None:
    """function that processes missions, maps jobs to missions
    and calls for annotating tl if there is mission state change"""
    logger.debug(mission)
    if mission.created_at > datetime_upper_limit:
        logger.debug("mission#%d will not be processed, created at %s", mission.id, str(mission.created_at))
        return
    # add to/update global missions dict
    if old_mission_record := MISSIONS.get(mission.id):
        mission.jobs = old_mission_record.jobs
        mission.tls_bucket = old_mission_record.tls_bucket
        mission.waypoints_processing_order = old_mission_record.waypoints_processing_order

    # match unmatched jobs if any
    for job in unmapped_jobs.get(mission.id, {}).values():
        logger.debug("job#{job.id} mapped to mission #{mission.id}")
        mission.add_job(job)
    unmapped_jobs.pop(mission.id, None)

    # remove mission if it's done and the tls are annotated
    if mission.courier_id:
        if not mission.is_done:
            # add to the lookup table
            courier_id_to_mission_id[mission.courier_id] = mission.id
    MISSIONS[mission.id] = mission


@measure("process_job")
def process_job(job: Job, datetime_upper_limit: datetime64) -> None:
    """function to fill that processes jobs to map waypoints to missions"""
    logger.debug(job)
    if job.created_at > datetime_upper_limit:
        logger.debug("job#%d will not be processed, created at %s", job.id, str(job.created_at))
        return

    if old_job_record := JOBS.get(job.id):
        job.waypoints = old_job_record.waypoints
        # if there is mission_id change
        if job.mission_id != old_job_record.mission_id:
            # umap job from old mission
            if old_mission := old_job_record.mission():
                old_mission.remove_job(old_job_record)
                # clear both tl buckets in case of mission change
                old_mission.tls_bucket.clear()
                logger.debug("unmapping job#%d from mission#%d", job.id, old_mission.id)
            if new_mission := job.mission():
                new_mission.jobs_from_other_missions.add(job.id)
                new_mission.intermediate_tls_bucket = new_mission.tls_bucket
                new_mission.tls_bucket.clear()

    if mission := job.mission():
        mission.add_job(job)
    else:
        logger.debug("job#%d added to unmapped_jobs, mission not found", job.id)
        if job.mission_id:
            unmapped_jobs[job.mission_id][job.id] = job

    for waypoint in unmapped_waypoints.get(job.id, {}).values():
        logger.debug("waypoint#%d mapped to job #%d", waypoint.id, job.id)
        job.add_waypoint(waypoint)
    unmapped_waypoints.pop(job.id, None)

    JOBS[job.id] = job


@measure("process_waypoint")
def process_waypoint(waypoint: Waypoint, annotator: Annotator, datetime_upper_limit: datetime64) -> None:
    """function to fill processes waypoints and calls for
    annotating tl in case there is waypoint state change"""
    logger.debug(waypoint)
    if waypoint.created_at > datetime_upper_limit:
        logger.debug("waypoint#%d will not be processed, created at %s", waypoint.id, str(waypoint.created_at))
        return
    # check if waypoint are out of order
    if mission := waypoint.mission():
        out_of_order_waypoints = False
        if len(mission.waypoints_processing_order) > 1:
            if (
                waypoint.state != "pending"
                and mission.waypoints_processing_order[-1] != waypoint.id
                and waypoint.id in mission.waypoints_processing_order
            ):
                # order not istablished
                if mission.tls_bucket:
                    logger.debug(
                        "waypoints out of order for mission#%d, clearing bucket of size %d",
                        mission.id,
                        len(mission.tls_bucket),
                    )
                out_of_order_waypoints = True
                mission.tls_bucket.clear()
        if waypoint.state != "pending" and not out_of_order_waypoints:
            add_if_not_on_top(mission.waypoints_processing_order, waypoint.id)

    if job := waypoint.job():
        if old_waypoint := job.waypoints.get(waypoint.id):
            if not old_waypoint.state == waypoint.state:
                if mission := waypoint.mission():
                    if waypoint.job_id in mission.jobs_from_other_missions:
                        mission.intermediate_tls_bucket.clear()
                        mission.jobs_from_other_missions.discard(waypoint.job_id)
                    else:
                        mission.tls_bucket.extend(mission.intermediate_tls_bucket)
                        mission.intermediate_tls_bucket.clear()
                        if job_id := waypoint.job_id:
                            mission.jobs_from_other_missions.discard(job_id)
                annotator.annotate(old_waypoint=old_waypoint, new_waypoint=waypoint)
        job.add_waypoint(waypoint)
    else:
        logger.debug("waypoint added to unmapped_waypoints, job not found: %s", waypoint)
        if waypoint.job_id:
            unmapped_waypoints[waypoint.job_id][waypoint.id] = waypoint


@measure("process_tl")
def process_tl(tl: TrackingLocation) -> None:
    """function to add TLs to mission bucket"""
    logger.debug(tl)
    # if courier is in misison, add tls to mission bucket
    if mission_id := courier_id_to_mission_id.get(tl.user_id):
        if mission := MISSIONS.get(mission_id):
            tl.mission_state = mission.state  # type: ignore
            mission.tls_bucket.append(tl)
            return

        logger.debug("courier's mission#%d not found", mission_id)
    # else
    logger.debug("courier %d not assigned to any missions", tl.user_id)


def run(data_provider: DataProvider, data_sink: Sink) -> None:
    """itrate over dataframe records partitioned by minute and process them"""

    @measure("app.sink.flush")
    def flush_sink():
        if data_sink.name != "memory_sink":
            data_sink.flush()

    annotator = Annotator(data_sink)
    with measure("app.run.for_loop"):
        for entry in get_data(data_provider=data_provider, on_batch_end=flush_sink):
            if hasattr(entry, "id") and not maybe_int(entry.id):
                continue

            if entry.record_type == "mission":
                process_mission(Mission(*entry), data_provider.end_date.astype("datetime64[h]"))
            elif entry.record_type == "waypoint":
                process_waypoint(
                    Waypoint(*entry),
                    annotator=annotator,
                    datetime_upper_limit=data_provider.end_date.astype("datetime64[h]"),
                )
            elif entry.record_type == "job":
                process_job(Job(*entry), data_provider.end_date.astype("datetime64[h]"))
            elif entry.record_type == "tl":
                process_tl(TrackingLocation(entry))
