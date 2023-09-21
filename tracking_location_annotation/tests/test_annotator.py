import numpy as np
import pandas as pd
import pytest
from testfixtures import ShouldRaise, log_capture

from tracking_location_annotation import app
from tracking_location_annotation.annotator import Annotator
from tracking_location_annotation.data.csv_consumer import CSVConsumer
from tracking_location_annotation.models import Waypoint
from tracking_location_annotation.sink.memory_sink import MemorySink


@log_capture()
def test_mission_done_waypoint_arrived(log_capture):

    scenario_dir = "tracking_location_annotation/tests/business_scenarios/mission_done_waypoint_arrived"
    start_date = np.datetime64("2022-02-02")
    consumer = CSVConsumer(start_date=start_date, batch_size_in_days=1, data_path=scenario_dir)
    sink = MemorySink().connect()
    result = pd.read_csv((f"{scenario_dir}/results.csv"))
    app.run(data_provider=consumer, data_sink=sink)

    assert result.uuid.to_list() == [tl.uuid for tl in sink.tls]

    df = sink.get_dataframe()
    df = df.sort_values("uuid").reset_index(drop=True)
    result = result.sort_values("uuid").reset_index(drop=True)

    assert df.equals(result)
    sink.flush()

    log_capture.check_present(
        ("tracking_location_annotation.annotator", "DEBUG", "Tracking locations bucket cleared for mission#4378366")
    )


@log_capture()
def test_mission_not_available_for_annotation(log_capture):
    scenario_dir = "tracking_location_annotation/tests/business_scenarios/no_mission"
    start_date = np.datetime64("2022-02-02")
    consumer = CSVConsumer(start_date=start_date, batch_size_in_days=1, data_path=scenario_dir)
    sink = MemorySink().connect()
    result = pd.read_csv((f"{scenario_dir}/results.csv"))
    app.run(data_provider=consumer, data_sink=sink)

    assert result.uuid.to_list() == [tl.uuid for tl in sink.tls]

    log_capture.check_present(
        (
            "tracking_location_annotation.annotator",
            "WARNING",
            "annotation skipped for waypoint #13215730, mission not found, (job_id = 6332297)",
        )
    )


@log_capture()
@pytest.mark.parametrize("state", ["pending", "arrived", "finished"])
def test_annotating_same_state_waypoints(log_capture, state):
    annotator = Annotator(MemorySink())

    waypoint_1 = Waypoint(
        id=1,
        courier_id=None,
        state=state,
        created_at="",
        updated_at="",
        job_id=1,
        timestamp=1002,
        record_type="waypoint",
    )
    waypoint_2 = Waypoint(
        id=1,
        courier_id=None,
        state=state,
        created_at="",
        updated_at="",
        job_id=1,
        timestamp=1003,
        record_type="waypoint",
    )
    annotator.annotate(new_waypoint=waypoint_1, old_waypoint=waypoint_2)
    log_capture.check_present(
        (
            "tracking_location_annotation.annotator",
            "WARNING",
            f"new and old have same state ({state}), waypoint_id = #1",
        )
    )


@log_capture()
def test_annotator_args():
    annotator = Annotator(MemorySink())
    waypoint_1 = Waypoint(
        id=1,
        courier_id=None,
        state="pending",
        created_at="",
        updated_at="",
        job_id=1,
        timestamp=1002,
        record_type="waypoint",
    )
    waypoint_2 = Waypoint(
        id=2,
        courier_id=None,
        state="arrived",
        created_at="",
        updated_at="",
        job_id=1,
        timestamp=1003,
        record_type="waypoint",
    )
    with ShouldRaise(ValueError("waypoints must have same id (got #1 and #2)")):
        annotator.annotate(new_waypoint=waypoint_1, old_waypoint=waypoint_2)

    with ShouldRaise(ValueError("both old and new waypoints should be passed for annotation")):
        annotator.annotate(new_waypoint=waypoint_1, old_waypoint=None)
