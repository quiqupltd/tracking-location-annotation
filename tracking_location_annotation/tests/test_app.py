from pathlib import Path
from typing import Text
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from tracking_location_annotation import annotator, app, db
from tracking_location_annotation.data.csv_consumer import CSVConsumer
from tracking_location_annotation.models import Job, Mission, TrackingLocation, Waypoint
from tracking_location_annotation.sink.memory_sink import MemorySink

folders = list(Path("tracking_location_annotation/tests/fixtures").glob("sample*"))
folders_str = map(str, folders)


@pytest.mark.parametrize("scenario_dir", folders_str)
def test_scenario(scenario_dir: Text):
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


class TestJobMissionChange:
    @pytest.fixture
    def annotator(self):
        return annotator.Annotator(MemorySink())

    def test_job_from_mission_to_unmapped(self, annotator):
        app.process_mission(
            Mission(
                id=1,
                state="pending",
                created_at=1000,
                updated_at="",
                timestamp=1000,
                courier_id=1,
                record_type="mission",
            ),
            2000,
        )
        app.process_job(
            Job(id=1, state="pending", created_at=1000, updated_at="", mission_id=1, timestamp=1001, record_type="job"),
            2000,
        )
        app.process_waypoint(
            Waypoint(
                id=1,
                courier_id=None,
                state="pending",
                created_at=1000,
                updated_at="",
                job_id=1,
                timestamp=1002,
                record_type="waypoint",
            ),
            annotator,
            2000,
        )
        app.process_tl(TrackingLocation(row=mock.Mock(user_id=1, uuid="foo1", timestamp=1003)))

        app.process_waypoint(
            Waypoint(
                id=1,
                courier_id=None,
                state="arrived",
                created_at=1000,
                updated_at="",
                job_id=1,
                timestamp=1004,
                record_type="waypoint",
            ),
            annotator,
            2000,
        )
        assert len(annotator.sink.tls) == 1

        assert len(app.unmapped_jobs) == 0
        app.process_job(
            Job(id=1, state="pending", created_at=1000, updated_at="", mission_id=2, timestamp=1005, record_type="job"),
            2000,
        )
        assert app.unmapped_jobs[2][1].timestamp == 1005
        assert db.JOBS[1].mission_id == 2

    def test_job_from_mission_to_mission(self, annotator):
        app.process_mission(
            Mission(
                id=2,
                state="pending",
                created_at=1000,
                updated_at="",
                timestamp=999,
                courier_id=1,
                record_type="mission",
            ),
            2000,
        )
        app.process_mission(
            Mission(
                id=1,
                state="pending",
                created_at=1000,
                updated_at="",
                timestamp=1000,
                courier_id=1,
                record_type="mission",
            ),
            2000,
        )
        app.process_job(
            Job(id=1, state="pending", created_at=1000, updated_at="", mission_id=1, timestamp=1001, record_type="job"),
            2000,
        )
        app.process_waypoint(
            Waypoint(
                id=1,
                courier_id=None,
                state="pending",
                created_at=1000,
                updated_at="",
                job_id=1,
                timestamp=1002,
                record_type="waypoint",
            ),
            annotator,
            2000,
        )
        app.process_tl(TrackingLocation(row=mock.Mock(user_id=1, uuid="foo1", timestamp=1003)))

        app.process_waypoint(
            Waypoint(
                id=1,
                courier_id=None,
                state="arrived",
                created_at=1000,
                updated_at="",
                job_id=1,
                timestamp=1004,
                record_type="waypoint",
            ),
            annotator,
            2000,
        )
        assert len(annotator.sink.tls) == 1

        assert len(app.unmapped_jobs) == 0
        app.process_job(
            Job(id=1, state="pending", created_at=1000, updated_at="", mission_id=2, timestamp=1005, record_type="job"),
            2000,
        )
        assert len(app.unmapped_jobs) == 0
        assert db.JOBS[1].mission_id == 2

    def test_job_from_unmapped_to_mission(self):
        job = Job(
            id=1, state="pending", created_at=1000, updated_at="", mission_id=10, timestamp=1001, record_type="job"
        )
        app.unmapped_jobs[10] = {1: job}

        app.process_mission(
            Mission(
                id=10,
                state="pending",
                created_at=1000,
                updated_at="",
                timestamp=1000,
                courier_id=1,
                record_type="mission",
            ),
            2000,
        )

        assert len(app.unmapped_jobs) == 0
        assert db.MISSIONS[10].jobs[1] == job
