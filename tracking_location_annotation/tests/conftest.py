from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def reset_db():
    from tracking_location_annotation import app, db

    with mock.patch.dict(db.MISSIONS, clear=True):
        with mock.patch.dict(db.JOBS, clear=True):
            with mock.patch.dict(app.unmapped_jobs, clear=True):
                with mock.patch.dict(app.unmapped_waypoints, clear=True):
                    yield
