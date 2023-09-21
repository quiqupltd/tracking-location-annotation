from tracking_location_annotation.models import Mission, Waypoint


class TestWaypoint:
    def test_waypoint_equal(self):
        w1 = Waypoint(
            id=1,
            state="pending",
            job_id=1,
            courier_id=None,
            created_at="",
            updated_at="",
            timestamp="",
            record_type="waypoint",
        )

        w2 = Waypoint(
            id=1,
            state="arrived",
            job_id=2,
            courier_id=None,
            created_at="",
            updated_at="",
            timestamp="",
            record_type="waypoint",
        )

        assert w1 == w2

        m1 = Mission(
            id=1, state="pending", created_at="", updated_at="", timestamp="", courier_id=1, record_type="mission"
        )

        assert w1 != m1

    def test_mission(self):
        w = Waypoint(
            id=1,
            state="pending",
            job_id=1,
            courier_id=None,
            created_at="",
            updated_at="",
            timestamp="",
            record_type="waypoint",
        )

        assert w.mission() is None

        w = Waypoint(
            id=1,
            state="pending",
            job_id=None,
            courier_id=None,
            created_at="",
            updated_at="",
            timestamp="",
            record_type="waypoint",
        )

        assert w.mission() is None
