"""
module to dfine the app database which are
dictionaries holding values for missions, jobs and waypoints records
"""
from collections import defaultdict
from typing import Dict

from tracking_location_annotation.models import Job, Mission, Waypoint

MISSIONS: Dict[int, Mission] = {}  # takes mission_id -> returns mission

JOBS: Dict[int, Job] = {}  # takes job_id -> returns job

# to cover receiving waypoints before jobs
unmapped_waypoints: Dict[int, Dict[int, Waypoint]] = defaultdict(lambda: {})
# to cover receiving jobs before missions
unmapped_jobs: Dict[int, Dict[int, Job]] = defaultdict(lambda: {})
# to see if courier is in shift
courier_id_to_mission_id: Dict[int, int] = {}  # takes mission id -> returns courier id
