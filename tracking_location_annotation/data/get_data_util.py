"""
Module to read data from BQ or CSV File based on env
clean data and parse it into multiple bartches for processing
"""
from datetime import datetime, timedelta
from typing import Callable, List, Optional, Tuple

import pandas as pd

from tracking_location_annotation.common.benchmark import measure
from tracking_location_annotation.common.log import get_logger
from tracking_location_annotation.data.data_provider import DataProvider
from tracking_location_annotation.db import JOBS, MISSIONS, courier_id_to_mission_id

logger = get_logger(__name__)


@measure("data.clean_data")
def clean_data(data_provider: DataProvider) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    clean data by removing unwanted columns, rows
    and by setting time related columns type to np.datetime64
    """

    df_missions, df_waypoints, df_jobs, df_tl = data_provider.fetch_data()
    start_date = data_provider.start_date
    end_date = data_provider.end_date
    # changing datetime types

    # for missions
    df_missions["timestamp"] = pd.to_datetime(df_missions["timestamp"]).dt.tz_localize(None)
    df_missions["created_at"] = pd.to_datetime(df_missions["created_at"]).dt.tz_localize(None)
    df_missions["updated_at"] = pd.to_datetime(df_missions["updated_at"]).dt.tz_localize(None)

    # for waypoints
    df_waypoints["timestamp"] = pd.to_datetime(df_waypoints["timestamp"]).dt.tz_localize(None)
    df_waypoints["created_at"] = pd.to_datetime(df_waypoints["created_at"]).dt.tz_localize(None)
    df_waypoints["updated_at"] = pd.to_datetime(df_waypoints["updated_at"]).dt.tz_localize(None)

    # for jobs
    df_jobs["timestamp"] = pd.to_datetime(df_jobs["timestamp"]).dt.tz_localize(None)
    df_jobs["created_at"] = pd.to_datetime(df_jobs["created_at"]).dt.tz_localize(None)
    df_jobs["updated_at"] = pd.to_datetime(df_jobs["updated_at"]).dt.tz_localize(None)

    # for tracking location
    df_tl["recorded_at"] = pd.to_datetime(df_tl["recorded_at"]).dt.tz_localize(None)
    df_tl["timestamp"] = pd.to_datetime(df_tl["timestamp"]).dt.tz_localize(None)

    # filtering out unwanted data

    # for missions
    df_missions = df_missions[df_missions["timestamp"].between(start_date, end_date)]
    df_missions = df_missions.drop(df_missions.filter(regex="Unname"), axis=1)

    # for waypoints
    df_waypoints = df_waypoints[df_waypoints["timestamp"].between(start_date, end_date)]
    df_waypoints = df_waypoints.drop(df_waypoints.filter(regex="Unname"), axis=1)

    # for jobs
    df_jobs = df_jobs[df_jobs["timestamp"].between(start_date, end_date)]
    df_jobs = df_jobs.drop(df_jobs.filter(regex="Unname"), axis=1)

    # for tracking location
    df_tl = df_tl[df_tl["timestamp"].between(start_date, end_date)]
    df_tl = df_tl.drop(df_tl.filter(regex="Unname"), axis=1)

    # adding type column
    df_missions = df_missions.assign(record_type="mission")
    df_waypoints = df_waypoints.assign(record_type="waypoint")
    df_jobs = df_jobs.assign(record_type="job")
    df_tl = df_tl.assign(record_type="tl")

    return df_missions, df_waypoints, df_jobs, df_tl


def filter_df(dataframe, *, start: datetime, end: datetime) -> pd.DataFrame:
    "filter dataframe to get data between two timestamps only"
    return dataframe[dataframe.timestamp.between(start, end, inclusive="left")]


@measure("data.get_step_data")
def get_step_data(
    *,
    prev_step: datetime,
    step: datetime,
    df_missions: pd.DataFrame,
    df_waypoints: pd.DataFrame,
    df_jobs: pd.DataFrame,
    df_tl: pd.DataFrame,
) -> List:
    """use filter_df to get data between two timestamps from
    all dataframes, transform it to tuples and sort it by timestamp
    """
    logger.info("getting data between %s and %s", prev_step.strftime("%m/%d/%Y %H"), step.strftime("%m/%d/%Y %H"))
    # get data within step
    df_missions = filter_df(df_missions, start=prev_step, end=step)
    df_waypoints = filter_df(df_waypoints, start=prev_step, end=step)
    df_jobs = filter_df(df_jobs, start=prev_step, end=step)
    df_tl = filter_df(df_tl, start=prev_step, end=step)

    # iterating over data
    missions = list(df_missions.itertuples(index=False))
    waypoints = list(df_waypoints.itertuples(index=False))
    jobs = list(df_jobs.itertuples(index=False))
    tracking_location = list(df_tl.itertuples(index=False))

    # sorting by timestamp
    return sorted(
        [*missions, *waypoints, *jobs, *tracking_location], key=lambda element: element.timestamp
    )  # type: ignore


def get_data(data_provider: DataProvider, on_batch_end: Optional[Callable] = None):
    """
    read data from csv and pass it to cleaning function
    """
    df_missions, df_waypoints, df_jobs, df_tl = clean_data(data_provider)

    # get minimum timestamp in all the dataframes
    min_ts = min(
        df_missions.timestamp.min(), df_waypoints.timestamp.min(), df_jobs.timestamp.min(), df_tl.timestamp.min()
    )
    # get maximum timestamp in all the dataframes
    max_ts = max(
        df_missions.timestamp.max(), df_waypoints.timestamp.max(), df_jobs.timestamp.min(), df_tl.timestamp.max()
    )
    # create a list ranging between the max and min timestamps
    # with 1 minute step
    steps = pd.date_range(min_ts, max_ts, freq="24h")
    steps = steps.union([steps[-1] + steps.freq * 1])  # type: ignore
    for prev_step, step in zip(steps[:-1], steps[1:]):
        all_entries = get_step_data(
            prev_step=prev_step,
            step=step,
            df_missions=df_missions,
            df_jobs=df_jobs,
            df_waypoints=df_waypoints,
            df_tl=df_tl,
        )
        # clear data
        # clear jobs and missions from db
        for k in list(MISSIONS.keys()):
            if MISSIONS[k].timestamp < prev_step - timedelta(hours=3):
                MISSIONS[k].tls_bucket.clear()
                if mission := MISSIONS.pop(k, None):
                    courier_id_to_mission_id.pop(mission.id, None)

        for k in list(JOBS.keys()):
            if JOBS[k].timestamp < prev_step - timedelta(hours=3):
                JOBS.pop(k, None)

        def clear_df(df: pd.DataFrame, timestamp):
            old_rows = df.timestamp < timestamp
            df.drop(df[old_rows].index, axis=0, inplace=True)

        # clear used data from dataframes
        # df_missions = df_missions[df_missions.timestamp > prev_step]
        # df_jobs = df_jobs[df_jobs.timestamp > prev_step]
        # df_waypoints = df_waypoints[df_waypoints.timestamp > prev_step]
        # df_tl = df_tl[df_tl.timestamp > prev_step]
        clear_df(df_missions, prev_step)
        clear_df(df_jobs, prev_step)
        clear_df(df_waypoints, prev_step)
        clear_df(df_tl, prev_step)

        for entry in all_entries:
            yield entry

        if on_batch_end:
            on_batch_end()
