"""
Define BigQuery consumer class to read data
from BigQuery between two partition dates
"""
import time
from typing import Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery, bigquery_storage  # type: ignore

from tracking_location_annotation.common.log import get_logger
from tracking_location_annotation.data.data_provider import DataProvider

logger = get_logger(__name__)


def _run_query(
    client: bigquery.Client,
    bqstorageclient: bigquery_storage.BigQueryReadClient,
    query: str,  # pylint: disable=too-many-arguments
    start_date: np.datetime64,
    end_date: np.datetime64,
    end_datetime: np.datetime64,
) -> pd.DataFrame:
    logger.info("running query: %s", query)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", str(start_date)),
            bigquery.ScalarQueryParameter("end_date", "STRING", str(end_date)),
            bigquery.ScalarQueryParameter("start_timestamp", "INT64", pd.Timestamp(start_date).value // 1000_000),  # type: ignore
            bigquery.ScalarQueryParameter("end_timestamp", "INT64", pd.Timestamp(end_datetime).value // 1000_000),  # type: ignore
            bigquery.ScalarQueryParameter("start_datetime", "STRING", str(start_date.astype("datetime64[h]"))),
            bigquery.ScalarQueryParameter("end_datetime", "STRING", str(end_datetime)),
        ]
    )
    # timer
    timer = time.time()
    dataframe = (
        client.query(query, job_config=job_config)
        .result()
        .to_dataframe(
            bqstorage_client=bqstorageclient,
            progress_bar_type="tqdm",
        )
    )

    assert len(dataframe) > 0, "dataframe can't be empty"
    logger.info(
        "query finished running in %s seconds, and got %d records of size %d MB",
        round(time.time() - timer, 2),
        len(dataframe),
        dataframe.memory_usage(deep=True).sum() // 1000_000,
    )
    return dataframe


class BqConsumer(DataProvider):
    """
    Bigquery consumer to query data between two partition dates
    for the following tables:
    - quiqup.core.prod_ae_1_job_pickups
    - quiqup.core.prod_ae_1_missions
    - quiqup.core.prod_ae_1_job_pickups
    - quiqup.core.prod_ae_tracking_locations
    """

    def __init__(self, start_date: np.datetime64, batch_size_in_days: int):
        self.start_date = start_date
        self.end_date = start_date + np.timedelta64(batch_size_in_days + 1)
        self.end_datetime = self.end_date - np.timedelta64(21, "h")
        self.client = bigquery.Client(project="quiqup")
        self.bqstorageclient = bigquery_storage.BigQueryReadClient()

    def __str__(self) -> str:
        return f"BQ Consumer - date= {self.start_date}"

    def get_waypoints(self) -> pd.DataFrame:
        """
        sql query to get data from quiqup.core.prod_ae_1_job_pickups table
        """
        query = """
        SELECT
            id, job_id, courier_id, state,
            created_at, updated_at, updated_at as timestamp
        FROM `quiqup.core_2022.ae_job_pickups`
        WHERE updated_at between @start_date and @end_date
        """
        return _run_query(
            self.client,
            self.bqstorageclient,
            query,
            start_date=self.start_date,
            end_date=self.end_date,
            end_datetime=self.end_datetime,
        )

    def get_missions(self) -> pd.DataFrame:
        """
        sql query to get data from quiqup.core.prod_ae_1_missions table
        """
        query = """
        SELECT
            id, courier_id, state,
            created_at, updated_at, updated_at as timestamp
            FROM `quiqup.core_2022.ae_missions`
            WHERE updated_at between @start_date and @end_date
        """

        return _run_query(
            self.client,
            self.bqstorageclient,
            query,
            start_date=self.start_date,
            end_date=self.end_date,
            end_datetime=self.end_datetime,
        )

    def get_jobs(self) -> pd.DataFrame:
        """
        sql query to get data from quiqup.core.prod_ae_1_jobs table
        """
        query = """
        SELECT
            id, state, created_at,
            updated_at, mission_id, updated_at as timestamp
        FROM `quiqup.core_2022.ae_jobs`
        WHERE updated_at between @start_date and @end_date
        """

        return _run_query(
            self.client,
            self.bqstorageclient,
            query,
            start_date=self.start_date,
            end_date=self.end_date,
            end_datetime=self.end_datetime,
        )

    def get_tracking_locations(self) -> pd.DataFrame:
        """
        sql query to get data from quiqup.core.prod_ae_tracking_locations table
        """
        query = """
        SELECT
            location.user_id,
            location.recorded_at,
            location.is_moving,
            location.uuid,
            location.timestamp,
            location.odometer,
            -- location.battery,
            location.battery.level as battery_level,
            -- location.extras,
            -- location.coords,
            location.coords.altitude,
            location.coords.longitude,
            location.coords.altitude_accuracy as altitude_accuracy,
            location.coords.latitude,
            location.coords.speed,
            location.coords.heading,
            location.coords.accuracy as coords_accuracy,
            location.activity.type as activity_type,
            location.activity.confidence as activity_confidence,
            -- location.salesforce_user_id
        FROM `quiqup.core.prod_ae_tracking_locations`
        WHERE _PARTITIONDATE between @start_date and @end_date
        AND
        location.timestamp between @start_datetime and @end_datetime
        ORDER By location.timestamp
        """

        return _run_query(
            self.client,
            self.bqstorageclient,
            query,
            start_date=self.start_date,
            end_date=self.end_date,
            end_datetime=self.end_datetime,
        )

    def fetch_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """function that combines the BQ calls
        to return a tuple of dataframes"""
        logger.info("getting data from BQ")

        df_missions = self.get_missions()
        df_jobs = self.get_jobs()
        df_waypoints = self.get_waypoints()
        df_tl = self.get_tracking_locations()
        # df_tl = pd.DataFrame(columns=['recorded_at','timestamp'])

        return df_missions, df_waypoints, df_jobs, df_tl
