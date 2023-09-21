"""
module to generate csv files for local development
"""
import numpy as np

from tracking_location_annotation.data.bigquery_consumer import BqConsumer

start_date = np.datetime64("2022-02-01")
batch_size_in_days = 1

consumer = BqConsumer(start_date=start_date, batch_size_in_days=batch_size_in_days)

consumer.get_missions().to_csv("missions_data.csv", index=False)
consumer.get_jobs().to_csv("jobs_data.csv", index=False)
consumer.get_waypoints().to_csv("waypoints_data.csv", index=False)
consumer.get_tracking_locations().to_csv("tl_data.csv", index=False)
