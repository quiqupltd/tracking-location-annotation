"""
main module that runs the program
"""
import numpy as np

from tracking_location_annotation import app
from tracking_location_annotation.common import benchmark
from tracking_location_annotation.common.log import get_logger

# from tracking_location_annotation.data.bigquery_consumer import BqConsumer
from tracking_location_annotation.data.csv_consumer import CSVConsumer
from tracking_location_annotation.sink.csv_sink import CSVSink

logger = get_logger(__name__)

if __name__ == "__main__":
    start_date = np.datetime64("2022-02-01")
    batch_size_in_days = 25
    logger.info(
        "running main function with start date: %s and batch size= %d day(s)", str(start_date), batch_size_in_days
    )
    # consumer = BqConsumer(start_date=start_date, batch_size_in_days=batch_size_in_days)
    consumer = CSVConsumer(
        start_date=start_date, batch_size_in_days=batch_size_in_days, data_path="tracking_location_annotation/resources"
    )
    sink = CSVSink(str(start_date) + ".csv").connect()

    with benchmark.memory_usage():
        app.run(data_provider=consumer, data_sink=sink)

    sink.close()

    benchmark.print_stats()
