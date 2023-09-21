import os
from metaflow import FlowSpec, step, Parameter, retry, kubernetes, conda, current, environment

class TLAnnotation(FlowSpec):

    batch_size_in_days = Parameter('batch_size_in_days', default=20, type=int)
    start_date = Parameter('start_date', required=True) # 2021-01-01
    end_date = Parameter('end_date', required=True) # 2022-05-01

    @step
    def start(self):
        print(f'starting flow with start_date={self.start_date}, end_date={self.end_date}, and batch_size={self.batch_size_in_days} day(s)')
        self.next(self.split_data)

    @conda(libraries={
        'numpy': '1.21.0'
    })
    @step
    def split_data(self):
        import numpy as np
        self.batch = np.arange(start=np.datetime64(self.start_date),
                               stop=np.datetime64(self.end_date),
                               dtype='datetime64[D]',
                               step=self.batch_size_in_days)
        self.next(self.run_batch, foreach='batch')

    @kubernetes(memory=28_000, cpu=4, secrets='metaflow')
    @retry(times=3)
    @conda(libraries={
        'pandas': '1.4.1',
        'google-cloud-bigquery': '2.34.0',
        'google-cloud-bigquery-storage': '2.11.0',
        'tqdm': '4.64.0',
        'python-dotenv': '0.19.2',
        'pandas-stubs' : '1.2.0.57',
        'google-cloud-storage': '2.1.0',
        'numpy': '1.21.0 '
    })
    @step
    def run_batch(self):
        import numpy as np

        from tracking_location_annotation.app import run
        from tracking_location_annotation.sink.csv_sink import CSVSink
        from tracking_location_annotation.google_cloud_storage import upload_filename
        from tracking_location_annotation.data.bigquery_consumer import BqConsumer
        
        self.batch_start_date = self.input
        run_batch_size_in_days = self.batch_size_in_days
        if (self.batch_start_date + np.timedelta64(self.batch_size_in_days)) > np.datetime64(self.end_date):
            run_batch_size_in_days = int ((np.datetime64(self.end_date) - self.batch_start_date ) / np.timedelta64(1, 'D')) + 1

        print(f'running task with start_date={self.batch_start_date} and batch_size={run_batch_size_in_days} day(s)')
        
        # initilizing sink and consumer
        bq_consumer = BqConsumer(start_date=self.batch_start_date, batch_size_in_days=run_batch_size_in_days)
        sink = CSVSink(filename=f'{self.batch_start_date}.csv').connect()
        
        # running algorithm
        run(bq_consumer, sink)
        
        # uploading result to google cloud storage
        upload_filename(local_filename=sink.filename, remote_dir=f'{current.run_id}')

        self.next(self.join)
    
    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        print('done')

if __name__ == '__main__':
    TLAnnotation()
