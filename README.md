# TL-Annotation
This projects annotates tracking location records from bigquery (table `quiqup.core.prod_ae_tracking_locations`) given a certain start date and end date.

The [bigquery_waypoints_quality](https://gitlab.quiqup.com/internal/data/research/-/tree/master/bigquery_waypoints_quality) folder under the research [repo](https://gitlab.quiqup.com/internal/data/research) suggested using the following tables for annotation:
1. `quiqup.core.prod_ae_1_jobs `
2. `quiqup.core.prod_ae_1_missions`
3. `quiqup.core.prod_ae_1_job_pickups`

The results are recorded in a **CSV File** named `start_date.csv` *(where start_date is the start date provided by the user)* or saved in memory depending on which **sink** you are using.
# Install dependencies

If using conda:

```
conda env update --file conda.yaml
```

If using (mamba you should) and running on a mac with M1 chip;:

```
CONDA_SUBDIR=osx-64 mamba env update --file conda.yaml
```
If using pip:
```
pip install -r requirements.txt
```
# Run project

```
python -m tracking_location_annotation
```

# Prepare the data 

There are two ways you could run the project:
* consumer is a BigQuery consumer
* consumer reads from csv files

In order to generate the csv files for csv consumer you should run the following commands:
1. `export PYTHONPATH=.`
2. `python tracking_location_annotation/resources/script.py`

# Run using metaflow
```
python flow.py --package-suffixes .env --environment conda run --start_date '2020-05-01' --end_date '2022-8-11' --max-workers 3
```