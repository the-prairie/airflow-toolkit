from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryGetDatasetOperator

from operators import degreed_to_cloud_storage_operator

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "owner": "airflow",
    "retries": 0,
    "start_date": "2020-01-16 00:00:00",
}

# TODO(developer): update for your specific settings
TASK_PARAMS_DICT = {
    "dataset_id": "degreed",
    "project_id": "its-my-data-pipeline",
    "gcp_conn_id": "my_gcp_connection",
}

with DAG(
    "degreed_daily_logins", default_args=default_args, schedule_interval="@once"
) as dag:
    degreed_daily_logins = BigQueryGetDatasetOperator(
        task_id="degreed-logins",
        dataset_id=TASK_PARAMS_DICT.get("dataset_id"),
        project_id=TASK_PARAMS_DICT.get("project_id"),
        gcp_conn_id=TASK_PARAMS_DICT.get("gcp_conn_id"),
    )

    degreed_daily_logins
