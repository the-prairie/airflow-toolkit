from airflow import DAG

from datetime import datetime, timedelta

from airflow.contrib.operators.bigquery_operator import BigQueryGetDatasetOperator
from airflow.operators.dummy_operator import DummyOperator


from operators.degreed_to_gcs import DegreedToCloudStorageOperator

DEGREED_CONN_ID = 'degreed_conn_id'
DEGREED_SCHEMA = ''
BIGQUERY_SCHEMA = 'degreed'
BIGQUERY_CONN_ID = 'my_gcp_connection'
GCS_CONN_ID = 'my_gcp_connection'
GCS_BUCKET = 'degreed_data/logins'

user_schema = {"name": "users",
             "type": "record",
             "fields": [{"name": "active",
                         "type": ["null", "boolean"],
                         "default": "null"},
                        {"name": "created_at",
                         "type": ["null", "string"],
                         "default": "null"},
                        {"name": "description",
                         "type": ["null", "string"],
                         "default": "null"},
                        {"name": "name",
                         "type": ["null", "string"],
                         "default": "null"},
                        {"name": "program_id",
                         "type": ["null", "int"],
                         "default": "null"},
                        {"name": "program_name",
                         "type": ["null", "string"],
                         "default": "null"},
                        {"name": "type",
                         "type": ["null", "string"],
                         "default": "null"},
                        {"name": "updated_at",
                         "type": ["null", "string"],
                         "default": "null"},
                        {"name": "workspace_name",
                         "type": ["null", "string"],
                         "default": "null"}
                        ]
             }

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

daily_id = '{}_to_bigqury_daily_backfill'.format(DEGREED_CONN_ID)

def create_dag(dag_id,
            schedule,
            degreed_conn_id,
            bigquery_conn_id,
            bigquery_schema,
            gcs_conn_id,
            gcs_bucket,
            default_args,
            catchup=False):

    dag = DAG(dag_id,
            schedule_interval=schedule,
            default_args=default_args,
            catchup=catchup)

    if 'backfill' in dag_id:
        endpoints = ['logins']
    else:
        endpoints = ['users',
                    'logins']



    # TODO(developer): update for your specific settings
    TASK_PARAMS_DICT = {
        "dataset_id": "degreed",
        "project_id": "its-my-data-pipeline",
        "gcp_conn_id": "my_gcp_connection",
    }

    with dag:
        d = DummyOperator(task_id='kick_off_dag')


        for endpoint in endpoints:

            DEGREED_SCHEMA = user_schema
            TABLE_NAME = 'degreed_{0}'.format(endpoint)

            GCS_KEY = 'degreed/{0}/{1}_{2}.json'.format(bigquery_schema,
                                                       endpoint,
                                                       "{{ ts_nodash }}")

            DEGREED_TASK_ID = 'get_{0}_degreed_data'.format(endpoint)
            BIGQUERY_TASK_ID = 'degreed_{0}_to_bigquery'.format(endpoint)
            START_AT = "{{ execution_date.isoformat() }}"
            END_AT = "{{ next_execution_date.isoformat() }}"




        dg = DegreedToCloudStorageOperator(task_id=DEGREED_TASK_ID,
                                                degreed_conn_id=degreed_conn_id,
                                                endpoint=endpoint,
                                                gcs_conn_id=gcs_conn_id,
                                                gcs_bucket=gcs_bucket,
                                                gcs_key=GCS_KEY,
                                                output_format='json',
                                                start_at=START_AT,
                                                end_at=END_AT)
        
        d >> dg

    return dag


globals()[daily_id] = create_dag(daily_id,
                                 '@daily',
                                 DEGREED_CONN_ID,
                                 BIGQUERY_CONN_ID,
                                 BIGQUERY_SCHEMA,
                                 GCS_CONN_ID,
                                 GCS_BUCKET,
                                 {'start_date': datetime(2021, 1, 1),
                                  'end_date': datetime(2021, 1, 5),
                                  'retries': 2,
                                  'retry_delay': timedelta(minutes=5),
                                  'email': [],
                                  'email_on_failure': True},
                                 catchup=True)