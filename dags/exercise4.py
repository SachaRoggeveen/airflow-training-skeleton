import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_training.operators.postgres_to_gcs.py import PostgresToGoogleCloudStorageOperator


args = {"owner": "sacha_roggeveen",
        "schedule_interval": "@daily",
        "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise4",
    default_args=args,
    description="postgres_naar_google"
)

t_start = BashOperator(task_id="print_execution_date",
                  bash_command="date",
                  dag=dag)

pgs_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pgs_to_gcs",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="buckster",
    filename="daily_load_{{ ds }}",
    postgres_conn_id="GddPostGres",
    dag=dag)

t_end = DummyOperator(task_id="the_end",
                  dag=dag)

t_start >> pgs_to_gcs >> t_end