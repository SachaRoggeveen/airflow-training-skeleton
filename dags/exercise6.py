import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_training.operators.postgres_to_gcs import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator


dag = DAG(dag_id="daggerd", default_args=args, description="http_naar_google")

t_start = BashOperator(task_id="print_execution_date", bash_command="date", dag=dag)


dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="dataproc_create_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-may2829-257c0046",
    num_workers=4,
    zone="europe-west4-a",
    dag=dag
)

compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="https://console.cloud.google.com/storage/browser/europe-west1-training-airfl-48bde282-bucket/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=[
        "land_registry_price_datafile_location",
        "currency_datafile_location",
        "output_datafile_location",
    ],
    dag=dag
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="dataproc_delete_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-may2829-257c0046",
    dag=dag
)


t_end = DummyOperator(task_id="the_end", dag=dag)

t_start >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster >> t_end