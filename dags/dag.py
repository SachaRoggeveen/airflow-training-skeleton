import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
    dag_id="demo",
    description="Execution interval test",
    start_date=datetime.datetime(2019,1,1),
    schedule_interval="@daily",
)

BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)
