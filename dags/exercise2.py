from airflow.operators.bash_operator import Bashoperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator PythonOperator

args = {"owner": "sacha_roggeveen", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
        dag_id="exercise2",
        default_args=args
)