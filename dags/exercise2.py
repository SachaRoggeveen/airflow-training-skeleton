from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {"owner": "sacha_roggeveen", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
        dag_id="exercise2",
        default_args=args
)

t1 = BashOperator(task_id="print_execution_date",
                  bash_command="date",
                  dag=dag)

t2 = BashOperator(task_id="wait_5",
                  bash_command="sleep 5s",
                  dag=dag)

t3 = BashOperator(task_id="wait_1",
                  bash_command="sleep 1s",
                  dag=dag)


t4 = BashOperator(task_id="wait_10",
                  bash_command="sleep 10s",
                  dag=dag)

t5 = BashOperator(task_id="the_end",
                  bash_command="echo 1",
                  dag=dag)

t1 >> [t2,t3,t4] >> t5