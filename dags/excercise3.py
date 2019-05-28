import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {"owner": "sacha_roggeveen", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
        dag_id="exercise3",
        default_args=args
)


def _get_weekday(execution_date, **context):
    return execution_date.strftime("%a")

t1 = BashOperator(task_id="print_weekday",
                  bash_command="date +%A",
                  dag=dag)

branching = BranchPythonOperator(
    task_id="branching",
    dag=dag)

t1 >> branching


for person in ["Bob", "Joe", "Alice"]:
    branching >> DummyOperator(task_id=person, dag=dag)

email = DummyOperator(task_id="mail_ff",
                        dag=dag)

final_task = BashOperator(task_id="final_task",
                          bash_command="sleep 5",
                          dag=dag)

branching >> email >> final_task
