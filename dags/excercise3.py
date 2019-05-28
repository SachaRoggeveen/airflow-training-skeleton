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


branching = BranchPythonOperator(
    task_id="branching",
    python_callable=_get_weekday,
    provide_context=True,
    dag=dag)

t1 = BashOperator(task_id="print_weekday",
                  bash_command="date +%A",
                  dag=dag)

weekday_person_to_email = {
    0: "Bob",    # Monday
    1: "Joe",    # Tuesday
    2: "Alice",  # Wednesday
    3: "Joe",    # Thursday
    4: "Alice",  # Friday
    5: "Alice",  # Saturday
    6: "Alice",  # Sunday
}

for person in weekday_person_to_email:
    branching >> DummyOperator(task_id=person, dag=dag)

email = DummyOperator(task_id="mail_ff",
                  dag=dag)

final = DummyOperator(task_id="the_end",
                  dag=dag)


t1 >> branching >> email >> final
