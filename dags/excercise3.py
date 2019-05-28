import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

args = {"owner": "sacha_roggeveen", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise3",
    default_args=args,
    description="Demo DAG showing BranchPythonOperator.",
    schedule_interval="0 0 * * *",
)


def _get_weekday(execution_date, **context):
    return execution_date.strftime("%a")

print_weekday = PythonOperator(
    task_id="print_weekday",
    python_callable=print_weekday,
    provide_context=True,
    dag=dag,
)

# Definition of who is emailed on which weekday
weekday_person_to_email = {
    0: "Bob",
    1: "Joe",
    2: "Alice",
    3: "Joe",
    4: "Alice",
    5: "Alice",
    6: "Alice"
}

# Function returning name of task to execute
def _get_person_to_email(execution_date, **context):
    person = weekday_person_to_email[execution_date.weekday()]
    return f"email_{person.lower()}"

# Branching task, the function above is passed to python_callable
branching = BranchPythonOperator(
    task_id="branching",
    python_callable=_get_person_to_email,
    provide_context=True,
    dag=dag)

# Execute branching task after create_report task
print_weekday >> branching

final_task = BashOperator(
    task_id="final_task",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    bash_command="sleep 5",
    dag=dag)

# Create dummy tasks for names in the dict, and execute all after the branching task
for name in set(weekday_person_to_email.values()):
    email_task = DummyOperator(task_id=f"email_{name.lower()}", dag=dag)
    branching >> email_task >> final_task