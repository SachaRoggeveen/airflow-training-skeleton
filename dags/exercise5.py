import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



#
# class MyOwnHook(BaseHook):
#     def __init__(self, conn_id):
#         super().__init__(source=None)
#         self._conn_id = conn_id
#         self._conn = None
#
#     def get_conn(self):
#         if self._conn is None:
#             self._conn = object()  # create connection instance here
#             return self._conn
#
#     def do_stuff(self, arg1, arg2, **kwargs):
#         session = self.get_conn()
#         session.do_stuff()
#



class MyOwnOperator(BaseOperator):

    template_fields = ('endpoint', 'data',)
    template_ext = ()
    ui_color = '#00000'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 method='POST',
                 data=None,
                 headers=None,
                 response_check=None,
                 extra_options=None,
                 xcom_push=False,
                 http_conn_id='http_default',
                 log_response=False,
                 *args, **kwargs):
        super(MyOwnOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.response_check = response_check
        self.extra_options = extra_options or {}
        self.xcom_push_flag = xcom_push
        self.log_response = log_response

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        if self.xcom_push_flag:
            return response.text


args = {
    "owner": "sacha_roggeveen",
    "schedule_interval": "@daily",
    "start_date": airflow.utils.dates.days_ago(14),
}

dag = DAG(dag_id="exercise5", default_args=args, description="http_naar_google")

t_start = BashOperator(task_id="print_execution_date", bash_command="date", dag=dag)


http_to_gcs = MyOwnOperator(
    task_id="http2gcs",
    endpoint="https://europe-west2-gdd-airflow-training.cloudfunctions.net/airflow-training-transform-valutas?date=2018-01-01&to=EUR",
    dag=dag
)


# gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
#     task_id="…",
#     bucket="buckster",
#     source_objects=["average_prices/transfer_date={{ ds }}/*"],
#     destination_project_dataset_table="your_project:prices.land_registry_price${{ ds_nodash }}",
#     source_format="PARQUET",
#     write_disposition="WRITE_TRUNCATE",
#     dag=dag,
# )


t_end = DummyOperator(task_id="the_end", dag=dag)

t_start >> http_to_gcs >> t_end
