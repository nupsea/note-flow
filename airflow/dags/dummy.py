import imp
import logging
import pendulum
from airflow.operators.dummy import DummyOperator
import papermill as pm
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

log = logging.getLogger(__name__)

with DAG(
    dag_id='dummy_note_dag',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 6, 5, tz="UTC"),
    catchup=False,
    tags=['notebook'],
) as dag:

    @task(task_id="init")
    def init_notebook(**kwargs):
        print("Initializing notebook")

    init = init_notebook()

    dummy = DummyOperator(
        task_id="dummy"
    )

    @task(task_id="run")
    def run_notebook(**kwargs):
        my_file = "dummy"
        inp_note_path = f"/opt/airflow/dags/notebooks/{my_file}.ipynb"
        out_note_path = f"/opt/airflow/logs/airflow_runs/{my_file}_run_{int(datetime.now().timestamp())}.ipynb"
        print("Running jupyter notebook")
        pm.execute_notebook(inp_note_path, out_note_path)

    note_run = run_notebook()

    init >> dummy >> note_run


