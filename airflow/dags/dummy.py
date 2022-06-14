import logging
from operator import ge
import pendulum
import subprocess
import os
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
        log.info("Initializing notebook")

    init = init_notebook()

    dummy = DummyOperator(
        task_id="dummy"
    )

    @task(task_id="run")
    def run_notebook(**kwargs):
        my_file = "dummy"
        inp_note_path = f"/opt/airflow/dags/notebooks/{my_file}.ipynb"
        out_note_path = f"/opt/airflow/logs/airflow_runs/{my_file}_run_{int(datetime.now().timestamp())}.ipynb"
        log.info("Running jupyter notebook")
        pm.execute_notebook(inp_note_path, out_note_path)
        return out_note_path.split(".")[0]

    @task
    def display_run(notebook_file):
        """
        A function that converts a notebook into an ascii doc file.
        """
        generate = subprocess.run(
            ["jupyter", "nbconvert", f"{notebook_file}.ipynb", "--to=asciidoc"]
        )
        log.info("HTML Report was generated")
        log.info(f" {generate}\n Notebook Run Output: ")
        adoc_file = f"{notebook_file}.asciidoc"
        with open(adoc_file, 'r') as af:
            log.info(af.read())

        return [adoc_file]

    @task
    def clean_up(files: list):
        for f in files:
            if os.path.exists(f):
                os.remove(f)
                log.info(f"Removed file : {f}")
            else:
                log.error(f"File {f} does not exist.")
        
        return True


    note_run = run_notebook()

    init >> dummy >> clean_up(
        display_run(
            note_run
        )
    )


