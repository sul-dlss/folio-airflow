"""Imports exported MARC records from Symphony into FOLIO"""

from datetime import datetime, timedelta
import logging
import pathlib

import shutil
from textwrap import dedent
from typing_extensions import TypeAlias  # noqa

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


from folio_post import (
    folio_login,
    post_folio_instance_records,
    post_folio_holding_records,
    preprocess_marc,
    run_bibs_transformer,
    run_holdings_tranformer,
    process_records,
)

logger = logging.getLogger(__name__)


def move_marc_files(*args, **kwargs) -> list:
    """Function moves MARC files to instances and holdings"""
    airflow = "/opt/airflow/"
    marc_files = []
    for path in pathlib.Path(f"{airflow}symphony/").glob("*.*rc"):
        target = pathlib.Path(f"{airflow}migration/data/instances/{path.name}")
        shutil.move(path, target)
        logger.info(f"Moved MARC file to {target}")
        marc_files.append(path.name)
    return marc_files


parallel_posts = Variable.get("parallel_posts", 3)

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "symphony_marc_import",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2022, 1, 3),
    catchup=False,
    tags=["bib_import"],
) as dag:

    dag.doc_md = dedent(
        """
    # Import Symphony MARC Records to FOLIO
    Workflow for monitoring a file mount of exported MARC21 records from
    Symphony ILS into [FOLIO](https://www.folio.org/) LSM.
    """
    )

    monitor_file_mount = FileSensor(
        task_id="marc21_monitor",
        fs_conn_id="bib_path",
        filepath="/opt/airflow/symphony/*.*rc",
        timeout=270,  # 4 1/2 minutes
    )

    monitor_file_mount.doc_md = dedent(
        """\
        ####  Monitor File Mount
        Monitor's `/s/SUL/Dataload/Folio` for new MARC21 export files"""
    )

    preprocess_marc_files = PythonOperator(
        task_id="preprocess_marc", python_callable=preprocess_marc
    )

    move_marc = PythonOperator(
        task_id="move_marc_files", python_callable=move_marc_files
    )

    with TaskGroup(group_id="marc21-to-folio") as marc_to_folio:

        convert_marc_to_folio_instances = PythonOperator(
            task_id="convert_marc_to_folio_instances",
            python_callable=run_bibs_transformer,
            execution_timeout=timedelta(minutes=10),
        )

        convert_marc_to_folio_holdings = PythonOperator(
            task_id="convert_marc_to_folio_holdings",
            python_callable=run_holdings_tranformer,
        )

        convert_instances_valid_json = PythonOperator(
            task_id="instances_to_valid_json",
            python_callable=process_records,
            op_kwargs={
                "pattern": "folio_instances_*.json",
                "out_filename": "instances",
                "jobs": int(parallel_posts),
            },
        )

        convert_holdings_valid_json = PythonOperator(
            task_id="holdings_to_valid_json",
            python_callable=process_records,
            op_kwargs={
                "pattern": "folio_holdings_*.json",
                "out_filename": "holdings",
                "jobs": int(parallel_posts),
            },
        )

        finish_conversion = DummyOperator(task_id="finished-conversion")

        (
            convert_marc_to_folio_instances
            >> convert_marc_to_folio_holdings
            >> convert_holdings_valid_json
            >> finish_conversion
        )
        (
            convert_marc_to_folio_instances
            >> convert_instances_valid_json
            >> finish_conversion
        )

    with TaskGroup(group_id="post-to-folio") as post_to_folio:

        login = PythonOperator(task_id="folio_login",
                               python_callable=folio_login)

        finish_instances = DummyOperator(task_id="finish-posting-instances")

        for i in range(int(parallel_posts)):
            post_instances = PythonOperator(
                task_id=f"post_to_folio_instances_{i}",
                python_callable=post_folio_instance_records,
                op_kwargs={"job": i},
            )

            login >> post_instances >> finish_instances

        finish_holdings = DummyOperator(task_id="finish-posting-holdings")

        for i in range(int(parallel_posts)):
            post_holdings = PythonOperator(
                task_id=f"post_to_folio_holdings_{i}",
                python_callable=post_folio_holding_records,
                op_kwargs={"job": i},
            )

            finish_instances >> post_holdings >> finish_holdings

    archive_instance_files = BashOperator(
        task_id="archive_coverted_files",
        bash_command="mv /opt/airflow/migration/data/instances/* /opt/airflow/migration/archive/.; mv /opt/airflow/migration/results/folio_instances_*.json /opt/airflow/migration/archive/.",  # noqa
    )

    finish_loading = DummyOperator(
        task_id="finish_loading",
    )

    monitor_file_mount >> preprocess_marc_files >> move_marc
    move_marc >> marc_to_folio >> post_to_folio
    post_to_folio >> archive_instance_files >> finish_loading
