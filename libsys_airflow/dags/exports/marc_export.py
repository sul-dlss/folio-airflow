import logging

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.timetables.interval import CronDataIntervalTimetable

from folioclient import FolioClient

from libsys_airflow.plugins.exports.harvest import calculate_batches, marc_from_srs

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2024, 2, 14),
    schedule=CronDataIntervalTimetable(
        cron="0 18 * * 5", timezone="America/Los_Angeles"  # 6 pm on Friday
    ),
    catchup=False,
    tags=["folio", "export"],
)
def marc_records_export():
    finished = EmptyOperator(task_id="finished")

    @task
    def start_task(**kwargs):
        context = get_current_context()
        params = context.get("params")
        batch_size = params.get("batch_size", 50_000)
        shard_size = params.get("shard_size", 5_000)  # used for each SRS query
        return {"sizes": {"batch": batch_size, "shard": shard_size}}

    @task
    def calculate_batches_task(sizes: str):
        logger.info(f"Sizes is {sizes}")
        return calculate_batches(
            sizes['sizes']['batch'],
            FolioClient(
                Variable.get("OKAPI_URL"),
                "sul",
                Variable.get("FOLIO_USER"),
                Variable.get("FOLIO_PASSWORD"),
            ),
        )

    sizes = start_task()

    batches = calculate_batches_task(sizes)

    batches >> finished


marc_records_export()
