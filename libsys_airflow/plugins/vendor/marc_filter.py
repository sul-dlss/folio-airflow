import logging
import pathlib

import pymarc

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def filter_fields_task(marc_path: str, fields: list):
    """
    Filters 905, 920, 986 from MARC records
    """
    filter_fields(pathlib.Path(marc_path), ["905", "920", "986"])


def filter_fields(marc_path: pathlib.Path, fields: list):
    """
    Filters specified fields from MARC records
    """
    logger.info(f"Filtering fields from {marc_path}")
    filtered_records = []
    with marc_path.open("rb") as fo:
        marc_reader = pymarc.MARCReader(
            fo, to_unicode=True, permissive=True, utf8_handling="replace"
        )
        for record in marc_reader:
            record.remove_fields(*fields)
            filtered_records.append(record)

    with marc_path.open("wb") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in filtered_records:
            record.force_utf8 = True
            marc_writer.write(record)

    logger.info(f"Finished filtering fields from {marc_path}")