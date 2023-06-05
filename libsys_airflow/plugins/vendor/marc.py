import logging
import pathlib
from typing import Optional

import pymarc
import magic
from pydantic import BaseModel, Field

from airflow.decorators import task
from airflow.models import Variable

logger = logging.getLogger(__name__)


class ChangeField(BaseModel):
    from_: str = Field(alias='from')
    to: str


class MarcSubfield(BaseModel):
    code: str
    value: str


class MarcField(BaseModel):
    tag: str
    indicator1: Optional[str] = None
    indicator2: Optional[str] = None
    subfields: list[MarcSubfield]


class AddField(MarcField):
    unless: Optional[MarcField]


@task
def process_marc_task(
    download_path: str,
    filename: str,
    remove_fields: Optional[list[str]] = None,
    change_fields: Optional[list[dict]] = None,
    add_fields: Optional[list[dict]] = None,
) -> str:
    """
    Applies changes to MARC records.

    change_fields example: [{ from: "520" to: "920" }, { from: "528" to: "928" }]
    add_fields example: [
            { tag: "910", indicator1: '2', subfields: [{code: "a", value: "MARCit"}] },
            { tag: "590", subfields: [{code: "a", value: "MARCit brief record"}], unless: { tag: "035", subfields: [{code: "a", value: "OCoLC"}]} },
        ]
    """
    marc_path = pathlib.Path(download_path) / filename
    if not is_marc(marc_path):
        logger.info(f"Skipping filtering fields from {marc_path}")
        return filename
    change_fields_models = None
    if change_fields:
        change_fields_models = _to_change_fields_models(change_fields)
    add_fields_models = None
    if add_fields:
        add_fields_models = _to_add_fields_models(add_fields)
    new_marc_path = process_marc(
        pathlib.Path(marc_path), remove_fields, change_fields_models, add_fields_models
    )
    return new_marc_path.name


def process_marc(
    marc_path: pathlib.Path,
    remove_fields: Optional[list[str]] = None,
    change_fields: Optional[list[ChangeField]] = None,
    add_fields: Optional[list[AddField]] = None,
) -> pathlib.Path:
    logger.info(f"Processing from {marc_path}")
    records = []
    with marc_path.open("rb") as fo:
        reader = _marc_reader(fo)
        for record in reader:
            if record is None:
                logger.info(
                    f"Error reading MARC. Current chunk: {reader.current_chunk}. Error: {reader.current_exception}"
                )
            else:
                if remove_fields:
                    record.remove_fields(*remove_fields)
                if change_fields:
                    _change_fields(record, change_fields)
                if add_fields:
                    _add_fields(record, add_fields)
                records.append(record)

    new_marc_path = marc_path.with_stem(f"{marc_path.stem}-processed")
    _write_records(records, new_marc_path)

    logger.info(f"Finished processing from {marc_path}")

    return new_marc_path


def is_marc(path: pathlib.Path):
    return magic.from_file(str(path), mime=True) == "application/marc"


def _marc_reader(file):
    return pymarc.MARCReader(
        file, to_unicode=True, permissive=True, utf8_handling="replace"
    )


@task
def batch_task(download_path: str, filename: str) -> list[str]:
    """
    Splits a MARC file into batches.
    """
    marc_path = pathlib.Path(download_path) / filename
    if not is_marc(marc_path):
        logger.info(f"Skipping batching {filename}")
        return [filename]
    max_records = Variable.get("MAX_ENTITIES", 500)
    return batch(download_path, filename, max_records)


def batch(download_path: str, filename: str, max_records: int) -> list[str]:
    """
    Splits a MARC file into batches.
    """
    records = []
    index = 1
    batch_filenames = []
    with open(pathlib.Path(download_path) / filename, "rb") as fo:
        reader = _marc_reader(fo)
        for record in reader:
            if record is None:
                logger.info(
                    f"Error reading MARC. Current chunk: {reader.current_chunk}. Error: {reader.current_exception} "
                )
            else:
                records.append(record)
                if len(records) == max_records:
                    records, index = _new_batch(
                        download_path, filename, index, batch_filenames, records
                    )
    if len(records) > 0:
        _new_batch(download_path, filename, index, batch_filenames, records)
    logger.info(f"Finished batching {filename} into {index} files")
    return batch_filenames


def _new_batch(download_path, filename, index, batch_filenames, records):
    batch_filename = _batch_filename(filename, index)
    batch_filenames.append(batch_filename)
    _write_records(records, pathlib.Path(download_path) / batch_filename)
    return [], index + 1


def _batch_filename(filename, index):
    file_path = pathlib.Path(filename)
    return f"{file_path.stem}_{index}{file_path.suffix}"


def _write_records(records, marc_path):
    logger.info(f"Writing {len(records)} records to {marc_path}")
    with marc_path.open("wb") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in records:
            record.force_utf8 = True
            marc_writer.write(record)


def _to_change_fields_models(change_fields):
    return [ChangeField(**change) for change in change_fields]


def _to_add_fields_models(add_fields):
    return [AddField(**add) for add in add_fields]


def _change_fields(record, change_fields):
    for change in change_fields:
        for field in record.get_fields(change.from_):
            field.tag = change.to


def _add_fields(record, add_fields):
    for add_field in add_fields:
        if _skip(record, add_field.unless):
            continue
        field = pymarc.field.Field(
            tag=add_field.tag,
            indicators=[add_field.indicator1 or ' ', add_field.indicator2 or ' '],
        )
        for subfield in add_field.subfields:
            field.add_subfield(subfield.code, subfield.value)
        record.add_field(field)


def _skip(record, unless):
    if unless and _has_matching_field(record, unless):
        return True
    return False


def _has_matching_field(record: pymarc.Record, field: MarcField):
    for check_field in record.get_fields(field.tag):
        if _field_match(field, check_field):
            return True
    return False


def _field_match(field: MarcField, check_field: pymarc.Field):
    if field.indicator1 and check_field.indicators[0] != field.indicator1:
        return False
    if field.indicator2 and check_field.indicators[1] != field.indicator2:
        return False
    for subfield in field.subfields:
        check_subfield_value = check_field[subfield.code]
        if not check_subfield_value or check_subfield_value != subfield.value:
            return False
    return True
