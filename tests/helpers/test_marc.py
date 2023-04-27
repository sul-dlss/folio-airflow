import json
import pathlib
import sqlite3

import requests

import pytest  # noqa

import folio_migration_tools.migration_tasks.batch_poster as batch_poster

from pytest_mock import MockerFixture
from pymarc import Field, MARCWriter, Record

from libsys_airflow.plugins.folio.audit import setup_audit_db

from libsys_airflow.plugins.folio.helpers.marc import (
    _add_electronic_holdings,
    discover_srs_files,
    _extract_e_holdings_fields,
    filter_mhlds,
    _get_library,
    handle_srs_files,
    marc_only,
    move_marc_files,
    _move_001_to_035,
    post_marc_to_srs,
)

from libsys_airflow.plugins.folio.helpers.marc import process as process_marc

from tests.mocks import (  # noqa
    mock_okapi_success,
    mock_dag_run,
    mock_file_system,
    mock_okapi_variable,
    MockFOLIOClient,
    MockLibraryConfig,
    MockTaskInstance,
)

import tests.mocks as mocks


@pytest.fixture
def mock_marc_record():
    record = Record()
    field_245 = Field(
        tag="245",
        indicators=["0", "1"],
        subfields=[
            "a",
            "The pragmatic programmer : ",
            "b",
            "from journeyman to master /",
            "c",
            "Andrew Hunt, David Thomas.",
        ],
    )
    field_001_1 = Field(tag="001", data="a123456789")
    field_001_2 = Field(tag="001", data="gls_0987654321")

    record.add_field(field_001_1, field_001_2, field_245)
    return record


@pytest.fixture
def mock_get_req_size(monkeypatch):
    def mock_size(response):
        return "150.00MB"

    monkeypatch.setattr(batch_poster, "get_req_size", mock_size)


@pytest.fixture
def mock_srs_requests(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post-result")
        post_response.status_code = 201
        if not args[0].endswith("snapshots"):
            if kwargs["json"]["id"] == "c9198b05-8d7e-4769-b0cf-a8ca579c0fb4":
                post_response.status_code = 422
                post_response.text = "Invalid user"
        return post_response

    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get-response")
        if args[0].endswith("e9a161b7-3541-54d6-bd1d-e4f2c3a3db79"):
            get_response.status_code = 200
        if args[0].endswith("9cb89c9a-1184-4969-ae0d-19e4667bcea3") or args[0].endswith(
            "c9198b05-8d7e-4769-b0cf-a8ca579c0fb4"
        ):
            get_response.status_code = 404
        if args[0].endswith("d0c4f6ef-770d-44de-9d91-0bc6aa654391"):
            get_response.status_code = 422
            get_response.text = "Missing Required Fields"
        return get_response

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def srs_file(mock_file_system):  # noqa
    results_dir = mock_file_system[3]

    srs_filepath = results_dir / "folio_srs_instances_bibs-transformer.json"

    records = [
        {
            "id": "e9a161b7-3541-54d6-bd1d-e4f2c3a3db79",
            "generation": "0",
            "rawRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
            "externalIdsHolder": {"instanceHrid": "a34567"},
        },
        {
            "id": "9cb89c9a-1184-4969-ae0d-19e4667bcea3",
            "generation": "0",
            "rawRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
            "externalIdsHolder": {"instanceHrid": "a13981569"},
        },
        {
            "id": "c9198b05-8d7e-4769-b0cf-a8ca579c0fb4",
            "generation": "0",
            "rawRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
            "externalIdsHolder": {"instanceHrid": "a165578"},
        },
        {
            "id": "d0c4f6ef-770d-44de-9d91-0bc6aa654391",
            "generation": "0",
            "rawRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
            "externalIdsHolder": {"instanceHrid": "a11665261"},
        },
    ]

    with srs_filepath.open("w+") as fo:
        for record in records:
            fo.write(f"{json.dumps(record)}\n")
    return srs_filepath


def test_add_electronic_holdings_skip():
    skip_856_field = Field(
        tag="856",
        indicators=["0", "1"],
        subfields=["z", "table of contents", "u", "http://example.com/"],
    )
    assert _add_electronic_holdings(skip_856_field) is False


def test_add_electronic_holdings_skip_multiple_fields():
    skip_856_field = Field(
        tag="856",
        indicators=["0", "1"],
        subfields=[
            "z",
            "Online Abstract from PCI available",
            "3",
            "More information",
            "u",
            "http://example.com/",
        ],
    )
    assert _add_electronic_holdings(skip_856_field) is False


def test_add_electronic_holdings():
    field_856 = Field(
        tag="856", indicators=["0", "0"], subfields=["u", "http://example.com/"]
    )
    assert _add_electronic_holdings(field_856) is True


def test_discover_srs_files(mock_file_system, srs_file):  # noqa
    airflow = mock_file_system[0]
    iteration_two_results = airflow / "migration/iterations/manual_2023-03-09/results/"
    iteration_two_results.mkdir(parents=True)
    folio_srs2 = iteration_two_results / "folio_srs_instances_bibs-transformer.json"
    folio_srs2.write_text("""{ "id": "5bfd2479-6773-4100-99dd-fd042063c2ec" }""")

    discover_srs_files(
        airflow=mock_file_system[0],
        jobs=2,
        task_instance=MockTaskInstance(task_id="find-srs-files"),
    )
    assert len(mocks.messages["find-srs-files"]["job-1"]) == 1
    assert mocks.messages["find-srs-files"]["job-0"] == [str(mock_file_system[2])]
    mocks.messages = {}


def test_extract_856s():
    catkey = "34456"
    all856_fields = [
        Field(
            tag="856",
            indicators=["0", "1"],
            subfields=[
                "3",
                "Finding Aid",
                "u",
                "https://purl.stanford.edu/123345",
                "x",
                "purchased",
                "x",
                "cz4",
                "x",
                "Provider: Cambridge University Press",
                "y",
                "Access on Campus Only",
                "z",
                "Stanford Use Only",
                "z",
                "Restricted",
            ],
        ),
        Field(
            tag="856",
            indicators=["4", "1"],
            subfields=[
                "u",
                "http://purl.stanford.edu/bh752xn6465",
                "x",
                "SDR-PURL",
                "x",
                "file:bh752xn6465%2FSC1228_Powwow_program_2014_001.jp2",
                "x",
                "collection:cy369sj5591:10719939:Stanford University, Native American Cultural Center, records",
                "x",
                "label:2014",
                "x",
                "rights:world",
            ],
        ),
        Field(
            tag="856",
            indicators=["0", "0"],
            subfields=[
                "u",
                "http://doi.org/34456",
                "y",
                "Public Document All Access",
                "z",
                "World Available",
            ],
        ),
        Field(
            tag="856",
            indicators=["0", "1"],
            subfields=["u", "https://example.doi.org/4566", "3", "sample text"],
        ),
        Field(
            tag="856",
            indicators=["0", "8"],
            subfields=["u", "https://example.doi.org/45668"],
        ),
    ]
    output = _extract_e_holdings_fields(
        catkey=catkey, fields=all856_fields, library="SUL"
    )
    assert len(output) == 3
    assert output[0] == {
        "CATKEY": "34456",
        "HOMELOCATION": "SDR",
        "LIBRARY": "SUL",
        "COPY": 0,
        "MAT_SPEC": "Finding Aid",
    }
    assert output[1]["HOMELOCATION"].startswith("SDR")
    assert output[2]["HOMELOCATION"] == "INTERNET"


def test_extract_956s():
    catkey = "a591929"
    all_956_fields = [
        Field(
            tag="956",
            indicators=["4", "1"],
            subfields=[
                "u",
                "http://library.stanford.edu/sfx?url%5Fver=Z39.88-2004&9008",
            ],
        ),
        Field(
            tag="956",
            indicators=["0", "1"],
            subfields=[
                "u",
                "http://library.stanford.edu/sfx?url%5Fver=Z39.88-2004&8998",
                "z",
                "Sample text",
            ],
        ),
    ]

    output = _extract_e_holdings_fields(
        catkey=catkey, fields=all_956_fields, library="SUL"
    )
    assert len(output) == 1
    assert output[0]["HOMELOCATION"].startswith("INTERNET")
    assert output[0]["LIBRARY"] == "SUL"


def test_handle_srs_files(
    mock_file_system, mock_dag_run, srs_file, mock_srs_requests, caplog  # noqa
):
    airflow = mock_file_system[0]
    iteration_dir = mock_file_system[2]
    results_dir = mock_file_system[3]

    mocks.messages["find-srs-files"] = {"job-0": [str(mock_file_system[2])]}

    current_file = pathlib.Path(__file__)
    db_init_file = current_file.parent.parent / "qa.sql"
    mock_db_init_file = airflow / "plugins/folio/qa.sql"
    mock_db_init_file.write_text(db_init_file.read_text())

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)

    handle_srs_files(
        task_instance=MockTaskInstance(task_id="find-srs-files"),
        job=0,
        folio_client=MockFOLIOClient(),
    )

    audit_db = sqlite3.connect(results_dir / "audit-remediation.db")
    cur = audit_db.cursor()

    assert "Starting Check/Add SRS Bibs files for 1" in caplog.text

    total_records = cur.execute("SELECT count(id) FROM Record;").fetchone()[0]
    assert total_records == 4
    existing_records = cur.execute(
        """SELECT count(id) FROM AuditLog WHERE status=1;"""
    ).fetchone()[0]
    assert existing_records == 1
    missing_records = cur.execute(
        """SELECT count(id) FROM AuditLog WHERE status=2;"""
    ).fetchone()[0]
    assert missing_records == 2
    error_records = cur.execute(
        """SELECT count(id) FROM AuditLog WHERE status=3;"""
    ).fetchone()[0]
    assert error_records == 1

    cur.close()
    audit_db.close()

    assert (iteration_dir / "reports/report_srs-audit.md").exists()

    mocks.messages = {}


def test_filter_mhlds(tmp_path, caplog):

    mhld_mock = tmp_path / "mock-mhld.mrc"

    record_one = Record()
    record_one.add_field(
        Field(tag="852",
              indicators=[" ", " "],
              subfields=['a', 'CSt'])
    )
    record_two = Record()
    record_two.add_field(
        Field(tag="852",
              indicators=[" ", " "],
              subfields=['a', '**REQUIRED Field**'])
    )
    record_three = Record()
    record_three.add_field(
        Field(tag="852",
              indicators=[" ", " "],
              subfields=['z', 'All holdings transferred to CSt'])
    )

    with mhld_mock.open("wb+") as fo:
        marc_writer = MARCWriter(fo)
        for record in [record_one, record_two, record_three]:
            marc_writer.write(record)

    filter_mhlds(mhld_mock)

    assert "Finished filtering MHLD, start 3 removed 2" in caplog.text


def test_marc_only():
    mocks.messages["bib-files-group"] = {
        "tsv-files": [],
        "tsv-base": None,
    }

    next_task = marc_only(
        task_instance=MockTaskInstance(),
        default_task="tsv-holdings",
        marc_only_task="marc-only",
    )

    assert next_task.startswith("marc-only")

    mocks.messages = {}


def test_get_library_default():
    library = _get_library([])
    assert library.startswith("SUL")


def test_get_library_law():
    fields = [Field(tag="596", subfields=["a", "24"])]
    library = _get_library(fields)
    assert library.startswith("LAW")


def test_get_library_hoover():
    fields_25 = [Field(tag="596", subfields=["a", "25 22"])]
    library_hoover = _get_library(fields_25)
    assert library_hoover.startswith("HOOVER")
    fields_27 = [Field(tag="596", subfields=["a", "27 22"])]
    library_hoover2 = _get_library(fields_27)
    assert library_hoover2.startswith("HOOVER")


def test_get_library_business():
    fields = [
        Field(tag="596", subfields=["a", "28"]),
        Field(tag="596", subfields=["a", "28 22"]),
    ]
    library = _get_library(fields)
    assert library.startswith("BUSINESS")


def test_marc_only_with_tsv():
    mocks.messages["bib-files-group"] = {
        "tsv-files": ["circnotes.tsv"],
        "tsv-base": "base.tsv",
    }

    next_task = marc_only(
        task_instance=MockTaskInstance(),
        default_task="tsv-holdings",
        marc_only_task="marc-only",
    )

    assert next_task.startswith("tsv-holdings")

    mocks.messages = {}


def test_missing_001_to_034(mock_marc_record):
    record = mock_marc_record
    record.remove_fields("001")
    _move_001_to_035(record)
    assert record.get_fields("035") == []


def test_move_001_to_035(mock_marc_record):
    record = mock_marc_record
    _move_001_to_035(record)
    assert record.get_fields("035")[0].get_subfields("a")[0] == "gls_0987654321"  # noqa


def test_move_marc_files(mock_file_system, mock_dag_run):  # noqa
    task_instance = MockTaskInstance()
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]

    sample_mrc = source_dir / "sample.mrc"
    with sample_mrc.open("wb+") as fo:
        marc_record = Record()
        marc_record.add_field(
            Field(tag="245", indicators=[" ", " "], subfields=["a", "A Test Title"])
        )
        writer = MARCWriter(fo)
        writer.write(marc_record)

    sample_mhld_mrc = source_dir / "sample-mhld.mrc"
    with sample_mhld_mrc.open("wb+") as mhld_fo:
        marc_record = Record()
        marc_record.add_field(Field(tag="001", data="a123456"))
        marc_record.add_field(
            Field(
                tag="852",
                indicators=[" ", " "],
                subfields=["a", "CSt", "b", "GREEN", "c", "STACKS"],
            )
        )
        writer = MARCWriter(mhld_fo)
        writer.write(marc_record)

    mocks.messages["bib-files-group"] = {
        "marc-file": str(sample_mrc),
        "mhld-file": str(sample_mhld_mrc),
    }

    move_marc_files(
        task_instance=task_instance,
        airflow=airflow_path,
        source="symphony",
        dag_run=mock_dag_run,
    )  # noqa
    assert not (source_dir / "sample.mrc").exists()
    assert not (source_dir / "sample-mfld.mrc").exists()

    assert (
        airflow_path
        / f"migration/iterations/{mock_dag_run.run_id}/source_data/holdings/sample-mhld.mrc"
    ).exists()

    assert (
        airflow_path
        / f"migration/iterations/{mock_dag_run.run_id}/source_data/instances/sample.mrc"
    ).exists()

    mocks.messages = {}


def test_post_marc_to_srs(
    srs_file,
    mock_okapi_success,  # noqa
    mock_dag_run,  # noqa
    mock_file_system,  # noqa
    mock_get_req_size,
    mock_okapi_variable,  # noqa
    caplog,
):
    airflow = mock_file_system[0]
    dag = mock_dag_run

    base_folder = airflow / "migration"

    library_config = MockLibraryConfig(
        base_folder=str(base_folder), iteration_identifier=mock_dag_run.run_id
    )

    test_srs = base_folder / f"iterations/{mock_dag_run.run_id}/results/test-srs.json"

    test_srs.write_text(json.dumps({}))

    post_marc_to_srs(
        airflow=airflow,
        dag_run=dag,
        library_config=library_config,
        iteration_id="manual_2022-03-05",
        srs_filename="test-srs.json",
    )

    assert library_config.iteration_identifier == dag.run_id
    assert "Finished posting MARC json to SRS" in caplog.text


def test_missing_file_post_marc_to_srs(
    srs_file, mock_dag_run, mock_file_system, caplog  # noqa  # noqa
):

    airflow = mock_file_system[0]

    base_folder = airflow / "migration"

    library_config = MockLibraryConfig(
        base_folder=str(base_folder), iteration_identifier=mock_dag_run.run_id
    )

    post_marc_to_srs(
        airflow=airflow,
        dag_run=mock_dag_run,
        library_config=library_config,
        iteration_id="manual_2022-03-05",
        srs_filename="test-mhlds-srs.json",
    )

    assert "test-mhlds-srs.json does not exist, existing task" in caplog.text


def test_process_marc():
    assert process_marc