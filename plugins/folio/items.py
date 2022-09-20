import json
import logging
import pathlib

import pandas as pd
import requests

from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer
from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)


def _generate_holdings_keys(results_dir: pathlib.Path, holdings_pattern: str) -> dict:
    """Initializes Holdings lookup and counter for hrid generation"""
    holdings_keys = {}

    for holdings_file in results_dir.glob(holdings_pattern):
        with holdings_file.open() as fo:
            for line in fo.readlines():
                holdings_record = json.loads(line)
                holdings_keys[holdings_record["id"]] = {
                    "formerId": holdings_record["formerIds"][0],
                    "counter": 0,
                }

    return holdings_keys

def _generate_item_notes(
        barcode,
        tsv_note_df: pd.DataFrame, 
        item_note_ids: dict) -> list:
    """Takes TSV notes dataframe and returns a list of generated Item notes"""
    if barcode is None:
        logger.error(f"Item missing barcode, cannot generate notes")
        return []
    item_notes = tsv_note_df.loc[tsv_note_df['BARCODE'] == barcode]
    item_notes["itemNoteTypeId"] = item_notes["TYPE_NAME"].apply(lambda x: item_note_ids.get(x))
    item_notes = item_notes.drop(columns=['BARCODE', 'TYPE_NAME'])

    logger.info(f"Total item notes for {barcode} {len(item_notes)} {len(tsv_note_df)}")
    return json.loads(item_notes.to_json(index=False, orient="table"))['data']

def _retrieve_item_notes_ids(folio_client) -> dict:
    """Retrieves itemNoteTypes from Okapi"""
    note_types = dict()
    note_types_response = requests.get(f"{folio_client.okapi_url}/item-note-types",
                                       headers=folio_client.okapi_headers)

    if note_types_response.status_code > 399:
        raise ValueError (f"Cannot retrieve item note types from {folio_client.okapi_url}\n{note_types_response.text}")

    for note_type in note_types_response.json()['itemNoteTypes']:
        note_types[note_type['name']] = note_type['id']

    return note_types


def _add_additional_info(**kwargs):
    """Adds an HRID based on Holdings formerIds and generates notes from 
    tsv files"""
    okapi_url: str = kwargs['okapi_url']
    airflow: str = kwargs['airflow']
    holdings_pattern: str = kwargs['holdings_pattern']
    items_pattern: str = kwargs['items_pattern']
    tsv_notes_path = kwargs['tsv_notes_path']
    folio_client = kwargs['folio_client']

    results_dir = pathlib.Path(f"{airflow}/migration/results")
    tsv_notes_path = pathlib.Path(tsv_notes_path)

    holdings_keys = _generate_holdings_keys(results_dir, holdings_pattern)

    tsv_notes_df = pd.read_csv(tsv_notes_path, sep="\t", dtype=object)

    item_note_types = _retrieve_item_notes_ids(folio_client)

    items = []
    for items_file in results_dir.glob(items_pattern):

        with items_file.open() as fo:
            for line in fo.readlines():
                item = json.loads(line)
                holding = holdings_keys[item["holdingsRecordId"]]
                former_id = holding["formerId"]
                holding["counter"] = holding["counter"] + 1
                hrid_prefix = former_id[:1] + "i" + former_id[1:]
                item["hrid"] = f"{hrid_prefix}_{holding['counter']}"
                if "barcode" in item:
                    id_seed = item["barcode"]
                else:
                    id_seed = item["hrid"]
                item["id"] = str(
                    FolioUUID(
                        okapi_url,
                        FOLIONamespaces.items,
                        id_seed,
                    )
                )
                # To handle optimistic locking
                item["_version"] = 1
                item["notes"] = _generate_item_notes(
                    item.get('barcode'), 
                    tsv_notes_df, 
                    item_note_types)
                items.append(item)

        with open(items_file, "w+") as write_output:
            for item in items:
                write_output.write(f"{json.dumps(item)}\n")

    # tsv_notes_path.unlink()

        


def post_folio_items_records(**kwargs):
    """Creates/overlays Items records in FOLIO"""
    dag = kwargs["dag_run"]

    batch_size = int(kwargs.get("MAX_ENTITIES", 1000))
    job_number = kwargs.get("job")

    with open(f"/tmp/items-{dag.run_id}-{job_number}.json") as fo:
        items_records = json.load(fo)

    for i in range(0, len(items_records), batch_size):
        items_batch = items_records[i:i + batch_size]
        logger.info(f"Posting {len(items_batch)} in batch {i/batch_size}")
        post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=items_batch,
            endpoint="/item-storage/batch/synchronous?upsert=true",
            payload_key="items",
            **kwargs,
        )


def run_items_transformer(*args, **kwargs) -> bool:
    """Runs item tranformer"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    instance = kwargs["task_instance"]
    library_config = kwargs["library_config"]

    library_config.iteration_identifier = dag.run_id

    items_stem = kwargs["items_stem"]

    item_config = ItemsTransformer.TaskConfiguration(
        name="items-transformer",
        migration_task_type="ItemsTransformer",
        hrid_handling="preserve001",
        files=[{"file_name": f"{items_stem}.tsv", "suppress": False}],
        items_mapping_file_name="item_mapping.json",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        material_types_map_file_name="material_types.tsv",
        loan_types_map_file_name="loan_types.tsv",
        statistical_codes_map_file_name="statcodes.tsv",
        item_statuses_map_file_name="item_statuses.tsv",
        call_number_type_map_file_name="call_number_type_mapping.tsv",
    )

    items_transformer = ItemsTransformer(item_config, library_config, use_logging=False)

    setup_data_logging(items_transformer)

    items_transformer.do_work()

    items_transformer.wrap_up()

    _add_additional_info(
        okapi_url=items_transformer.folio_client.okapi_url,
        airflow=airflow,
        holdings_pattern=f"folio_holdings_{dag.run_id}_holdings-*transformer.json",
        items_pattern=f"folio_items_{dag.run_id}_items-*transformer.json",
        tsv_notes_path=instance.xcom_pull(task_ids="move-transform.symphony-tsv-processing", key="tsv-notes"),
        folio_client=items_transformer.folio_client
    )
