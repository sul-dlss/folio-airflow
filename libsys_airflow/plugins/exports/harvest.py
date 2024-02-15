import json
import logging
import pathlib
import zipfile

logger = logging.getLogger(__name__)

from folioclient import FolioClient

def _get_srs_records(folio_client: FolioClient, offset: int, limit: int):
    url = f"/source-storage/source-records?limit={limit}&offset={offset}"
    srs_result = folio_client.folio_get(url)
    return srs_result["sourceRecords"]


def _save_batch(dag_run_location: pathlib.Path, batch: list, batch_count: int):
    batch_file_base = f"srs-marc-export-{batch_count:03}"
    zip_file_name = dag_run_location / f"{batch_file_base}.zip"
    
    with zipfile.ZipFile(zip_file_name, "w") as zip_file:
        zip_file.writestr(f"{batch_file_base}.jsonl",
                          "\n".join([json.dumps(row) for row in batch]))



def calculate_batches(batch_size: str, folio_client: FolioClient) -> list:
    """
    Calculates SRS batches
    """
    batch_ids = []
    
    size_result = folio_client.folio_get("/source-storage/source-records?limit=1")
    total_records = size_result["totalRecords"]

    total_batches = total_records / int(batch_size)
    batch_ids = [i for i in range(int(total_batches))]
    return batch_ids


def marc_from_srs(**kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag"]

    export_path = pathlib.Path(airflow) / f"{dag.run}"

    batch_size = kwargs.get('batch_size', 50_000)
    limit = kwargs.get('limit', 5_000)
    folio_client = kwargs['folio_client']

    

    size_srs = int(batch_size / limit)

    batch = []

    for i in range(total_batches):
        batch = []
        offset_start = i * batch_size
        for j in range(size_srs):
            offset = offset_start + j * limit
            batch.extend(_get_srs_records(folio_client, offset, limit))
        _save_batch(export_path, batch, i)
        

