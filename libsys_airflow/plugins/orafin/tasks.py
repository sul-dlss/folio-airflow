import logging

from airflow.decorators import task
from airflow.models import Variable

from libsys_airflow.plugins.folio.folio_client import FolioClient

from libsys_airflow.plugins.orafin.payments import (
    generate_excluded_email,
    get_invoice,
    init_feeder_file,
    models_converter,
)


logger = logging.getLogger(__name__)


def _folio_client():
    try:
        return FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )
    except ValueError as error:
        logger.error(error)
        raise


@task
def email_excluded_task(invoices: list):
    folio_url = Variable.get("FOLIO_URL")
    if len(invoices) > 0:
        generate_excluded_email(invoices, folio_url)
    return f"Emailed report for {len(invoices):,} invoices"


@task(multiple_outputs=True)
def filter_invoices_task(invoices: list):
    feeder_file, excluded = [], []
    for row in invoices:
        if row['exclude'] is True:
            excluded.append(row['invoice'])
        else:
            feeder_file.append(row['invoice'])
    return {"feed": feeder_file, "excluded": excluded}


@task(max_active_tis_per_dag=5)
def transform_folio_data_task(invoice_id: str):
    """
    Takes Invoice ID and retrieves invoice information and tax status
    from the invoice's organization
    """
    folio_client = _folio_client()
    converter = models_converter()
    # Call to Okapi invoice endpoint
    invoice, exclude = get_invoice(invoice_id, folio_client, converter)
    return {"invoice": converter.unstructure(invoice), "exclude": exclude}


@task
def feeder_file_task(invoices: list):
    converter = models_converter()
    folio_client = _folio_client()
    feeder_file = init_feeder_file(invoices, folio_client, converter)

    return converter.unstructure(feeder_file)


# TODO: can un-ignore type checking here once this function is less of a stub
@task
def sftp_file_task(feeder_file: task):  # type: ignore
    return "foo"