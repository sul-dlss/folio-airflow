import logging
from datetime import datetime

from folioclient import FolioClient
from sqlalchemy.orm import Session
from sqlalchemy import select

from libsys_airflow.plugins.vendor.models import Vendor, VendorInterface
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.decorators import task

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


def _get_vendors(folio_client):
    """
    Returns organizations (vendors) from FOLIO
    """
    return folio_client.organizations


def _get_vendor_interface(vendor_interface_id, folio_client):
    """
    Returns interface from FOLIO
    """
    return folio_client.folio_get(
        f"/organizations-storage/interfaces/{vendor_interface_id}"
    )


def _get_acquisitions_unit_names(acq_uuids, folio_client):
    acq_data = {}
    for uuid in acq_uuids:
        acq_data[uuid] = folio_client.folio_get(
            f"/acquisitions-units/units/{uuid}", "name"
        )
    return acq_data


@task()
def sync_data_task():
    folio_client = _folio_client()
    sync_data(folio_client)


def sync_data(folio_client):
    organizations = folio_client.organizations
    acq_names = _get_acquisitions_unit_names(
        set([org["acqUnitIds"][0] for org in organizations]), folio_client
    )
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        for organization in organizations:
            vendor = (
                session.query(Vendor)
                .filter(Vendor.folio_organization_uuid == organization["id"])
                .first()
            )
            # Add new vendor
            if vendor is None:
                vendor = Vendor(
                    display_name=organization["name"],
                    folio_organization_uuid=organization["id"],
                    vendor_code_from_folio=organization["code"],
                    acquisitions_unit_from_folio=organization["acqUnitIds"][0],
                    acquisitions_unit_name_from_folio=acq_names[
                        organization["acqUnitIds"][0]
                    ],
                    last_folio_update=datetime.now(),
                )
                logging.info(f"Adding vendor {vendor.display_name}")
                session.add(vendor)
            # Update existing vendor
            else:
                vendor.display_name = organization["name"]
                vendor.vendor_code_from_folio = organization["code"]
                vendor.acquisitions_unit_from_folio = organization["acqUnitIds"][0]
                vendor.acquisitions_unit_name_from_folio = acq_names[
                    organization["acqUnitIds"][0]
                ]
                vendor.last_folio_update = datetime.now()

            # Sync interfaces
            for vendor_interface_uuid in organization["interfaces"]:
                vendor_interface_data = _get_vendor_interface(
                    vendor_interface_uuid, folio_client
                )
                vendor_interface = (
                    session.query(VendorInterface)
                    .filter(
                        VendorInterface.folio_interface_uuid == vendor_interface_uuid
                    )
                    .filter(VendorInterface.vendor_id == vendor.id)
                    .first()
                )
                # Add new interfaces
                if vendor_interface is None:
                    vendor_interface = VendorInterface(
                        vendor_id=vendor.id,
                        display_name=vendor_interface_data["name"],
                        folio_interface_uuid=vendor_interface_uuid,
                    )
                    session.add(vendor_interface)
                    logging.info(
                        f"Adding interface {vendor_interface.display_name} to {vendor.display_name}"
                    )
                # Update existing interface including is restored to vendor in FOLIO
                else:
                    vendor_interface.display_name = vendor_interface_data["name"]
                    vendor_interface.removed_from_folio = False

            # Deprecate removed interfaces
            existing_vendor_interfaces = session.scalars(
                select(VendorInterface.folio_interface_uuid).where(
                    VendorInterface.vendor_id == vendor.id
                )
            ).all()
            removed = list(
                set(existing_vendor_interfaces).difference(organization["interfaces"])
            )
            for interface_uuid in removed:
                removed_interface = (
                    session.query(VendorInterface)
                    .filter(VendorInterface.folio_interface_uuid == interface_uuid)
                    .first()
                )
                removed_interface.active = False
                removed_interface.removed_from_folio = True
                logging.info(
                    f"Marking removed {removed_interface.display_name} - {removed_interface.folio_interface_uuid}"
                )

        session.commit()
