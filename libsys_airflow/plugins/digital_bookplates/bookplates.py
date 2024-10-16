import logging

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import Session
from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate
from libsys_airflow.plugins.shared import utils

logger = logging.getLogger(__name__)


def _get_bookplate_metadata_with_fund_uuids() -> dict:
    funds = {}
    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        for f in (
            session.query(DigitalBookplate)
            .where(DigitalBookplate.fund_uuid.is_not(None))
            .all()
        ):
            funds[f.fund_uuid] = {
                "fund_name": f.fund_name,
                "druid": f.druid,
                "image_filename": f.image_filename,
                "title": f.title,
            }

    return funds


def _new_bookplates(funds: list) -> dict:
    """
    Transforms new funds list into dictionary with fund_uuid as key
    """
    bookplates = {}
    for row in funds:
        bookplates[row["fund_uuid"]] = {
            "fund_name": row["fund_name"],
            "druid": row["druid"],
            "image_filename": row["image_filename"],
            "title": row["title"],
        }

    return bookplates


@task
def bookplate_funds_polines(**kwargs) -> list:
    """
    Checks if fund Id from invoice lines contains bookplate fund
    This task gets digital bookplates data from the table or uses
    a list of new funds from params and returns a list of bookplates
    metadata and poline ids:
    [
      {
        "bookplate_metadata": { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        "poline_id": "798596da-12a6-4c6d-8d3a-3bb6c54cb2f1"
      },
      {
        "bookplate_metadata": { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        "poline_id": "7a5888fe-689b-4cfe-a27d-c2675a235203"
      }
    ]
    """
    bookplates_polines: list = []
    invoice_lines = kwargs["invoice_lines"]
    params = kwargs.get("params", {})
    funds = params.get("funds", [])
    if len(funds) > 0:
        bookplates = _new_bookplates(funds)
    else:
        bookplates = _get_bookplate_metadata_with_fund_uuids()

    for row in invoice_lines:
        fund_distribution = row.get("fundDistributions")
        poline_id = row.get("poLineId")
        if fund_distribution and poline_id:
            for fund in fund_distribution:
                bookplate = bookplates.get(fund["fundId"])
                if bookplate:
                    bookplates_polines.append(
                        {"bookplate_metadata": bookplate, "poline_id": poline_id}
                    )

    return bookplates_polines


@task
def launch_add_979_fields_task(**kwargs):
    """
    Trigger add a tag dag with instance UUIDs and fund 979 data. Returns a dict:
    "242c6000-8485-5fcd-9b5e-adb60788ca59": [
        { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
    ]
    """
    params = kwargs.get("params", {})
    return params.get("druids_for_instance_id", {})


@task
def add_marc_tags_to_record(**kwargs):
    marc_tags = kwargs["marc_instances_tags"]
    instance_id = kwargs["instance_uuid"]
    folio_add_marc_tags = utils.FolioAddMarcTags()
    return folio_add_marc_tags.put_folio_records(marc_tags, instance_id)


@task
def instance_id_for_druids(**kwargs) -> list:
    druids_instances = kwargs["druid_instances"]
    if druids_instances is None:
        return []
    return list(list(druids_instances.keys())[0])


@task
def add_979_marc_tags(druid_instances: dict) -> dict:
    """
    get the bookplate data from the bookplates table and contruct a 979 tag with the
    fund name in subfield f, druid in subfield b, image filename in subfield c, and title in subfield d:
    {
        '979': {'ind1': ' ', 'ind2': ' ', 'subfields': [
                {'f': 'ABBOTT'}, {'b': 'druid:ws066yy0421'}, {'c': 'ws066yy0421_00_0001.jp2'}, {'d': 'The The Donald P. Abbott Fund for Marine Invertebrates'}
            ]
        }
    }
    """

    marc_instances_tags: dict = {'979': []}
    for _instance_uuid, druids in druid_instances.items():
        for tag_data in druids:
            marc_instances_tags['979'].append(
                {
                    'ind1': ' ',
                    'ind2': ' ',
                    'subfields': [
                        {'f': tag_data['fund_name']},
                        {'b': tag_data['druid']},
                        {'c': tag_data['image_filename']},
                        {'d': tag_data['title']},
                    ],
                }
            )

    return marc_instances_tags
