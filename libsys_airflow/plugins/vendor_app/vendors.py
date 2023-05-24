import re
import logging
import os
from datetime import datetime

from airflow.api.common.trigger_dag import trigger_dag
from flask_appbuilder import expose, BaseView
from flask import request, redirect, url_for, flash
from sqlalchemy import select

from libsys_airflow.plugins.vendor.job_profiles import job_profiles
from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
    VendorFile,
    FileStatus,
)
from libsys_airflow.plugins.vendor.paths import download_path, archive_path
from libsys_airflow.plugins.vendor_app.database import Session
from libsys_airflow.plugins.vendor.archive import archive

logger = logging.getLogger(__name__)


class VendorManagementView(BaseView):
    default_view = "index"
    route_base = "/vendors"

    @expose("/")
    def index(self):
        vendors = Session().query(Vendor).order_by(Vendor.display_name)
        return self.render_template("vendors/index.html", vendors=vendors)

    @expose("/<int:vendor_id>")
    def vendor(self, vendor_id):
        vendor = Session().query(Vendor).get(vendor_id)
        return self.render_template("vendors/vendor.html", vendor=vendor)

    @expose("/interface/<int:interface_id>")
    def interface(self, interface_id):
        interface = Session().query(VendorInterface).get(interface_id)
        return self.render_template("vendors/interface.html", interface=interface)

    @expose("/interface/<int:interface_id>/edit", methods=['GET', 'POST'])
    def interface_edit(self, interface_id):
        session = Session()
        interface = session.query(VendorInterface).get(interface_id)

        if request.method == 'GET':
            return self.render_template(
                "vendors/interface-edit.html",
                interface=interface,
                job_profiles=job_profiles(),
            )
        else:
            self._update_vendor_interface_form(interface, request.form)
            session.commit()
            return redirect(
                url_for('VendorManagementView.interface', interface_id=interface.id)
            )

    def _update_vendor_interface_form(self, interface, form):
        """
        Save the supplied vendor interface data to the database and return the
        VendorInterface object.
        """

        if 'folio-data-import-profile-uuid' in form.keys():
            interface.folio_data_import_profile_uuid = form[
                'folio-data-import-profile-uuid'
            ]

        if 'folio-data-import-processing-name' in form.keys():
            interface.folio_data_import_processing_name = form[
                'folio-data-import-processing-name'
            ]

        if 'processing-delay-in-days' in form.keys():
            interface.processing_delay_in_days = int(form['processing-delay-in-days'])

        if 'remote-path' in form.keys():
            interface.remote_path = form['remote-path']

        if 'file-pattern' in form.keys():
            interface.file_pattern = form['file-pattern']

        if 'active' in form.keys():
            interface.active = form['active'] == 'true'

        if 'package-name' in form.keys():
            processing_options = {}
            processing_options['package_name'] = form['package-name']
            processing_options['change_marc'] = []
            processing_options['delete_marc'] = []

            for name, value in form.items():
                if name.startswith('remove-field'):
                    processing_options['delete_marc'].append(value)
                if m := re.match(r'^move-field-from-(\d+)', name):
                    # use the identifier on the "from" form name to determine the
                    # corresponding name for the "to" form name
                    to_name = f"move-field-to-{m.group(1)}"
                    to_value = form.get(to_name)
                    if to_value:
                        processing_options['change_marc'].append(
                            {"from": value, "to": to_value}
                        )

            interface.processing_options = processing_options

        return interface

    @expose("/interface/<int:interface_id>/file", methods=["POST"])
    def file_upload(self, interface_id):
        if "file-upload" not in request.files:
            flash("No file uploaded")
        else:
            file_upload = request.files.get("file-upload", "")
            self._handle_file_upload(interface_id, file_upload)
            flash("File uploaded and queued for processing")
        return redirect(
            url_for("VendorManagementView.interface", interface_id=interface_id)
        )

    def _handle_file_upload(self, interface_id, file_upload):
        session = Session()
        execution_date = datetime.now()
        interface = session.query(VendorInterface).get(interface_id)
        filepath = self._save_file(
            interface.vendor.folio_organization_uuid,
            interface.interface_uuid,
            file_upload,
            execution_date,
        )

        existing_vendor_file = session.scalars(
            select(VendorFile)
            .where(VendorFile.vendor_filename == file_upload.filename)
            .where(VendorFile.vendor_interface_id == interface.id)
        ).first()
        if existing_vendor_file:
            session.delete(existing_vendor_file)
        new_vendor_file = VendorFile(
            created=datetime.now(),
            updated=datetime.now(),
            vendor_interface_id=interface.id,
            vendor_filename=file_upload.filename,
            filesize=os.path.getsize(filepath),
            status=FileStatus.uploaded,
            expected_execution=execution_date,
        )
        session.add(new_vendor_file)
        session.commit()
        self._trigger_processing_dag(new_vendor_file)

    def _save_file(self, vendor_uuid, interface_uuid, file_upload, execution_date):
        path = download_path(vendor_uuid, interface_uuid)
        os.makedirs(path, exist_ok=True)
        filepath = os.path.join(path, file_upload.filename)
        file_upload.save(filepath)
        archive(
            [file_upload.filename],
            path,
            archive_path(vendor_uuid, interface_uuid, execution_date),
        )
        return filepath

    @expose("/file/<int:file_id>/load", methods=["POST"])
    def load_file(self, file_id):
        session = Session()
        file = session.query(VendorFile).get(file_id)

        file.status = FileStatus.loading
        session.commit()
        self._trigger_processing_dag(file)
        flash(f"Requested reload of {file.vendor_filename}")

        return redirect(
            url_for(
                "VendorManagementView.interface", interface_id=file.vendor_interface_id
            )
        )

    def _trigger_processing_dag(self, file):
        dag = trigger_dag(
            'default_data_processor',
            conf={
                "filename": file.vendor_filename,
                "vendor_uuid": file.vendor_interface.vendor.folio_organization_uuid,
                "vendor_interface_uuid": file.vendor_interface.interface_uuid,
                "dataload_profile_uuid": file.vendor_interface.folio_data_import_profile_uuid,
            },
        )
        logger.info(f"Triggered DAG {dag} for {file.vendor_filename}")
