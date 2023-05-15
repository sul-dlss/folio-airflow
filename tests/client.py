import pathlib

import pytest
from airflow.www import app as application

from conftest import root_directory
from libsys_airflow.plugins.vendor_app.vendors import VendorManagementView


@pytest.fixture
def test_airflow_client():
    """
    A test fixture to start up the Airflow test app with the Vendor Management plugin, and return
    a client for it for interacting with the application at the HTTP level.
    """
    templates_folder = f"{root_directory}/libsys_airflow/plugins/vendor_app/templates"

    app = application.create_app(testing=True)
    app.appbuilder.add_view(
        VendorManagementView, "Vendors", category="Vendor Management"
    )
    app.blueprints['VendorManagementView'].template_folder = templates_folder

    with app.test_client() as client:
        yield client