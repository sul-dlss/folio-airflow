import pytest

from unittest.mock import MagicMock

from libsys_airflow.plugins.exports.harvest import (
    calculate_batches,
)


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        if args[0].startswith("/source-storage/source-records?limit=1"):
            return {"totalRecords": 50000}

    mock_client = MagicMock()
    mock_client.folio_get = mock_get
    return mock_client


def test_calculate_batches(mock_folio_client):
    batches = calculate_batches("5000", mock_folio_client)

    assert batches == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
