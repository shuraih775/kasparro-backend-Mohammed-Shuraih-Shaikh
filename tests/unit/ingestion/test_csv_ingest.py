import pytest
from sqlalchemy import create_engine, select, func
from unittest.mock import MagicMock

from app.schemas.tables import metadata, raw_csv
from app.ingestion.csv_source import ingest_csv


def test_ingest_csv_inserts_and_dedupes(mocker):
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)

    csv_data = """Symbol,Date,Price
AAPL,2024-01-01,100
AAPL,2024-01-01,100
"""

    mock_response = MagicMock()
    mock_response.text = csv_data
    mock_response.raise_for_status.return_value = None

    mocker.patch(
        "app.ingestion.csv_source.csv_http.get",
        return_value=mock_response,
    )

    inserted = ingest_csv(engine)

    with engine.connect() as conn:
        count = conn.execute(
            select(func.count()).select_from(raw_csv)
        ).scalar()

    assert inserted == 1
    assert count == 1
