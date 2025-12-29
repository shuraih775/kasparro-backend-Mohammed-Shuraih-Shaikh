import pytest
from unittest.mock import MagicMock, call,ANY

from app.services.etl_service import run_etl


def make_engine_with_conn(mocker):
    engine = mocker.MagicMock()
    conn =mocker.MagicMock()

    conn.execute.return_value.mappings.return_value = []

    begin_ctx = MagicMock()
    begin_ctx.__enter__.return_value = conn
    begin_ctx.__exit__.return_value = None

    engine.begin.return_value = begin_ctx
    return engine, conn


def test_run_etl_happy_path(mocker):
    engine, conn = make_engine_with_conn(mocker)

    ingest_cp = mocker.patch(
        "app.services.etl_service.ingest_coinpaprika"
    )
    ingest_cg = mocker.patch(
        "app.services.etl_service.ingest_coingecko"
    )
    ingest_csv = mocker.patch(
        "app.services.etl_service.ingest_csv"
    )

    cp_rows = [{"id": 1}, {"id": 2}]
    cg_rows = [{"id": 3}]
    csv_rows = [{"id": 4}]

    mocker.patch(
        "app.services.etl_service.load_raw_coinpaprika",
        return_value=cp_rows,
    )
    mocker.patch(
        "app.services.etl_service.load_raw_coingecko",
        return_value=cg_rows,
    )
    mocker.patch(
        "app.services.etl_service.load_raw_csv",
        return_value=csv_rows,
    )

    tx_cp = mocker.patch(
        "app.services.etl_service.transform_coinpaprika",
        return_value=True,
    )
    tx_cg = mocker.patch(
        "app.services.etl_service.transform_coingecko",
        return_value=True,
    )
    tx_csv = mocker.patch(
        "app.services.etl_service.transform_csv",
        return_value=True,
    )


    run_etl(engine)

    ingest_cp.assert_called_once_with(engine)
    ingest_cg.assert_called_once_with(engine)
    ingest_csv.assert_called_once_with(engine)

    tx_cp.assert_has_calls([call(conn, row=r, run_id=ANY) for r in cp_rows])
    tx_cg.assert_has_calls([call(conn, row=r, run_id=ANY) for r in cg_rows])
    tx_csv.assert_has_calls([call(conn, row=r, run_id=ANY) for r in csv_rows])


def test_run_etl_fails_on_ingest_error(mocker):
    engine, _ = make_engine_with_conn(mocker)

    mocker.patch(
        "app.services.etl_service.ingest_coinpaprika",
        side_effect=RuntimeError("boom"),
    )

    with pytest.raises(RuntimeError):
        run_etl(engine)


def test_run_etl_fails_on_transform_error(mocker):
    engine, conn = make_engine_with_conn(mocker)

    mocker.patch("app.services.etl_service.ingest_coinpaprika")
    mocker.patch("app.services.etl_service.ingest_coingecko")
    mocker.patch("app.services.etl_service.ingest_csv")

    mocker.patch(
        "app.services.etl_service.load_raw_coinpaprika",
        return_value=[{"id": 1}],
    )
    mocker.patch(
        "app.services.etl_service.load_raw_coingecko",
        return_value=[],
    )
    mocker.patch(
        "app.services.etl_service.load_raw_csv",
        return_value=[],
    )

    mocker.patch(
        "app.services.etl_service.transform_coinpaprika",
        side_effect=ValueError("bad row"),
    )

    with pytest.raises(ValueError):
        run_etl(engine)
