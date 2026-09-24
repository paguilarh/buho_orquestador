import pytest

from buho.operators.bq import BuhoBigQueryOperator


@pytest.fixture
def raiz_sql(tmp_path, monkeypatch):
    sql_dir = tmp_path / "include" / "sql"
    sql_dir.mkdir(parents=True)
    (sql_dir / "trf_manifiestos.sql").write_text(
        "MERGE trf_wms.manifiestos AS t USING ...", encoding="utf-8"
    )
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
    return sql_dir


def test_carga_el_sql_por_nombre(raiz_sql):
    op = BuhoBigQueryOperator(
        task_id="merge",
        sql_name="trf_manifiestos",
        gcp_conn_id="gcp_logistics",
        pipeline="kpis_wms_manifiestos",
    )

    assert "MERGE trf_wms.manifiestos" in op.configuration["query"]["query"]


def test_desactiva_legacy_sql(raiz_sql):
    op = BuhoBigQueryOperator(
        task_id="merge",
        sql_name="trf_manifiestos",
        gcp_conn_id="gcp_logistics",
        pipeline="kpis_wms_manifiestos",
    )

    assert op.configuration["query"]["useLegacySql"] is False


def test_etiqueta_el_pipeline_para_rastrear_costo(raiz_sql):
    op = BuhoBigQueryOperator(
        task_id="merge",
        sql_name="trf_manifiestos",
        gcp_conn_id="gcp_logistics",
        pipeline="kpis_wms_manifiestos",
    )

    assert op.configuration["labels"] == {"pipeline": "kpis_wms_manifiestos"}


def test_falla_temprano_si_el_sql_no_existe(raiz_sql):
    with pytest.raises(FileNotFoundError, match="no_existe.sql"):
        BuhoBigQueryOperator(
            task_id="merge",
            sql_name="no_existe",
            gcp_conn_id="gcp_logistics",
            pipeline="cualquiera",
        )
