"""Wrapper de BigQueryInsertJobOperator.

Los DAGs nombran SQL; no lo embeben. El project ID nunca va literal en el SQL:
viaja en la Connection (`gcp_conn_id`), que es lo que distingue los tres
proyectos de GCP.
"""

import os
from pathlib import Path

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def _raiz_sql() -> Path:
    return Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "include" / "sql"


class BuhoBigQueryOperator(BigQueryInsertJobOperator):
    """Ejecuta un archivo de `include/sql/` en BigQuery.

    Falla al construirse —es decir, al parsear el DAG— si el SQL no existe.

    Las labels van dentro de `configuration`, no como parámetro del operator:
    es donde las espera la API de BigQuery, y `BigQueryInsertJobOperator` no
    acepta un kwarg `labels`.
    """

    def __init__(self, *, sql_name: str, gcp_conn_id: str, pipeline: str, **kwargs):
        ruta = _raiz_sql() / f"{sql_name}.sql"
        if not ruta.exists():
            raise FileNotFoundError(f"SQL no encontrado: {ruta}")

        super().__init__(
            configuration={
                "labels": {"pipeline": pipeline},
                "query": {
                    "query": ruta.read_text(encoding="utf-8"),
                    "useLegacySql": False,
                },
            },
            gcp_conn_id=gcp_conn_id,
            **kwargs,
        )
