"""Latido horario — valida que scheduler, alertas y despliegue funcionan.

Para probar la cadena de alertas sin romper nada:
    airflow variables set healthcheck_forzar_fallo true
"""

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from buho.defaults import TIMEZONE, buho_default_args


def _latido():
    forzar = Variable.get("healthcheck_forzar_fallo", default_var="false")
    if forzar.lower() == "true":
        raise RuntimeError("Fallo forzado para validar la cadena de alertas")
    print("healthcheck OK")


with DAG(
    dag_id="healthcheck",
    default_args=buho_default_args(owner="paguilar", area="logistics"),
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 9, 23, tz=TIMEZONE),
    catchup=False,
    tags=["logistics", "infra"],
    description="Latido horario — valida scheduler, alertas y despliegue.",
) as dag:
    PythonOperator(task_id="latido", python_callable=_latido)
