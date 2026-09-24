# Orquestador Airflow — Plan de implementación Fase 0

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dejar un Airflow 2.10.5 self-hosted en producción con un DAG de latido horario cuyo fallo llega a Slack y correo, CI que bloquea merges de DAGs inválidos, y despliegue automático al hacer merge a `main`.

**Architecture:** Repo nuevo `buho_orchestration`, separado del monorepo. Docker Compose sobre un droplet de DigitalOcean 4 GB: Postgres 16 + Airflow webserver + scheduler con LocalExecutor, detrás de Caddy con TLS automático. Tres Airflow Connections, una por proyecto de GCP. Los componentes reutilizables (`defaults`, `callbacks`, `operators/bq`) se construyen con TDD antes de que exista el primer DAG real.

**Tech Stack:** Airflow 2.10.5 · Python 3.11 · Postgres 16 · Docker Compose · Caddy 2 · pytest · GitHub Actions

**Diseño de referencia:** `docs/2026-09-23-orquestador-airflow-design.md`

---

## Restricciones que condicionan el plan

1. **Airflow no corre nativamente en Windows.** Todos los tests se ejecutan dentro del contenedor:
   `docker compose run --rm airflow-test pytest tests/ -v`. No hay ejecución local fuera de Docker.
2. **No hay `gcloud` en la máquina de desarrollo.** Las tareas 8 y 9 las ejecuta el usuario desde
   Cloud Shell o tras instalar el SDK. El plan da los comandos exactos.
3. **Commits:** este repo hereda la política de `buho_data_pipelines` — los comandos de commit
   se pasan al usuario, el agente no commitea por su cuenta.
4. **Secrets:** el agente genera `.env.example` con las llaves vacías. El usuario llena los valores.

---

## Estructura de archivos

| Archivo | Responsabilidad |
|---|---|
| `Dockerfile` | Imagen de Airflow 2.10.5 + providers, resueltos por constraints |
| `requirements.txt` | Providers y pytest, sin pines (los pone el constraint file) |
| `infra/docker-compose.yml` | Postgres, init, webserver, scheduler, test runner, Caddy |
| `infra/Caddyfile` | Reverse proxy + TLS automático |
| `infra/bootstrap.sh` | Provisión desde cero de un droplet vacío |
| `plugins/buho/callbacks.py` | Notificación de fallo — único punto de Slack + correo |
| `plugins/buho/defaults.py` | `buho_default_args()` — política común de todos los DAGs |
| `plugins/buho/operators/bq.py` | `BuhoBigQueryOperator` — carga SQL de `include/sql/` |
| `dags/logistics/dag_healthcheck.py` | Latido horario que valida la cadena completa |
| `tests/test_dag_integrity.py` | Guardián del estándar — corre en CI |
| `tests/test_defaults.py` · `test_callbacks.py` · `test_bq_operator.py` | Unitarios de los componentes |
| `.github/workflows/ci.yml` | Lint + tests en cada PR |
| `.github/workflows/deploy.yml` | Merge a `main` → despliegue por SSH |

`callbacks.py` no importa de `defaults.py`, y `defaults.py` importa de `callbacks.py`. La
dirección es una sola: si se invierte, hay ciclo de imports.

---

## Task 1: Fundar el repo y la estructura base

**Files:**
- Create: `README.md`, `.gitignore`, `requirements.txt`, `Dockerfile`
- Create: `plugins/buho/__init__.py`, `plugins/buho/operators/__init__.py`
- Create: `tests/__init__.py`, `include/sql/.gitkeep`, `dags/logistics/.gitkeep`, `dags/media/.gitkeep`

- [ ] **Step 1: Crear el repo local**

```bash
mkdir buho_orchestration && cd buho_orchestration
git init
mkdir -p plugins/buho/operators dags/logistics dags/media include/sql tests infra .github/workflows
touch plugins/buho/__init__.py plugins/buho/operators/__init__.py tests/__init__.py
touch include/sql/.gitkeep dags/logistics/.gitkeep dags/media/.gitkeep
```

- [ ] **Step 2: Escribir `.gitignore`**

```gitignore
.env
*.pyc
__pycache__/
.pytest_cache/
logs/
secrets/
*.json
!package.json
.agents/
```

La línea `*.json` es deliberada: evita que una llave de service account entre al repo por
descuido. `include/sql/` no contiene JSON, así que no estorba.

- [ ] **Step 3: Escribir `requirements.txt`**

```
apache-airflow-providers-google
apache-airflow-providers-slack
pytest
```

Sin versiones: las fija el constraint file de Airflow en el Dockerfile. Pinear a mano aquí
produce conflictos con las dependencias que ya trae la imagen base.

- [ ] **Step 4: Escribir `Dockerfile`**

```dockerfile
FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
```

- [ ] **Step 5: Escribir `README.md`**

```markdown
# buho_orchestration

Orquestación de los pipelines de datos de Buho — Airflow 2.10.5 self-hosted.

Diseño: `buho_data_pipelines/standards/specs/2026-09-23-orquestador-airflow-design.md`

## Desarrollo local

Airflow no corre nativamente en Windows. Todo pasa por Docker:

    docker compose -f infra/docker-compose.yml run --rm airflow-test pytest tests/ -v

## Despliegue

Merge a `main` dispara `.github/workflows/deploy.yml`.
```

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "chore: estructura base del repo de orquestación"
```

---

## Task 2: `callbacks.py` — notificación de fallo

**Files:**
- Create: `plugins/buho/callbacks.py`
- Test: `tests/test_callbacks.py`

- [ ] **Step 1: Escribir los tests que fallan**

`tests/test_callbacks.py`:

```python
from unittest.mock import MagicMock, patch

import pendulum

from buho.callbacks import notify_failure


def _contexto_falso():
    ti = MagicMock()
    ti.dag_id = "healthcheck"
    ti.task_id = "latido"
    ti.log_url = "https://airflow.example.com/log"
    return {"task_instance": ti, "logical_date": pendulum.datetime(2026, 9, 23)}


@patch("buho.callbacks.send_email")
@patch("buho.callbacks.SlackWebhookHook")
def test_notifica_a_slack_y_correo(mock_slack, mock_email):
    notify_failure(_contexto_falso())

    assert mock_slack.return_value.send.called
    assert mock_email.called


@patch("buho.callbacks.send_email")
@patch("buho.callbacks.SlackWebhookHook")
def test_el_mensaje_incluye_dag_task_y_log(mock_slack, mock_email):
    notify_failure(_contexto_falso())

    texto = mock_slack.return_value.send.call_args.kwargs["text"]
    assert "healthcheck" in texto
    assert "latido" in texto
    assert "https://airflow.example.com/log" in texto


@patch("buho.callbacks.send_email")
@patch("buho.callbacks.SlackWebhookHook")
def test_si_slack_falla_el_correo_igual_sale(mock_slack, mock_email):
    mock_slack.return_value.send.side_effect = RuntimeError("slack caido")

    notify_failure(_contexto_falso())

    assert mock_email.called


@patch("buho.callbacks.send_email")
@patch("buho.callbacks.SlackWebhookHook")
def test_si_el_correo_falla_no_revienta_la_tarea(mock_slack, mock_email):
    mock_email.side_effect = RuntimeError("smtp caido")

    notify_failure(_contexto_falso())  # no debe levantar excepción
```

Los dos últimos tests son el corazón del componente: un canal de alertas caído no puede
tumbar la tarea ni silenciar al otro canal.

- [ ] **Step 2: Correr los tests para verificar que fallan**

```bash
docker compose -f infra/docker-compose.yml run --rm airflow-test pytest tests/test_callbacks.py -v
```

Esperado: FAIL con `ModuleNotFoundError: No module named 'buho.callbacks'`

> **`infra/docker-compose.yml` no existe hasta la Task 5.** Hasta entonces, correr los tests
> con la imagen construida desde el Dockerfile de la Task 1 —que sí trae `pytest` y los
> providers instalados, a diferencia de la imagen base:
>
> ```bash
> docker build -t buho-orchestration:dev .
> docker run --rm -v "$PWD:/opt/airflow" -w /opt/airflow >   -e PYTHONPATH=/opt/airflow/plugins >   -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow-dev.db >   buho-orchestration:dev pytest tests/test_callbacks.py -v
> ```
>
> Lo mismo aplica a las tareas 3 y 4.

- [ ] **Step 3: Escribir la implementación mínima**

`plugins/buho/callbacks.py`:

```python
"""Notificación de fallo de tareas — único punto de Slack y correo.

Ningún DAG debe reimplementar notificación: todos enganchan `notify_failure`
vía `buho.defaults.buho_default_args`.
"""

import logging

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.email import send_email

log = logging.getLogger(__name__)

SLACK_CONN_ID = "slack_alertas"
ALERT_EMAILS = ["paguilar@buhoms.com"]


def _construir_mensaje(context) -> str:
    ti = context["task_instance"]
    return (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"Fecha lógica: {context['logical_date']}\n"
        f"Log: {ti.log_url}"
    )


def notify_failure(context) -> None:
    """Callback de fallo. Cada canal falla de forma aislada: que Slack esté
    caído no puede impedir el correo, y ningún fallo de notificación puede
    propagarse a la tarea."""
    mensaje = _construir_mensaje(context)

    try:
        SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send(
            text=f":rotating_light: Fallo en Airflow\n{mensaje}"
        )
    except Exception:
        log.exception("No se pudo notificar a Slack")

    try:
        send_email(
            to=ALERT_EMAILS,
            subject=f"[Airflow] Fallo en {context['task_instance'].dag_id}",
            html_content=mensaje.replace("\n", "<br>"),
        )
    except Exception:
        log.exception("No se pudo notificar por correo")
```

- [ ] **Step 4: Correr los tests para verificar que pasan**

```bash
docker compose -f infra/docker-compose.yml run --rm airflow-test pytest tests/test_callbacks.py -v
```

Esperado: 4 passed

- [ ] **Step 5: Commit**

```bash
git add plugins/buho/callbacks.py tests/test_callbacks.py
git commit -m "feat: callback de fallo con Slack y correo aislados entre sí"
```

---

## Task 3: `defaults.py` — política común de DAGs

**Files:**
- Create: `plugins/buho/defaults.py`
- Test: `tests/test_defaults.py`

- [ ] **Step 1: Escribir los tests que fallan**

`tests/test_defaults.py`:

```python
from datetime import timedelta

import pytest

from buho.callbacks import notify_failure
from buho.defaults import TIMEZONE, buho_default_args


def test_incluye_el_callback_de_fallo():
    args = buho_default_args(owner="paguilar", area="media")

    assert args["on_failure_callback"] is notify_failure


def test_define_reintentos_con_espera():
    args = buho_default_args(owner="paguilar", area="media")

    assert args["retries"] == 2
    assert args["retry_delay"] == timedelta(minutes=5)


def test_propaga_el_owner():
    args = buho_default_args(owner="clopez", area="logistics")

    assert args["owner"] == "clopez"


def test_rechaza_area_desconocida():
    with pytest.raises(ValueError, match="area inválida"):
        buho_default_args(owner="paguilar", area="ventas")


def test_timezone_es_monterrey():
    assert TIMEZONE.name == "America/Monterrey"
```

El último test fija por código la decisión de §3.8 del diseño. Si alguien cambia el timezone
sin discutirlo, el CI lo detiene.

- [ ] **Step 2: Correr los tests para verificar que fallan**

```bash
docker compose -f infra/docker-compose.yml run --rm airflow-test pytest tests/test_defaults.py -v
```

Esperado: FAIL con `ModuleNotFoundError: No module named 'buho.defaults'`

- [ ] **Step 3: Escribir la implementación mínima**

`plugins/buho/defaults.py`:

```python
"""Política común de todos los DAGs de Buho.

Cambiar aquí la política de reintentos la cambia en todos los pipelines.
"""

from datetime import timedelta

import pendulum

from buho.callbacks import notify_failure

TIMEZONE = pendulum.timezone("America/Monterrey")

AREAS_VALIDAS = ("media", "logistics")


def buho_default_args(owner: str, area: str) -> dict:
    """`default_args` estándar. El área se valida aquí para que un typo
    en un tag no pase silenciosamente al DAG."""
    if area not in AREAS_VALIDAS:
        raise ValueError(f"area inválida: {area!r} — debe ser una de {AREAS_VALIDAS}")

    return {
        "owner": owner,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
        "depends_on_past": False,
    }
```

- [ ] **Step 4: Correr los tests para verificar que pasan**

```bash
docker compose -f infra/docker-compose.yml run --rm airflow-test pytest tests/test_defaults.py -v
```

Esperado: 5 passed

- [ ] **Step 5: Commit**

```bash
git add plugins/buho/defaults.py tests/test_defaults.py
git commit -m "feat: default_args estándar con timezone Monterrey"
```

---

## Task 4: `BuhoBigQueryOperator`

**Files:**
- Create: `plugins/buho/operators/bq.py`
- Test: `tests/test_bq_operator.py`

- [ ] **Step 1: Escribir los tests que fallan**

`tests/test_bq_operator.py`:

```python
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
```

El último test es el que justifica el wrapper: un SQL mal nombrado revienta al **parsear el
DAG**, no 6 horas después cuando la tarea corre en producción.

- [ ] **Step 2: Correr los tests para verificar que fallan**

```bash
docker compose -f infra/docker-compose.yml run --rm airflow-test pytest tests/test_bq_operator.py -v
```

Esperado: FAIL con `ModuleNotFoundError: No module named 'buho.operators.bq'`

- [ ] **Step 3: Escribir la implementación mínima**

`plugins/buho/operators/bq.py`:

```python
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
```

- [ ] **Step 4: Correr los tests para verificar que pasan**

```bash
docker compose -f infra/docker-compose.yml run --rm airflow-test pytest tests/test_bq_operator.py -v
```

Esperado: 4 passed

- [ ] **Step 5: Commit**

```bash
git add plugins/buho/operators/bq.py tests/test_bq_operator.py
git commit -m "feat: BuhoBigQueryOperator que falla al parsear si el SQL no existe"
```

---

## Task 5: Docker Compose y entorno de ejecución

**Files:**
- Create: `infra/docker-compose.yml`, `infra/Caddyfile`, `.env.example`

- [ ] **Step 1: Escribir `.env.example` con las llaves vacías**

```bash
# Dominio donde vive el UI — Caddy pide el certificado para este nombre
AIRFLOW_DOMAIN=

# Postgres de metadata
POSTGRES_PASSWORD=

# Cifrado de Connections y Variables.
# Generar con:
#   docker run --rm apache/airflow:2.10.5-python3.11 python -c \
#     "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
FERNET_KEY=

# Usuario admin inicial del UI
AIRFLOW_ADMIN_USER=
AIRFLOW_ADMIN_PASSWORD=

# SMTP para las alertas por correo
SMTP_HOST=
SMTP_PORT=587
SMTP_USER=
SMTP_PASSWORD=
SMTP_MAIL_FROM=
```

**El usuario llena estos valores a mano.** El agente no los escribe ni los lee.

- [ ] **Step 2: Escribir `infra/docker-compose.yml`**

```yaml
x-airflow-common: &airflow-common
  build:
    context: ..
    dockerfile: Dockerfile
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DEFAULT_TIMEZONE: America/Monterrey
    AIRFLOW__CORE__FERNET_KEY: ${FERNET_KEY}
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${POSTGRES_PASSWORD}@postgres:5432/airflow
    AIRFLOW__WEBSERVER__BASE_URL: https://${AIRFLOW_DOMAIN}
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "false"
    AIRFLOW__SMTP__SMTP_HOST: ${SMTP_HOST}
    AIRFLOW__SMTP__SMTP_PORT: ${SMTP_PORT}
    AIRFLOW__SMTP__SMTP_USER: ${SMTP_USER}
    AIRFLOW__SMTP__SMTP_PASSWORD: ${SMTP_PASSWORD}
    AIRFLOW__SMTP__SMTP_MAIL_FROM: ${SMTP_MAIL_FROM}
    AIRFLOW__SMTP__SMTP_STARTTLS: "true"
  volumes:
    - ../dags:/opt/airflow/dags
    - ../plugins:/opt/airflow/plugins
    - ../include:/opt/airflow/include
    - ../tests:/opt/airflow/tests
    - airflow-logs:/opt/airflow/logs
    - /opt/airflow/secrets:/opt/airflow/secrets:ro
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
    restart: unless-stopped

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create \
          --username "${AIRFLOW_ADMIN_USER}" \
          --password "${AIRFLOW_ADMIN_PASSWORD}" \
          --firstname Admin --lastname Buho \
          --role Admin --email "${SMTP_MAIL_FROM}" || true
    restart: on-failure

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    expose:
      - "8080"
    restart: unless-stopped

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: unless-stopped

  airflow-test:
    <<: *airflow-common
    entrypoint: ""
    command: ["pytest", "tests/", "-v"]
    environment:
      <<: *airflow-env
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////tmp/airflow-test.db
      PYTHONPATH: /opt/airflow/plugins
    depends_on: []
    profiles: ["test"]

  caddy:
    image: caddy:2
    ports:
      - "80:80"
      - "443:443"
    environment:
      AIRFLOW_DOMAIN: ${AIRFLOW_DOMAIN}
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy-data:/data
    depends_on:
      - airflow-webserver
    restart: unless-stopped

volumes:
  postgres-data:
  airflow-logs:
  caddy-data:
```

**Desviación deliberada del diseño §4:** el diseño dice que las credenciales SMTP van en
Airflow Connections. Aquí van como variables `AIRFLOW__SMTP__*` porque es lo que lee
`airflow.utils.email.send_email` sin configuración extra; una Connection exigiría montar
`smtp_default` y no aporta nada. El webhook de Slack sí va como Connection, tal como dice
el diseño.

El servicio `airflow-test` usa SQLite y no depende de Postgres: los tests son unitarios y no
deben necesitar la base de metadata levantada. Está bajo `profiles: ["test"]` para que
`docker compose up` en el droplet no lo arranque.

El webserver solo hace `expose`, no `ports`: **el único puerto abierto al exterior es el de
Caddy.** Si se publicara 8080, el UI quedaría accesible por HTTP sin certificado.

- [ ] **Step 3: Escribir `infra/Caddyfile`**

```
{$AIRFLOW_DOMAIN} {
	reverse_proxy airflow-webserver:8080

	header {
		Strict-Transport-Security "max-age=31536000;"
		X-Content-Type-Options "nosniff"
		X-Frame-Options "DENY"
	}
}
```

- [ ] **Step 4: Verificar que el stack de test levanta**

```bash
cp .env.example .env    # llenar los valores antes de continuar
docker compose -f infra/docker-compose.yml --profile test run --rm airflow-test pytest tests/ -v
```

Esperado: los 13 tests de las tareas 2, 3 y 4 en verde.

- [ ] **Step 5: Commit**

```bash
git add infra/docker-compose.yml infra/Caddyfile .env.example
git commit -m "feat: stack de Docker Compose con Caddy y runner de tests"
```

---

## Task 6: `test_dag_integrity.py` — el guardián del estándar

**Files:**
- Create: `tests/test_dag_integrity.py`

Este test se escribe **antes** del primer DAG. Con `dags/` vacío pasa trivialmente; su valor
aparece en la Task 7, cuando empieza a tener algo que vigilar.

- [ ] **Step 1: Escribir el test**

`tests/test_dag_integrity.py`:

```python
"""Guardián del estándar de DAGs.

Las convenciones del diseño dejan de ser un documento que se ignora y pasan a
ser un test que bloquea el merge.
"""

import pytest
from airflow.models import DagBag

AREAS_VALIDAS = {"media", "logistics"}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_ningun_dag_tiene_error_de_import(dagbag):
    assert not dagbag.import_errors, f"DAGs que no importan: {dagbag.import_errors}"


def test_todo_dag_declara_su_area_en_tags(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert AREAS_VALIDAS & set(dag.tags), (
            f"{dag_id} no declara área — los tags deben incluir "
            f"uno de {sorted(AREAS_VALIDAS)}"
        )


def test_toda_tarea_tiene_callback_de_fallo(dagbag):
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            assert task.on_failure_callback is not None, (
                f"{dag_id}.{task.task_id} sin on_failure_callback — "
                f"usar buho_default_args()"
            )


def test_catchup_desactivado(dagbag):
    for dag_id, dag in dagbag.dags.items():
        if "catchup-ok" in dag.tags:
            continue
        assert dag.catchup is False, (
            f"{dag_id} tiene catchup activo — si es deliberado, "
            f"agregar el tag 'catchup-ok' y documentar por qué"
        )


def test_todo_dag_tiene_descripcion(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.description, f"{dag_id} sin description"
```

`DagBag` detecta ciclos como error de import, así que el primer test los cubre sin necesidad
de un test aparte.

- [ ] **Step 2: Correr el test — pasa en vacío**

```bash
docker compose -f infra/docker-compose.yml --profile test run --rm airflow-test pytest tests/test_dag_integrity.py -v
```

Esperado: 5 passed (sin DAGs que revisar todavía)

- [ ] **Step 3: Commit**

```bash
git add tests/test_dag_integrity.py
git commit -m "test: guardián de integridad de DAGs"
```

---

## Task 7: DAG de healthcheck

**Files:**
- Create: `dags/logistics/dag_healthcheck.py`

- [ ] **Step 1: Escribir el DAG**

```python
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
```

- [ ] **Step 2: Correr el guardián contra el DAG nuevo**

```bash
docker compose -f infra/docker-compose.yml --profile test run --rm airflow-test pytest tests/test_dag_integrity.py -v
```

Esperado: 5 passed. Ahora sí está revisando algo: si falta el tag de área, el callback o la
descripción, el test lo detiene.

- [ ] **Step 3: Verificar que el guardián realmente muerde**

Comentar temporalmente la línea `tags=["logistics", "infra"],` y correr de nuevo:

```bash
docker compose -f infra/docker-compose.yml --profile test run --rm airflow-test pytest tests/test_dag_integrity.py -v
```

Esperado: FAIL con `healthcheck no declara área`. **Restaurar la línea** y confirmar verde.

Un guardián que nunca se vio fallar no es un guardián: es una suposición.

- [ ] **Step 4: Commit**

```bash
git add dags/logistics/dag_healthcheck.py
git commit -m "feat: DAG de healthcheck horario"
```

---

## Task 8: Provisionar el droplet

**Files:**
- Create: `infra/bootstrap.sh`

Esta tarea la ejecuta el usuario. Requiere una cuenta de DigitalOcean y control del DNS del
dominio.

- [ ] **Step 1: Crear el droplet**

En el panel de DigitalOcean: Ubuntu 24.04 LTS, plan **Basic Regular 4 GB / 2 vCPU**, región
`nyc3` o `sfo3`, con llave SSH. Activar **Backups** en el momento de crearlo.

- [ ] **Step 2: Apuntar el DNS**

Crear un registro `A` del subdominio elegido hacia la IP pública del droplet. Verificar antes
de seguir, porque Caddy no emite certificado si el DNS no resuelve:

```bash
dig +short <subdominio>
```

Esperado: la IP del droplet.

- [ ] **Step 3: Escribir `infra/bootstrap.sh`**

```bash
#!/usr/bin/env bash
# Provisión de un droplet vacío hasta dejar Airflow corriendo.
# Uso:  ./bootstrap.sh <url-del-repo>
set -euo pipefail

REPO_URL="${1:?Falta la URL del repo}"
DESTINO=/opt/buho_orchestration

echo "==> Instalando Docker"
apt-get update
apt-get install -y ca-certificates curl git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

echo "==> Preparando el directorio de secretos"
mkdir -p /opt/airflow/secrets
chmod 700 /opt/airflow/secrets

echo "==> Clonando el repo"
git clone "$REPO_URL" "$DESTINO"

echo "==> Falta un paso manual:"
echo "    1. cp $DESTINO/.env.example $DESTINO/.env"
echo "    2. llenar los valores de $DESTINO/.env"
echo "    3. copiar las 3 llaves SA a /opt/airflow/secrets/ con chmod 600"
echo "    4. cd $DESTINO && docker compose -f infra/docker-compose.yml up -d"
```

El script **no** arranca el stack: se detiene antes de los pasos que requieren secretos. Un
bootstrap que arranca con un `.env` vacío falla de forma confusa.

- [ ] **Step 4: Ejecutar el bootstrap**

```bash
ssh root@<ip-del-droplet>
curl -fsSL <url-raw-del-bootstrap.sh> -o bootstrap.sh
chmod +x bootstrap.sh
./bootstrap.sh <url-del-repo>
```

- [ ] **Step 5: Llenar `.env` y levantar**

```bash
cd /opt/buho_orchestration
cp .env.example .env
nano .env                 # llenar a mano — incluida la FERNET_KEY generada
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
```

Esperado: `postgres`, `airflow-webserver`, `airflow-scheduler` y `caddy` en estado `Up`.
`airflow-init` en `Exited (0)`.

- [ ] **Step 6: Verificar el UI con TLS**

Abrir `https://<subdominio>` en el navegador. Esperado: pantalla de login de Airflow con
candado válido, y el DAG `healthcheck` visible en la lista.

- [ ] **Step 7: Commit**

```bash
git add infra/bootstrap.sh
git commit -m "feat: bootstrap de provisión del droplet"
```

---

## Task 9: Service accounts y Connections

Esta tarea la ejecuta el usuario. Requiere `gcloud` — desde Cloud Shell o tras instalar el SDK.
**No hay `gcloud` en la máquina de desarrollo actual.**

- [ ] **Step 1: Crear una SA por proyecto**

Repetir para los tres proyectos, cambiando `PROYECTO` y `NOMBRE`:

| Proyecto | Nombre de la SA | Connection en Airflow |
|---|---|---|
| `arquim` | `airflow-media` | `gcp_media` |
| `arquimides-441521` | `airflow-logistics` | `gcp_logistics` |
| `b-materials` | `airflow-entradas-salidas` | `gcp_entradas_salidas` |

```bash
PROYECTO=arquim
NOMBRE=airflow-media

gcloud iam service-accounts create "$NOMBRE" \
  --project="$PROYECTO" \
  --display-name="Airflow self-hosted — $PROYECTO"
```

- [ ] **Step 2: Otorgar los roles mínimos**

```bash
SA="${NOMBRE}@${PROYECTO}.iam.gserviceaccount.com"

# Ejecutar queries y escribir en BigQuery
gcloud projects add-iam-policy-binding "$PROYECTO" \
  --member="serviceAccount:${SA}" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding "$PROYECTO" \
  --member="serviceAccount:${SA}" --role="roles/bigquery.dataEditor"

# Disparar Cloud Run Jobs
gcloud projects add-iam-policy-binding "$PROYECTO" \
  --member="serviceAccount:${SA}" --role="roles/run.developer"
```

**No otorgar `roles/editor` ni `roles/owner`.** El diseño (§3.7) depende de que cada llave
esté acotada; un rol amplio anula la razón de tener tres SA en vez de una.

- [ ] **Step 3: Generar y colocar las llaves**

```bash
gcloud iam service-accounts keys create "${NOMBRE}.json" \
  --iam-account="$SA" --project="$PROYECTO"
```

Copiarlas al droplet y asegurar permisos:

```bash
scp airflow-*.json root@<ip>:/opt/airflow/secrets/
ssh root@<ip> 'chmod 600 /opt/airflow/secrets/*.json'
```

Borrar las copias locales después de subirlas.

- [ ] **Step 4: Crear las tres Connections de GCP**

En el droplet, una por proyecto:

```bash
cd /opt/buho_orchestration
docker compose -f infra/docker-compose.yml exec airflow-scheduler \
  airflow connections add gcp_media \
    --conn-type google_cloud_platform \
    --conn-extra '{"key_path":"/opt/airflow/secrets/airflow-media.json","project":"arquim"}'
```

Repetir para `gcp_logistics` (`arquimides-441521`) y `gcp_entradas_salidas` (`b-materials`).

- [ ] **Step 5: Crear la Connection de Slack**

```bash
docker compose -f infra/docker-compose.yml exec airflow-scheduler \
  airflow connections add slack_alertas \
    --conn-type slackwebhook \
    --conn-password '<webhook-url-de-slack>'
```

El `conn_id` debe ser exactamente `slack_alertas`: es el valor de `SLACK_CONN_ID` en
`callbacks.py`.

- [ ] **Step 6: Verificar que las cuatro Connections existen**

```bash
docker compose -f infra/docker-compose.yml exec airflow-scheduler \
  airflow connections list --output table
```

Esperado: `gcp_media`, `gcp_logistics`, `gcp_entradas_salidas`, `slack_alertas`.

---

## Task 10: CI y despliegue automático

**Files:**
- Create: `.github/workflows/ci.yml`, `.github/workflows/deploy.yml`

- [ ] **Step 1: Escribir `ci.yml`**

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Construir la imagen
        run: docker build -t buho-orchestration:ci .

      - name: Correr los tests
        run: |
          docker run --rm \
            -v "$PWD/dags:/opt/airflow/dags" \
            -v "$PWD/plugins:/opt/airflow/plugins" \
            -v "$PWD/include:/opt/airflow/include" \
            -v "$PWD/tests:/opt/airflow/tests" \
            -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
            -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow-ci.db             -e PYTHONPATH=/opt/airflow/plugins \
            buho-orchestration:ci \
            pytest tests/ -v
```

- [ ] **Step 2: Escribir `deploy.yml`**

```yaml
name: Deploy

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Desplegar por SSH
        uses: appleboy/ssh-action@v1.0.3
        with:
          host: ${{ secrets.DROPLET_HOST }}
          username: root
          key: ${{ secrets.DROPLET_SSH_KEY }}
          script: |
            set -euo pipefail
            cd /opt/buho_orchestration
            git pull --ff-only
            docker compose -f infra/docker-compose.yml up -d --build
            docker compose -f infra/docker-compose.yml ps
```

Los secretos `DROPLET_HOST` y `DROPLET_SSH_KEY` se cargan en Settings → Secrets del repo de
GitHub. **El usuario los pone; el agente no los ve.**

- [ ] **Step 3: Encadenar deploy después de CI**

Cambiar en `deploy.yml`:

```yaml
on:
  workflow_run:
    workflows: ["CI"]
    types: [completed]
    branches: [main]

jobs:
  deploy:
    if: ${{ github.event.workflow_run.conclusion == 'success' }}
    runs-on: ubuntu-latest
```

Así un DAG que no pasa el guardián de integridad nunca llega al droplet.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml .github/workflows/deploy.yml
git commit -m "ci: tests en PR y despliegue automático tras CI verde"
```

- [ ] **Step 5: Verificar el despliegue de punta a punta**

Hacer un cambio trivial (una línea en `README.md`), abrir PR, mergear, y confirmar:

1. El workflow **CI** corre y queda verde.
2. El workflow **Deploy** arranca solo después.
3. En el droplet: `git log -1` muestra el commit nuevo.

---

## Task 11: Validación de la cadena de alertas

Esta es la tarea que cierra la fase. Sin ella, el criterio de salida no está demostrado.

- [ ] **Step 1: Confirmar que el healthcheck corre solo**

Esperar al siguiente cambio de hora y revisar en el UI que `healthcheck` tenga una corrida
exitosa. O disparar a mano:

```bash
docker compose -f infra/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger healthcheck
```

Esperado: la corrida aparece en verde.

- [ ] **Step 2: Forzar el fallo**

```bash
docker compose -f infra/docker-compose.yml exec airflow-scheduler \
  airflow variables set healthcheck_forzar_fallo true

docker compose -f infra/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger healthcheck
```

- [ ] **Step 3: Confirmar que llegan las dos alertas**

`buho_default_args` define `retries: 2` con `retry_delay` de 5 minutos, así que **el callback
dispara ~10 minutos después del primer intento**, no de inmediato. Es el comportamiento
correcto, pero hay que esperarlo.

Verificar:
1. Llega el mensaje al canal de Slack, con dag_id, task_id y link al log.
2. Llega el correo a `paguilar@buhoms.com`.
3. El link del log abre la corrida fallida en el UI.

- [ ] **Step 4: Restaurar**

```bash
docker compose -f infra/docker-compose.yml exec airflow-scheduler \
  airflow variables set healthcheck_forzar_fallo false

docker compose -f infra/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger healthcheck
```

Esperado: corrida en verde.

- [ ] **Step 5: Verificar el backup**

Confirmar en el panel de DigitalOcean que Backups está activo y tiene al menos un snapshot.

Respaldar la metadata:

```bash
docker compose -f infra/docker-compose.yml exec postgres \
  pg_dump -U airflow airflow > /opt/airflow/backup_$(date +%F).sql
```

Esperado: archivo no vacío. Automatizar este `pg_dump` hacia GCS queda como tarea de fase 1.

---

## Criterio de salida de la Fase 0

Todo lo siguiente debe ser cierto y estar demostrado:

- [ ] `https://<subdominio>` sirve el UI de Airflow con certificado válido
- [ ] El DAG `healthcheck` corre solo cada hora
- [ ] Un fallo forzado llega a Slack **y** a correo, con link al log que funciona
- [ ] Las cuatro Connections existen: 3 de GCP + Slack
- [ ] Un PR con un DAG inválido es bloqueado por CI
- [ ] Un merge a `main` se despliega solo al droplet, y solo si CI pasó
- [ ] Backups del droplet activos con al menos un snapshot

## Lo que esta fase deliberadamente NO hace

- **No migra ningún pipeline.** Eso es fase 1 en adelante, con su propio plan.
- **No toca Composer.** Sigue corriendo y facturando hasta la fase 2.
- **No construye la DAG factory.** Se extrae en fase 4, si el patrón aparece (diseño §4).
- **No automatiza el `pg_dump` a GCS.** Queda para fase 1.

## Pendientes bloqueantes heredados del diseño

Ninguno bloquea esta fase, pero sí las siguientes:

1. **Costo real de Composer** en la facturación de `b-materials` — bloquea el caso de negocio
   y la presentación a dirección (diseño §6).
2. **Idempotencia del EL de `wms-in-out-analytics`** — decide cómo se hace el shadow-run
   (diseño §5, fase 1).
