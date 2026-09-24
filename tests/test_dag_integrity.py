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
