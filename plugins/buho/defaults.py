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
