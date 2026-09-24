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
