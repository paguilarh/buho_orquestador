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
