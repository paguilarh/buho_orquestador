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
