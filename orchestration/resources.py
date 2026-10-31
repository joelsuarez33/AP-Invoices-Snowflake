"""Recurso de Snowflake para Dagster.

Envuelve load.connect(): la misma autenticacion key-pair que usan los
scripts del pipeline, sin una segunda implementacion.

No tiene campos de configuracion a proposito. Las credenciales salen del
entorno (.env en local, GitHub Secrets en CI) y nunca pasan por la config de
Dagster, que queda guardada en el historial de cada corrida.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg

import config
import load


class SnowflakeResource(dg.ConfigurableResource):
    """Una conexion por asset: cada resume del warehouse factura 60 segundos minimo."""

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        connection = load.connect()
        try:
            yield connection
        finally:
            connection.close()

    def objects(self) -> dict[str, str]:
        return config.snowflake_objects()
