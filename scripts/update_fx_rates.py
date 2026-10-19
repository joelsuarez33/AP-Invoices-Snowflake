"""Reescribe dbt_ap/seeds/fx_rates.csv con las cotizaciones oficiales del BCRA.

Fuente: API de estadisticas cambiarias del Banco Central de la Republica
Argentina (https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones),
campo `tipoCotizacion`: pesos por unidad de moneda extranjera.

Hasta 2026-10 la serie era sintetica (un random walk con drift) y se habia
alejado de la real: al 2026-10-06 el dolar sintetico estaba en 1.770,78 y el
real en 1.520. Desde este script, la unica fuente de cotizaciones del proyecto
es el BCRA.

La API publica dias habiles. El seed necesita un valor por dia calendario: el
generador busca la cotizacion exacta de BLDAT, y int_ap_items_enriched hace el
join por fecha. Un fin de semana o un feriado toma la ultima cotizacion habil
anterior, que es la que estaba vigente ese dia.

El archivo se reescribe entero, desde SERIES_START hasta --hasta (por defecto,
ayer en hora argentina). Las cotizaciones historicas no cambian, asi que dos
corridas con la misma fecha producen el mismo archivo.

Uso
---
    python scripts/update_fx_rates.py                    # hasta ayer
    python scripts/update_fx_rates.py --hasta 2026-10-16
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import ssl
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH = REPO_ROOT / "dbt_ap" / "seeds" / "fx_rates.csv"

API = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/{currency}"
CURRENCIES = ("USD", "EUR")
LOCAL_CURRENCY = "ARS"

# Mismo inicio que dim_date (date_spine_start en dbt_project.yml).
SERIES_START = dt.date(2026, 1, 1)
# Margen hacia atras para tener una cotizacion habil vigente el 1 de enero,
# que es feriado.
LOOKBACK_DAYS = 15
PAGE_SIZE = 1000

# Argentina no tiene horario de verano: UTC-3 todo el ano.
AR_TZ = dt.timezone(dt.timedelta(hours=-3))


class FxError(RuntimeError):
    """La serie del BCRA no se pudo obtener completa."""


def _ssl_context() -> ssl.SSLContext:
    try:
        import certifi
    except ImportError:
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=certifi.where())


def _get(url: str, retries: int = 3) -> dict[str, Any]:
    context = _ssl_context()
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=30, context=context) as response:
                return json.load(response)
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == retries:
                raise FxError(f"No se pudo consultar {url}: {exc}") from exc
            time.sleep(2 * attempt)
    raise AssertionError("inalcanzable")


def fetch(currency: str, since: dt.date, until: dt.date) -> dict[dt.date, float]:
    """Cotizaciones habiles de una moneda, paginando hasta traer todo el rango."""
    rates: dict[dt.date, float] = {}
    offset = 0
    while True:
        url = (
            API.format(currency=currency)
            + f"?fechadesde={since.isoformat()}&fechahasta={until.isoformat()}"
            + f"&limit={PAGE_SIZE}&offset={offset}"
        )
        payload = _get(url)
        if payload.get("status") != 200:
            raise FxError(f"El BCRA respondio {payload.get('status')} para {currency}: {payload}")
        for result in payload["results"]:
            detail = next(d for d in result["detalle"] if d["codigoMoneda"] == currency)
            rates[dt.date.fromisoformat(result["fecha"])] = float(detail["tipoCotizacion"])
        total = payload["metadata"]["resultset"]["count"]
        offset += PAGE_SIZE
        if offset >= total:
            return rates


def daily_series(business: dict[dt.date, float], start: dt.date, end: dt.date) -> dict[dt.date, float]:
    """Un valor por dia calendario: cada dia toma la ultima cotizacion habil vigente."""
    if not business or min(business) > start:
        raise FxError(f"La serie no tiene una cotizacion vigente al {start}.")
    series: dict[dt.date, float] = {}
    current: float | None = None
    day = min(business)
    while day <= end:
        current = business.get(day, current)
        if day >= start:
            series[day] = current
        day += dt.timedelta(days=1)
    return series


def write_seed(series: dict[str, dict[dt.date, float]], start: dt.date, end: dt.date) -> int:
    lines = ["rate_date,currency,rate_to_ars"]
    day = start
    while day <= end:
        lines.append(f"{day.isoformat()},{LOCAL_CURRENCY},1.0")
        for currency in CURRENCIES:
            lines.append(f"{day.isoformat()},{currency},{series[currency][day]:.6f}")
        day += dt.timedelta(days=1)
    SEED_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return len(lines) - 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Actualiza el seed de cotizaciones desde el BCRA.")
    parser.add_argument(
        "--hasta",
        type=dt.date.fromisoformat,
        default=dt.datetime.now(AR_TZ).date() - dt.timedelta(days=1),
        metavar="YYYY-MM-DD",
        help="ultimo dia de la serie (por defecto, ayer en hora argentina)",
    )
    args = parser.parse_args(argv)

    try:
        series = {}
        for currency in CURRENCIES:
            business = fetch(currency, SERIES_START - dt.timedelta(days=LOOKBACK_DAYS), args.hasta)
            series[currency] = daily_series(business, SERIES_START, args.hasta)
            last = max(business)
            print(f"{currency}: {len(business)} dias habiles, ultima cotizacion {last} "
                  f"= {business[last]:,.4f}")
    except FxError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    rows = write_seed(series, SERIES_START, args.hasta)
    print(f"{SEED_PATH.relative_to(REPO_ROOT)}: {rows} filas, {SERIES_START} -> {args.hasta}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
