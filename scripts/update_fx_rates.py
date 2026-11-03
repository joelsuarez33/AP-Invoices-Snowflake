"""Completa dbt_ap/seeds/fx_rates.csv hasta ayer con cotizaciones diarias de ARS.

Fuentes (ADR-016)
-----------------
- Hasta 2026-10-18: serie oficial del BCRA (API de estadisticas cambiarias,
  `tipoCotizacion`). Esas filas ya estan en el seed y no se tocan.
- Desde 2026-10-19: currency-api (https://github.com/fawazahmed0/exchange-api),
  un archivo JSON por dia publicado en el CDN de jsDelivr, con espejo en
  Cloudflare Pages. El BCRA puso Cloudflare delante de su API y rechaza a
  cualquier cliente que no sea un navegador (error 1010); lo mismo pasa con
  datos.gob.ar, ArgentinaDatos y Bluelytics. currency-api se sirve como archivos
  estaticos versionados por fecha y no tiene ese bloqueo.

  Contra el BCRA, en los dias que se superponen, la diferencia es menor a 0,5 %
  en USD y a 1 % en EUR. EUR/ARS sale del cruce: (ARS por USD) / (EUR por USD).

Las filas existentes nunca se reescriben. Cambiar una cotizacion ya usada haria
que el generador re-exprese todos los documentos de esa fecha (ADR-012). Solo se
agregan los dias que faltan, asi que dos corridas con la misma fecha producen el
mismo archivo: los archivos de currency-api de una fecha no cambian.

El seed necesita un valor por dia calendario: el generador busca la cotizacion
exacta de BLDAT, y int_ap_items_enriched hace el join por fecha. Sabado y domingo
toman la cotizacion del ultimo dia habil, igual que en la serie del BCRA. Un dia
habil sin archivo publicado tambien arrastra la anterior, con un aviso.

Uso
---
    python scripts/update_fx_rates.py                    # hasta ayer
    python scripts/update_fx_rates.py --hasta 2026-10-30
"""

from __future__ import annotations

import argparse
import csv
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

# Primero el CDN de jsDelivr; si falla, el espejo de la misma publicacion.
SOURCES = (
    "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/usd.json",
    "https://{date}.currency-api.pages.dev/v1/currencies/usd.json",
)
CURRENCIES = ("USD", "EUR")
LOCAL_CURRENCY = "ARS"
# Mismos decimales que publicaba el BCRA.
DECIMALS = 4

# Argentina no tiene horario de verano: UTC-3 todo el ano.
AR_TZ = dt.timezone(dt.timedelta(hours=-3))


class FxError(RuntimeError):
    """La serie no se pudo completar."""


def _ssl_context() -> ssl.SSLContext:
    try:
        import certifi
    except ImportError:
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=certifi.where())


def _get(url: str, retries: int = 3) -> dict[str, Any] | None:
    """El JSON de la URL, o None si el archivo de esa fecha no existe (404)."""
    context = _ssl_context()
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=30, context=context) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            if attempt == retries:
                raise FxError(f"No se pudo consultar {url}: HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == retries:
                raise FxError(f"No se pudo consultar {url}: {exc}") from exc
        time.sleep(2 * attempt)
    raise AssertionError("inalcanzable")


def fetch_day(day: dt.date) -> dict[str, float] | None:
    """Pesos por unidad de cada moneda publicados para `day`, o None si no hay archivo."""
    errors: list[str] = []
    for template in SOURCES:
        try:
            payload = _get(template.format(date=day.isoformat()))
        except FxError as exc:
            errors.append(str(exc))
            continue
        if payload is None:
            continue
        per_usd = payload["usd"]
        ars_per_usd = float(per_usd["ars"])
        return {
            "USD": round(ars_per_usd, DECIMALS),
            "EUR": round(ars_per_usd / float(per_usd["eur"]), DECIMALS),
        }
    if errors:
        raise FxError("; ".join(errors))
    return None


def read_seed() -> dict[dt.date, dict[str, float]]:
    rows: dict[dt.date, dict[str, float]] = {}
    with SEED_PATH.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            day = dt.date.fromisoformat(row["rate_date"])
            rows.setdefault(day, {})[row["currency"]] = float(row["rate_to_ars"])
    return rows


def extend(
    seed: dict[dt.date, dict[str, float]], until: dt.date
) -> list[tuple[dt.date, dict[str, float], str]]:
    """Dias que faltan entre el ultimo del seed y `until`, con el origen de cada valor."""
    last = max(seed)
    current = {c: seed[last][c] for c in CURRENCIES}
    added = []
    day = last + dt.timedelta(days=1)
    while day <= until:
        origin = "fin de semana"
        if day.weekday() < 5:
            published = fetch_day(day)
            if published is None:
                origin = "sin archivo publicado"
                print(f"AVISO: {day} sin cotizacion publicada; se arrastra la anterior.")
            else:
                current, origin = published, "currency-api"
        added.append((day, dict(current), origin))
        day += dt.timedelta(days=1)
    return added


def append_rows(added: list[tuple[dt.date, dict[str, float], str]]) -> None:
    lines = []
    for day, rates, _ in added:
        lines.append(f"{day.isoformat()},{LOCAL_CURRENCY},1.0")
        for currency in CURRENCIES:
            lines.append(f"{day.isoformat()},{currency},{rates[currency]:.6f}")
    with SEED_PATH.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Completa el seed de cotizaciones hasta una fecha.")
    parser.add_argument(
        "--hasta",
        type=dt.date.fromisoformat,
        default=dt.datetime.now(AR_TZ).date() - dt.timedelta(days=1),
        metavar="YYYY-MM-DD",
        help="ultimo dia de la serie (por defecto, ayer en hora argentina)",
    )
    args = parser.parse_args(argv)

    seed = read_seed()
    last = max(seed)
    if last >= args.hasta:
        print(f"{SEED_PATH.relative_to(REPO_ROOT)} ya cubre hasta {last}.")
        return 0

    try:
        added = extend(seed, args.hasta)
    except FxError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    append_rows(added)
    for day, rates, origin in added:
        print(f"{day}  USD {rates['USD']:>12,.4f}  EUR {rates['EUR']:>12,.4f}  ({origin})")
    print(f"{SEED_PATH.relative_to(REPO_ROOT)}: +{len(added)} dias, {last} -> {args.hasta}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
