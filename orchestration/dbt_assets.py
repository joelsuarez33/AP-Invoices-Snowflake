"""El proyecto dbt como assets de Dagster.

Cada seed, modelo y snapshot del manifest es un asset, con el linaje que dbt
ya conoce. La source sap_fi.fbl1n_items declara en su meta que la carga el
asset raw_fbl1n_change_feed, asi que el grafo queda conectado de punta a
punta sin mapear nada a mano.

Los assets de dbt comparten la particion diaria de la ingesta y su politica
de backfill: una corrida de N dias hace un solo `dbt build`, con la ventana
de la corrida como vars. fct_ap_open_items usa esa ventana para el filtro
incremental.
"""

import json
import os
import shutil
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

import config
from orchestration.partitions import daily_partitions, dbt_window_vars, partition_dates

DBT_PROJECT_DIR = config.REPO_ROOT / "dbt_ap"

# dbt se instala junto al Python que corre Dagster. Si esa carpeta no esta en
# el PATH -- `dagster dev` desde una terminal de Windows sin la venv activada --,
# ni el DbtCliResource ni el que crea prepare_if_dev() encuentran `dbt`.
_SCRIPTS_DIR = str(Path(sys.executable).parent)
if shutil.which("dbt") is None and _SCRIPTS_DIR not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _SCRIPTS_DIR + os.pathsep + os.environ.get("PATH", "")
DBT_EXECUTABLE = shutil.which("dbt") or "dbt"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    # dev usa la clave en disco (SNOWFLAKE_PRIVATE_KEY_PATH); el workflow de
    # GitHub escribe la clave a un archivo temporal y usa el mismo modo.
    target=os.environ.get("DBT_TARGET", "dev"),
)
# En `dagster dev` regenera el manifest al arrancar. En CI y en el workflow
# diario el manifest lo genera un `dbt parse` explicito antes de cargar.
dbt_project.prepare_if_dev()


class LayeredTranslator(DagsterDbtTranslator):
    """Agrupa los assets por capa del proyecto: staging, intermediate, marts, seeds."""

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        resource_type = dbt_resource_props["resource_type"]
        if resource_type in ("seed", "snapshot"):
            return f"{resource_type}s"
        fqn = dbt_resource_props.get("fqn", [])
        return fqn[1] if len(fqn) > 2 else super().get_group_name(dbt_resource_props)


class DbtBuildConfig(dg.Config):
    """Configuracion de la corrida de dbt, elegible al lanzarla (UI, CLI o workflow)."""

    full_refresh: bool = False
    """Reconstruye los incrementales desde cero. Hace falta una vez cuando cambia
    la clave del merge de un incremental: con el contrato v2, fct_ap_open_items
    paso de BELNR a (BUKRS, BELNR, GJAHR, BUZEI), y mergear contra la tabla vieja
    -- sin esas columnas -- insertaria cada documento de nuevo."""


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    dagster_dbt_translator=LayeredTranslator(),
)
def ap_dbt_assets(
    context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtBuildConfig
) -> Iterator[Any]:
    dates = partition_dates(context)
    window = dbt_window_vars(dates[0], dates[-1])
    args = ["build", "--vars", json.dumps(window)]
    if config.full_refresh:
        args.append("--full-refresh")
    context.log.info("dbt %s", " ".join(args))
    yield from dbt.cli(args, context=context).stream()
