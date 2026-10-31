# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A daily incremental ELT pipeline for SAP FI-AP vendor line items (transaction **FBL1N**) on Snowflake + dbt, orchestrated by Dagster, with a static Next.js dashboard. The dataset is synthetic, but the design follows from one fact: **the extract is a change feed, not a snapshot**. Each file holds only what changed that day. The same `BELNR` comes back when it is corrected or cleared, and some documents arrive with a `BUDAT` 15–45 days in the past.

The code, comments, docs and commit messages are in Spanish, written without accents (except the literal SAP Excel headers in `config.py`, which are part of the file contract). Follow that style. `docs/DECISIONS.md` holds 15 ADRs (context / decision / alternatives rejected / consequences): 8 design decisions, 3 failures the tests caught, ADR-012 (real BCRA exchange rates, re-expression as a change-feed event), and ADR-013..015 (Dagster assets with single-run backfills, per-partition DELETE + COPY, versioned extract contract and the full document key). Read the relevant one before changing a design choice.

Published from the `gh-pages` branch: the dashboard at https://joelsuarez33.github.io/AP-Invoices-Snowflake/ and the dbt docs (lineage graph) at `/dbt/`.

## Commands

Windows local dev (venv in `.venv`). All Snowflake access is RSA key-pair auth read from `.env`; see `.env.example`.

```bash
# Pipeline, in order (each script with no args processes everything pending, chronologically)
python generate_extract.py                     # next day after landing/_state.json last_run_date
python generate_extract.py --days N            # or --until YYYY-MM-DD (--date only for last_run + 1)
python normalize.py                            # xlsx -> staging_files/*.csv (--all, --file, --pattern)
python load.py                                 # per day: PUT + DELETE/COPY in one transaction (--dry-run prints SQL)
python export_mart.py                          # marts -> web/public/data/*.json (--dry-run)

# Orchestration (same steps as Dagster assets; see orchestration/)
dagster dev                                    # UI + asset graph; module from pyproject [tool.dagster]
dagster definitions validate -m orchestration.definitions
dagster job execute -m orchestration.definitions -j fbl1n_daily --tags '{"dagster/asset_partition_range_start": "2026-10-01", "dagster/asset_partition_range_end": "2026-10-03"}'

# Ops
python scripts/diagnose_auth.py                # key-pair diagnosis, never prints secrets
python scripts/cleanup_test_failures.py        # inventory TEST_FAILURES (--drop = DROP SCHEMA CASCADE)
```

dbt does **not** read `.env`. In PowerShell, load the variables into the session first (dot-space), then work from `dbt_ap/`:

```powershell
. .\scripts\load_env.ps1
cd dbt_ap
dbt deps
dbt build                                      # seeds + snapshot + models + tests; both targets read SNOWFLAKE_PRIVATE_KEY_PATH
dbt build --select fct_ap_open_items           # one model
dbt test --select assert_mart_reconciles_with_raw   # one singular test
dbt parse                                      # validates without a Snowflake connection
dbt docs generate --static                     # target/static_index.html, what Pages serves
```

```bash
# Lint + tests (same as ci.yml)
ruff check .
sqlfluff lint dbt_ap/models dbt_ap/tests       # jinja templater, no connection needed
pytest -q                                      # contract, ingestion, assets; Snowflake faked
pre-commit install                             # gitleaks + ruff on each commit

# Dashboard
cd web && npm install && npm run dev           # static export (output: 'export'), reads public/data at build time
```

Python tests (`tests/`, 36 of them) cover the contract, ingestion planning and the Dagster assets, with Snowflake replaced by `FakeWarehouse` in `tests/conftest.py` (COPY with FORCE adds rows, so a missing DELETE shows up as duplicates). Data quality lives in dbt tests: 146 of them, generic tests in YAML (`dbt_utils` is used **only** for YAML tests such as `expression_is_true` and `unique_combination_of_columns`) plus four singular tests in `dbt_ap/tests/`.

## Architecture

```
generate_extract.py -> landing/FBL1N_YYYYMMDD.xlsx      synthetic daily change feed      [asset fbl1n_extract]
normalize.py        -> staging_files/FBL1N_YYYYMMDD.csv  schema contract + checksum      [asset raw_fbl1n_change_feed]
load.py             -> PUT @STG_FBL1N + DELETE/COPY INTO RAW.FBL1N_ITEMS per day
dbt build           -> STAGING -> INTERMEDIATE -> MARTS (+ SEEDS, SNAPSHOTS)              [@dbt_assets]
export_mart.py      -> web/public/data/*.json            dashboard never queries Snowflake
```

**The extract layout lives in `contracts/fbl1n_extract.yml`** (24 columns: Excel header, SAP field, type, optional `since`), loaded by `contract.py`. **`config.py` is the single source for everything else** and derives `COLUMNS`/`EXCEL_HEADERS`/`RAW_COLUMNS` from the contract. It also holds the date/amount columns, the totals-row marker, checksum tolerance, file-name templates and paths. The scripts import from it instead of repeating literals. Env vars go through `require_env()`, which has **no defaults on purpose**. `export_mart.py` reuses `load.connect()`, and `normalize.py` reuses `generate_extract.extract_date_from_filename()`.

### Invariants that the whole design depends on

1. **RAW is append-only.** Every business column is `STRING`, plus metadata `_source_file`, `_file_row_number`, `_loaded_at`, `_extract_date`. `_extract_date` is derived from the **file name** (regex over `METADATA$FILENAME`), not from load time. A day is the unit of idempotency (ADR-014): `PUT ... OVERWRITE = TRUE`, then in one transaction `DELETE WHERE _EXTRACT_DATE = d` + `COPY INTO ... FILES = (that file) FORCE = TRUE`, `ROLLBACK` on error. Reloading a day replaces only that day's rows, never other days' versions. Never use MERGE on RAW.
2. **Casting happens only in `stg_fbl1n__items`**, through the macros in `dbt_ap/macros/sap_casts.sql` (dates are `DD.MM.YYYY` text in RAW). The document key is `(bukrs, belnr, gjahr, buzei)`; for pre-v2 extracts staging derives it as `var("default_company_code")`, `year(budat)`, `var("default_line_item")`. That view collapses the change feed to current state with `QUALIFY row_number() over (partition by bukrs, belnr, gjahr, buzei order by _extract_date desc, _file_row_number desc) = 1`. The generator and `assert_mart_reconciles_with_raw` repeat that rule on purpose — keep the three in sync.
3. **The incremental filter is `_extract_date`, never `BUDAT`.** If it were `BUDAT`, late-arriving documents would be lost. `fct_ap_open_items` (merge on the 4-field key) filters on the run's partition window (`extract_window_start`/`extract_window_end` vars passed by Dagster; without them, `> max(_extract_date)`) and also re-merges `or is_open` each run so aging doesn't freeze. Changing the merge key needs one `full_refresh` run (workflow input or `ap_dbt_assets` config). It holds the **entire ledger** with an `is_open` flag, because dropping cleared rows would leave stale open rows behind. An extract loaded out of order (older than the mart's max) does not enter through that filter; the reconciliation test catches it and `dbt build --full-refresh --select fct_ap_open_items` fixes it.
4. **Everything is measured against `ledger_cutoff_date = max(_extract_date)`, never `CURRENT_DATE`**, so reruns are reproducible. Aging is recalculated in `int_ap_aging`. `VERZN` is exposed as `verzn_report_unreliable`; don't use it for metrics.
5. **Never aggregate `WRBTR`** (document currency, mixed USD/EUR/ARS). Aggregates use `DMBTR` (local ARS). FX exposure lives in its own model, `int_ap_fx_revaluation`.
6. **Intermediate uses `inner join` to the seed masters** (vendors, fx_rates, document_types). A vendor missing from `seeds/vendors.csv` silently drops rows there, and `assert_mart_reconciles_with_raw` is what catches it. When new vendors show up, update the seed. The FX seed is the **official BCRA series** (estadisticas cambiarias API), rebuilt from 2026-01-01 up to yesterday by `scripts/update_fx_rates.py`, one row per calendar day (weekends/holidays carry the last business-day rate). Never hand-edit it or reintroduce synthetic rates. `dim_date` ends 2027-12-31.
7. **Only one snapshot**: `snap_ap_amounts`, SCD2 with `strategy='check'` on `wrbtr, dmbtr` (amount corrections), still keyed on `belnr` (single company code; rekeying would rewrite its history). `timestamp` would be wrong, because VERZN changes daily. A run covering several days records only the last state of that window.

### Extract contract (`contracts/`, `contract.py`)

Types: `string`, `code` (int or text; `GKONT` arrives both ways), `integer`, `decimal`, `date`. A column with `since: YYYY-MM-DD` is required only for extracts from that date on (v2 added `BUKRS`/`GJAHR`/`BUZEI` since 2026-10-07). `check_extract()` returns a `SchemaReport`: missing column or wrong type is **blocking**; an unknown column is a warning and is dropped. `normalize.py` always writes the full contract width in contract order, and `load.py` adds new contract columns to RAW with `ADD COLUMN IF NOT EXISTS ... STRING`. Adding a column = edit the YAML (with `since`), `sql/01_setup_raw.sql`, staging, and docs.

### Orchestration (`orchestration/`)

Dagster 1.13 / dagster-dbt 0.29, `Definitions` in `orchestration/definitions.py`. Daily partitions (`partitions.py`: America/Argentina/Buenos_Aires, from 2026-07-08, last = yesterday) and `BackfillPolicy.single_run()` everywhere: a range is one run, processed in date order (the generator is sequential).

- `fbl1n_extract` (`ingestion.py`): reuses archived extracts or generates the next day (`plan_extracts`; a gap raises `IngestionError`). Ensures BCRA coverage first. Blocking check `schema_contract`: an invalid extract never reaches Snowflake and dbt doesn't run.
- `raw_fbl1n_change_feed`: normalize + `load.load_partition` per day on one connection. Blocking check `partition_row_count` (> 0 per day).
- `ap_dbt_assets` (`dbt_assets.py`): `dbt build --vars {window}`; the source's `meta.dagster.asset_key` links it to RAW. `DbtBuildConfig.full_refresh`.
- `checks.py`: `raw_freshness` (lag ≤ 1 day vs yesterday AR, measured on RAW, not the instance) and `stg_document_key_unique`.
- Job `fbl1n_daily`, schedule `fbl1n_daily_0600_art` (stopped by default; Actions triggers the job).
- `SnowflakeResource` wraps `load.connect()` and has **no config fields**, so credentials never land in run config.
- No `from __future__ import annotations` in asset modules: Dagster rejects the stringified `context` annotation.
- `dbt_assets.py` prepends the venv `Scripts/` dir to PATH, otherwise `dagster dev` without an activated venv can't find `dbt`.

`setup/snowflake_cost_guardrails.sql` (XSMALL warehouse, 10-credit resource monitor, least-privilege role and service user) is run **manually by the user** as ACCOUNTADMIN. Never execute it.

### dbt conventions

- `generate_schema_name` is overridden so custom schemas are **literal** (`STAGING`, `INTERMEDIATE`, `MARTS`, `SEEDS`, `SNAPSHOTS`, `TEST_FAILURES`), with no target prefix. These schemas and their grants are created by `sql/00_setup_account.sql`. `export_mart.py` hardcodes `<database>.MARTS`.
- Staging and intermediate are views, marts are tables/incremental. SAP field names are kept as-is (lowercase) in staging; don't rename them to English.
- Models don't call package macros, so that sqlfluff's **jinja templater** (not the dbt templater) can lint offline. If a model uses a new `var()`, mirror it in `.sqlfluff` `[sqlfluff:templater:jinja:context]`. Macros and snapshots are in `.sqlfluffignore`.
- sqlfluff style: lowercase everything, explicit aliases, and column aliases aligned within the select clause.
- `store_failures` is on for 14 tests only: the 10 `relationships` (each via its own `config:` in YAML) and the 4 singular tests. **Do not set `+store_failures` in `dbt_project.yml`** — for data tests the project config wins over the test's own, so a `false` there silently disables all 14.

### Generator

`generate_extract.py` is idempotent per date: the RNG is seeded with `(seed, date)`, so the same date produces the same file. It continues the ledger from `landing/_state.json`, whose `documents` key is a full checkpoint of the live universe; `landing/` is only replayed for extracts between the checkpoint and the target date. The generator refuses to emit a file dated after the system date. Only the baseline `landing/FBL1N_20260906.xlsx` is tracked in git; other extracts and `staging_files/*` are gitignored.

The **live** state lives in the `artifacts` branch, not in `main`. Before generating locally, sync it, and don't commit the result to `main`:

```bash
git show origin/artifacts:state/_state.json > landing/_state.json
```

New documents take `DMBTR` as the reference and derive `WRBTR = DMBTR / rate(BLDAT)`. Every run also checks existing foreign-currency documents: if `DMBTR` doesn't match `WRBTR x rate(BLDAT)` within rounding tolerance (`REEXPRESSION_TOLERANCE`), the document is re-emitted with `DMBTR` recomputed and `WRBTR` kept (`_reexpress_amounts`). That ran once, for 2,315 documents in the 2026-10-07 extract, when the synthetic rates were replaced; afterwards it finds nothing. It doesn't consume the RNG, so it never changes the rest of a day's feed.

`scripts/enrich_baseline.py` records the dataset's provenance (including the original synthetic FX walk, no longer used) and is excluded from ruff and from the pipeline.

### CI/CD (`.github/workflows/`)

All Snowflake config arrives as **repository secrets** (including object names); there are no repository variables. The private key secret is written to `$RUNNER_TEMP/snowflake_key.p8` (umask 077) by one step, only its path is exported (`SNOWFLAKE_PRIVATE_KEY_PATH`), and an `if: always()` step deletes it; both dbt targets use `private_key_path`. `DBT_PROFILES_DIR` must be absolute (`${{ github.workspace }}/dbt_ap`) because the dbt steps already run with `working-directory: dbt_ap`.

- `ci.yml` (PRs): gitleaks (release binary verified by checksum), `ruff check .`, `sqlfluff lint`, `dbt deps`, `dbt parse`, `dagster definitions validate`, `pytest`, with placeholder env values and **no connection**. `dbt compile` is deliberately absent: it opens a connection to resolve `is_incremental()`.
- `dagster_daily.yml` (cron `0 9 * * *` = 06:00 AR, manual `partition_start`/`partition_end`/`full_refresh`) replaced `pipeline.yml`; never run both. It restores `_state.json` and the archived extracts (`extracts/`) from the orphan `artifacts` branch, refreshes BCRA rates as best effort (the API returns 403 to runners; run `scripts/update_fx_rates.py` locally and commit the seed), runs `dbt parse`, computes the range (pending days **up to yesterday in America/Argentina/Buenos_Aires**, or re-runs yesterday if nothing is pending), runs `dagster job execute -j fbl1n_daily` with the partition range tags, publishes state + extracts + the newest `run_results.json`/`manifest.json` to `artifacts` (failing loudly if the push doesn't land), then exports and commits `web/public/data` plus `dbt_ap/seeds/fx_rates.csv` to main with `[skip ci]`.
- `pages.yml` (after each green "Pipeline diario (Dagster)" run, or manual): `dbt docs generate --static` plus the Next.js build, published to `gh-pages` — dashboard at the root, docs under `/dbt/`. The branch is rewritten with a single commit per run. `web/next.config.mjs` reads `NEXT_PUBLIC_BASE_PATH` (empty locally, `/<repo>` on Pages).
- Keep it to **one Snowflake connection per script run / asset**, since each warehouse resume bills at least 60s. `load.py` and `raw_fbl1n_change_feed` load all days of a run over a single connection.

Local hooks: `.pre-commit-config.yaml` runs gitleaks and ruff. Secrets only in `.env` (gitignored); `.env.example` has names, no values.

### Gotchas already paid for

- `COPY INTO` with no new files returns **one row of one column** (`Copy executed with 0 files processed.`), not zero rows.
- In Snowflake `ORDER BY`, a column alias wins over the underlying column: `order by abs(sum(x))` where `x` is also an alias for `sum(...)` nests two aggregates and fails to compile.
- Scheduled runs start hours after the cron time; the "up to yesterday in AR" rule makes that harmless.
- Git Bash on Windows rewrites env values that start with `/` into Windows paths. Run `NEXT_PUBLIC_BASE_PATH=/x npm run build` from PowerShell instead.
- On Windows, `click` expands wildcards in CLI args, so `dagster asset materialize --select '*'` turns into a file list. Use `dagster job execute -j fbl1n_daily` with the range tags.
- dagster pulls `grpcio-health-checking`, whose latest version needs protobuf 7 and breaks dbt 1.8. Keep `protobuf==4.25.9` and `grpcio-health-checking==1.62.3` pinned. `dagster-snowflake` only resolves to an ancient 0.11.x here; don't add it.

### Query agent (`agent/`)

Terminal agent over the ledger, documented in `docs/AGENTE.md`. Dependencies live in `requirements-agent.txt`, not in the pipeline's `requirements.txt`.

- `ledger.py` builds an in-memory DuckDB table `items` from the most recent state available plus the seeds, re-implementing the mart rules (aging vs cutoff, FX revaluation). **It works without Snowflake.** The state is either the `artifacts` one (cached in `agent/data/`) or a local projection in `agent/data/landing/` built by `--avanzar` (refresh BCRA rates, then run the generator up to yesterday with `LANDING_DIR` pointing there, exactly as the pipeline would). `python -m agent.ledger` reconciles against `web/public/data/summary.json` only when both are at the same cutoff; if the dbt models change, keep both in sync and re-run that check. `--sql` and `--export` (Excel/CSV) are for manual verification.
- `tools.py`: typed `@beta_tool` functions, each a fixed parameterized query. The model never writes SQL. Raise `ToolError` for bad input (it reaches the model as a readable `is_error` result). Return the percentages the model needs (`pct_*`) instead of letting it compute them: Haiku did arithmetic on its own and got it wrong. Keep `FUNCTIONS` order stable (it is the start of the cached prompt).
- `chat.py`: Tool Runner with streaming, `claude-haiku-5-5` by default (`AGENT_MODEL`, `AGENT_EFFORT`). Append-only history; a failed turn is dropped whole. Needs a **workspace-scoped** `ANTHROPIC_API_KEY` (org-scoped keys require `anthropic-workspace-id`). `load_dotenv` does not override a session variable: a stale key exported by `load_env.ps1` wins silently, and the chat warns about it.
- `audit.py`: every question is appended to `agent/logs/audit.jsonl` (question, answer, each tool call, the SQL it ran, SHA-256 of every result). `python -m agent.audit replay <id>` re-runs the SQL and compares hashes; that is the idempotency proof. The wording of answers may vary between runs; tool calls and results must not.

### SQL scripts

`sql/00_setup_account.sql` (ACCOUNTADMIN; replace `<<RSA_PUBLIC_KEY>>`), `01_setup_raw.sql` (stage, file format, RAW table), `02_copy_fbl1n.sql` (reference copy of `load.py::load_partition`: ADD COLUMN IF NOT EXISTS, then BEGIN/DELETE/COPY FILES FORCE/COMMIT; keep the two in sync), `03_verify_pipeline.sql` (read-only checks: volume, versions, mart vs RAW, FX coverage, snapshot, stored failures), `99_teardown.sql`.
