# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A daily incremental ELT pipeline for SAP FI-AP vendor line items (transaction **FBL1N**) on Snowflake + dbt, with a static Next.js dashboard. The dataset is synthetic, but the design follows from one fact: **the extract is a change feed, not a snapshot**. Each file holds only what changed that day. The same `BELNR` comes back when it is corrected or cleared, and some documents arrive with a `BUDAT` 15–45 days in the past.

The code, comments, docs and commit messages are in Spanish, written without accents (except the literal SAP Excel headers in `config.py`, which are part of the file contract). Follow that style. `docs/DECISIONS.md` holds 11 ADRs (context / decision / alternatives rejected / consequences): 8 design decisions and 3 failures the tests caught. Read the relevant one before changing a design choice.

Published from the `gh-pages` branch: the dashboard at https://joelsuarez33.github.io/AP-Invoices-Snowflake/ and the dbt docs (lineage graph) at `/dbt/`.

## Commands

Windows local dev (venv in `.venv`). All Snowflake access is RSA key-pair auth read from `.env`; see `.env.example`.

```bash
# Pipeline, in order (each script with no args processes everything pending, chronologically)
python generate_extract.py                     # next day after landing/_state.json last_run_date
python generate_extract.py --days N            # or --until YYYY-MM-DD (--date only for last_run + 1)
python normalize.py                            # xlsx -> staging_files/*.csv (--all, --file, --pattern)
python load.py                                 # PUT + COPY INTO RAW (--dry-run prints SQL, no connection)
python export_mart.py                          # marts -> web/public/data/*.json (--dry-run)

# Ops
python scripts/diagnose_auth.py                # key-pair diagnosis, never prints secrets
python scripts/cleanup_test_failures.py        # inventory TEST_FAILURES (--drop = DROP SCHEMA CASCADE)
```

dbt does **not** read `.env`. In PowerShell, load the variables into the session first (dot-space), then work from `dbt_ap/`:

```powershell
. .\scripts\load_env.ps1
cd dbt_ap
dbt deps
dbt build                                      # seeds + snapshot + models + tests; dev = key path, ci = PEM in env
dbt build --select fct_ap_open_items           # one model
dbt test --select assert_mart_reconciles_with_raw   # one singular test
dbt parse                                      # validates without a Snowflake connection
dbt docs generate --static                     # target/static_index.html, what Pages serves
```

```bash
# Lint (same as ci.yml)
ruff check .
sqlfluff lint dbt_ap/models dbt_ap/tests       # jinja templater, no connection needed

# Dashboard
cd web && npm install && npm run dev           # static export (output: 'export'), reads public/data at build time
```

There is no Python test suite. Data quality lives in dbt tests: 142 of them, generic tests in YAML (`dbt_utils` is used **only** for YAML tests such as `expression_is_true`) plus four singular tests in `dbt_ap/tests/`.

## Architecture

```
generate_extract.py -> landing/FBL1N_YYYYMMDD.xlsx      synthetic daily change feed
normalize.py        -> staging_files/FBL1N_YYYYMMDD.csv  header contract + checksum vs SAP totals row
load.py             -> PUT @STG_FBL1N + COPY INTO RAW.FBL1N_ITEMS (append-only)
dbt build           -> STAGING -> INTERMEDIATE -> MARTS (+ SEEDS, SNAPSHOTS)
export_mart.py      -> web/public/data/*.json            dashboard never queries Snowflake
```

**`config.py` is the single contract.** It holds the 21-column layout (Excel header ↔ SAP field name, order matters), the date/amount columns, the totals-row marker, checksum tolerance, file-name templates and paths. The scripts import from it instead of repeating literals. Env vars go through `require_env()`, which has **no defaults on purpose**. `export_mart.py` reuses `load.connect()`, and `normalize.py` reuses `generate_extract.extract_date_from_filename()`.

### Invariants that the whole design depends on

1. **RAW is append-only.** Every business column is `STRING`, plus metadata `_source_file`, `_file_row_number`, `_loaded_at`, `_extract_date`. `_extract_date` is derived from the **file name** (regex over `METADATA$FILENAME`), not from load time. Idempotency comes from the `COPY INTO` load history (64 days): no `FORCE`, `OVERWRITE=FALSE`, and no custom dedup. Never `REMOVE`/`PURGE` files from the stage: re-uploading one after the window would load it twice.
2. **Casting happens only in `stg_fbl1n__items`**, through the macros in `dbt_ap/macros/sap_casts.sql` (dates are `DD.MM.YYYY` text in RAW). That view also collapses the change feed to current state with `QUALIFY row_number() over (partition by belnr order by _extract_date desc, _file_row_number desc) = 1`. The generator and `assert_mart_reconciles_with_raw` repeat that rule on purpose — keep the three in sync.
3. **The incremental filter is `_extract_date`, never `BUDAT`.** If it were `BUDAT`, late-arriving documents would be lost. `fct_ap_open_items` (merge on `belnr`) also re-merges `or is_open` each run so aging doesn't freeze. It holds the **entire ledger** with an `is_open` flag, because dropping cleared rows would leave stale open rows behind. An extract loaded out of order (older than the mart's max) does not enter through that filter; the reconciliation test catches it and `dbt build --full-refresh --select fct_ap_open_items` fixes it.
4. **Everything is measured against `ledger_cutoff_date = max(_extract_date)`, never `CURRENT_DATE`**, so reruns are reproducible. Aging is recalculated in `int_ap_aging`. `VERZN` is exposed as `verzn_report_unreliable`; don't use it for metrics.
5. **Never aggregate `WRBTR`** (document currency, mixed USD/EUR/ARS). Aggregates use `DMBTR` (local ARS). FX exposure lives in its own model, `int_ap_fx_revaluation`.
6. **Intermediate uses `inner join` to the seed masters** (vendors, fx_rates, document_types). A vendor missing from `seeds/vendors.csv` silently drops rows there, and `assert_mart_reconciles_with_raw` is what catches it. When new vendors show up, update the seed. The FX series and `dim_date` both end 2027-12-31; extend both before that date.
7. **Only one snapshot**: `snap_ap_amounts`, SCD2 with `strategy='check'` on `wrbtr, dmbtr` (amount corrections). `timestamp` would be wrong, because VERZN changes daily. A run covering several days records only the last state of that window.

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

`scripts/enrich_baseline.py` records the dataset's provenance and is excluded from ruff and from the pipeline.

### CI/CD (`.github/workflows/`)

All Snowflake config arrives as **repository secrets** (including object names); there are no repository variables. `DBT_PROFILES_DIR` must be absolute (`${{ github.workspace }}/dbt_ap`) because the dbt steps already run with `working-directory: dbt_ap`.

- `ci.yml` (PRs): `ruff check .`, `sqlfluff lint`, `dbt deps`, `dbt parse`, with placeholder env values and **no connection**. `dbt compile` is deliberately absent: it opens a connection to resolve `is_incremental()`.
- `pipeline.yml` (cron `0 9 * * 1-5`, push to main outside `**.md`/`docs/**`, manual `days`/`until`): restores `_state.json` from the orphan `artifacts` branch, generates from `last_run_date + 1` **up to yesterday in America/Argentina/Buenos_Aires**, then normalize → load → `dbt build --target ci`, publishes `_state.json`/`manifest.json`/`run_results.json` to `artifacts` (failing loudly if the push doesn't land), and finally exports and commits `web/public/data` to main with `[skip ci]`.
- `pages.yml` (after each green pipeline, or manual): `dbt docs generate --static` plus the Next.js build, published to `gh-pages` — dashboard at the root, docs under `/dbt/`. The branch is rewritten with a single commit per run. `web/next.config.mjs` reads `NEXT_PUBLIC_BASE_PATH` (empty locally, `/<repo>` on Pages).
- Keep it to **one Snowflake connection per script run**, since each warehouse resume bills at least 60s. `load.py` does a single `COPY` for all pending files.

### Gotchas already paid for

- `COPY INTO` with no new files returns **one row of one column** (`Copy executed with 0 files processed.`), not zero rows.
- In Snowflake `ORDER BY`, a column alias wins over the underlying column: `order by abs(sum(x))` where `x` is also an alias for `sum(...)` nests two aggregates and fails to compile.
- Scheduled runs start hours after the cron time; the "up to yesterday in AR" rule makes that harmless.
- Git Bash on Windows rewrites env values that start with `/` into Windows paths. Run `NEXT_PUBLIC_BASE_PATH=/x npm run build` from PowerShell instead.

### SQL scripts

`sql/00_setup_account.sql` (ACCOUNTADMIN; replace `<<RSA_PUBLIC_KEY>>`), `01_setup_raw.sql` (stage, file format, RAW table), `02_copy_fbl1n.sql` (reference copy of `load.py::copy_statement`; keep the two in sync), `03_verify_pipeline.sql` (read-only checks: volume, versions, mart vs RAW, FX coverage, snapshot, stored failures), `99_teardown.sql`.
