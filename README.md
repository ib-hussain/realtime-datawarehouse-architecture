# DWBI retail operations observatory

This repository implements the DS3003/DS3004 retail data-warehouse project: deterministic data preparation, online and periodic input flows, disk-paged two-pass MESHJOIN enrichment, load shedding, a PostgreSQL star schema, twenty OLAP analyses, a command-line interface, and a multipage Streamlit observatory.

The warehouse keeps the required physical tables `data_warehouse.transactions` and `data_warehouse.products_store`. Online rows use a durable live cache during peak mode; physical-store rows can use the persistent shed queue when the stream buffer is full.

## Quick start

Requirements: Python 3.11+, PostgreSQL 14+, and a local project database.

```bash
python -m venv .venv
python -m pip install -r requirements.txt
```

Copy `.env.example` to `.env` and set the local PostgreSQL password if required. Safe defaults already cover the host, port, database name, user, source paths, and runtime sizes.

Create the database once if it does not exist:

```bash
createdb -U postgres realtime_data_stream
```

Then run:

```bash
python cli_app.py doctor
python cli_app.py init-db --yes
python cli_app.py prepare --load-db
python -m streamlit run app.py
```

`init-db --yes` is deliberately explicit because `src/warehouse-schema.sql` drops and recreates the five project schemas. Use it only on the intended project database after checking backups.

The default preparation source is `data/raw/online-retail.csv`; no input path is required. The recorded deterministic result is:

| Measure | Rows |
|---|---:|
| Raw rows | 541,909 |
| Rows passing validation | 397,884 |
| Exact duplicates removed | 5,192 |
| Clean rows | 392,692 |
| Online rows | 275,154 |
| Periodic rows | 117,538 |
| Customer master | 4,338 |
| Product master | 3,665 |

Dataset provenance, preparation rules, counts, and the unavailable original download-date record are documented in [data/SOURCE_NOTE.md](data/SOURCE_NOTE.md). The source is Chen, D., Sain, S. L., and Guo, K., *Online Retail*, UCI Machine Learning Repository: <https://archive.ics.uci.edu/dataset/352/online+retail>.

## Root CLI

```text
python cli_app.py clean
python cli_app.py split
python cli_app.py prepare
python cli_app.py prepare --load-db
python cli_app.py init-db --yes
python cli_app.py start --mode peak
python cli_app.py control non-peak
python cli_app.py control both
python cli_app.py status
python cli_app.py logs --lines 100
python cli_app.py query "SELECT COUNT(*) FROM data_warehouse.transactions"
python cli_app.py stop
python cli_app.py doctor
```

Every common file argument has a project default. `clean` and `prepare` also accept either a positional source path or `--input PATH`. `start` checks the schema, prepared tables, online CSV columns, row counts, and the preparation SHA-256 manifest before reporting a PID. It also waits for a healthy worker telemetry heartbeat. `status` remains usable when the database is unavailable or the schema is incomplete.

## Web app

The app uses native Streamlit navigation and theming, with light and dark palettes defined in `.streamlit/config.toml`.

| Page | Purpose |
|---|---|
| Overview | Live KPIs, revenue trajectory, sales mix, and recent facts |
| Live operations | Start, control, stop, and observe the MESHJOIN worker |
| Data preparation | Clean, split, generate masters, and explicitly load PostgreSQL |
| Data explorer | Bounded inspection of approved project relations |
| OLAP query studio | Curated/custom read-only SELECTs, exports, and charts |
| MESHJOIN internals | Pipeline structure, configuration, counters, and runtime trace |
| System health | Dependency, file, configuration, schema, database, and worker checks |

The SQL studio permits one bounded SELECT, starts a read-only transaction, applies a five-second statement timeout, and blocks DML, system schemas, server-file functions, and other high-risk operations. For any non-local deployment, use a dedicated PostgreSQL read-only role, authentication, HTTPS, network restrictions, and backups.

## Runtime guarantees

- The stream buffer is bounded and prioritizes online, periodic, then recovered shed rows.
- Customer and product master data are read in cyclic disk pages; the full master tables are not loaded into lookup dictionaries.
- Source identity is stored as `(source_type, source_row_id)` and is unique in facts and cache, so replay does not duplicate facts.
- A preparation manifest binds the selected online file to the exact file loaded into PostgreSQL; a same-sized but different source is rejected at startup.
- Periodic rows are marked `released` only in the same database transaction that commits their warehouse fact.
- Shed rows use `in_progress` claims and are marked processed only after their fact commits. A restarted worker releases stale claims.
- Cached online rows move to the fact table through one atomic `DELETE ... RETURNING` / `INSERT` transaction in non-peak mode.
- Replacing prepared source tables is refused while facts, cache rows, or shed rows exist. Use an intentional schema reset for a clean replay.

## Project structure

```text
app.py                         Streamlit entry point
app_pages/                     Seven direct Streamlit page scripts
dashboard/                     Shared UI, query catalog, and data services
cli_app.py                     Root command-line interface
observatory.py                 Diagnostics, safe queries, and worker control
data/static/                   Canonical cleaning, split, generation, DB load
data/dynamic/                  Importable dynamic pipeline and generated files
data/SOURCE_NOTE.md            Dataset provenance, rules, and measured counts
data/dataFeeder.py             Optional two-connection v1 feeder demonstration
meshjoin/                      Buffer, pagers, workers, loader, schema helpers
data_enrichment.py             Three-thread MESHJOIN runtime
src/warehouse-schema.sql       PostgreSQL schemas, constraints, and indexes
src/queries.sql                Q1-Q20 OLAP analyses
src/checks.sql                 Post-run validation queries
tests/                         Unit, SQL-syntax, CLI, and Streamlit smoke tests
Project_Report.tex/.pdf        Editable and rendered two-page project report
AUDIT.md                       Problems found, fixes, and validation record
```

The optional feeder demo is run as a module:

```bash
python -m data.dataFeeder --demo-drain
```

To prepare a selected dynamic source first:

```bash
python -m data.dataFeeder --dynamic-source path/to/source.csv
```

## Validation

Install development checks with:

```bash
python -m pip install -r requirements-dev.txt
```

Then run:

```bash
python -m compileall -q app.py app_pages dashboard cli_app.py observatory.py data_enrichment.py data meshjoin tests
python -m ruff check .
python -m unittest discover -s tests -v
```

`pglast` validates all PostgreSQL source files in the test suite when the development requirements are installed.

## Troubleshooting

- **`data_warehouse.live_cache` does not exist:** the old/incomplete schema is installed. Verify the target database, then run `python cli_app.py init-db --yes` and `python cli_app.py prepare --load-db`.
- **`Runtime is not running`:** run `python cli_app.py doctor`, then inspect `python cli_app.py logs`. Startup no longer reports success if the child exits during initialization.
- **Unsure how to run cleaning:** use `python cli_app.py clean`; the raw source and output paths now have defaults. For another file use `python cli_app.py clean --input path/to/file.csv`.
- **Database load is refused:** warehouse runtime data already exists. This guard prevents source/fact identity mismatches; use a backed-up clean reset for a new ingestion run.
- **Web app shows a database error:** open System health or run `python cli_app.py doctor` for the exact missing dependency, file, relation, or connection error.

