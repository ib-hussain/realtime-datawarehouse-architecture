# DWBI Retail Operations Observatory

An executable PostgreSQL-backed DWBI demonstration package built from the prior retail warehouse project, retaining its original static cleaning, invoice-level MD5 splitting, 48 release groups, and MESHJOIN code. The attached telecom dashboard served **only** as the visual reference. This package does not use telecom data.

## Setup

1. Create a Python 3.11+ virtual environment and run `pip install -r requirements.txt`.
2. Create a PostgreSQL database; apply `src/warehouse-schema.sql` **only to a disposable/backup-protected project database**, because it drops and recreates five schemas.
3. Copy `.env.example` to `.env`, fill in connection settings, and keep it out of Git. PostgreSQL must be reachable from the host running Streamlit.
4. Put the UCI Online Retail source at `data/raw/online-retail.csv` or point to an existing absolute path. A copy of the dataset is **not** included.
5. Run `python cli_app.py prepare data/raw/online-retail.csv --load-db`. This populates the source and master tables. For the original/static inputs use `python -m data.static.feed_db`.
6. Run `streamlit run app.py` to open the interface. Open **Live operations**, start the MESHJOIN worker, then explore query results. Or use `python cli_app.py start` in a terminal.
7. Switch to `non-peak` using the Live operations command selector to move cached online rows to the fact table. Use `quit` to stop the runtime gracefully.

### Root CLI

```bash
python cli_app.py clean data/raw/online-retail.csv
python cli_app.py split
python cli_app.py prepare data/raw/online-retail.csv --load-db
python cli_app.py start --mode peak
python cli_app.py control status
python cli_app.py control non-peak
python cli_app.py control both
python cli_app.py status
python cli_app.py logs
python cli_app.py query "SELECT COUNT(*) FROM data_warehouse.transactions"
```

`data/dataFeeder.py` now supports `--dynamic-source FILE --dynamic-output-dir DIR` as an opt-in preparation path; its original demonstration/parallel-feeding behavior remains available. To use the CLI producer demo: `python -m data.dataFeeder --dynamic-source data/raw/online-retail.csv --demo-drain` (database arguments or interactive prompts required). **This is a demonstration feed**, and does not replace the authoritative `data_enrichment.py` three-thread runtime.

The dynamic directory has `clean_data.py`, `data_split.py`, and `feed_db.py` with importable `run()` functions and independent CLIs (`python -m data.dynamic.feed_db --input ... [--load-db]`). It writes cleaning counters to `data/dynamic/last_run.json`.

## Actual bonus semantics

The schema creates `data_warehouse.live_cache`. Joined online transactions are inserted into that table **during peak** instead of directly into the fact table. During non-peak, the MESHJOIN worker uses a single PostgreSQL transaction with row locking and `DELETE ... RETURNING` to move cached rows to the fact table, preserving data on rollback. Periodic transactions and recovered shed rows never enter this online cache. Cache flow and fact row counts can be observed separately.

At shutdown the worker flushes remaining staged rows to the cache or facts; cached rows persist until a non-peak worker drains them. There is no automatic scheduler without the worker running.

## Security and operational caveats

This is a **local demonstration/integration package**, not certified production software. The SQL explorer accepts one `SELECT`, runs `READ ONLY` with a five-second statement timeout and caps returned rows; for hostile users use a separate restricted PostgreSQL role, network isolation, authentication and resource quotas. The app currently has no login layer. Its buttons can launch the worker or replace source data: **do not expose it on a public interface**. Protect `.env`, configure database backups, supervise worker restarts and monitor logs. The app does not claim a live DB connection until configured.

The source package did not include the precise Queries-DW submission SQL file or raw source CSV, so curated OLAP queries are useful **new** analysis templates, not replacements for the assignment's twenty mandatory queries. Confirm their expected outputs on your local dataset and integrate the original submission query script when available.

## File map

- `app.py` — full responsive Streamlit dashboard, automatic snapshots and query explorer.
- `cli_app.py` / `observatory.py` — CLI, runtime control and read-only SQL.
- `data/dynamic/{clean_data,data_split,feed_db}.py` — dynamic preprocessing and source load.
- `data/static/*.py` — original supplied cleaning, split and feed logic.
- `data/dataFeeder.py` — compatible optional dynamic-source feeder.
- `meshjoin/*.py` and `data_enrichment.py` — existing workers with online-cache integration and file-based runtime control.
- `src/warehouse-schema.sql` — original PostgreSQL schema plus new `live_cache`.
- `tests/` — deterministic unit tests; database integration requires local PostgreSQL.

**Dynamic streaming:** after `prepare ... --load-db`, choose `data/dynamic/online_sales_stream.csv` in the Live operations start control, or run `python cli_app.py start --online-file data/dynamic/online_sales_stream.csv`. If using a static preparation, choose the static file. Do not mix the source tables from one preparation run with the CSV from a different run.
