# Repository audit and repair record

Audit date: 20 November 2026

## Problems found and corrected

### CLI and process control

- `clean` and `prepare` required paths even though the repository has standard locations. Both now work without file arguments and accept `--input` as an explicit alternative.
- The v1 feeder referenced `args.dynamic_source` and `args.dynamic_output_dir` without declaring either argument. Both options are now defined, and preparation-only use exits successfully instead of demanding the demo consumer.
- PostgreSQL driver errors escaped the root CLI and printed full tracebacks. The CLI now emits one concise error line by default and exposes `--debug` when a traceback is intentionally needed.
- `status` crashed when any relation was missing. It now returns process state, live worker telemetry, and a partial schema-aware warehouse snapshot.
- A worker PID was reported before initialization completed. Startup now performs file/schema/data preflight checks and waits for a healthy telemetry heartbeat; a silent, failed, or hung child is terminated before success can be reported.
- Stale or recycled PID records were left behind. They are detected and removed.
- There was no graceful root stop command. `python cli_app.py stop` now sends `quit` and waits with a bounded timeout.
- Schema creation had no safe CLI workflow. `init-db` now exists and requires explicit `--yes` confirmation.
- The missing `data_warehouse.live_cache` error is now detected before source loading or worker launch, with the exact recovery command.

### Runtime correctness

- Online replays could duplicate facts. The schema and loader now preserve a stable `(source_type, source_row_id)` identity and use conflict-safe inserts.
- Periodic rows were marked released when placed in memory, before their fact committed. Release now occurs in the same transaction as fact insertion.
- Shed rows were marked processed when placed in memory, creating a crash-loss window. They now use `in_progress` claims and become processed only with the fact commit.
- Crashed shed claims could become stranded. Worker startup releases stale uncommitted claims.
- A restarted periodic thread waited through already completed release hours before resuming. It now queries only still-pending groups and resumes immediately at the first one.
- Directly launched workers could bypass the PID file and process one database concurrently. A session-level PostgreSQL advisory lock now permits exactly one MESHJOIN worker per database.
- Replacing operational sources while facts/cache/shed data existed could mix runs. The loader now refuses this unsafe reseed and directs the operator to an intentional reset.
- A different online CSV could be paired with the staged database. Preparation now records file hashes and row counts in `staging_area.load_manifest`; startup verifies required columns, the database count, and the exact SHA-256 digest.
- The bonus online cache lacked the fact table's revenue, dimensional, time, store, and bridge integrity constraints. Equivalent checks and foreign keys now protect cached rows before they move into the fact.
- Runtime numeric settings accepted zero, negative, invalid, or internally inconsistent values. Configuration now validates ports, rates, group counts, partition sizes, and buffer/chunk sizing.
- The Windows run log exposed an enum mismatch when online rows were inserted through the `VALUES`-derived live-cache query. Supplier and processing-mode parameters are now explicitly cast to their PostgreSQL enum types at the insert boundary.
- The Windows run log also exposed a telemetry crash when another process briefly held `telemetry.json` open. Telemetry now uses unique temporary files, bounded replacement retries, a direct-write fallback, and a surfaced error field without terminating the worker.

### Web app and UX

- The app was a 244-line monolith with manual radio navigation. It is now a seven-page native `st.navigation` app with shared services.
- The old app used a large block of fragile internal CSS selectors and HTML. It now uses `.streamlit/config.toml`, native containers, borders, badges, icons, metrics, charts, and accessible widget labels.
- Every chart and table used deprecated `use_container_width`, producing the repeated log warnings. All occurrences were removed in favor of current width behavior.
- The sidebar read nonexistent `RuntimeConfig.host` and `.port` fields, so valid configuration appeared missing. It now reads `RuntimeConfig.database` correctly.
- Six fixed KPI columns were cramped. KPI rows now use responsive horizontal containers.
- Expensive database sections reran more often than needed. Live counters use fragments; overview queries use bounded short caches; forms batch expensive actions.
- Data-preparation and runtime errors were inconsistent. Pages now use explicit status, warning, success, empty, and recovery states.
- Query results disappeared on rerun. Page-specific session state now preserves the latest result and timing.
- The data explorer now uses a fixed relation allow-list and bounded result sizes.
- The runtime page now displays the effective source mode, arrival mode, transactions per second, stream-buffer occupancy, and hash-table occupancy from an atomic telemetry file.

### Security and configuration

- `.env.example` contained a database password value. It now contains no credential.
- A password was mandatory even when PostgreSQL peer/trust authentication was configured. Blank password configuration is now supported without inventing a secret.
- Database connections had no timeout or application name. Both are now set on the runtime, preparation, and v1 feeder connection paths.
- The query explorer allowed dangerous SELECT-callable server functions and system-schema access. DML, locking, server-file, backend-control, advisory-lock, large-object, `public`, `pg_catalog`, and `information_schema` operations are blocked, including quoted-identifier forms, in addition to the read-only transaction and timeout.
- `.gitignore` patterns were broad, duplicated, and missed Streamlit secrets. The file now has scoped Python, credential, runtime, generated-data, and editor rules.
- The supplied `.env` is excluded from the repaired archive.

### Documentation and quality

- Three README variants contradicted one another about the dashboard, runtime entry point, cache semantics, and file layout. `README.md` is now authoritative; legacy filenames point to it.
- The repository carried an unfinished C/C++ experiment: three empty translation units, placeholder headers, and a makefile with no targets. The project specification explicitly requires Python, and the production implementation is Python, so the dead scaffold was removed instead of shipping a second broken implementation path.
- The required dataset provenance note was missing. `data/SOURCE_NOTE.md` now records the citation, deterministic rules, measured counts, repository timestamp, and the fact that the original download date was not recoverable.
- The report said the dashboard was untested and had no editable source. The updated two-page PDF and LaTeX source now reflect the verified app, current durability work, test totals, and honest PostgreSQL environment boundary; both pages were rendered and visually checked.
- Only one narrow test existed. The 23-test suite now covers deterministic preparation, CLI defaults and error handling, configuration validation, telemetry, manifest hashing, missing-schema status, stale PID cleanup, SQL safety, feeder arguments, source identities, enum-cast SQL, Windows telemetry-lock recovery, single-worker locking, periodic resume, runtime mode validation, PostgreSQL syntax, and every Streamlit page.
- A Ruff configuration and development requirements were added.

## Validation completed

- Python compilation: passed.
- Ruff static analysis: passed.
- Unit and Streamlit smoke tests: 23 passed.
- All seven registered Streamlit pages: rendered without uncaught exceptions.
- Streamlit theme/configuration parsing: passed.
- PostgreSQL syntax parsing with `pglast`:
  - `src/warehouse-schema.sql`: 57 statements
  - `src/queries.sql`: 23 statements
  - `src/checks.sql`: 5 statements
- Full 541,909-row preparation run: passed after the Windows log fixes.
- Reproduced counts: 392,692 clean, 275,154 online, 117,538 periodic, 4,338 customers, 3,665 products.
- SHA-256 comparison: all six regenerated dynamic outputs exactly matched the supplied outputs.
- Updated project report: two A4 pages; LaTeX build, text extraction, and rendered-page inspection passed without layout warnings.

## Environment boundary

The repair environment did not include a running PostgreSQL server or `psql`, so a live schema installation and end-to-end database load were not executed here. SQL syntax, connection failure handling, schema preflight behavior, deterministic data preparation, CLI parsing, and all web pages were verified. Run `python cli_app.py doctor` on the target machine before schema initialization.

