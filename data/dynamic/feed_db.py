"""Dynamic pipeline for an explicitly selected UCI-format source CSV.

Uses the canonical static rules for master-data derivation and PostgreSQL inserts.
Warning: this is an initial-seed/reseed operation, not an append-only CDC ingestor.
It must not be run while the MESHJOIN worker is processing the same database.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from data.dynamic.clean_data import run as clean
from data.dynamic.data_split import run as split
from data.static.feed_db import write_project_csvs, load_database, DatabaseConfig


def run(source: Path, output_dir: Path, config: DatabaseConfig | None = None) -> dict:
    output_dir.mkdir(parents=True,exist_ok=True)
    processed=output_dir/'processed-retail.csv'
    metrics=clean(source,processed)
    metrics.update(split(processed,output_dir))
    customer=output_dir/'customer_master_data.csv'
    product=output_dir/'product_master_data.csv'
    online_feed=output_dir/'online_sales_stream.csv'
    customer_n,product_n=write_project_csvs(processed,output_dir/'online.csv',online_feed,customer,product)
    metrics.update({'customer_master_rows':customer_n,'product_master_rows':product_n,
                    'database_loaded':False})
    if config is not None:
        load_database(config,online_feed,output_dir/'periodical.csv',customer,product)
        metrics['database_loaded']=True
    (output_dir/'last_run.json').write_text(json.dumps(metrics,indent=2),encoding='utf-8')
    return metrics


def main() -> None:
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',required=True,type=Path)
    p.add_argument('--output-dir',type=Path,default=Path('data/dynamic'))
    p.add_argument('--load-db',action='store_true')
    p.add_argument('--host');p.add_argument('--port',type=int,default=5432)
    p.add_argument('--database');p.add_argument('--user')
    a=p.parse_args()
    config=None
    if a.load_db:
        import os
        from getpass import getpass
        config=DatabaseConfig(a.host or os.getenv('PGHOST','localhost'),a.port,
              a.database or os.environ['PGDATABASE'],a.user or os.environ['PGUSER'],
              os.getenv('PGPASSWORD') or getpass('PostgreSQL password: '))
    print(json.dumps(run(a.input,a.output_dir,config),indent=2))

if __name__=='__main__': main()
