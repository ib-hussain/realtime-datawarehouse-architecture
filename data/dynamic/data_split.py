"""Dynamic retail classification: fixed invoice-based MD5 split and 48 periodic groups."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from data.static.data_split import split_file, GROUP_COUNT


def run(source: Path, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True,exist_ok=True)
    online=output_dir/'online.csv'
    periodic=output_dir/'periodical.csv'
    if source.resolve() in {online.resolve(), periodic.resolve()}:
        raise ValueError('Input cannot overwrite either output')
    online_n,periodic_n=split_file(source,online,periodic)
    return {'online_rows':online_n,'periodic_rows':periodic_n,'periodic_groups':GROUP_COUNT,
            'online_file':str(online),'periodic_file':str(periodic)}


def main() -> None:
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,default=Path('data/dynamic/processed-retail.csv'))
    p.add_argument('--output-dir',type=Path,default=Path('data/dynamic'))
    a=p.parse_args()
    print(json.dumps(run(a.input,a.output_dir),indent=2))

if __name__=='__main__': main()
