"""DWBI CLI — one entry point for prep, runtime, status and OLAP querying."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
from observatory import snapshot, query, start, command, is_running, tail
from data.dynamic.clean_data import run as clean
from data.dynamic.data_split import run as split
from data.dynamic.feed_db import run as prepare
from meshjoin.base import load_runtime_config


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    subs=p.add_subparsers(dest='action',required=True)
    a=subs.add_parser('clean');a.add_argument('input',type=Path);a.add_argument('--output',type=Path,default=Path('data/dynamic/processed-retail.csv'))
    a=subs.add_parser('split');a.add_argument('--input',type=Path,default=Path('data/dynamic/processed-retail.csv'));a.add_argument('--output-dir',type=Path,default=Path('data/dynamic'))
    a=subs.add_parser('prepare');a.add_argument('input',type=Path);a.add_argument('--load-db',action='store_true')
    a=subs.add_parser('start');a.add_argument('--mode',choices=['peak','non-peak'],default='peak');a.add_argument('--online-file',type=Path)
    a=subs.add_parser('control');a.add_argument('value',choices=['online','periodic','both','peak','non-peak','status','quit'])
    subs.add_parser('status');subs.add_parser('logs')
    a=subs.add_parser('query');a.add_argument('sql');a.add_argument('--limit',type=int,default=100)
    args=p.parse_args(argv)
    if args.action=='clean': result=clean(args.input,args.output)
    elif args.action=='split':result=split(args.input,args.output_dir)
    elif args.action=='prepare':result=prepare(args.input,Path('data/dynamic'),load_runtime_config().database if args.load_db else None)
    elif args.action=='start':result={'pid':start(args.mode,args.online_file)}
    elif args.action=='control':command(args.value);result={'command':args.value,'accepted':True}
    elif args.action=='status':result={'runtime_running':is_running(),**snapshot()}
    elif args.action=='logs':print(tail(100));return
    else:
        names,rows=query(args.sql,args.limit)
        result={'columns':names,'rows':[[str(value) if value is not None else None for value in row] for row in rows]}
    print(json.dumps(result,indent=2,default=str))

if __name__=='__main__':
    try:main()
    except (Exception) as error:
        print(f'ERROR: {error}',file=sys.stderr)
        raise SystemExit(1)
