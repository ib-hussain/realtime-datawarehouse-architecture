"""Shared PostgreSQL observations and safe runtime process control."""
from __future__ import annotations
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import uuid
from datetime import datetime,timezone
from meshjoin.base import connect_database,load_runtime_config

ROOT=Path(__file__).resolve().parent
RUN_DIR=ROOT/'logs'
PID_PATH=RUN_DIR/'runtime.json'
CONTROL=RUN_DIR/'control.json'
LOG=RUN_DIR/'meshjoin.log'


def database():
    return connect_database(load_runtime_config().database)


def query(sql:str,limit:int=500)->tuple[list[str],list[tuple]]:
    """Run a *single SELECT* in a read-only, time-limited transaction."""
    import sqlparse
    statements=sqlparse.parse(sql)
    if len(statements)!=1 or statements[0].get_type()!='SELECT':
        raise ValueError('Only one SELECT statement is permitted')
    if re.search(r'\b(for\s+update|for\s+share|into\s+\w+|pg_sleep|dblink|lo_export)\b',sql,re.I):
        raise ValueError('Unsupported SQL operation in read-only explorer')
    if not 1<=limit<=2000: raise ValueError('Limit must be 1..2000')
    conn=database()
    try:
        with conn.cursor() as c:
            c.execute('BEGIN READ ONLY')
            c.execute("SET LOCAL statement_timeout = '5000ms'")
            c.execute(sql)
            names=[d[0] for d in c.description]
            rows=c.fetchmany(limit+1)
        conn.rollback()
        return names,rows[:limit]
    finally:
        conn.close()


def snapshot()->dict:
    conn=database()
    try:
        statements={
          'fact_rows':'SELECT COUNT(*) FROM data_warehouse.transactions',
          'revenue':'SELECT COALESCE(SUM(revenue),0) FROM data_warehouse.transactions',
          'online_rows':'SELECT COUNT(*) FROM data_warehouse.transactions WHERE sales_mode',
          'periodic_rows':'SELECT COUNT(*) FROM data_warehouse.transactions WHERE NOT sales_mode',
          'cached_rows':'SELECT COUNT(*) FROM data_warehouse.live_cache',
          'shed_rows':'SELECT COUNT(*) FROM waiting_area.shed_table_physical WHERE NOT processed',
          'online_source':'SELECT COUNT(*) FROM oltp_online.online_sales_stream',
          'periodic_source':'SELECT COUNT(*) FROM oltp_physical.periodic_sales_data',
          'customer_master':'SELECT COUNT(*) FROM staging_area.customer_master',
          'product_master':'SELECT COUNT(*) FROM staging_area.product_master',
        }
        with conn.cursor() as cur:
            return {k:scalar(cur,sql) for k,sql in statements.items()}
    finally: conn.close()


def scalar(cur,sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def is_running():
    if not PID_PATH.exists(): return False
    try:
        data=json.loads(PID_PATH.read_text());pid=int(data['pid'])
        os.kill(pid,0)
        # On Linux reject stale/recycled PID if the command is not the project runtime.
        proc=Path(f'/proc/{pid}/cmdline')
        if proc.exists() and b'data_enrichment' not in proc.read_bytes():return False
        return True
    except (OSError,ValueError,KeyError,json.JSONDecodeError):return False


def start(mode='peak', online_file=None):
    if is_running():raise RuntimeError('Runtime is already running')
    RUN_DIR.mkdir(exist_ok=True)
    CONTROL.unlink(missing_ok=True)
    command=[sys.executable,'-u',str(ROOT/'data_enrichment.py'),'--no-console','--arrival-mode',mode,'--control-file',str(CONTROL)]
    if online_file is not None:
        source=Path(online_file).resolve()
        if not source.is_file(): raise FileNotFoundError(source)
        command.extend(['--online-file',str(source)])
    with LOG.open('a',encoding='utf-8') as logfile:
        proc=subprocess.Popen(command,cwd=ROOT,stdout=logfile,stderr=subprocess.STDOUT,
                              start_new_session=True)
    PID_PATH.write_text(json.dumps({'pid':proc.pid,'started_at':datetime.now(timezone.utc).isoformat()}))
    return proc.pid


def command(value:str):
    if value not in {'online','periodic','both','peak','non-peak','status','quit'}:
        raise ValueError('Invalid runtime command')
    if not is_running():raise RuntimeError('Runtime is not running')
    tmp=CONTROL.with_suffix('.tmp')
    tmp.write_text(json.dumps({'id':str(uuid.uuid4()),'command':value}),encoding='utf-8')
    tmp.replace(CONTROL)


def tail(n=60):
    if not LOG.exists():return ''
    from collections import deque
    with LOG.open(encoding='utf-8',errors='replace') as f:
        return ''.join(deque(f,maxlen=n))
