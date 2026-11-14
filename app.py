"""Retail DWBI Observatory · Streamlit front end for the actual PostgreSQL project."""
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from observatory import ROOT,snapshot,query,start,command,is_running,tail
from data.dynamic.feed_db import run as prepare
from data.dynamic.clean_data import run as clean
from data.dynamic.data_split import run as split
from meshjoin.base import load_runtime_config

PINE='#304D3D'; SAGE='#A7B88D'; MOSS='#6B705C'; FOREST='#3A5A40'; TEAL='#2C837A'; GOLD='#D5A65A'; CREAM='#F5F6F0'; RED='#B56759'
PALETTE=[PINE,TEAL,SAGE,GOLD,MOSS,RED]
st.set_page_config(page_title='DWBI | Retail Operations Observatory',page_icon='◈',layout='wide',initial_sidebar_state='expanded')

st.markdown(f'''<style>
:root{{--pine:{PINE};--sage:{SAGE};--teal:{TEAL};--gold:{GOLD}}}
.stApp{{background:radial-gradient(circle at 90% 3%,#e5ecdf,transparent 30%),{CREAM};color:{PINE}}}
[data-testid="stSidebar"]{{background:linear-gradient(160deg,{PINE},{FOREST});border-right:2px solid {SAGE}}}
[data-testid="stSidebar"] *{{color:#EDF4E6!important}}
[data-testid="stSidebar"] button{{background:#496859!important;border:1px solid #819A80!important}}
.block-container{{max-width:1650px;padding-top:1.5rem}}
.hero{{background:linear-gradient(115deg,{PINE},#376356 70%,{TEAL});color:white;padding:28px 34px;border-radius:20px;margin-bottom:20px;box-shadow:0 14px 30px #304D3D25}}
.hero h1{{font-size:clamp(24px,2.7vw,39px);letter-spacing:-.04em;margin:4px 0;color:#F6F8F1}}
.hero p{{color:#DCE9D8;margin:0;max-width:960px}}
.eyebrow{{text-transform:uppercase;letter-spacing:.2em;font-size:12px;font-weight:700;color:{SAGE}}}
[data-testid="stMetric"]{{background:white;border:1px solid #D6E0D5;border-radius:15px;padding:16px;box-shadow:0 4px 16px #304D3D09}}
[data-testid="stMetricValue"]{{color:{PINE}}}
h1,h2,h3{{color:{PINE}}}
[data-testid="stVerticalBlockBorderWrapper"]{{border-radius:16px}}
div[data-testid="stDataFrame"]{{border:1px solid #C8D7CC;border-radius:12px}}
button:focus-visible,a:focus-visible{{outline:3px solid {GOLD}!important;outline-offset:3px}}
</style>''',unsafe_allow_html=True)

CATALOG={
 'Sales performance':'''SELECT date_trunc('month',event_time)::date AS month, ROUND(SUM(revenue),2) AS revenue,COUNT(*) AS lines FROM data_warehouse.transactions GROUP BY 1 ORDER BY 1''',
 'Sales mode comparison':'''SELECT CASE WHEN sales_mode THEN 'Online' ELSE 'Physical' END AS channel,COUNT(*) AS lines,ROUND(SUM(revenue),2) AS revenue FROM data_warehouse.transactions GROUP BY 1 ORDER BY revenue DESC''',
 'Top product categories':'''SELECT p.product_category,COUNT(*) AS lines,ROUND(SUM(t.revenue),2) AS revenue FROM data_warehouse.transactions t JOIN data_warehouse.dim_product p USING(product_id) GROUP BY 1 ORDER BY revenue DESC''',
 'Store leaderboard':'''SELECT s.store_name,COUNT(*) AS lines,ROUND(SUM(t.revenue),2) AS revenue FROM data_warehouse.transactions t JOIN data_warehouse.dim_store s USING(store_id) GROUP BY 1 ORDER BY revenue DESC''',
 'Top products':'''SELECT p.product_name,COUNT(*) AS lines,ROUND(SUM(t.revenue),2) AS revenue FROM data_warehouse.transactions t JOIN data_warehouse.dim_product p USING(product_id) GROUP BY 1 ORDER BY revenue DESC LIMIT 15''',
 'Top customers':'''SELECT t.customer_id,COUNT(*) AS lines,ROUND(SUM(t.revenue),2) AS revenue FROM data_warehouse.transactions t GROUP BY 1 ORDER BY revenue DESC LIMIT 20''',
 'Weekly throughput':'''SELECT date_trunc('week',loaded_at)::date AS week, COUNT(*) AS loaded FROM data_warehouse.transactions GROUP BY 1 ORDER BY 1''',
 'Hourly sales distribution':'''SELECT EXTRACT(HOUR FROM event_time)::int AS hour, COUNT(*) AS lines,ROUND(SUM(revenue),2) AS revenue FROM data_warehouse.transactions GROUP BY 1 ORDER BY 1''',
 'Supplier performance':'''SELECT s.supplier_name,COUNT(*) AS lines,ROUND(SUM(t.revenue),2) AS revenue FROM data_warehouse.transactions t JOIN data_warehouse.dim_supplier s USING(supplier_id) GROUP BY 1 ORDER BY revenue DESC''',
 'Processing quality':'''SELECT processing_mode, CASE WHEN sales_mode THEN 'Online' ELSE 'Periodic' END AS channel, COUNT(*) AS lines FROM data_warehouse.transactions GROUP BY 1,2 ORDER BY 1,2''',
 'Cached online arrivals':'''SELECT cache_id,transaction_id,customer_id,product_id,revenue,cached_at FROM data_warehouse.live_cache ORDER BY cache_id DESC LIMIT 100''',
 'Recent fact insertions':'''SELECT transaction_line_id,transaction_id,store_id,revenue,loaded_at FROM data_warehouse.transactions ORDER BY transaction_line_id DESC LIMIT 100''',
 'Pending periodic shed':'''SELECT COUNT(*) AS pending_rows FROM waiting_area.shed_table_physical WHERE processed=FALSE''',
}

def data(sql,limit=2000):
    names,rows=query(sql,limit)
    return pd.DataFrame(rows,columns=names)

def chart(df,x,y,kind='bar',height=370):
    if df.empty:st.info('No warehouse records yet. Load sources and start the pipeline.');return
    values=df.copy()
    for c in values.columns:
        if c!=x:
            try:values[c]=pd.to_numeric(values[c])
            except (ValueError,TypeError):pass
    fig=(px.line(values,x=x,y=y,markers=True,color_discrete_sequence=PALETTE) if kind=='line' else
         px.bar(values,x=x,y=y,color_discrete_sequence=PALETTE))
    fig.update_layout(height=height,paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)',
      font={'color':PINE,'family':'Inter, Segoe UI, sans-serif'},margin=dict(l=12,r=10,b=15,t=20),
      hovermode='x unified')
    fig.update_xaxes(gridcolor='#e3eadd');fig.update_yaxes(gridcolor='#e3eadd')
    st.plotly_chart(fig,use_container_width=True,config={'displaylogo':False})

def metric_row(s):
    c=st.columns(6)
    vals=[('Fact records',f"{s['fact_rows']:,}"),('Revenue',f"£{s['revenue']:,.2f}"),
          ('Online facts',f"{s['online_rows']:,}"),('Physical facts',f"{s['periodic_rows']:,}"),
          ('Online cache',f"{s['cached_rows']:,}"),('Shed backlog',f"{s['shed_rows']:,}")]
    for el,(label,value) in zip(c,vals):el.metric(label,value)

def get_stats():
    try:return snapshot(),None
    except Exception as exc:return None,str(exc)

st.sidebar.markdown('## ◈ RETAIL / DWBI')
st.sidebar.caption('Warehouse operations & business intelligence')
PAGE=st.sidebar.radio('Workspace',['Overview','Live operations','Data preparation','Data explorer','OLAP query studio','MESHJOIN internals','System health'],label_visibility='collapsed')
st.sidebar.markdown('---')
st.sidebar.write('**Database**')
try:
    cfg=load_runtime_config()
    st.sidebar.caption(f'{cfg.host}:{cfg.port} / {cfg.database}')
except Exception:st.sidebar.caption('Not configured · create .env')
st.sidebar.caption('Worker ' + ('ACTIVE' if is_running() else 'STOPPED'))
st.sidebar.caption('Live figures query PostgreSQL; no example metrics are substituted.')

st.markdown(f'''<div class="hero"><div class="eyebrow">DS3003 / DS3004 · Data Warehousing & Business Intelligence</div>
<h1>{PAGE}</h1><p>Online + periodic retail streams · MESHJOIN enrichment · load shedding · non-peak cache drain · auditable PostgreSQL warehouse</p></div>''',unsafe_allow_html=True)

if PAGE=='Overview':
    st.subheader('Executive warehouse pulse')
    @st.fragment(run_every='3s')
    def pulse():
        s,error=get_stats()
        if error:st.error(f'Database unavailable: {error}');return
        metric_row(s)
        a,b=st.columns([1.4,1])
        with a:
            st.markdown('#### Revenue trajectory')
            try:chart(data(CATALOG['Sales performance']), 'month','revenue','line')
            except Exception as e:st.warning(str(e))
        with b:
            st.markdown('#### Sales mix')
            try:chart(data(CATALOG['Sales mode comparison']),'channel','revenue')
            except Exception as e:st.warning(str(e))
        st.markdown('#### Latest warehouse rows')
        try:st.dataframe(data(CATALOG['Recent fact insertions'],50),use_container_width=True,hide_index=True)
        except Exception as e:st.warning(str(e))
    pulse()

elif PAGE=='Live operations':
    st.subheader('Control room')
    st.caption('Commands target the real MESHJOIN process through its shared control file; values below are read from PostgreSQL.')
    left,right=st.columns([1.5,1])
    with left:
        if not is_running():
            mode=st.selectbox('Startup arrival rate',['peak','non-peak'])
            online_file=st.selectbox('Online stream file', [str(v) for v in [ROOT/'data/static/online_sales_stream.csv',ROOT/'data/dynamic/online_sales_stream.csv']])
            if st.button('Start actual MESHJOIN worker',type='primary'):
                try:st.success(f'Launched process PID {start(mode,online_file)}');st.rerun()
                except Exception as e:st.error(str(e))
        else:
            cmd=st.selectbox('Runtime command',['status','online','periodic','both','peak','non-peak','quit'])
            if st.button('Send command',type='primary'):
                try:command(cmd);st.success('Control request sent to worker')
                except Exception as e:st.error(str(e))
    with right:
        st.markdown('#### Ingestion policy')
        st.write('**Peak:** online records are cached after joining. **Non-peak:** cached online rows are moved into the warehouse. Physical-store rows load directly after their scheduled release and enrichment.')
    @st.fragment(run_every='2s')
    def live():
        s,e=get_stats()
        if e:st.error(e)
        else:
            metric_row(s)
            st.progress(min(1.,s['cached_rows']/max(1,s['online_source'])),text=f"Online cache {s['cached_rows']:,} / {s['online_source']:,} source records")
            st.progress(min(1.,s['shed_rows']/max(1,s['periodic_source'])),text=f"Periodic shed {s['shed_rows']:,} / {s['periodic_source']:,} source records")
        st.markdown('#### Worker console · latest lines')
        st.code(tail(50) or 'No runtime logs yet.',language='text')
    live()

elif PAGE=='Data preparation':
    st.subheader('Dynamic ingestion pipeline')
    st.caption('Actual processing of the supplied UCI Online Retail CSV, with deterministic cleaning and invoice-level MD5 split. Database feed is intentionally explicit.')
    path=st.text_input('Existing CSV path on the host running Streamlit',str(ROOT/'data/raw/online-retail.csv'))
    output=ROOT/'data/dynamic'
    a,b,c=st.columns(3)
    with a:
        if st.button('1 · Clean data',use_container_width=True):
            try:st.json(clean(Path(path),output/'processed-retail.csv'))
            except Exception as e:st.error(str(e))
    with b:
        if st.button('2 · Split online / periodic',use_container_width=True):
            try:st.json(split(output/'processed-retail.csv',output))
            except Exception as e:st.error(str(e))
    with c:
        if st.button('3 · Prepare all CSVs',use_container_width=True):
            try:st.json(prepare(Path(path),output))
            except Exception as e:st.error(str(e))
    st.divider()
    st.warning('Database feeding resets/repopulates input schemas. Stop MESHJOIN first and back up PostgreSQL before loading.')
    confirm=st.checkbox('I have stopped the worker, backed up my database, and authorize source-table replacement.')
    if st.button('Feed prepared source tables into PostgreSQL',disabled=not confirm or is_running()):
        try:st.json(prepare(Path(path),output,load_runtime_config().database))
        except Exception as e:st.error(str(e))
    report=output/'last_run.json'
    if report.exists():st.markdown('#### Last preparation report');st.json(report.read_text())

elif PAGE=='Data explorer':
    st.subheader('Warehouse tables and source layers')
    source={
       'Warehouse transactions':'data_warehouse.transactions','Online source':'oltp_online.online_sales_stream',
       'Periodic source':'oltp_physical.periodic_sales_data','Online cache':'data_warehouse.live_cache',
       'Shed backlog':'waiting_area.shed_table_physical','Customer dimension':'data_warehouse.dim_customer',
       'Product dimension':'data_warehouse.dim_product','Customer master':'staging_area.customer_master',
       'Product master':'staging_area.product_master'}
    which=st.selectbox('Select an actual relation',list(source))
    limit=st.slider('Rows to inspect',10,500,100,10)
    try:
        table=source[which]
        df=data(f'SELECT * FROM {table} LIMIT {limit}',limit)
        st.metric('Returned rows',len(df))
        st.dataframe(df,use_container_width=True,hide_index=True,height=480)
        st.download_button('Export displayed rows',df.to_csv(index=False).encode(),file_name='warehouse-inspection.csv',mime='text/csv')
    except Exception as e:st.error(str(e))

elif PAGE=='OLAP query studio':
    st.subheader('Live SQL + visible results')
    name=st.selectbox('Curated business query',list(CATALOG))
    sql=st.text_area('PostgreSQL SELECT editor',value=CATALOG[name],height=185,key=name)
    maxrows=st.slider('Maximum result rows',10,1000,250,10)
    if st.button('Execute against local PostgreSQL',type='primary'):
        try:
            t=datetime.now()
            df=data(sql,maxrows)
            st.success(f'{len(df):,} rows fetched · {(datetime.now()-t).total_seconds():.3f} seconds')
            st.dataframe(df,use_container_width=True,hide_index=True,height=410)
            st.download_button('Export query results',df.to_csv(index=False).encode(),file_name='olap-results.csv',mime='text/csv')
            numeric=[c for c in df if pd.api.types.is_numeric_dtype(pd.to_numeric(df[c],errors='coerce')) and pd.to_numeric(df[c],errors='coerce').notna().all()]
            if len(df.columns)>1 and numeric:
                xcol=st.selectbox('Chart dimension',[c for c in df.columns if c not in numeric] or [df.columns[0]])
                ycol=st.selectbox('Chart measure',numeric)
                chart(df,xcol,ycol)
        except Exception as e:st.error(str(e))
    st.caption('Security: single read-only SELECT, database read-only transaction and five-second statement timeout. Deploy behind authentication for multi-user or remote access.')

elif PAGE=='MESHJOIN internals':
    st.subheader('Two-pass master-data enrichment')
    st.markdown('**Stream → bounded priority buffer → customer partitions → product partitions → joined records → online cache / periodic facts → non-peak cache drain**')
    a,b,c=st.columns(3)
    a.metric('Master partition size',500);b.metric('Hash/stream chunk',500);c.metric('Buffer capacity',1000)
    @st.fragment(run_every='3s')
    def internals():
        s,e=get_stats()
        if e:st.error(e);return
        st.dataframe(pd.DataFrame([{'Layer':k.replace('_',' ').title(),'Rows':v} for k,v in s.items() if k not in {'revenue'}]),use_container_width=True,hide_index=True)
        st.code(tail(70) or 'Start the worker to observe MESHJOIN progress.',language='text')
    internals()
    st.caption('Counters for active in-memory hash entries are emitted by worker status logs. Persistent counters are read directly from PostgreSQL.')

else:
    st.subheader('Database & process health')
    try:
        cfg=load_runtime_config()
        st.write({'host':cfg.database.host,'port':cfg.database.port,'database':cfg.database.database,
                  'configured_online_file':str(cfg.online_file),'peak_rate':cfg.peak_rate,
                  'non_peak_rate':cfg.non_peak_rate,'simulated_hour_seconds':cfg.simulated_hour_seconds,
                  'periodic_groups':cfg.periodic_group_count})
        s,e=get_stats()
        if e:st.error(e)
        else:st.json({k:str(v) for k,v in s.items()})
    except Exception as e:st.error(str(e))
    st.write('**Worker status:**','running' if is_running() else 'stopped')
    st.write('**Runtime log:**',str(ROOT/'logs/meshjoin.log'))
    st.info('The web process and CLI share PostgreSQL and worker control files. Use a trusted local machine; authentication, role permissions, HTTPS and backups must be configured before external deployment.')
