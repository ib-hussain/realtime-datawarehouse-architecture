"""Retail DWBI Observatory Streamlit entry point."""

import streamlit as st

from meshjoin.base import load_runtime_config
from observatory import is_running

st.set_page_config(
    page_title="DWBI · Retail operations observatory",
    page_icon=":material/analytics:",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.session_state.setdefault("query_result", None)
st.session_state.setdefault("query_elapsed", None)

page = st.navigation(
    {
        "Operations": [
            st.Page(
                "app_pages/overview.py", title="Overview", icon=":material/home:", default=True
            ),
            st.Page(
                "app_pages/live_operations.py",
                title="Live operations",
                icon=":material/play_arrow:",
            ),
        ],
        "Data workspace": [
            st.Page(
                "app_pages/data_preparation.py", title="Data preparation", icon=":material/build:"
            ),
            st.Page(
                "app_pages/data_explorer.py", title="Data explorer", icon=":material/table_chart:"
            ),
            st.Page(
                "app_pages/query_studio.py",
                title="OLAP query studio",
                icon=":material/query_stats:",
            ),
        ],
        "Engineering": [
            st.Page(
                "app_pages/meshjoin_internals.py",
                title="MESHJOIN internals",
                icon=":material/code:",
            ),
            st.Page(
                "app_pages/system_health.py", title="System health", icon=":material/settings:"
            ),
        ],
    },
    position="sidebar",
)

with st.sidebar:
    st.caption("DS3003 / DS3004 · Retail DWBI")
    if is_running():
        st.badge("Worker active", icon=":material/check_circle:", color="green")
    else:
        st.badge("Worker stopped", icon=":material/pause_circle:", color="gray")
    try:
        runtime = load_runtime_config()
        database = runtime.database
        st.caption(f"{database.host}:{database.port} · {database.database}")
    except Exception as error:
        st.caption(f"Configuration error: {error}")
    st.caption("Live values come from local PostgreSQL; sample metrics are never substituted.")

st.title(page.title, icon=page.icon)
page.run()
