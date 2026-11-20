"""Bounded inspection of approved project relations."""

import streamlit as st

from dashboard.catalog import RELATIONS
from dashboard.services import run_query
from dashboard.ui import dataframe, intro

intro(
    "Inspect approved warehouse, operational, staging, and waiting-area relations. "
    "Relation names come from a fixed allow-list; result size is always bounded."
)

with st.form("relation_explorer"):
    relation_label = st.selectbox("Relation", list(RELATIONS))
    limit = st.slider("Rows to inspect", 10, 500, 100, 10)
    submitted = st.form_submit_button(
        "Load rows",
        icon=":material/search:",
        type="primary",
    )

if submitted:
    try:
        relation = RELATIONS[relation_label]
        result = run_query(f"SELECT * FROM {relation} LIMIT {limit}", limit)
        st.session_state["explorer_result"] = result
        st.session_state["explorer_label"] = relation_label
    except Exception as error:
        st.error(str(error), icon=":material/error:")

result = st.session_state.get("explorer_result")
if result is not None:
    with st.container(horizontal=True):
        st.metric("Returned rows", len(result), border=True)
        st.metric("Columns", len(result.columns), border=True)
    st.caption(f"Showing: {st.session_state.get('explorer_label', relation_label)}")
    dataframe(result, key="explorer_table", height=500)
    st.download_button(
        "Export displayed rows",
        result.to_csv(index=False).encode("utf-8"),
        file_name="warehouse-inspection.csv",
        mime="text/csv",
        icon=":material/download:",
    )
