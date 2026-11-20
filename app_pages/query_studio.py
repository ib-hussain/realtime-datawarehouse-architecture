"""Curated and custom read-only OLAP query workspace."""

import time

import streamlit as st

from dashboard.catalog import CATALOG
from dashboard.services import numeric_columns, run_query
from dashboard.ui import chart, dataframe, intro

intro(
    "Run one bounded SELECT against local PostgreSQL, inspect its result, export it, "
    "and chart a numeric measure. Writes and high-risk server functions are blocked."
)

first_query = next(iter(CATALOG))
st.session_state.setdefault("studio_query_name", first_query)
st.session_state.setdefault("studio_sql", CATALOG[first_query].strip())


def load_selected_query() -> None:
    st.session_state["studio_sql"] = CATALOG[st.session_state["studio_query_name"]].strip()


st.selectbox(
    "Curated business query",
    list(CATALOG),
    key="studio_query_name",
    on_change=load_selected_query,
)

with st.form("query_form"):
    st.text_area(
        "PostgreSQL SELECT editor",
        key="studio_sql",
        height=220,
    )
    maximum_rows = st.slider("Maximum result rows", 10, 2_000, 250, 10)
    submitted = st.form_submit_button(
        "Execute query",
        icon=":material/play_arrow:",
        type="primary",
    )

if submitted:
    try:
        started = time.perf_counter()
        result = run_query(st.session_state["studio_sql"], maximum_rows)
        st.session_state["query_result"] = result
        st.session_state["query_elapsed"] = time.perf_counter() - started
    except Exception as error:
        st.error(str(error), icon=":material/error:")

result = st.session_state.get("query_result")
if result is not None:
    elapsed = st.session_state.get("query_elapsed") or 0
    st.success(
        f"{len(result):,} rows fetched in {elapsed:.3f} seconds.",
        icon=":material/check_circle:",
    )
    dataframe(result, key="studio_results", height=430)
    st.download_button(
        "Export query results",
        result.to_csv(index=False).encode("utf-8"),
        file_name="olap-results.csv",
        mime="text/csv",
        icon=":material/download:",
    )

    numeric = numeric_columns(result)
    if len(result.columns) > 1 and numeric:
        with st.container(border=True):
            st.subheader("Result chart", icon=":material/bar_chart:")
            dimensions = [column for column in result.columns if column not in numeric]
            x_column = st.selectbox("Dimension", dimensions or [result.columns[0]])
            y_column = st.selectbox("Measure", numeric)
            chart_type = st.segmented_control(
                "Chart type",
                ["bar", "line"],
                default="bar",
            )
            chart(
                result,
                x_column,
                y_column,
                kind=chart_type or "bar",
                key="studio_chart",
            )

st.caption(
    "Use a dedicated read-only PostgreSQL role before exposing this app beyond a trusted local machine."
)
