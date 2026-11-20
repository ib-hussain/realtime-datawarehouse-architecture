"""Executive warehouse overview."""

import streamlit as st

from dashboard.catalog import CATALOG
from dashboard.services import cached_query, live_snapshot
from dashboard.ui import chart, database_error, dataframe, intro, metric_row

intro(
    "Live warehouse pulse across online and physical-store flows. Counters refresh "
    "independently; charts use short bounded caches to keep navigation responsive."
)

with st.container(horizontal=True, horizontal_alignment="right"):
    st.button(
        "Refresh dashboard",
        icon=":material/refresh:",
        on_click=cached_query.clear,
    )


@st.fragment(run_every="5s")
def pulse() -> None:
    values, error = live_snapshot()
    if error:
        database_error(error)
        return
    metric_row(values)


pulse()

left, right = st.columns([1.4, 1], vertical_alignment="top")
with left, st.container(border=True):
    st.subheader("Revenue trajectory", icon=":material/show_chart:")
    try:
        monthly = cached_query(CATALOG["Sales performance"])
        chart(monthly, "month", "revenue", kind="line", key="overview_revenue")
    except Exception as error:
        st.warning(str(error), icon=":material/warning:")

with right, st.container(border=True):
    st.subheader("Sales mix", icon=":material/bar_chart:")
    try:
        channels = cached_query(CATALOG["Sales mode comparison"])
        chart(channels, "channel", "revenue", kind="bar", key="overview_mix")
    except Exception as error:
        st.warning(str(error), icon=":material/warning:")

with st.container(border=True):
    st.subheader("Latest fact insertions", icon=":material/table_chart:")
    try:
        latest = cached_query(CATALOG["Recent fact insertions"], 50)
        dataframe(latest, key="overview_latest", height=360)
    except Exception as error:
        st.warning(str(error), icon=":material/warning:")
