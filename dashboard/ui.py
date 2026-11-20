"""Consistent native Streamlit presentation helpers."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd
import streamlit as st

from dashboard.services import chart_frame


def intro(text: str) -> None:
    st.caption(text)


def database_error(message: str) -> None:
    st.error(f"PostgreSQL is unavailable or incomplete: {message}", icon=":material/error:")
    st.caption(
        "Run `python cli_app.py doctor`. If relations are missing, initialize the "
        "intended disposable project database and then load prepared inputs."
    )


def metric_row(values: dict) -> None:
    metrics = (
        ("Fact records", f"{int(values['fact_rows']):,}"),
        ("Revenue", f"£{Decimal(values['revenue']):,.2f}"),
        ("Online facts", f"{int(values['online_rows']):,}"),
        ("Physical facts", f"{int(values['periodic_rows']):,}"),
        ("Online cache", f"{int(values['cached_rows']):,}"),
        ("Shed backlog", f"{int(values['shed_rows']):,}"),
    )
    with st.container(horizontal=True):
        for label, value in metrics:
            st.metric(label, value, border=True)

    missing = values.get("missing_relations", [])
    if missing:
        st.warning(
            "Schema is incomplete: " + ", ".join(missing),
            icon=":material/warning:",
        )


def dataframe(
    frame: pd.DataFrame,
    *,
    key: str,
    height: int = 420,
) -> None:
    if frame.empty:
        st.info("No rows were returned.", icon=":material/info:")
        return
    st.dataframe(
        frame,
        hide_index=True,
        height=height,
        width="stretch",
        key=key,
    )


def chart(frame: pd.DataFrame, x: str, y: str, *, kind: str, key: str) -> None:
    values = chart_frame(frame, x, y)
    if values.empty:
        st.info("No chartable rows are available.", icon=":material/info:")
        return
    if kind == "line":
        st.line_chart(values, x=x, y=y, key=key)
    else:
        st.bar_chart(values, x=x, y=y, key=key)


def progress_ratio(value: int, total: int, label: str) -> None:
    st.progress(
        min(1.0, value / max(1, total)),
        text=f"{label}: {value:,} / {total:,}",
    )
