"""Environment, file, database, schema, and process diagnostics."""

import pandas as pd
import streamlit as st

from dashboard.catalog import CLI_COMMANDS
from dashboard.ui import dataframe, intro
from meshjoin.base import load_runtime_config
from observatory import diagnostics

intro(
    "Run the same bounded checks as `python cli_app.py doctor` and review exact "
    "recovery commands without exposing database credentials."
)


@st.cache_data(ttl="5s", show_spinner=False)
def health_report() -> dict:
    return diagnostics()


with st.container(horizontal=True, horizontal_alignment="right"):
    st.button("Recheck", icon=":material/refresh:", on_click=health_report.clear)

report = health_report()
if report["ok"]:
    st.success("All required checks passed.", icon=":material/check_circle:")
else:
    st.error(
        "One or more required checks failed. Resolve errors before starting MESHJOIN.",
        icon=":material/error:",
    )

checks = pd.DataFrame(report["checks"])
dataframe(checks, key="health_checks", height=430)

with st.container(border=True):
    st.subheader("Effective configuration", icon=":material/settings:")
    try:
        runtime = load_runtime_config()
        config = {
            "Database host": runtime.database.host,
            "Database port": str(runtime.database.port),
            "Database name": runtime.database.database,
            "Database user": runtime.database.user,
            "Password configured": "Yes" if runtime.database.password else "No",
            "Online stream": str(runtime.online_file),
            "Peak rate": str(runtime.peak_rate),
            "Non-peak rate": str(runtime.non_peak_rate),
            "Simulated hour (seconds)": str(runtime.simulated_hour_seconds),
        }
        st.table(config, border="horizontal", width="stretch")
    except Exception as error:
        st.error(str(error), icon=":material/error:")

with st.container(border=True):
    st.subheader("CLI command map", icon=":material/terminal:")
    st.table(CLI_COMMANDS, border="horizontal", width="stretch")
    st.warning("`init-db --yes` is destructive: it drops and recreates all five project schemas.")
