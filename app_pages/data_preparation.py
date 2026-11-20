"""CSV cleaning, splitting, master-data generation, and explicit DB load."""

from pathlib import Path

import streamlit as st

from dashboard.services import load_json_report
from dashboard.ui import intro
from data.dynamic.clean_data import run as clean
from data.dynamic.data_split import run as split
from data.dynamic.feed_db import run as prepare
from meshjoin.base import load_runtime_config
from observatory import ROOT, is_running

intro(
    "Run the deterministic UCI Online Retail preparation stages with project "
    "defaults. Database loading remains explicit because it replaces source and master tables."
)

output = ROOT / "data" / "dynamic"
source_text = st.text_input(
    "Source CSV path",
    value=str(ROOT / "data" / "raw" / "online-retail.csv"),
    help="The file must use the original UCI column names and DD/MM/YYYY HH:MM timestamps.",
)
source = Path(source_text).expanduser()

with st.container(border=True):
    st.subheader("Preparation stages", icon=":material/build:")
    with st.container(horizontal=True):
        clean_clicked = st.button("Clean data", icon=":material/cleaning_services:")
        split_clicked = st.button("Split streams", icon=":material/call_split:")
        prepare_clicked = st.button(
            "Prepare all CSVs",
            icon=":material/playlist_add_check:",
            type="primary",
        )

    try:
        if clean_clicked:
            with st.status("Cleaning and deduplicating…") as status:
                result = clean(source, output / "processed-retail.csv")
                status.update(label="Cleaning complete", state="complete")
            st.json(result)
        if split_clicked:
            with st.status("Splitting online and periodic streams…") as status:
                result = split(output / "processed-retail.csv", output)
                status.update(label="Split complete", state="complete")
            st.json(result)
        if prepare_clicked:
            with st.status("Preparing all CSV and master-data outputs…") as status:
                result = prepare(source, output)
                status.update(label="Preparation complete", state="complete")
            st.json(result)
    except Exception as error:
        st.error(str(error), icon=":material/error:")

with st.container(border=True):
    st.subheader("Load PostgreSQL inputs", icon=":material/database:")
    st.warning(
        "This operation truncates and repopulates the operational source and staging "
        "tables. Stop MESHJOIN and confirm your target database first.",
        icon=":material/warning:",
    )
    confirmed = st.checkbox(
        "I confirmed the database target, stopped the worker, and accept source-table replacement."
    )
    load_clicked = st.button(
        "Prepare and load PostgreSQL",
        icon=":material/upload:",
        disabled=not confirmed or is_running(),
    )
    if load_clicked:
        try:
            with st.status("Preparing files and loading PostgreSQL…", expanded=True) as status:
                result = prepare(source, output, load_runtime_config().database)
                status.update(label="Database inputs loaded", state="complete")
            st.json(result)
        except Exception as error:
            st.error(str(error), icon=":material/error:")

report, report_error = load_json_report(output / "last_run.json")
if report_error:
    st.warning(f"Could not read the last report: {report_error}")
elif report:
    with st.expander("Last preparation report", icon=":material/description:"):
        st.json(report)
