"""MESHJOIN worker controls and live telemetry."""

from pathlib import Path

import streamlit as st

from dashboard.services import live_snapshot
from dashboard.ui import database_error, intro, metric_row, progress_ratio
from meshjoin.base import load_runtime_config
from observatory import ROOT, command, is_running, runtime_info, start, stop, tail

intro(
    "Start, switch, inspect, and stop the real background MESHJOIN process. "
    "Commands are exchanged atomically through the shared runtime control file."
)

left, right = st.columns([1.35, 1], vertical_alignment="top")
with left:
    with st.container(border=True):
        st.subheader("Worker control", icon=":material/tune:")
        running = is_running()
        if not running:
            runtime = load_runtime_config()
            candidates = [
                runtime.online_file,
                ROOT / "data" / "static" / "online_sales_stream.csv",
                ROOT / "data" / "dynamic" / "online_sales_stream.csv",
            ]
            options = list(dict.fromkeys(str(path.resolve()) for path in candidates))
            with st.form("start_worker"):
                mode = st.segmented_control(
                    "Initial arrival rate",
                    ["peak", "non-peak"],
                    default="peak",
                )
                online_file = st.selectbox("Online stream file", options)
                submitted = st.form_submit_button(
                    "Start MESHJOIN worker",
                    icon=":material/play_arrow:",
                    type="primary",
                )
            if submitted:
                try:
                    with st.status("Running startup checks…", expanded=True) as status:
                        pid = start(mode or "peak", Path(online_file))
                        status.update(label=f"Worker started as PID {pid}", state="complete")
                    st.rerun()
                except Exception as error:
                    st.error(str(error), icon=":material/error:")
        else:
            runtime_command = st.segmented_control(
                "Command",
                ["status", "online", "periodic", "both", "peak", "non-peak"],
                default="status",
                key="live_runtime_command",
            )
            with st.container(horizontal=True):
                if st.button("Send command", icon=":material/send:", type="primary"):
                    try:
                        command(runtime_command or "status")
                        st.toast("Command accepted", icon=":material/check_circle:")
                    except Exception as error:
                        st.error(str(error), icon=":material/error:")
                if st.button("Stop gracefully", icon=":material/stop:"):
                    try:
                        stop()
                        st.success("Worker stopped.", icon=":material/check_circle:")
                        st.rerun()
                    except Exception as error:
                        st.error(str(error), icon=":material/error:")

with right, st.container(border=True):
    st.subheader("Ingestion policy", icon=":material/policy:")
    st.markdown(
        "**Peak mode** enriches online rows into the durable live cache and may "
        "shed periodic rows if the bounded buffer is full. **Non-peak mode** "
        "drains cached online rows into the fact table and requeues shed periodic rows."
    )
    st.caption(
        "The start action performs schema, file, and staged-data checks before a PID is reported."
    )


@st.fragment(run_every="2s")
def live_telemetry() -> None:
    process = runtime_info()
    telemetry = process.get("telemetry") or {}
    if telemetry and process.get("running"):
        with st.container(horizontal=True):
            st.metric("Source mode", telemetry.get("source_mode", "—"), border=True)
            st.metric("Arrival mode", telemetry.get("arrival_mode", "—"), border=True)
            st.metric(
                "Current speed",
                f"{int(telemetry.get('rate_per_second', 0)):,}/s",
                border=True,
            )
            st.metric(
                "Stream buffer",
                f"{int(telemetry.get('stream_buffer', 0)):,} / "
                f"{int(telemetry.get('stream_buffer_limit', 0)):,}",
                border=True,
            )
            st.metric(
                "Hash table H",
                f"{int(telemetry.get('hash_transactions', 0)):,}",
                border=True,
            )
    elif process.get("running"):
        st.caption("Waiting for the worker's first telemetry update…")

    values, error = live_snapshot()
    if error:
        database_error(error)
    else:
        metric_row(values)
        progress_ratio(
            int(values["cached_rows"]),
            int(values["online_source"]),
            "Online cache",
        )
        progress_ratio(
            int(values["shed_rows"]),
            int(values["periodic_source"]),
            "Periodic shed backlog",
        )
    with st.container(border=True):
        st.subheader("Worker console", icon=":material/terminal:")
        st.code(tail(60) or "No runtime log entries yet.", language="text", height=360)


live_telemetry()
