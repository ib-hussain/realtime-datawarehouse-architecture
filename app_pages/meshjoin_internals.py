"""MESHJOIN architecture and live engineering counters."""

import pandas as pd
import streamlit as st

from dashboard.services import live_snapshot
from dashboard.ui import database_error, dataframe, intro
from meshjoin.base import load_runtime_config
from observatory import tail

intro(
    "Observe the two-pass disk-paged enrichment path, bounded buffer configuration, "
    "persistent cache, and periodic load-shedding layers."
)

st.table(
    {
        ":material/input: 1. Input": "Online CSV + 48 periodic release groups",
        ":material/low_priority: 2. Buffer": "Online → periodic → recovered shed priority",
        ":material/group: 3. Customer pass": "Cyclic customer master pages",
        ":material/inventory_2: 4. Product pass": "Cyclic product/supplier master pages",
        ":material/database: 5. Load": "Dimensions + bridge + cache/fact",
    },
    border="horizontal",
    width="stretch",
)

runtime = load_runtime_config()
with st.container(horizontal=True):
    st.metric("Master partition", runtime.master_partition_size, border=True)
    st.metric("Stream chunk", runtime.stream_chunk_size, border=True)
    st.metric("Buffer capacity", runtime.stream_buffer_limit, border=True)
    st.metric("Periodic groups", runtime.periodic_group_count, border=True)


@st.fragment(run_every="3s")
def internals() -> None:
    values, error = live_snapshot()
    if error:
        database_error(error)
    else:
        rows = [
            {"Layer": key.replace("_", " ").title(), "Rows": value}
            for key, value in values.items()
            if key not in {"revenue", "schema_ready", "missing_relations"}
        ]
        dataframe(pd.DataFrame(rows), key="meshjoin_counters", height=420)
    with st.container(border=True):
        st.subheader("Runtime trace", icon=":material/terminal:")
        st.code(
            tail(80) or "Start the worker to observe MESHJOIN progress.",
            language="text",
            height=380,
        )


internals()
