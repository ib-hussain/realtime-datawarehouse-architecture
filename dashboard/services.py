"""Testable data-access helpers used by Streamlit pages."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from observatory import query, snapshot


def run_query(sql: str, limit: int = 2_000) -> pd.DataFrame:
    columns, rows = query(sql, limit)
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl="10s", max_entries=32, show_spinner=False)
def cached_query(sql: str, limit: int = 2_000) -> pd.DataFrame:
    return run_query(sql, limit)


def live_snapshot() -> tuple[dict | None, str | None]:
    try:
        return snapshot(), None
    except Exception as error:
        return None, str(error)


def load_json_report(path: Path) -> tuple[dict | None, str | None]:
    if not path.is_file():
        return None, None
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (OSError, json.JSONDecodeError) as error:
        return None, str(error)


def numeric_columns(frame: pd.DataFrame) -> list[str]:
    result: list[str] = []
    for column in frame.columns:
        converted = pd.to_numeric(frame[column], errors="coerce")
        if len(converted) and converted.notna().all():
            result.append(column)
    return result


def chart_frame(frame: pd.DataFrame, x: str, y: str) -> pd.DataFrame:
    result = frame[[x, y]].copy()
    result[y] = pd.to_numeric(result[y], errors="coerce")
    return result.dropna(subset=[y])
