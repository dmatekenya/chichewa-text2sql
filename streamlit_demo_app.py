"""
demo_app.py — Chichewa Text-to-SQL Demo
Run: .venv/Scripts/streamlit.exe run demo_app.py

100% local. No model, no HF API. Uses dataset retrieval + SQLite execution.
Perfect for a live demo.
"""
from __future__ import annotations

import json
import re
import sqlite3
import difflib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

BASE_DIR  = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data" / "all.json"
DB_PATH   = BASE_DIR / "data" / "database" / "chichewa_text2sql.db"

FORBIDDEN = {
    "insert", "update", "delete", "drop", "alter",
    "attach", "pragma", "create", "replace", "truncate",
}

# ── Data loading ──────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_examples() -> List[Dict[str, Any]]:
    if not DATA_PATH.exists():
        return []
    with DATA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

@st.cache_data(show_spinner=False)
def get_tables() -> List[str]:
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

@st.cache_data(show_spinner=False)
def get_schema() -> Dict[str, List[str]]:
    if not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(DB_PATH)
    schema = {}
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        for t in tables:
            cols = conn.execute(f"PRAGMA table_info({t});").fetchall()
            schema[t] = [f"{c[1]} ({c[2]})" for c in cols]
    finally:
        conn.close()
    return schema

# ── Matching ──────────────────────────────────────────────────────────────
def _norm(t: str) -> str:
    return " ".join(t.lower().strip().split())

def find_match(question: str, language: str) -> Tuple[Optional[Dict], float, str]:
    examples = load_examples()
    key = "question_ny" if language == "ny" else "question_en"
    q = _norm(question)

    for ex in examples:
        if _norm(ex.get(key, "")) == q:
            return ex, 1.0, "exact"

    corpus = [_norm(ex.get(key, "")) for ex in examples]
    hits = difflib.get_close_matches(q, corpus, n=1, cutoff=0.5)
    if hits:
        idx = corpus.index(hits[0])
        score = difflib.SequenceMatcher(None, q, hits[0]).ratio()
        return examples[idx], round(score, 3), "fuzzy"

    return None, 0.0, "none"

# ── SQL execution ─────────────────────────────────────────────────────────
def run_query(sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    s = sql.strip().rstrip(";")
    if not s.lower().startswith("select"):
        return None, "Only SELECT statements are allowed."
    if ";" in s:
        return None, "Multiple statements not allowed."
    if any(kw in s.lower() for kw in FORBIDDEN):
        return None, "Forbidden keyword in SQL."
    if not DB_PATH.exists():
        return None, f"Database not found: {DB_PATH}"
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql).fetchall()
        if not rows:
            return pd.DataFrame(), None
        return pd.DataFrame([dict(r) for r in rows]), None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()

# ── UI ────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(
        page_title="Chichewa Text-to-SQL",
        page_icon="🌍",
        layout="wide",
    )

    # Header
    st.title("🌍 Chichewa Text-to-SQL")
    st.caption(
        "Ask questions about agriculture, population, food security, commodity prices, "
        "and market exchange rates in **Chichewa** or **English**."
    )

    # Sidebar — schema reference
    with st.sidebar:
        st.header("📋 Database Schema")
        schema = get_schema()
        for table, cols in schema.items():
            with st.expander(f"**{table}**", expanded=False):
                for c in cols:
                    st.markdown(f"- `{c}`")

        st.divider()
        st.markdown("**Dataset:** 3,602 Chichewa/English Q&A pairs")
        st.markdown("**Model:** `johneze/Llama-3.1-8B-Instruct-chichewa-text2sql`")
        st.markdown("**Mode:** Baseline retrieval (local)")

    # Main input
    col_q, col_lang = st.columns([3, 1])
    with col_q:
        question = st.text_area(
            "Your question",
            placeholder="Ndi boma liti komwe anakolola chimanga chambiri?",
            height=100,
            label_visibility="collapsed",
        )
    with col_lang:
        language = st.radio(
            "Language",
            ["ny", "en"],
            format_func=lambda x: "🇲🇼 Chichewa" if x == "ny" else "🇬🇧 English",
            index=0,
        )

    # Example buttons
    st.markdown("**Try an example:**")
    examples_ny = [
        "Ndi boma liti komwe anakolola chimanga chambiri?",
        "Ndi anthu angati ku Lilongwe?",
        "Ndi mtengo wanji wa chimanga ku January 2020?",
    ]
    examples_en = [
        "Which district produced the most Maize?",
        "What is the population of Lilongwe?",
        "What is the food insecurity level in Nsanje?",
    ]
    shown = examples_ny if language == "ny" else examples_en
    btn_cols = st.columns(len(shown))
    for i, ex in enumerate(shown):
        if btn_cols[i].button(ex, key=f"ex_{i}", use_container_width=True):
            question = ex
            st.session_state["_q"] = ex

    if "_q" in st.session_state and not question:
        question = st.session_state["_q"]

    st.divider()

    if st.button("🔍 Generate SQL & Run Query", type="primary", use_container_width=True):
        if not question.strip():
            st.warning("Please enter a question.")
            st.stop()

        with st.spinner("Searching dataset and running query..."):
            example, score, mode = find_match(question, language)

        if not example:
            st.error("No matching question found in the dataset. Try rephrasing.")
            st.stop()

        sql = example.get("sql_statement", "")

        # ── Results layout ────────────────────────────────────────────────
        left, right = st.columns([1, 1])

        with left:
            st.subheader("Generated SQL")
            st.code(sql, language="sql")

            # Match details
            match_color = "green" if mode == "exact" else "orange"
            st.markdown(
                f":{match_color}[**{mode.upper()} match**] — score: `{score}`"
            )
            with st.expander("Matched dataset entry", expanded=False):
                st.markdown(f"**Chichewa:** {example.get('question_ny', '')}")
                st.markdown(f"**English:** {example.get('question_en', '')}")
                st.markdown(f"**Table:** `{example.get('table', '')}` &nbsp;|&nbsp; **Difficulty:** `{example.get('difficulty_level', '')}`")

        with right:
            st.subheader("Query Results")
            df, err = run_query(sql)
            if err:
                st.error(f"SQL Error: {err}")
            elif df is not None and not df.empty:
                st.dataframe(df, use_container_width=True)
                st.caption(f"{len(df)} row(s) returned.")
            else:
                st.info("Query returned no rows.")

            # Show expected result from dataset if available
            expected = example.get("sql_result")
            if expected:
                with st.expander("Expected result (from dataset)", expanded=False):
                    st.write(expected)


if __name__ == "__main__":
    main()
