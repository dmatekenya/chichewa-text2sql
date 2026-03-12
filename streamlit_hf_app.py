from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import difflib
import pandas as pd
import streamlit as st
from huggingface_hub import InferenceClient
try:
    from gradio_client import Client as GradioClient
    _GRADIO_CLIENT_AVAILABLE = True
except ImportError:
    _GRADIO_CLIENT_AVAILABLE = False

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data" / "all.json"
DB_PATH = BASE_DIR / "data" / "database" / "chichewa_text2sql.db"
MODEL_ID = "johneze/Llama-3.1-8B-Instruct-chichewa-text2sql"

FORBIDDEN_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "attach",
    "pragma",
    "create",
    "replace",
    "truncate",
}


@st.cache_data(show_spinner=False)
def load_examples() -> List[Dict[str, Any]]:
    if not DATA_PATH.exists():
        return []
    with DATA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def normalize_text(text: str) -> str:
    return " ".join(text.lower().strip().split())


def find_best_match(text: str, language: str) -> Tuple[Optional[Dict[str, Any]], float, str]:
    examples = load_examples()
    key = "question_ny" if language == "ny" else "question_en"
    normalized = normalize_text(text)

    for ex in examples:
        if normalize_text(ex.get(key, "")) == normalized:
            return ex, 1.0, "exact"

    corpus = [normalize_text(ex.get(key, "")) for ex in examples]
    matches = difflib.get_close_matches(normalized, corpus, n=1, cutoff=0.6)
    if matches:
        matched = matches[0]
        idx = corpus.index(matched)
        score = difflib.SequenceMatcher(None, normalized, matched).ratio()
        return examples[idx], score, "fuzzy"

    return None, 0.0, "none"


def is_safe_select(sql: str) -> bool:
    stripped = sql.strip().rstrip(";").strip()
    if not stripped.lower().startswith("select"):
        return False
    if ";" in stripped:
        return False
    lowered = stripped.lower()
    if any(keyword in lowered for keyword in FORBIDDEN_KEYWORDS):
        return False
    return True


def extract_tables(sql: str) -> List[str]:
    lowered = sql.lower()
    tables: List[str] = []
    for match in re.finditer(r"\bfrom\s+([a-zA-Z_][\w]*)|\bjoin\s+([a-zA-Z_][\w]*)", lowered):
        table = match.group(1) or match.group(2)
        if table:
            tables.append(table)
    return tables


@st.cache_data(show_spinner=False)
def get_allowed_tables() -> List[str]:
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    finally:
        conn.close()
    return [row[0].lower() for row in rows]


def validate_sql(sql: str) -> Tuple[bool, str]:
    if not is_safe_select(sql):
        return False, "Only single SELECT statements are allowed."

    tables = extract_tables(sql)
    if not tables:
        return False, "No table found in SQL."

    allowed = set(get_allowed_tables())
    if allowed and any(table not in allowed for table in tables):
        return False, "SQL references unknown table(s)."

    return True, "ok"


def run_query(sql: str) -> List[Dict[str, Any]]:
    if not DB_PATH.exists():
        raise FileNotFoundError("Database file not found.")
    valid, reason = validate_sql(sql)
    if not valid:
        raise ValueError(reason)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def extract_sql_from_output(text: str) -> str:
    match = re.search(r"(?is)select\s.+", text)
    if not match:
        return text.strip()
    sql = match.group(0)
    for sep in [";", "\n"]:
        if sep in sql:
            sql = sql.split(sep)[0]
    return sql.strip()


def generate_sql_with_model(text: str, language: str, hf_token: Optional[str] = None, endpoint: Optional[str] = None) -> str:
    # If endpoint looks like a HF Space URL, use gradio_client
    if endpoint and _GRADIO_CLIENT_AVAILABLE:
        client = GradioClient(endpoint, hf_token=hf_token)
        result = client.predict(
            question=text,
            language=language,
            api_name="/generate_sql",
        )
        return extract_sql_from_output(result)

    # Otherwise try HF InferenceClient (dedicated endpoint or serverless)
    model_or_url = endpoint if endpoint else MODEL_ID
    client = InferenceClient(model=model_or_url, token=hf_token or None)
    lang_name = "Chichewa" if language == "ny" else "English"

    system_msg = (
        "You are an expert Text-to-SQL model for a SQLite database. "
        "Given a natural language question, generate ONE valid SQL SELECT query. "
        "Return ONLY the SQL query, no explanation."
    )
    user_msg = f"Language: {lang_name}\nQuestion: {text}"

    try:
        # Try chat completion first (instruction-tuned models)
        response = client.chat_completion(
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=128,
        )
        raw = response.choices[0].message.content
    except Exception:
        # Fallback to text generation
        prompt = f"{system_msg}\n\nLanguage: {lang_name}\nQuestion: {text}\n\nSQL:"
        raw = client.text_generation(prompt, max_new_tokens=128)

    return extract_sql_from_output(raw)


def main() -> None:
    st.set_page_config(page_title="Chichewa Text-to-SQL", layout="centered")
    st.title("🌍 Chichewa Text-to-SQL")
    st.caption("Query databases in Chichewa or English — no SQL knowledge needed.")

    text = st.text_area("Your question", placeholder="Lembani funso la Chichewa pano...")
    col1, col2 = st.columns(2)
    with col1:
        language = st.selectbox("Language", ["ny", "en"],
                                format_func=lambda x: "Chichewa (Nyanja)" if x == "ny" else "English",
                                index=0)
    with col2:
        execute = st.checkbox("Execute SQL on database", value=False)

    with st.expander("⚙️ Model settings", expanded=False):
        st.info(
            "**`johneze/Llama-3.1-8B-Instruct-chichewa-text2sql`** is not yet deployed on "
            "the HF free Inference API. To use the model, either:\n"
            "- Provide a **custom endpoint URL** (e.g. HF Dedicated Endpoint, Ollama, vLLM), or\n"
            "- Leave blank to use **Baseline retrieval** mode (works offline).",
            icon="ℹ️",
        )
        use_model = st.checkbox("Use HF model", value=False)
        hf_endpoint = st.text_input(
            "Custom endpoint URL (optional)",
            placeholder="https://your-endpoint.huggingface.cloud",
        ).strip() or None
        hf_token: Optional[str] = st.text_input(
            "HuggingFace token (optional)",
            type="password",
            placeholder="hf_...",
        ).strip() or None

    if st.button("Generate SQL", type="primary"):
        if not text.strip():
            st.warning("Please enter a question.")
            return

        with st.spinner("Finding best SQL..."):
            example, score, mode = find_best_match(text, language)
            baseline_sql = example.get("sql_statement") if example else None

            model_sql = None
            model_valid = False
            model_reason = "Model not used."

            if use_model:
                try:
                    model_sql = generate_sql_with_model(text, language, hf_token, hf_endpoint)
                    model_valid, model_reason = validate_sql(model_sql)
                except Exception as exc:
                    model_valid = False
                    model_reason = f"Model error: {exc}"

            model_low_confidence = (
                model_valid
                and baseline_sql is not None
                and mode in {"exact", "fuzzy"}
                and score >= 0.85
                and model_sql.strip().lower() != baseline_sql.strip().lower()
            )

            if model_valid and not model_low_confidence:
                sql = model_sql
                source = "model"
            elif baseline_sql:
                sql = baseline_sql
                source = "baseline"
            else:
                st.error(model_reason)
                return

        # --- SQL output ---
        st.subheader("Generated SQL")
        st.code(sql, language="sql")

        badge = "🤖 Model" if source == "model" else "📚 Baseline retrieval"
        st.caption(badge)

        # --- Baseline match info (collapsible) ---
        if example:
            with st.expander("Matched example from dataset", expanded=False):
                st.write({
                    "question_ny": example.get("question_ny"),
                    "question_en": example.get("question_en"),
                    "table": example.get("table"),
                    "score": round(score, 3),
                    "mode": mode,
                })

        # --- Execute ---
        if execute:
            with st.spinner("Running query..."):
                try:
                    rows = run_query(sql)
                    st.subheader("Results")
                    if rows:
                        st.dataframe(pd.DataFrame(rows))
                    else:
                        st.info("Query returned no rows.")
                except Exception as exc:
                    st.error(str(exc))


if __name__ == "__main__":
    main()
