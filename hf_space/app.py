"""
Chichewa Text-to-SQL — HuggingFace Space
- Always works: baseline dataset retrieval + SQLite execution (no GPU needed)
- Bonus when GPU is available: also runs the fine-tuned model
"""
from __future__ import annotations

import json
import re
import sqlite3
import difflib
import traceback
from pathlib import Path

import spaces
import gradio as gr
import torch
import pandas as pd
from huggingface_hub import snapshot_download
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

MODEL_ID = "johneze/Llama-3.1-8B-Instruct-chichewa-text2sql"

_HERE     = Path(__file__).parent
DATA_PATH = _HERE / "data" / "all.json"
DB_PATH   = _HERE / "data" / "database" / "chichewa_text2sql.db"

FORBIDDEN = {
    "insert", "update", "delete", "drop", "alter",
    "attach", "pragma", "create", "replace", "truncate",
}

# ── Dataset (CPU, loads at startup) ───────────────────────────────────────
_examples: list = []
if DATA_PATH.exists():
    with DATA_PATH.open("r", encoding="utf-8") as _f:
        _examples = json.load(_f)
    print(f"Loaded {len(_examples)} dataset examples.")
else:
    print(f"WARNING: dataset not found at {DATA_PATH}")


def _norm(t: str) -> str:
    return " ".join(t.lower().strip().split())


def find_match(question: str, language: str):
    key = "question_ny" if language == "ny" else "question_en"
    q = _norm(question)
    for ex in _examples:
        if _norm(ex.get(key, "")) == q:
            return ex, 1.0, "exact"
    corpus = [_norm(ex.get(key, "")) for ex in _examples]
    hits = difflib.get_close_matches(q, corpus, n=1, cutoff=0.5)
    if hits:
        idx = corpus.index(hits[0])
        score = difflib.SequenceMatcher(None, q, hits[0]).ratio()
        return _examples[idx], round(score, 3), "fuzzy"
    return None, 0.0, "none"


# ── SQL execution (CPU, no GPU needed) ────────────────────────────────────
def extract_sql(text: str) -> str:
    m = re.search(r"(?is)select\s.+", text)
    if not m:
        return text.strip()
    sql = m.group(0)
    for sep in [";", "\n"]:
        if sep in sql:
            sql = sql.split(sep)[0]
    return sql.strip() + ";"


def run_query(sql: str):
    """Returns (DataFrame | None, error_str | None)."""
    s = sql.strip().rstrip(";")
    if not s.lower().startswith("select"):
        return None, "Only SELECT statements are allowed."
    if ";" in s:
        return None, "Multiple statements not allowed."
    if any(kw in s.lower() for kw in FORBIDDEN):
        return None, "Forbidden keyword detected."
    if not DB_PATH.exists():
        return None, f"Database not found at {DB_PATH}"
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql).fetchall()
        if not rows:
            return pd.DataFrame(), None
        return pd.DataFrame([dict(r) for r in rows]), None
    except Exception as exc:
        return None, str(exc)
    finally:
        conn.close()


# ── Model (pre-downloaded at startup, loaded into GPU lazily) ─────────────
print("Pre-downloading model weights ...")
try:
    _model_cache = snapshot_download(repo_id=MODEL_ID)
    tokenizer    = AutoTokenizer.from_pretrained(_model_cache)
    print(f"Tokenizer ready. Weights at: {_model_cache}")
except Exception as e:
    _model_cache = None
    tokenizer    = None
    print(f"WARNING: model download failed: {e}")

_pipe = None


@spaces.GPU(duration=300)
def _run_model(question: str, language: str) -> str:
    """GPU-decorated inference. Only called when GPU is confirmed available."""
    global _pipe
    if _pipe is None:
        model = AutoModelForCausalLM.from_pretrained(
            _model_cache,
            torch_dtype=torch.bfloat16,
            device_map="auto",
            low_cpu_mem_usage=True,
        )
        _pipe = pipeline("text-generation", model=model, tokenizer=tokenizer)

    lang_name = "Chichewa" if language == "ny" else "English"
    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert Text-to-SQL model for a SQLite database "
                "with tables: production, population, food_insecurity, "
                "commodity_prices, mse_daily. "
                "Generate ONE valid SQL SELECT query. Return ONLY the SQL, no explanation."
            ),
        },
        {"role": "user", "content": f"Language: {lang_name}\nQuestion: {question}"},
    ]
    prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    out = _pipe(prompt, max_new_tokens=128, do_sample=False,
                pad_token_id=tokenizer.eos_token_id)[0]["generated_text"]
    generated = out[len(prompt):] if out.startswith(prompt) else out
    return extract_sql(generated)


# ── Main handler ──────────────────────────────────────────────────────────
def generate_sql(question: str, language: str = "ny"):
    """Always returns (sql, source, match_info, results_df) — works without GPU."""
    empty_df = pd.DataFrame([{"info": "No results."}])

    if not question.strip():
        return "", "—", "_Please enter a question._", empty_df

    # 1. Dataset match (always works, no GPU)
    example, score, mode = find_match(question, language)
    baseline_sql = example.get("sql_statement", "") if example else ""

    if example:
        match_info = (
            f"**Match:** {mode} &nbsp;|&nbsp; **Score:** {score}\n\n"
            f"**ny:** {example.get('question_ny', '')}\n\n"
            f"**en:** {example.get('question_en', '')}\n\n"
            f"**Table:** `{example.get('table', '')}` &nbsp;|&nbsp; "
            f"**Difficulty:** `{example.get('difficulty_level', '')}`\n\n"
            f"**Dataset SQL:** `{example.get('sql_statement', '')}`"
        )
    else:
        match_info = "_No close match found in the dataset._"

    # 2. Try model only if GPU is present
    model_sql = None
    if _model_cache and tokenizer and torch.cuda.is_available():
        try:
            model_sql = _run_model(question, language)
        except Exception:
            model_sql = None

    # 3. Pick best SQL
    if model_sql:
        sql    = model_sql
        source = "Model (fine-tuned LLaMA 3.1 8B)"
    elif baseline_sql:
        sql    = baseline_sql
        source = "Baseline retrieval (dataset match)"
    else:
        return "", "No match", match_info, pd.DataFrame([{"error": "No matching question found."}])

    # 4. Execute SQL against database
    df, err = run_query(sql)
    if err:
        results = pd.DataFrame([{"error": err}])
    elif df is not None and not df.empty:
        results = df
    else:
        results = pd.DataFrame([{"info": "Query returned no rows."}])

    return sql, source, match_info, results


# ── Gradio UI ──────────────────────────────────────────────────────────────
with gr.Blocks(title="Chichewa Text-to-SQL") as demo:
    gr.Markdown(
        "# Chichewa Text-to-SQL\n"
        "Enter a question in **Chichewa** or **English** — generates SQL and runs it on the database."
    )

    with gr.Row():
        question_box = gr.Textbox(
            label="Question",
            placeholder="Ndi boma liti komwe anakolola chimanga chambiri?",
            lines=3,
        )
        language_box = gr.Radio(["ny", "en"], value="ny", label="Language")

    submit_btn = gr.Button("Generate SQL & Run", variant="primary")

    with gr.Row():
        sql_output    = gr.Textbox(label="Generated SQL", lines=4, show_copy_button=True)
        source_output = gr.Textbox(label="Source", lines=1, interactive=False)

    match_output  = gr.Markdown()
    result_output = gr.Dataframe(label="Query Results", wrap=True)

    submit_btn.click(
        fn=generate_sql,
        inputs=[question_box, language_box],
        outputs=[sql_output, source_output, match_output, result_output],
    )

    gr.Examples(
        examples=[
            ["Ndi boma liti komwe anakolola chimanga chambiri?", "ny"],
            ["Which district produced the most Maize?", "en"],
            ["Ndi anthu angati ku Lilongwe?", "ny"],
            ["What is the food insecurity level in Nsanje?", "en"],
        ],
        inputs=[question_box, language_box],
    )

demo.launch()
