"""
Malawi Crop & Market Intelligence — HuggingFace Space
Ask about crop production and commodity prices in Chichewa or English.
Scoped to: commodity_prices, production tables only.
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

ALLOWED_TABLES = {"commodity_prices", "production"}
FORBIDDEN = {
    "insert", "update", "delete", "drop", "alter",
    "attach", "pragma", "create", "replace", "truncate",
}

# ── Schema context shown in UI sidebar ────────────────────────────────────
SCHEMA_INFO = """
**commodity_prices** — Market prices across Malawi
- `district` · `market` · `commodity` (Maize, Rice, Beans, Groundnuts, Cow peas, Soya beans)
- `price` (MK per kg) · `month_name` · `year` · `collection_date`

**production** — Crop yields by district
- `district` · `crop` (46 crops including Maize, Rice, Tobacco, Groundnuts…)
- `yield` (metric tonnes) · `season` (2023-2024)
"""

# ── Dataset (filtered to relevant tables only) ────────────────────────────
_examples: list = []
if DATA_PATH.exists():
    with DATA_PATH.open("r", encoding="utf-8") as _f:
        all_data = json.load(_f)
    _examples = [e for e in all_data if e.get("table") in ALLOWED_TABLES]
    print(f"Loaded {len(_examples)} examples for commodity_prices + production.")
else:
    print(f"WARNING: dataset not found at {DATA_PATH}")


# ── Chichewa commodity/crop name mappings ─────────────────────────────────
CHICHEWA_TO_EN: dict[str, str] = {
    "chimanga":  "Maize",
    "mpunga":    "Rice",
    "mtedza":    "Groundnuts",
    "nyemba":    "Cow peas",
    "soya":      "Soya beans",
    "thonje":    "Cotton",
    "fodya":     "Tobacco",
    "mbatata":   "Sweet potato",
    "choroko":   "Green gram",
    "nandolo":   "Pigeon peas",
    "mchiwi":    "Beans",
    "nyunde":    "Beans",
}

# Reverse: English commodity/crop name → primary Chichewa name
EN_TO_CHICHEWA: dict[str, str] = {
    "Maize":        "Chimanga",
    "Rice":         "Mpunga",
    "Groundnuts":   "Mtedza",
    "Cow peas":     "Nyemba",
    "Soya beans":   "Soya",
    "Beans":        "Nyunde",
    "Cotton":       "Thonje",
    "Tobacco":      "Fodya",
    "Sweet potato": "Mbatata",
    "Green gram":   "Choroko",
    "Pigeon peas":  "Nandolo",
}

# ── Entity lists loaded from DB at startup ────────────────────────────────
_DISTRICTS:   list[str] = []
_COMMODITIES: list[str] = []
_CROPS:       list[str] = []

def _load_entities() -> None:
    global _DISTRICTS, _COMMODITIES, _CROPS
    if not DB_PATH.exists():
        return
    conn = sqlite3.connect(DB_PATH)
    try:
        _DISTRICTS   = [r[0] for r in conn.execute("SELECT DISTINCT district FROM commodity_prices").fetchall()]
        _COMMODITIES = [r[0] for r in conn.execute("SELECT DISTINCT commodity FROM commodity_prices").fetchall()]
        _CROPS       = [r[0] for r in conn.execute("SELECT DISTINCT crop FROM production").fetchall()]
        print(f"Entities loaded: {len(_DISTRICTS)} districts, {len(_COMMODITIES)} commodities, {len(_CROPS)} crops.")
    finally:
        conn.close()

_load_entities()


def _find_entity_in_text(text: str, entity_list: list[str]) -> str | None:
    """Return the first entity from entity_list found in text (case-insensitive)."""
    tl = text.lower()
    for ent in entity_list:
        if ent.lower() in tl:
            return ent
    return None


def _detect_commodity(question: str, language: str) -> str | None:
    """Detect commodity/crop name in question, handling Chichewa names."""
    if language == "ny":
        for chi, eng in CHICHEWA_TO_EN.items():
            if chi in question.lower():
                return eng
    return _find_entity_in_text(question, _COMMODITIES + _CROPS)


def _substitute_entities(user_q: str, sql: str, language: str) -> str:
    """
    Patch district and commodity/crop literals in `sql` to match what the user
    actually asked about — prevents a fuzzy-matched SQL for a different district
    (e.g. Rumphi) being returned when the user asked about Lilongwe.
    """
    user_district  = _find_entity_in_text(user_q, _DISTRICTS)
    user_commodity = _detect_commodity(user_q, language)

    new_sql = sql

    # Swap district literal
    if user_district:
        for d in _DISTRICTS:
            if f"'{d}'" in new_sql:
                new_sql = new_sql.replace(f"'{d}'", f"'{user_district}'")
                break
            if f'"{d}"' in new_sql:
                new_sql = new_sql.replace(f'"{d}"', f'"{user_district}"')
                break

    # Swap commodity/crop literal
    if user_commodity:
        for c in _COMMODITIES + _CROPS:
            if f"'{c}'" in new_sql:
                new_sql = new_sql.replace(f"'{c}'", f"'{user_commodity}'")
                break
            if f'"{c}"' in new_sql:
                new_sql = new_sql.replace(f'"{c}"', f'"{user_commodity}"')
                break

    return new_sql


def _norm(t: str) -> str:
    return " ".join(t.lower().strip().split())


def find_match(question: str, language: str):
    key = "question_ny" if language == "ny" else "question_en"
    q = _norm(question)
    for ex in _examples:
        if _norm(ex.get(key, "")) == q:
            return ex, 1.0, "exact"
    corpus = [_norm(ex.get(key, "")) for ex in _examples]
    hits = difflib.get_close_matches(q, corpus, n=1, cutoff=0.45)
    if hits:
        idx = corpus.index(hits[0])
        score = difflib.SequenceMatcher(None, q, hits[0]).ratio()
        return _examples[idx], round(score, 3), "fuzzy"
    return None, 0.0, "none"


# ── SQL execution ─────────────────────────────────────────────────────────
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
    # Scope to allowed tables only
    tables_used = re.findall(r"\bfrom\s+(\w+)|\bjoin\s+(\w+)", s.lower())
    for pair in tables_used:
        t = pair[0] or pair[1]
        if t and t not in ALLOWED_TABLES:
            return None, f"Table '{t}' is not available in this app. Use commodity_prices or production."
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


def make_plain_answer(question: str, df: pd.DataFrame, language: str = "en") -> str:
    """Generate a bilingual (EN + NY) summary of the result."""
    if df is None or df.empty:
        return (
            "**EN:** The query returned no results.\n\n"
            "**NY:** Kafukufuku sunabweze chilichonse."
        )
    cols = [c.lower() for c in df.columns]
    n = len(df)

    # ── Price query ────────────────────────────────────────────────────────
    if "price" in cols or "average_price" in cols or "avg_price" in cols:
        price_col = next((c for c in df.columns if "price" in c.lower()), df.columns[-1])
        if n == 1:
            row       = df.iloc[0]
            district  = row.get("district", "")
            price     = row.get(price_col, "")
            commodity = row.get("commodity", "") or _detect_commodity(question, language) or ""
            commodity_ny = EN_TO_CHICHEWA.get(commodity, commodity)
            price_fmt = f"MK {price:,.2f}" if isinstance(price, float) else f"MK {price}"

            en_parts = []
            if commodity: en_parts.append(commodity)
            if district:  en_parts.append(f"in {district}")
            en_label = " ".join(en_parts) or "The commodity"
            en_line = f"{en_label} costs approximately **{price_fmt} per kg**."

            ny_parts = []
            if commodity_ny: ny_parts.append(commodity_ny)
            if district:     ny_parts.append(f"ku {district}")
            ny_label = " ".join(ny_parts) or "Chinthu ichi"
            ny_line = f"{ny_label} imagulitsidwa pafupifupi **{price_fmt} pa kg**."

            return f"**EN:** {en_line}\n\n**NY:** {ny_line}"
        else:
            return (
                f"**EN:** Found **{n} records** matching your query. See the table below.\n\n"
                f"**NY:** Tapeza zolemba **{n}** zomwe zigwirizana ndi funso lanu. Onani tebulo pansipa."
            )

    # ── Production / yield query ───────────────────────────────────────────
    if "yield" in cols or "max_yield" in cols:
        yield_col = next((c for c in df.columns if "yield" in c.lower()), df.columns[-1])
        if n == 1:
            row      = df.iloc[0]
            district = row.get("district", "")
            yld      = row.get(yield_col, "")
            crop     = row.get("crop", "") or _detect_commodity(question, language) or ""
            crop_ny  = EN_TO_CHICHEWA.get(crop, crop)
            yld_fmt  = f"{yld:,.0f}" if isinstance(yld, float) else str(yld)

            en_parts = []
            if crop:     en_parts.append(crop)
            if district: en_parts.append(f"in {district}")
            en_label = " ".join(en_parts) or "The crop"
            en_line = f"{en_label} yield: **{yld_fmt} metric tonnes**."

            ny_parts = []
            if crop_ny:  ny_parts.append(crop_ny)
            if district: ny_parts.append(f"ku {district}")
            ny_label = " ".join(ny_parts) or "Mbewu"
            ny_line = f"Ulimi wa {ny_label} unali **{yld_fmt} metric tonnes**."

            return f"**EN:** {en_line}\n\n**NY:** {ny_line}"
        else:
            return (
                f"**EN:** Found **{n} districts** matching your query. See the table below.\n\n"
                f"**NY:** Tapeza maboma **{n}** omwe agwirizana ndi funso lanu. Onani tebulo pansipa."
            )

    return (
        f"**EN:** Query returned **{n} row(s)**. See the table below.\n\n"
        f"**NY:** Kafukufuku wabweza mizere **{n}**. Onani tebulo pansipa."
    )


# ── Model ─────────────────────────────────────────────────────────────────
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
                "You are an expert Text-to-SQL model for a SQLite database with two tables: "
                "commodity_prices (district, market, commodity, price, month_name, year, collection_date) and "
                "production (district, crop, yield, season). "
                "Generate ONE valid SQL SELECT query. Return ONLY the SQL, no explanation."
            ),
        },
        {"role": "user", "content": f"Language: {lang_name}\nQuestion: {question}"},
    ]
    
    prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    out = _pipe(prompt, max_new_tokens=150, do_sample=False,
                pad_token_id=tokenizer.eos_token_id)[0]["generated_text"]
    generated = out[len(prompt):] if out.startswith(prompt) else out
    return extract_sql(generated)


# ── Main handler ──────────────────────────────────────────────────────────
def answer_question(question: str, language: str = "ny"):
    """Returns (plain_answer, sql, source, results_df)."""
    empty_df = pd.DataFrame()

    if not question.strip():
        return "_Please enter a question._", "", "—", empty_df

    # 1. Dataset match (always works, no GPU)
    example, score, mode = find_match(question, language)
    baseline_sql = example.get("sql_statement", "") if example else ""
    # Patch retrieved SQL so district/commodity match what user actually asked
    if baseline_sql:
        baseline_sql = _substitute_entities(question, baseline_sql, language)

    # 2. Try model if GPU available
    model_sql = None
    if _model_cache and tokenizer and torch.cuda.is_available():
        try:
            model_sql = _run_model(question, language)
        except Exception:
            model_sql = None

    # 3. Pick SQL
    if model_sql:
        sql    = model_sql
        source = "Model (fine-tuned LLaMA 3.1 8B)"
    elif baseline_sql:
        sql    = baseline_sql
        source = "Baseline retrieval"
    else:
        return "Sorry, no matching question found. Try rephrasing.", "", "No match", empty_df

    # 4. Execute
    df, err = run_query(sql)
    if err:
        return f"SQL error: {err}", sql, source, pd.DataFrame([{"error": err}])
    elif df is not None and not df.empty:
        plain = make_plain_answer(question, df, language)
        return plain, sql, source, df
    else:
        return "The query returned no results for your question.", sql, source, pd.DataFrame([{"info": "No rows returned."}])


# ── Gradio UI ──────────────────────────────────────────────────────────────
with gr.Blocks(title="Malawi Crop & Market Intelligence", theme=gr.themes.Soft()) as demo:

    gr.Markdown(
        "# 🌽 Malawi Crop & Market Intelligence\n"
        "Ask about **crop production** and **commodity prices** across Malawi "
        "in **Chichewa** or **English**."
    )

    with gr.Row():
        with gr.Column(scale=3):
            question_box = gr.Textbox(
                label="Your question / Funso lanu",
                placeholder="Kodi chimanga chikugulitsidwa pa mtengo wanji ku Lilongwe?",
                lines=2,
            )
            language_box = gr.Radio(
                ["ny", "en"],
                value="ny",
                label="Language / Chiyankhulo",
                info="ny = Chichewa,  en = English",
            )
            submit_btn = gr.Button("Ask / Funsani", variant="primary", size="lg")

        with gr.Column(scale=2):
            gr.Markdown(SCHEMA_INFO)

    # Answer
    answer_output = gr.Markdown(label="Answer / Yankho")

    with gr.Row():
        sql_output    = gr.Textbox(label="SQL Query", lines=4, show_copy_button=True)
        source_output = gr.Textbox(label="Source", lines=1, interactive=False)

    result_output = gr.Dataframe(label="Results / Zotsatira", wrap=True)

    submit_btn.click(
        fn=answer_question,
        inputs=[question_box, language_box],
        outputs=[answer_output, sql_output, source_output, result_output],
    )

    gr.Examples(
        label="Example questions / Mafunso achitsanzo",
        examples=[
            ["Kodi chimanga chikugulitsidwa pa mtengo wanji ku Lilongwe?", "ny"],
            ["What is the price of Maize in Lilongwe?", "en"],
            ["Ndi boma liti komwe linakolola chimanga chambiri?", "ny"],
            ["Which district produced the most Maize?", "en"],
            ["Ndi maboma asanu amene anakolola chimanga chambiri?", "ny"],
            ["Which district has the cheapest groundnuts?", "en"],
            ["Mpunga ukugulitsidwa pa mtengo wanji ku Blantyre?", "ny"],
            ["What are the top 5 maize producing districts?", "en"],
        ],
        inputs=[question_box, language_box],
    )

demo.launch(ssr_mode=False)
