"""
Malawi Crop & Market Intelligence — Render demo
Baseline retrieval only (no GPU/model). Scoped to commodity_prices + production tables.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import difflib
from pathlib import Path

import gradio as gr
import pandas as pd

_HERE     = Path(__file__).parent
DATA_PATH = _HERE / "data" / "all.json"
DB_PATH   = _HERE / "data" / "database" / "chichewa_text2sql.db"

ALLOWED_TABLES = {"commodity_prices", "production"}
FORBIDDEN = {
    "insert", "update", "delete", "drop", "alter",
    "attach", "pragma", "create", "replace", "truncate",
}

SCHEMA_INFO = """
**commodity_prices** — Market prices across Malawi
- `district` · `market` · `commodity` (Maize, Rice, Beans, Groundnuts, Cow peas, Soya beans)
- `price` (MK per kg) · `month_name` · `year` · `collection_date`

**production** — Crop yields by district
- `district` · `crop` (46 crops including Maize, Rice, Tobacco, Groundnuts…)
- `yield` (metric tonnes) · `season` (2023-2024)
"""

CHICHEWA_TO_EN: dict[str, str] = {
    "chimanga": "Maize",
    "mpunga":   "Rice",
    "mtedza":   "Groundnuts",
    "nyemba":   "Cow peas",
    "soya":     "Soya beans",
    "thonje":   "Cotton",
    "fodya":    "Tobacco",
    "mbatata":  "Sweet potato",
    "choroko":  "Green gram",
    "nandolo":  "Pigeon peas",
    "mchiwi":   "Beans",
    "nyunde":   "Beans",
}

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

_examples: list = []
if DATA_PATH.exists():
    with DATA_PATH.open("r", encoding="utf-8") as _f:
        all_data = json.load(_f)
    _examples = [e for e in all_data if e.get("table") in ALLOWED_TABLES]

_DISTRICTS:   list[str] = []
_COMMODITIES: list[str] = []
_CROPS:       list[str] = []

if DB_PATH.exists():
    _conn = sqlite3.connect(DB_PATH)
    try:
        _DISTRICTS   = [r[0] for r in _conn.execute("SELECT DISTINCT district FROM commodity_prices").fetchall()]
        _COMMODITIES = [r[0] for r in _conn.execute("SELECT DISTINCT commodity FROM commodity_prices").fetchall()]
        _CROPS       = [r[0] for r in _conn.execute("SELECT DISTINCT crop FROM production").fetchall()]
    finally:
        _conn.close()


def _find_entity(text: str, entity_list: list[str]) -> str | None:
    tl = text.lower()
    for ent in entity_list:
        if ent.lower() in tl:
            return ent
    return None


def _detect_commodity(question: str, language: str) -> str | None:
    if language == "ny":
        for chi, eng in CHICHEWA_TO_EN.items():
            if chi in question.lower():
                return eng
    return _find_entity(question, _COMMODITIES + _CROPS)


def _substitute_entities(user_q: str, sql: str, language: str) -> str:
    user_district  = _find_entity(user_q, _DISTRICTS)
    user_commodity = _detect_commodity(user_q, language)
    new_sql = sql
    if user_district:
        for d in _DISTRICTS:
            if f"'{d}'" in new_sql:
                new_sql = new_sql.replace(f"'{d}'", f"'{user_district}'"); break
            if f'"{d}"' in new_sql:
                new_sql = new_sql.replace(f'"{d}"', f'"{user_district}"'); break
    if user_commodity:
        for c in _COMMODITIES + _CROPS:
            if f"'{c}'" in new_sql:
                new_sql = new_sql.replace(f"'{c}'", f"'{user_commodity}'"); break
            if f'"{c}"' in new_sql:
                new_sql = new_sql.replace(f'"{c}"', f'"{user_commodity}"'); break
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


def run_query(sql: str):
    s = sql.strip().rstrip(";")
    if not s.lower().startswith("select"):
        return None, "Only SELECT statements are allowed."
    if ";" in s:
        return None, "Multiple statements not allowed."
    if any(kw in s.lower() for kw in FORBIDDEN):
        return None, "Forbidden keyword detected."
    tables_used = re.findall(r"\bfrom\s+(\w+)|\bjoin\s+(\w+)", s.lower())
    for pair in tables_used:
        t = pair[0] or pair[1]
        if t and t not in ALLOWED_TABLES:
            return None, f"Table '{t}' is not available here."
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
    if df is None or df.empty:
        return "**EN:** The query returned no results.\n\n**NY:** Kafukufuku sunabweze chilichonse."
    cols = [c.lower() for c in df.columns]
    n = len(df)
    if "price" in cols or "average_price" in cols or "avg_price" in cols:
        price_col = next((c for c in df.columns if "price" in c.lower()), df.columns[-1])
        if n == 1:
            row = df.iloc[0]
            district  = row.get("district", "")
            price     = row.get(price_col, "")
            commodity = row.get("commodity", "") or _detect_commodity(question, language) or ""
            commodity_ny = EN_TO_CHICHEWA.get(commodity, commodity)
            price_fmt = f"MK {price:,.2f}" if isinstance(price, float) else f"MK {price}"
            en_label = " in ".join(filter(None, [commodity, district])) or "The commodity"
            ny_label = " ku ".join(filter(None, [commodity_ny, district])) or "Chinthu ichi"
            return (
                f"**EN:** {en_label} costs approximately **{price_fmt} per kg**.\n\n"
                f"**NY:** {ny_label} imagulitsidwa pafupifupi **{price_fmt} pa kg**."
            )
        return (
            f"**EN:** Found **{n} records** matching your query.\n\n"
            f"**NY:** Tapeza zolemba **{n}** zomwe zigwirizana ndi funso lanu."
        )
    if "yield" in cols or "max_yield" in cols:
        yield_col = next((c for c in df.columns if "yield" in c.lower()), df.columns[-1])
        if n == 1:
            row = df.iloc[0]
            district = row.get("district", "")
            yld      = row.get(yield_col, "")
            crop     = row.get("crop", "") or _detect_commodity(question, language) or ""
            crop_ny  = EN_TO_CHICHEWA.get(crop, crop)
            yld_fmt  = f"{yld:,.0f}" if isinstance(yld, float) else str(yld)
            en_label = " in ".join(filter(None, [crop, district])) or "The crop"
            ny_label = " ku ".join(filter(None, [crop_ny, district])) or "Mbewu"
            return (
                f"**EN:** {en_label} yield: **{yld_fmt} metric tonnes**.\n\n"
                f"**NY:** Ulimi wa {ny_label} unali **{yld_fmt} metric tonnes**."
            )
        return (
            f"**EN:** Found **{n} districts** matching your query.\n\n"
            f"**NY:** Tapeza maboma **{n}** omwe agwirizana ndi funso lanu."
        )
    return (
        f"**EN:** Query returned **{n} row(s)**.\n\n"
        f"**NY:** Kafukufuku wabweza mizere **{n}**."
    )


def answer_question(question: str, language: str = "ny"):
    if not question.strip():
        return "_Please enter a question._", "", "—", pd.DataFrame()
    example, score, mode = find_match(question, language)
    if not example:
        return "No matching question found. Try rephrasing.", "", "No match", pd.DataFrame()
    sql = _substitute_entities(question, example.get("sql_statement", ""), language)
    df, err = run_query(sql)
    if err:
        return f"SQL error: {err}", sql, "Baseline retrieval", pd.DataFrame([{"error": err}])
    plain = make_plain_answer(question, df, language)
    return plain, sql, f"Baseline retrieval ({mode}, score={score})", df or pd.DataFrame()


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
                ["ny", "en"], value="ny",
                label="Language / Chiyankhulo",
                info="ny = Chichewa,  en = English",
            )
            submit_btn = gr.Button("Ask / Funsani", variant="primary", size="lg")
        with gr.Column(scale=2):
            gr.Markdown(SCHEMA_INFO)

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
            ["Which district has the cheapest groundnuts?", "en"],
            ["Mpunga ukugulitsidwa pa mtengo wanji ku Blantyre?", "ny"],
            ["What are the top 5 maize producing districts?", "en"],
        ],
        inputs=[question_box, language_box],
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
