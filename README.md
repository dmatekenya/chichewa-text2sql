# Chichewa Text-to-SQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![HuggingFace Model](https://img.shields.io/badge/🤗%20Model-johneze%2FLlama--3.1--8B--chichewa--text2sql-blue)](https://huggingface.co/johneze/Llama-3.1-8B-Instruct-chichewa-text2sql)
[![HuggingFace Space](https://img.shields.io/badge/🤗%20Space-chichewa--text2sql-green)](https://huggingface.co/spaces/johneze/chichewa-text2sql)
[![HuggingFace Demo](https://img.shields.io/badge/🌽%20Demo-Malawi%20Crop%20%26%20Market-orange)](https://huggingface.co/spaces/johneze/malawi-crop-market)

> **The first Text-to-SQL benchmark for Chichewa** — a low-resource Bantu language spoken by over 12 million people in Malawi and neighboring regions.

## Overview

Recent advances in Large Language Models (LLMs) have significantly improved Text-to-SQL performance in high-resource languages. However, their effectiveness in low-resource language settings remains largely underexplored. This work investigates the adaptation of LLMs for Text-to-SQL generation in Chichewa.

We construct a structured Chichewa Text-to-SQL benchmark consisting of **400 manually curated natural language–SQL pairs** grounded in a unified relational database covering agriculture, commodity prices, population statistics, market data, and food insecurity. We systematically evaluate open-source LLMs under zero-shot, random few-shot, and retrieval-augmented few-shot prompting in both **English and Chichewa**, and fine-tune the best-performing model using QLoRA.

**Keywords:** Text-to-SQL · Low-Resource Languages · Chichewa · QLoRA Fine-Tuning · Semantic Parsing · Information Retrieval

---

## Dataset

The benchmark contains **3,602 natural language–SQL pairs** (400 curated + augmented splits) across 5 database tables:

| Table | Description | Examples |
|---|---|---|
| `commodity_prices` | Market prices for 6 crops across 27 districts (2024) | 80 |
| `production` | Crop yields for 46 crops across 28 districts (2023–2024) | 80 |
| `population` | District-level population statistics | 80 |
| `food_insecurity` | Food insecurity indicators by district | 80 |
| `mse_daily` | Malawi Stock Exchange daily market data | 80 |

Each example includes:
- `question_ny` — question in Chichewa (Nyanja)
- `question_en` — question in English
- `sql_statement` — ground-truth SQL query
- `table` — target database table

**Data splits:** `train.json` · `dev.json` · `test.json` · `all.json`

> The raw data files are not versioned here (see `.gitignore`). The SQLite database is bundled inside the HuggingFace Spaces.

---

## Results

### Zero-Shot & Few-Shot Prompting

**English** (`results/zero_and_few_shot_english_results.csv`)

| Model | Zero-Shot EA | Random 5-Shot EA | Retrieved 5-Shot EA |
|---|---|---|---|
| SQLCoder 7B | 13.3% | 45.0% | 63.3% |
| DeepSeek Coder Instruct | 18.3% | 41.7% | 60.0% |
| LLaMA 3.1 8B Instruct | 8.3% | 50.0% | **70.0%** |

**Chichewa** (`results/zero_and_few_shot_chichewa_results.csv`)

| Model | Zero-Shot EA | Random 5-Shot EA | Retrieved 5-Shot EA |
|---|---|---|---|
| SQLCoder 7B | 0.0% | 0.0% | 21.7% |
| DeepSeek Coder Instruct | 0.0% | 0.0% | 30.0% |
| LLaMA 3.1 8B Instruct | 0.0% | 3.3% | **41.7%** |

> EA = Execution Accuracy. Zero-shot Chichewa performance is 0% across all models, improving to 41.7% with retrieval-augmented prompting — revealing a substantial language-resource gap.

---

### QLoRA Fine-Tuning

**English** (`results/qlora_english_results.csv`)

| Model | Strategy | Exact Match | Execution Accuracy |
|---|---|---|---|
| LLaMA 3.1 8B | Zero-Shot | 81.7% | 78.3% |
| LLaMA 3.1 8B | Retrieved Few-Shot | 71.7% | 76.7% |
| DeepSeek Coder | Zero-Shot | 63.3% | 65.0% |
| DeepSeek Coder | Retrieved Few-Shot | 66.7% | 70.0% |

**Chichewa** (`results/qlora_chichewa_results.csv`)

| Model | Strategy | Exact Match | Execution Accuracy |
|---|---|---|---|
| LLaMA 3.1 8B | Zero-Shot | 41.7% | 41.7% |
| LLaMA 3.1 8B | Retrieved Few-Shot | 46.7% | **53.3%** |
| DeepSeek Coder | Zero-Shot | 20.0% | 18.3% |
| DeepSeek Coder | Retrieved Few-Shot | 25.0% | 26.7% |

> Fine-tuning with QLoRA yields large gains — LLaMA 3.1 8B jumps from 0% (zero-shot, no fine-tune) to **53.3% execution accuracy** in Chichewa.

---

## Fine-Tuned Model

The best-performing fine-tuned model is publicly available on HuggingFace:

**[johneze/Llama-3.1-8B-Instruct-chichewa-text2sql](https://huggingface.co/johneze/Llama-3.1-8B-Instruct-chichewa-text2sql)**

- Base: LLaMA 3.1 8B Instruct
- Fine-tuned with QLoRA (4-bit quantization)
- Trained on Chichewa + English Text-to-SQL pairs

---

## Live Demos

| Space | Description |
|---|---|
| [🤗 chichewa-text2sql](https://huggingface.co/spaces/johneze/chichewa-text2sql) | Full research demo — all 5 tables, model inference + baseline retrieval |
| [🌽 malawi-crop-market](https://huggingface.co/spaces/johneze/malawi-crop-market) | Audience-friendly demo — crop prices & production, bilingual EN/Chichewa answers |

---

## Repository Structure

```
chichewa-text2sql/
├── data/                        # Dataset & SQLite DB (gitignored, hosted on HF)
│   ├── all.json                 # Full dataset (3,602 examples)
│   ├── train.json / dev.json / test.json
│   └── database/
│       └── chichewa_text2sql.db
├── notebooks/                   # Training & evaluation notebooks
│   ├── qlora_llama38B_chichewa.ipynb
│   ├── qlora_llama38B_english.ipynb
│   ├── qlora_deepseekcoder_chichewa.ipynb
│   ├── qlora_deepseekcoder_english.ipynb
│   ├── zero_shot_and_few_shot_chichewa.ipynb
│   └── zero_shot_and_few_shot_english.ipynb
├── results/                     # Evaluation results (CSV)
│   ├── zero_and_few_shot_chichewa_results.csv
│   ├── zero_and_few_shot_english_results.csv
│   ├── qlora_chichewa_results.csv
│   └── qlora_english_results.csv
├── scripts/                     # Dataset construction scripts
├── sql/                         # Database schema (01_schema.sql)
├── src/                         # Source utilities
├── hf_space/                    # HuggingFace Space: chichewa-text2sql
├── hf_space_market/             # HuggingFace Space: malawi-crop-market
├── streamlit_demo_app.py        # Local Streamlit demo (no model required)
├── deploy_space.py              # Deploy script for chichewa-text2sql Space
├── deploy_market_space.py       # Deploy script for malawi-crop-market Space
└── .env.example                 # HuggingFace token template
```

---

## Running Locally

### Install dependencies

```bash
pip install -e .
```

### Local demo (no model, no internet required)

```bash
streamlit run streamlit_demo_app.py
```

### Deploy to HuggingFace Spaces

Copy `.env.example` to `.env` and add your HuggingFace write token:

```
HF_TOKEN=hf_your_token_here
```

Then run:

```bash
python deploy_space.py           # deploys johneze/chichewa-text2sql
python deploy_market_space.py    # deploys johneze/malawi-crop-market
```

---

## Citation

If you use this dataset or findings in your work, please cite:

```bibtex
@misc{chichewa-text2sql,
  title   = {Chichewa Text-to-SQL: A Low-Resource Benchmark for Natural Language Interfaces to Databases},
  author  = {Eze, John},
  year    = {2025},
  url     = {https://github.com/dmatekenya/chichewa-text2sql}
}
```

---

## License

This project is licensed under the [MIT License](LICENSE).

