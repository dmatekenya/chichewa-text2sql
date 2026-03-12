---
title: Chichewa Text2SQL
emoji: 🌍
colorFrom: blue
colorTo: green
sdk: gradio
sdk_version: "5.9.1"
app_file: app.py
pinned: false
hardware: zero-a10g
license: mit
---

# Chichewa Text-to-SQL

Query databases using natural language in **Chichewa** or English.

Model: [johneze/Llama-3.1-8B-Instruct-chichewa-text2sql](https://huggingface.co/johneze/Llama-3.1-8B-Instruct-chichewa-text2sql)

## API Usage

You can call this Space programmatically via `gradio_client`:

```python
from gradio_client import Client

client = Client("johneze/chichewa-text2sql")
result = client.predict(
    question="Ndi boma liti komwe anakolola chimanga chambiri?",
    language="ny",
    api_name="/generate_sql"
)
print(result)
```
