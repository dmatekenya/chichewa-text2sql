"""
Deploy hf_space_market/ to johneze/malawi-crop-market on HuggingFace Spaces.
Run:  .venv\\Scripts\\python.exe deploy_market_space.py
"""
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass  # python-dotenv not installed; rely on env var being set externally

try:
    from huggingface_hub import HfApi, create_repo
except ImportError:
    sys.exit("huggingface_hub not installed. Run: pip install huggingface_hub")

SPACE_REPO_ID = "johneze/malawi-crop-market"
SPACE_DIR     = Path(__file__).resolve().parent / "hf_space_market"
DB_PATH       = Path(__file__).resolve().parent / "data" / "database" / "chichewa_text2sql.db"
DATASET_PATH  = Path(__file__).resolve().parent / "data" / "all.json"

def deploy():
    token = os.getenv("HF_TOKEN", "").strip()
    if not token:
        token = input("HF_TOKEN not found in .env. Paste your HuggingFace token (hf_...): ").strip()
    if not token.startswith("hf_"):
        sys.exit("Token must start with hf_. Get one at https://huggingface.co/settings/tokens")

    api = HfApi(token=token)

    print(f"Creating / verifying Space: {SPACE_REPO_ID}")
    create_repo(
        repo_id=SPACE_REPO_ID,
        repo_type="space",
        space_sdk="gradio",
        exist_ok=True,
        private=False,
        token=token,
    )

    # ── Upload app files ──────────────────────────────────────────────────
    for fpath in sorted(SPACE_DIR.iterdir()):
        if fpath.name.startswith(".") or fpath.is_dir():
            continue
        print(f"  Uploading {fpath.name} ...")
        api.upload_file(
            path_or_fileobj=str(fpath),
            path_in_repo=fpath.name,
            repo_id=SPACE_REPO_ID,
            repo_type="space",
        )

    # ── Upload database ───────────────────────────────────────────────────
    if DB_PATH.exists():
        print(f"  Uploading {DB_PATH.name} ...")
        api.upload_file(
            path_or_fileobj=str(DB_PATH),
            path_in_repo="data/database/chichewa_text2sql.db",
            repo_id=SPACE_REPO_ID,
            repo_type="space",
        )
    else:
        print(f"WARNING: DB not found at {DB_PATH}")

    # ── Upload dataset ────────────────────────────────────────────────────
    if DATASET_PATH.exists():
        print(f"  Uploading {DATASET_PATH.name} ...")
        api.upload_file(
            path_or_fileobj=str(DATASET_PATH),
            path_in_repo="data/all.json",
            repo_id=SPACE_REPO_ID,
            repo_type="space",
        )
    else:
        print(f"WARNING: Dataset not found at {DATASET_PATH}")

    print(f"\n✅ Done!  Visit: https://huggingface.co/spaces/{SPACE_REPO_ID}")


if __name__ == "__main__":
    deploy()
