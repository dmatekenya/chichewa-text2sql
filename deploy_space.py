"""
deploy_space.py
Run once to create (or update) the HuggingFace Space and upload all files.

Usage:
    .venv/Scripts/python.exe deploy_space.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    from huggingface_hub import HfApi
except ImportError:
    sys.exit("huggingface_hub not installed. Run: pip install huggingface_hub")

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass  # python-dotenv not installed; rely on env var being set externally

# ── Configuration ──────────────────────────────────────────────────────────
SPACE_REPO_ID = "johneze/chichewa-text2sql"   # change owner if needed
SPACE_SDK     = "gradio"
SPACE_DIR     = Path(__file__).resolve().parent / "hf_space"
# ──────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  Chichewa Text-to-SQL  →  HuggingFace Space Deployer")
    print("=" * 60)

    token = os.getenv("HF_TOKEN", "").strip()
    if not token:
        token = input("\nHF_TOKEN not found in .env. Paste your HuggingFace token (hf_...): ").strip()
    if not token.startswith("hf_"):
        sys.exit("Token must start with hf_. Get one at https://huggingface.co/settings/tokens")

    api = HfApi(token=token)

    # 1. Create Space if it does not exist
    try:
        api.repo_info(SPACE_REPO_ID, repo_type="space")
        print(f"\n✅  Space already exists: https://huggingface.co/spaces/{SPACE_REPO_ID}")
    except Exception:
        print(f"\n🔨  Creating Space: {SPACE_REPO_ID} …")
        try:
            api.create_repo(
                repo_id=SPACE_REPO_ID,
                repo_type="space",
                space_sdk=SPACE_SDK,
                space_hardware="zero-a10g",   # free ZeroGPU tier
                private=False,
                exist_ok=True,
            )
            print("✅  Space created with ZeroGPU (zero-a10g).")
        except Exception as e:
            # ZeroGPU may not be available to all accounts; fall back to CPU
            print(f"   ZeroGPU not available ({e}). Falling back to cpu-basic …")
            api.create_repo(
                repo_id=SPACE_REPO_ID,
                repo_type="space",
                space_sdk=SPACE_SDK,
                private=False,
                exist_ok=True,
            )
            print("✅  Space created (cpu-basic). You can change hardware on the Space settings page.")

    # 2. Upload all files from hf_space/
    print(f"\n📤  Uploading files from: {SPACE_DIR}")
    files = list(SPACE_DIR.iterdir())
    for f in files:
        print(f"   {f.name}")

    api.upload_folder(
        folder_path=str(SPACE_DIR),
        repo_id=SPACE_REPO_ID,
        repo_type="space",
    )

    # 3. Upload dataset + database so the Space can do matching and query execution
    project_root = Path(__file__).resolve().parent
    data_files = [
        (project_root / "data" / "all.json",                          "data/all.json"),
        (project_root / "data" / "database" / "chichewa_text2sql.db", "data/database/chichewa_text2sql.db"),
    ]
    print("\n📤  Uploading dataset and database …")
    for local_path, repo_path in data_files:
        if local_path.exists():
            size_mb = local_path.stat().st_size / 1024 / 1024
            print(f"   {repo_path} ({size_mb:.1f} MB)")
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=SPACE_REPO_ID,
                repo_type="space",
            )
        else:
            print(f"   WARNING: {local_path} not found, skipping.")

    print("\n🚀  Upload complete!")
    print(f"   Space URL : https://huggingface.co/spaces/{SPACE_REPO_ID}")
    print(f"   API URL   : https://{SPACE_REPO_ID.replace('/', '-')}.hf.space")
    print("\nBuilding usually takes 2-5 minutes. Check the 'Logs' tab on the Space page.")
    print("\nIn Streamlit → ⚙️ Model settings → endpoint:")
    print(f"   {SPACE_REPO_ID}")

if __name__ == "__main__":
    main()
