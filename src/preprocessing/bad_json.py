import json
from pathlib import Path

raw_dir = Path("../../data/raw")

batch_files = sorted(raw_dir.glob("apps_batch_*.json"))

print(f"Checking {len(batch_files)} batch files...\n")

for file in batch_files:
    try:
        with open(file, "r", encoding="utf-8") as f:
            json.load(f)
        print(f"✔ OK     → {file.name}")
    except Exception as e:
        print(f"❌ ERROR  → {file.name}")
        print(f"   {e}\n")

