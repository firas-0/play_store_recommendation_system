import json
import pandas as pd
import sys
from pathlib import Path

def visualize_json(json_path):
    json_path = Path(json_path)

    if not json_path.exists():
        print(f"❌ File not found: {json_path}")
        return

    print(f"📂 Loading: {json_path}\n")

    # Read JSON
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load JSON: {e}")
        return

    # If it's not a list of objects → convert
    if isinstance(data, dict):
        # maybe nested like {"apps": [...]}
        for key in data:
            if isinstance(data[key], list):
                data = data[key]
                break

    # Convert to DataFrame for easy visualization
    df = pd.DataFrame(data)

    print("📌 Dataset Summary")
    print("------------------")
    print(f"🔢 Number of records: {len(df)}")
    print(f"🔑 Columns: {list(df.columns)}\n")

    print("📄 Preview (first 5 rows):")
    print(df.head(), "\n")

    # Show numeric statistics if exist
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        print("📊 Numeric Summary:")
        print(df[numeric_cols].describe())
    else:
        print("ℹ️ No numeric fields to summarize.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Usage: python show_json.py <path_to_json>")
        sys.exit(1)

    visualize_json(sys.argv[1])

