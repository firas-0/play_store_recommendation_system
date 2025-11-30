#!/usr/bin/env python3
"""
Clean Reviews Dataset Script
----------------------------
Usage:
    python clean_reviews.py path/to/reviews.json
    python clean_reviews.py path/to/reviews.csv
"""

import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------
# Helper: Text Cleaner
# ---------------------------------------------------------
def clean_text(text):
    if pd.isna(text):
        return None
    text = str(text).strip()
    if len(text) == 0:
        return None
    return text


# ---------------------------------------------------------
# File Loader (CSV or JSON)
# ---------------------------------------------------------
def load_reviews(path: str) -> pd.DataFrame:
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    print(f"Loading: {path}")

    # ----- JSON -----
    if path.suffix.lower() in [".json", ".jsonl"]:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # Ensure list
        if isinstance(raw, dict):
            raw = raw.get("reviews", [])

        df = pd.json_normalize(raw)

    # ----- CSV -----
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path)

    else:
        raise ValueError("Unsupported file format. Use JSON or CSV.")

    print(f"Loaded {len(df)} reviews.")
    return df


# ---------------------------------------------------------
# Cleaning Logic
# ---------------------------------------------------------
def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:

    print("\n--- Cleaning Reviews ---")

    # Standard expected columns
    expected = {
        "reviewId", "userName", "content", "score",
        "thumbsUpCount", "reviewCreatedVersion",
        "at", "replyContent", "repliedAt"
    }

    # Warn missing
    missing = expected - set(df.columns)
    if missing:
        print(f"WARNING: Missing fields: {missing}")

    # Fill missing columns with None
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # ------------------------------------
    # Clean text columns
    # ------------------------------------
    text_cols = ["userName", "content", "replyContent", "reviewCreatedVersion"]
    for col in text_cols:
        df[col] = df[col].apply(clean_text)

    # ------------------------------------
    # Fix types: Rating (score)
    # ------------------------------------
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df.loc[(df["score"] < 1) | (df["score"] > 5), "score"] = None

    # ------------------------------------
    # Fix dates
    # ------------------------------------
    date_cols = ["at", "repliedAt"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # ------------------------------------
    # Clean thumbsUpCount
    # ------------------------------------
    df["thumbsUpCount"] = pd.to_numeric(df["thumbsUpCount"], errors="coerce").fillna(0).astype(int)

    # ------------------------------------
    # Drop duplicates
    # ------------------------------------
    before = len(df)
    df = df.drop_duplicates(subset=["reviewId"], keep="first")
    print(f"Dropping duplicates: {before - len(df)} removed.")

    # ------------------------------------
    # Drop completely empty reviews
    # ------------------------------------
    df = df.dropna(subset=["content"], how="all")

    df = df.reset_index(drop=True)
    print(f"Final cleaned reviews: {len(df)}")

    return df


# ---------------------------------------------------------
# Main Runner
# ---------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python clean_reviews.py <reviews_file.json/csv>")
        sys.exit(1)

    input_path = sys.argv[1]
    df = load_reviews(input_path)
    cleaned = clean_reviews(df)

    # Output filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = Path(input_path).stem

    out_csv = f"{base}_cleaned_{timestamp}.csv"
    out_json = f"{base}_cleaned_{timestamp}.json"

    cleaned.to_csv(out_csv, index=False, encoding="utf-8")
    cleaned.to_json(out_json, orient="records", force_ascii=False, indent=2)

    print(f"\nSaved cleaned CSV → {out_csv}")
    print(f"Saved cleaned JSON → {out_json}")


if __name__ == "__main__":
    main()

