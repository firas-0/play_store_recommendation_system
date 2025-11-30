import pandas as pd
import numpy as np
import json
import ast
import re
from pathlib import Path


# -----------------------------------------
# SAFE EVAL (FIXED)
# -----------------------------------------
def safe_eval(value):
    """Safely parse list/dict-like strings into Python objects."""

    # Already list/dict → return as-is
    if isinstance(value, (list, dict)):
        return value

    # Handle None
    if value is None:
        return []

    # Handle numpy arrays
    if isinstance(value, np.ndarray):
        return list(value)

    # Handle NaN
    if isinstance(value, float) and np.isnan(value):
        return []

    # Parse JSON-like strings
    if isinstance(value, str):

        # Clean invalid formats
        value = value.strip()

        if value == "" or value.lower() == "nan":
            return []

        try:
            # Convert strings like "['a','b']" → list
            return ast.literal_eval(value)
        except:
            return []

    return []


# -----------------------------------------
# TEXT NORMALIZER
# -----------------------------------------
def normalize_text(text):
    if not isinstance(text, str):
        return ""

    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# -----------------------------------------
# NUMERIC CLEANERS
# -----------------------------------------
def clean_installs(x):
    if pd.isna(x):
        return np.nan
    x = str(x).replace(",", "").replace("+", "")
    return pd.to_numeric(x, errors="coerce")


def clean_price(x):
    if pd.isna(x):
        return 0.0
    x = str(x).replace("$", "").replace("MAD", "")
    return pd.to_numeric(x, errors="coerce")


def clean_rating(x):
    return pd.to_numeric(x, errors="coerce")


def extract_year(date):
    try:
        return pd.to_datetime(date).year
    except:
        return np.nan


# =========================================
#           MAIN CLEANING FUNCTION
# =========================================
def clean_apps_dataset(path_in, path_out):
    print(f"📂 Loading: {path_in}")

    df = pd.read_json(path_in)

    print(f"🔢 Loaded {len(df)} rows")

    # -----------------------------------------
    # Remove duplicate apps
    # -----------------------------------------
    if "appId" in df.columns:
        df = df.drop_duplicates(subset=["appId"])
        print(f"🧹 Removed duplicates → {len(df)} rows remain")

    # -----------------------------------------
    # CLEAN NUMERIC FIELDS
    # -----------------------------------------
    if "installs" in df.columns:
        df["installs_clean"] = df["installs"].apply(clean_installs)

    if "price" in df.columns:
        df["price_clean"] = df["price"].apply(clean_price)

    if "score" in df.columns:
        df["score_clean"] = df["score"].apply(clean_rating)

    # -----------------------------------------
    # NORMALIZE TEXT FIELDS
    # -----------------------------------------
    text_columns = ["title", "description", "summary"]

    for col in text_columns:
        if col in df.columns:
            df[col + "_clean"] = df[col].apply(normalize_text)

    # -----------------------------------------
    # PARSE LIST-LIKE COLUMNS
    # -----------------------------------------
    list_columns = ["screenshots", "comments", "categories", "histogram"]

    for col in list_columns:
        if col in df.columns:
            df[col] = df[col].apply(safe_eval)

    # -----------------------------------------
    # RELEASE YEAR
    # -----------------------------------------
    if "released" in df.columns:
        df["release_year"] = df["released"].apply(extract_year)

    # -----------------------------------------
    # FILL MISSING GENRE
    # -----------------------------------------
    if "genre" in df.columns:
        df["genre"] = df["genre"].fillna("unknown")

    # -----------------------------------------
    # SAVE CLEANED DATASET
    # -----------------------------------------
    print("💾 Saving cleaned dataset...")
    Path(path_out).parent.mkdir(parents=True, exist_ok=True)

    df.to_json(path_out, orient="records", indent=2)
    df.to_csv(path_out.replace(".json", ".csv"), index=False)

    print("✅ Cleaning completed!")
    print(f"📄 Saved to: {path_out}")


# =========================================
#               RUN SCRIPT
# =========================================
if __name__ == "__main__":
    clean_apps_dataset(
        path_in="../../data/processed/apps_clean.json",
        path_out="../../data/processed/apps_final_cleaned.json"
    )

