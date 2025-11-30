import pandas as pd
import numpy as np
import re
from pathlib import Path

class DataCleaner:

    def __init__(self):
        self.raw_dir = Path("../../data/raw")
        self.processed_dir = Path("../../data/processed")
        self.processed_dir.mkdir(exist_ok=True)

    def load_merged(self):
        """Load merged dataset created by DataMerger."""
        #merged_path = self.raw_dir / "apps_merged.json"
        #if not merged_path.exists():
        #    raise FileNotFoundError("❌ Missing apps_merged.json. Run merging first.")
        # pick the latest merged apps JSON in processed folder
        processed_files = sorted(self.processed_dir.glob("apps_metadata_*.json"), reverse=True)
        if not processed_files:
            raise FileNotFoundError("❌ No merged apps JSON found in processed data.")
        merged_path = processed_files[0]

        print(f"📥 Loading merged file: {merged_path}")
        return pd.read_json(merged_path)

    def clean_installs(self, x):
        if pd.isna(x):
            return np.nan
        x = str(x).replace("+", "").replace(",", "").strip()
        return pd.to_numeric(x, errors="coerce")

    def clean_price(self, x):
        if pd.isna(x): return 0.0
        x = str(x).replace("$", "").replace("MAD", "").strip()
        return pd.to_numeric(x, errors="coerce")

    def clean_rating(self, x):
        return pd.to_numeric(x, errors="coerce")

    def normalize_text(self, x):
        if not isinstance(x, str):
            return ""
        x = x.lower()
        x = re.sub(r"\s+", " ", x)
        x = x.strip()
        return x

    def extract_year(self, date):
        try:
            return pd.to_datetime(date).year
        except:
            return np.nan

    def clean(self):
        df = self.load_merged()

        print("🔧 Cleaning dataset...")

        # Drop exact duplicates
        df = df.drop_duplicates(subset=["appId"])

        # Clean structured fields
        df["installs_clean"] = df["installs"].apply(self.clean_installs)
        df["price_clean"] = df["price"].apply(self.clean_price)
        df["score_clean"] = df["score"].apply(self.clean_rating)

        # Normalize text fields
        df["title_clean"] = df["title"].apply(self.normalize_text)
        df["description_clean"] = df["description"].apply(self.normalize_text)

        # Extract year from released date
        if "released" in df.columns:
            df["release_year"] = df["released"].apply(self.extract_year)

        # ✓ Fill missing values
        df["genre"] = df["genre"].fillna("unknown")
        df["summary"] = df["summary"].fillna("")

        print("📦 Saving processed dataset...")

        df.to_json(self.processed_dir / "apps_clean.json", orient="records", indent=2)
        df.to_csv(self.processed_dir / "apps_clean.csv", index=False)

        print("✅ Data cleaning completed!")
        print(f"📂 Files written to: {self.processed_dir}")


if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.clean()
 
