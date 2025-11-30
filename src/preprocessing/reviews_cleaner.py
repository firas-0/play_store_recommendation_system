import pandas as pd
import numpy as np
import re
from pathlib import Path

class ReviewsCleaner:

    def __init__(self):
        self.raw_dir = Path("../../data/raw")
        self.processed_dir = Path("../../data/processed")
        self.processed_dir.mkdir(exist_ok=True)

    def load_reviews(self):
        """Load merged reviews dataset"""
        # Pick the latest reviews JSON
        review_files = sorted(self.processed_dir.glob("reviews_metadata_*.json"), reverse=True)
        if not review_files:
            raise FileNotFoundError("❌ No reviews_metadata JSON found in raw data.")
        
        reviews_path = review_files[0]
        print(f"📥 Loading reviews file: {reviews_path}")
        return pd.read_json(reviews_path)

    def clean_rating(self, x):
        """Ensure review scores are numeric"""
        return pd.to_numeric(x, errors="coerce")

    def normalize_text(self, x):
        """Lowercase, strip whitespace, remove extra spaces"""
        if not isinstance(x, str):
            return ""
        x = x.lower()
        x = re.sub(r"\s+", " ", x)
        x = x.strip()
        return x

    def clean(self):
        df = self.load_reviews()
        print("🔧 Cleaning reviews dataset...")

        # Drop exact duplicates
        df = df.drop_duplicates(subset=["reviewId"])

        # Clean ratings
        if "score" in df.columns:
            df["score_clean"] = df["score"].apply(self.clean_rating)

        # Normalize review text
        if "content" in df.columns:
            df["content_clean"] = df["content"].apply(self.normalize_text)

        # Fill missing values
        df["userName"] = df.get("userName", pd.Series(["unknown"]*len(df))).fillna("unknown")
        df["appId"] = df.get("appId", pd.Series(["unknown"]*len(df))).fillna("unknown")

        # Save processed reviews
        df.to_json(self.processed_dir / "reviews_clean.json", orient="records", indent=2)
        df.to_csv(self.processed_dir / "reviews_clean.csv", index=False)

        print("✅ Reviews cleaning completed!")
        print(f"📂 Files written to: {self.processed_dir}")

if __name__ == "__main__":
    cleaner = ReviewsCleaner()
    cleaner.clean()

