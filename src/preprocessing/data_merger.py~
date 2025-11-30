"""
Data Merger and Initial Exploration
Merges all batch files and prepares data for Hadoop/Spark processing
"""

import json
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import glob

class DataMerger:
    def __init__(self, raw_dir='../../data/raw', processed_dir='../../data/processed'):
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def merge_all_batches(self):
        """Merge all batch JSON files into one"""
        batch_files = sorted(self.raw_dir.glob('apps_batch_*.json'))
        
        if not batch_files:
            print("❌ No batch files found!")
            return None
        
        print(f"📦 Found {len(batch_files)} batch files")
        
        all_apps = []
        for batch_file in batch_files:
            with open(batch_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_apps.extend(data)
        
        print(f"✅ Merged {len(all_apps)} apps")
        return all_apps
    
    def separate_apps_and_reviews(self, apps_data):
        """
        Separate apps metadata and reviews into different datasets
        This is important for Hadoop/Spark processing
        """
        apps_clean = []
        reviews_all = []
        
        for app in apps_data:
            app_id = app.get('appId', 'unknown')
            
            # Extract reviews
            reviews = app.pop('scraped_reviews', [])
            
            # Add app metadata
            apps_clean.append(app)
            
            # Add reviews with app_id reference
            for review in reviews:
                review['appId'] = app_id
                reviews_all.append(review)
        
        return apps_clean, reviews_all
    
    def save_datasets(self, apps_data, reviews_data):
        """Save cleaned datasets in multiple formats"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save as JSON (for Hadoop/Spark)
        apps_json = self.processed_dir / f'apps_metadata_{timestamp}.json'
        reviews_json = self.processed_dir / f'reviews_metadata_{timestamp}.json'
        
        with open(apps_json, 'w', encoding='utf-8') as f:
            json.dump(apps_data, f, ensure_ascii=False, indent=2)
        
        with open(reviews_json, 'w', encoding='utf-8') as f:
            json.dump(reviews_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Saved apps to: {apps_json}")
        print(f"✅ Saved reviews to: {reviews_json}")
        
        # Save as CSV for quick analysis
        try:
            apps_df = pd.json_normalize(apps_data)
            apps_csv = self.processed_dir / f'apps_metadata_{timestamp}.csv'
            apps_df.to_csv(apps_csv, index=False, encoding='utf-8')
            print(f"✅ Saved apps CSV: {apps_csv}")
        except Exception as e:
            print(f"⚠️  CSV conversion warning: {e}")
        
        try:
            reviews_df = pd.DataFrame(reviews_data)
            reviews_csv = self.processed_dir / f'reviews_metadata_{timestamp}.csv'
            reviews_df.to_csv(reviews_csv, index=False, encoding='utf-8')
            print(f"✅ Saved reviews CSV: {reviews_csv}")
        except Exception as e:
            print(f"⚠️  Reviews CSV warning: {e}")
        
        return apps_df, reviews_df
    
    def explore_data(self, apps_df, reviews_df):
        """Generate data exploration report"""
        print("\n" + "="*60)
        print("📊 DATA EXPLORATION REPORT")
        print("="*60)
        
        # Apps statistics
        print(f"\n📱 APPS DATASET:")
        print(f"   Total apps: {len(apps_df)}")
        print(f"   Columns: {len(apps_df.columns)}")
        print(f"   Memory usage: {apps_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        if 'score' in apps_df.columns:
            print(f"   Average rating: {apps_df['score'].mean():.2f}")
            print(f"   Median rating: {apps_df['score'].median():.2f}")
        
        if 'installs' in apps_df.columns:
            print(f"   Apps with installs data: {apps_df['installs'].notna().sum()}")
        
        if 'genre' in apps_df.columns:
            print(f"\n   Top 5 genres:")
            for genre, count in apps_df['genre'].value_counts().head().items():
                print(f"      - {genre}: {count}")
        
        # Reviews statistics
        print(f"\n💬 REVIEWS DATASET:")
        print(f"   Total reviews: {len(reviews_df)}")
        print(f"   Columns: {len(reviews_df.columns)}")
        print(f"   Memory usage: {reviews_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        if 'score' in reviews_df.columns:
            print(f"   Average review rating: {reviews_df['score'].mean():.2f}")
            print(f"   Rating distribution:")
            for rating, count in sorted(reviews_df['score'].value_counts().items()):
                print(f"      {rating}⭐: {count}")
        
        if 'content' in reviews_df.columns:
            avg_length = reviews_df['content'].str.len().mean()
            print(f"   Average review length: {avg_length:.0f} characters")
        
        # Missing values
        print(f"\n🔍 DATA QUALITY:")
        apps_missing = apps_df.isnull().sum().sum()
        reviews_missing = reviews_df.isnull().sum().sum()
        print(f"   Apps missing values: {apps_missing}")
        print(f"   Reviews missing values: {reviews_missing}")
        
        print("\n" + "="*60)
    
    def create_hdfs_structure_plan(self):
        """Create directory structure plan for HDFS"""
        structure = """
        
📁 RECOMMENDED HDFS STRUCTURE:
================================

/playstore/
├── raw/
│   ├── apps/              # Raw app metadata JSON
│   └── reviews/           # Raw reviews JSON
│
├── processed/
│   ├── apps_clean/        # Cleaned app data (Parquet)
│   ├── reviews_clean/     # Cleaned reviews (Parquet)
│   └── features/          # Engineered features
│
├── nlp/
│   ├── embeddings/        # BERT/Word2Vec embeddings
│   ├── topics/            # LDA topic models
│   └── sentiment/         # Sentiment scores
│
└── models/
    ├── collaborative/     # Collaborative filtering models
    ├── content_based/     # Content-based models
    └── hybrid/            # Hybrid recommendation models

📝 NEXT STEPS:
===============
1. Install Hadoop (if not already)
2. Upload raw data to HDFS: /playstore/raw/
3. Process with Spark
4. Train ML models
5. Deploy API + Dashboard
        """
        print(structure)
    
    def run_full_pipeline(self):
        """Run complete data preparation pipeline"""
        print("🚀 Starting Data Preparation Pipeline\n")
        
        # Step 1: Merge batches
        print("Step 1: Merging batch files...")
        all_apps = self.merge_all_batches()
        
        if not all_apps:
            return
        
        # Step 2: Separate apps and reviews
        print("\nStep 2: Separating apps and reviews...")
        apps_clean, reviews_all = self.separate_apps_and_reviews(all_apps)
        print(f"   Apps: {len(apps_clean)}")
        print(f"   Reviews: {len(reviews_all)}")
        
        # Step 3: Save datasets
        print("\nStep 3: Saving datasets...")
        apps_df, reviews_df = self.save_datasets(apps_clean, reviews_all)
        
        # Step 4: Explore data
        print("\nStep 4: Exploring data...")
        self.explore_data(apps_df, reviews_df)
        
        # Step 5: Show HDFS plan
        print("\nStep 5: HDFS Structure Plan...")
        self.create_hdfs_structure_plan()
        
        print("\n✨ Data preparation complete!")
        print(f"📂 Check: {self.processed_dir}")


if __name__ == "__main__":
    merger = DataMerger()
    merger.run_full_pipeline()
