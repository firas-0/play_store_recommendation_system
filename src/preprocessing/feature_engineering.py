"""
Feature Engineering for Play Store Recommendation System
Generates TF-IDF and BERT embeddings for apps and reviews.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize
from sentence_transformers import SentenceTransformer

class FeatureEngineer:
    def __init__(self, apps_path='../../data/processed/apps_clean.json',
                 reviews_path='../../data/processed/reviews_clean.json',
                 processed_dir='../../data/processed/features'):
        self.apps_path = Path(apps_path)
        self.reviews_path = Path(reviews_path)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Load pre-trained BERT model
        print("🤖 Loading BERT model...")
        self.bert_model = SentenceTransformer('all-MiniLM-L6-v2')  # lightweight but strong

    def load_data(self):
        print(f"📥 Loading apps from: {self.apps_path}")
        self.apps_df = pd.read_json(self.apps_path)
        
        print(f"📥 Loading reviews from: {self.reviews_path}")
        self.reviews_df = pd.read_json(self.reviews_path)

    # ---------------- TF-IDF Features ----------------
    def compute_tfidf_features(self, text_column='description_clean', max_features=5000):
        print(f"💬 Computing TF-IDF for {text_column}...")
        vectorizer = TfidfVectorizer(max_features=max_features, stop_words='english')
        
        # Apps TF-IDF
        apps_text = self.apps_df[text_column].fillna("")
        self.apps_tfidf = vectorizer.fit_transform(apps_text)
        print(f"   Apps TF-IDF shape: {self.apps_tfidf.shape}")
        
        # Reviews TF-IDF
        reviews_text = self.reviews_df['content_clean'].fillna("")
        self.reviews_tfidf = vectorizer.transform(reviews_text)
        print(f"   Reviews TF-IDF shape: {self.reviews_tfidf.shape}")

        # Save TF-IDF features (sparse matrix)
        pd.to_pickle(self.apps_tfidf, self.processed_dir / 'apps_tfidf.pkl')
        pd.to_pickle(self.reviews_tfidf, self.processed_dir / 'reviews_tfidf.pkl')
        print(f"✅ TF-IDF features saved to {self.processed_dir}")

    # ---------------- BERT Embeddings ----------------
    def compute_bert_embeddings(self, text_column='description_clean', batch_size=64):
        print(f"🧠 Computing BERT embeddings for {text_column}...")
        
        # Apps embeddings
        apps_text = self.apps_df[text_column].fillna("").tolist()
        self.apps_bert = self.bert_model.encode(apps_text, batch_size=batch_size, show_progress_bar=True, normalize_embeddings=True)
        print(f"   Apps BERT shape: {self.apps_bert.shape}")
        
        # Reviews embeddings
        reviews_text = self.reviews_df['content_clean'].fillna("").tolist()
        self.reviews_bert = self.bert_model.encode(reviews_text, batch_size=batch_size, show_progress_bar=True, normalize_embeddings=True)
        print(f"   Reviews BERT shape: {self.reviews_bert.shape}")
        
        # Save embeddings
        np.save(self.processed_dir / 'apps_bert.npy', self.apps_bert)
        np.save(self.processed_dir / 'reviews_bert.npy', self.reviews_bert)
        print(f"✅ BERT embeddings saved to {self.processed_dir}")

    def run(self):
        self.load_data()
        self.compute_tfidf_features()
        self.compute_bert_embeddings()
        print("✨ Feature engineering completed!")

if __name__ == "__main__":
    fe = FeatureEngineer()
    fe.run()

