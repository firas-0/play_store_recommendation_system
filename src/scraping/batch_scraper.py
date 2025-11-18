"""
Advanced Play Store Batch Scraper with Progress Tracking
Handles large-scale scraping with checkpoints and error recovery
"""

import json
import os
import time
from datetime import datetime
from typing import List, Dict
import pandas as pd
from google_play_scraper import app, search, reviews, Sort
from tqdm import tqdm

class AdvancedPlayStoreScraper:
    def __init__(self, output_dir='../data/raw'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Checkpoint file to resume scraping
        self.checkpoint_file = os.path.join(output_dir, 'scraping_checkpoint.json')
        self.error_log_file = os.path.join(output_dir, 'scraping_errors.log')
        
    def load_checkpoint(self):
        """Load checkpoint to resume scraping"""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return {'scraped_app_ids': [], 'failed_app_ids': []}
    
    def save_checkpoint(self, checkpoint):
        """Save checkpoint"""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    
    def log_error(self, message):
        """Log errors to file"""
        with open(self.error_log_file, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"[{timestamp}] {message}\n")
    
    def scrape_app_safely(self, app_id, retries=3):
        """Scrape app with retry logic"""
        for attempt in range(retries):
            try:
                result = app(app_id, lang='en', country='us')
                return result
            except Exception as e:
                if attempt == retries - 1:
                    self.log_error(f"Failed to scrape {app_id}: {str(e)}")
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff
        return None
    
    def scrape_reviews_safely(self, app_id, count=100, sort=Sort.MOST_RELEVANT):
        """Scrape reviews with error handling"""
        try:
            result, _ = reviews(app_id, lang='en', country='us', 
                              sort=sort, count=count)
            return result
        except Exception as e:
            self.log_error(f"Failed to scrape reviews for {app_id}: {str(e)}")
            return []
    
    def discover_apps_by_categories(self, categories: List[str], 
                                   apps_per_category=50):
        """
        Discover apps across multiple categories
        Returns list of unique app IDs
        """
        all_app_ids = set()
        
        print("🔍 Discovering apps...")
        for category in tqdm(categories, desc="Categories"):
            try:
                results = search(category, lang='en', country='us', 
                               n_hits=apps_per_category)
                for result in results:
                    all_app_ids.add(result['appId'])
                time.sleep(1)
            except Exception as e:
                self.log_error(f"Search failed for '{category}': {str(e)}")
        
        return list(all_app_ids)
    
    def batch_scrape_apps(self, app_ids: List[str], 
                         include_reviews=False, 
                         reviews_per_app=50,
                         save_interval=10):
        """
        Scrape apps in batches with checkpoint system
        """
        checkpoint = self.load_checkpoint()
        scraped_ids = set(checkpoint['scraped_app_ids'])
        
        # Filter out already scraped apps
        remaining_ids = [aid for aid in app_ids if aid not in scraped_ids]
        
        if not remaining_ids:
            print("✅ All apps already scraped!")
            return
        
        print(f"📊 Total apps to scrape: {len(remaining_ids)}")
        print(f"✅ Already scraped: {len(scraped_ids)}")
        
        scraped_data = []
        
        for idx, app_id in enumerate(tqdm(remaining_ids, desc="Scraping apps")):
            # Scrape app details
            app_data = self.scrape_app_safely(app_id)
            
            if app_data:
                # Add reviews if requested
                if include_reviews:
                    app_reviews = self.scrape_reviews_safely(app_id, 
                                                            count=reviews_per_app)
                    app_data['scraped_reviews'] = app_reviews
                
                scraped_data.append(app_data)
                scraped_ids.add(app_id)
                
                # Save progress periodically
                if (idx + 1) % save_interval == 0:
                    self._save_batch(scraped_data)
                    checkpoint['scraped_app_ids'] = list(scraped_ids)
                    self.save_checkpoint(checkpoint)
                    scraped_data = []  # Clear memory
            
            time.sleep(1.2)  # Rate limiting
        
        # Save remaining data
        if scraped_data:
            self._save_batch(scraped_data)
        
        # Final checkpoint
        checkpoint['scraped_app_ids'] = list(scraped_ids)
        self.save_checkpoint(checkpoint)
        
        print(f"\n✨ Scraping complete! Total apps: {len(scraped_ids)}")
    
    def _save_batch(self, data):
        """Save batch of scraped data"""
        if not data:
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'apps_batch_{timestamp}.json'
        filepath = os.path.join(self.output_dir, filename)
        
        # Convert data to JSON-serializable format
        serializable_data = self._make_serializable(data)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, ensure_ascii=False, indent=2)
    
    def _make_serializable(self, obj):
        """Convert objects to JSON-serializable format"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: self._make_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        else:
            return obj
    
    def merge_batches(self, output_filename='apps_merged.json'):
        """Merge all batch files into one"""
        batch_files = [f for f in os.listdir(self.output_dir) 
                      if f.startswith('apps_batch_')]
        
        if not batch_files:
            print("No batch files found!")
            return
        
        all_data = []
        for batch_file in tqdm(batch_files, desc="Merging batches"):
            filepath = os.path.join(self.output_dir, batch_file)
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_data.extend(data)
        
        # Save merged file
        output_path = os.path.join(self.output_dir, output_filename)
        serializable_data = self._make_serializable(all_data)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✅ Merged {len(all_data)} apps into {output_filename}")
        
        # Convert to CSV - handle nested structures carefully
        try:
            df = pd.json_normalize(all_data, max_level=1)
            csv_path = output_path.replace('.json', '.csv')
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"✅ Also saved as CSV: {csv_path}")
        except Exception as e:
            print(f"⚠️  CSV conversion warning: {str(e)}")
            print("   JSON file is still available")
    
    def get_statistics(self):
        """Get scraping statistics"""
        checkpoint = self.load_checkpoint()
        return {
            'total_scraped': len(checkpoint['scraped_app_ids']),
            'failed': len(checkpoint.get('failed_app_ids', [])),
            'checkpoint_file': self.checkpoint_file
        }


# Example Usage
if __name__ == "__main__":
    scraper = AdvancedPlayStoreScraper()
    
    # Define broad categories for discovery
    categories = [
        'social media', 'messaging', 'puzzle games', 'action games',
        'productivity', 'business', 'education', 'learning',
        'fitness', 'health', 'music', 'video streaming',
        'photo editor', 'camera', 'shopping', 'food delivery',
        'travel', 'navigation', 'news', 'weather',
        'finance', 'banking', 'books', 'comics'
    ]
    
    print("🚀 Starting Advanced Play Store Scraper")
    print("=" * 60)
    
    # Step 1: Discover apps
    print("\n📱 Phase 1: App Discovery")
    app_ids = scraper.discover_apps_by_categories(categories, apps_per_category=30)
    print(f"✅ Discovered {len(app_ids)} unique apps")
    
    # Step 2: Scrape app details (with reviews)
    print("\n📊 Phase 2: Scraping App Details")
    scraper.batch_scrape_apps(
        app_ids, 
        include_reviews=True, 
        reviews_per_app=50,
        save_interval=20  # Save every 20 apps
    )
    
    # Step 3: Merge all batch files
    print("\n🔄 Phase 3: Merging Data")
    scraper.merge_batches(output_filename='play_store_apps_complete.json')
    
    # Show statistics
    stats = scraper.get_statistics()
    print(f"\n📈 Final Statistics:")
    print(f"   Total Apps Scraped: {stats['total_scraped']}")
    print(f"   Failed Attempts: {stats['failed']}")
    
    print("\n✨ All done! Check your data/raw/ folder")
