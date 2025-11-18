"""
Play Store App Scraper
Saves data to ../data/raw/
"""

from google_play_scraper import app, search, reviews, Sort
import pandas as pd
import json
import time
from datetime import datetime
import os

class PlayStoreScraper:
    def __init__(self, output_dir='../data/raw'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def scrape_app_details(self, app_id):
        """Scrape detailed information for a single app"""
        try:
            result = app(app_id, lang='en', country='us')
            return result
        except Exception as e:
            print(f"Error scraping {app_id}: {e}")
            return None
    
    def search_apps(self, query, num_results=50):
        """Search for apps by keyword"""
        try:
            results = search(query, lang='en', country='us', n_hits=num_results)
            return results
        except Exception as e:
            print(f"Error searching for '{query}': {e}")
            return []
    
    def scrape_reviews(self, app_id, count=100, sort=Sort.MOST_RELEVANT):
        """Scrape reviews for an app"""
        try:
            result, _ = reviews(
                app_id,
                lang='en',
                country='us',
                sort=sort,
                count=count
            )
            return result
        except Exception as e:
            print(f"Error scraping reviews for {app_id}: {e}")
            return []
    
    def scrape_category_apps(self, category_queries, apps_per_query=30):
        """
        Scrape apps from multiple categories
        
        Example categories:
        ['social media', 'games', 'productivity', 'education', 
         'entertainment', 'shopping', 'health fitness', 'photography']
        """
        all_apps = []
        app_ids_seen = set()
        
        for query in category_queries:
            print(f"\n📱 Searching for: {query}")
            search_results = self.search_apps(query, num_results=apps_per_query)
            
            for idx, search_result in enumerate(search_results):
                app_id = search_result['appId']
                
                # Skip duplicates
                if app_id in app_ids_seen:
                    continue
                
                app_ids_seen.add(app_id)
                print(f"  [{idx+1}/{len(search_results)}] Scraping: {search_result['title']}")
                
                # Get detailed app info
                app_details = self.scrape_app_details(app_id)
                if app_details:
                    app_details['search_query'] = query
                    all_apps.append(app_details)
                
                # Be polite to the server
                time.sleep(1)
            
            time.sleep(2)  # Longer pause between categories
        
        return all_apps
    
    def scrape_apps_with_reviews(self, app_ids, reviews_per_app=50):
        """Scrape apps and their reviews"""
        apps_data = []
        
        for idx, app_id in enumerate(app_ids):
            print(f"\n[{idx+1}/{len(app_ids)}] Processing: {app_id}")
            
            # Get app details
            app_details = self.scrape_app_details(app_id)
            if not app_details:
                continue
            
            # Get reviews
            print(f"  └─ Fetching reviews...")
            app_reviews = self.scrape_reviews(app_id, count=reviews_per_app)
            
            apps_data.append({
                'app_details': app_details,
                'reviews': app_reviews
            })
            
            time.sleep(1.5)
        
        return apps_data
    
    def save_to_json(self, data, filename):
        """Save data to JSON file"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n✅ Saved to: {filepath}")
    
    def save_to_csv(self, data, filename):
        """Save app details to CSV"""
        df = pd.DataFrame(data)
        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"\n✅ Saved to: {filepath}")


# Example Usage
if __name__ == "__main__":
    scraper = PlayStoreScraper()
    
    # Define categories to scrape
    categories = [
        'social media apps',
        'puzzle games',
        'productivity tools',
        'fitness workout',
        'photo editor',
        'music streaming',
        'food delivery',
        'language learning'
    ]
    
    print("🚀 Starting Play Store Scraper")
    print("=" * 50)
    
    # Scrape apps from multiple categories
    apps = scraper.scrape_category_apps(categories, apps_per_query=20)
    
    print(f"\n📊 Scraped {len(apps)} unique apps")
    
    # Save complete data to JSON
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    scraper.save_to_json(apps, f'apps_detailed_{timestamp}.json')
    
    # Save simplified version to CSV for quick analysis
    scraper.save_to_csv(apps, f'apps_basic_{timestamp}.csv')
    
    print("\n✨ Scraping complete!")
    print(f"📁 Check your data/raw/ folder for output files")
