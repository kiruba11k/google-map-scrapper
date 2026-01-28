import os
import time
import json
import threading
from datetime import datetime
import pandas as pd
from typing import Dict, Optional
import urllib.parse
import re

# Optimized scraping logic
class OptimizedScraper:
    def __init__(self):
        self.checkpoint_file = "checkpoint_results.csv"
        
    @staticmethod
    def clean_text(text):
        if not text:
            return ""
        return re.sub(r'\s+', ' ', str(text)).strip()
    
    @staticmethod
    def safe_float(text):
        try:
            return float(str(text).strip())
        except:
            return None
    
    @staticmethod
    def safe_int(text):
        try:
            return int(re.sub(r'[^\d]', '', str(text)))
        except:
            return None
    
    def save_checkpoint(self, df):
        """Save checkpoint with minimal I/O"""
        try:
            # Append to existing checkpoint to minimize writes
            if os.path.exists(self.checkpoint_file):
                existing = pd.read_csv(self.checkpoint_file)
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=['google_maps_link'], keep='last')
            df.to_csv(self.checkpoint_file, index=False)
        except Exception as e:
            print(f"Checkpoint save error: {e}")
    
    def build_search_url(self, query):
        """Build Google Maps search URL"""
        encoded = urllib.parse.quote_plus(query.strip())
        return f"https://www.google.com/maps/search/{encoded}"

class ScrapingTask:
    def __init__(self, task_id, config):
        self.task_id = task_id
        self.config = config
        self.status = "pending"
        self.progress = 0.0
        self.message = "Initializing..."
        self.results_file = None
        self.start_time = None
        self.total_results = 0
        self.scraper = OptimizedScraper()
        self._stop_flag = False
        
        # Create temp directory if not exists
        os.makedirs('temp', exist_ok=True)
        
        # Create task-specific results file path
        self.results_file = f"temp/results_{task_id}.csv"
        
    def run(self):
        """Main task execution method"""
        try:
            self.status = "running"
            self.start_time = datetime.now()
            self.message = "Starting scraping task..."
            
            # Initialize results DataFrame
            results_data = []
            
            if self.config['task_type'] == 'poi':
                results = self._run_poi_scraping_simulation()
            elif self.config['task_type'] == 'search':
                results = self._run_search_scraping_simulation()
            else:
                raise ValueError(f"Unknown task type: {self.config['task_type']}")
            
            # Convert results to DataFrame
            results_df = pd.DataFrame(results)
            self.total_results = len(results_df)
            
            if not self._stop_flag and not results_df.empty:
                # Save results to file
                results_df.to_csv(self.results_file, index=False)
                self.status = "completed"
                self.progress = 1.0
                self.message = f"Task completed with {self.total_results} results"
                
                # Also update checkpoint
                self.scraper.save_checkpoint(results_df)
            else:
                if self._stop_flag:
                    self.status = "stopped"
                    self.message = "Task stopped by user"
                else:
                    self.status = "completed"
                    self.message = "Task completed but no results found"
                    
        except Exception as e:
            print(f"Task error: {e}")  # Debug logging
            self.status = "failed"
            self.message = f"Error: {str(e)[:100]}"
            
            # Create empty results file to prevent download errors
            empty_df = pd.DataFrame(columns=['name', 'rating', 'reviews', 'phone', 'industry', 
                                             'full_address', 'website', 'google_maps_link', 'status'])
            empty_df.to_csv(self.results_file, index=False)
    
    def _run_poi_scraping_simulation(self):
        """Simulate POI scraping - replace with actual implementation"""
        self.message = "Starting POI scraping simulation..."
        results = []
        
        # Simulate scraping 100 results
        for i in range(1, 101):
            if self._stop_flag:
                break
            
            time.sleep(0.05)  # Simulated work
            
            # Update progress
            self.progress = i / 100
            self.message = f"Scraping POI result {i}/100"
            
            # Add simulated data
            results.append({
                'name': f'Business {i}',
                'rating': round(3.5 + (i % 50) / 10, 1),
                'reviews': 10 + (i * 5) % 500,
                'phone': f'555-{1000+i:04d}',
                'industry': 'Education',
                'full_address': f'{i} Main St, City {i}',
                'website': f'https://business{i}.com',
                'google_maps_link': f'https://maps.google.com/?q=business{i}',
                'status': 'ok'
            })
            
            # Periodically save checkpoint
            if i % 20 == 0:
                temp_df = pd.DataFrame(results)
                self.scraper.save_checkpoint(temp_df)
        
        return results
    
    def _run_search_scraping_simulation(self):
        """Simulate search scraping - replace with actual implementation"""
        self.message = "Starting search scraping simulation..."
        results = []
        
        # Simulate scraping 150 results
        for i in range(1, 151):
            if self._stop_flag:
                break
            
            time.sleep(0.03)  # Simulated work
            
            # Update progress
            self.progress = i / 150
            self.message = f"Scraping search result {i}/150"
            
            # Add simulated data
            results.append({
                'name': f'Search Result {i}',
                'rating': round(4.0 + (i % 40) / 10, 1),
                'reviews': 20 + (i * 3) % 1000,
                'phone': f'555-{2000+i:04d}',
                'industry': 'Training Institute',
                'full_address': f'{i+100} Search Ave, City {i}',
                'website': f'https://searchresult{i}.com',
                'google_maps_link': f'https://maps.google.com/?q=searchresult{i}',
                'status': 'ok'
            })
            
            # Periodically save checkpoint
            if i % 25 == 0:
                temp_df = pd.DataFrame(results)
                self.scraper.save_checkpoint(temp_df)
        
        return results
    
    def get_status(self):
        """Get current task status"""
        return {
            'task_id': self.task_id,
            'status': self.status,
            'progress': self.progress,
            'message': self.message,
            'started_at': self.start_time.isoformat() if self.start_time else None,
            'total_results': self.total_results,
            'results_file': self.results_file if os.path.exists(self.results_file) else None
        }
    
    def get_results_file(self):
        """Get path to results file"""
        if self.results_file and os.path.exists(self.results_file):
            return self.results_file
        return None

class TaskManager:
    """Manages background scraping tasks"""
    
    def __init__(self):
        self.tasks: Dict[str, ScrapingTask] = {}
        self.lock = threading.Lock()
    
    def add_task(self, task_id: str, task: ScrapingTask):
        """Add a new task"""
        with self.lock:
            self.tasks[task_id] = task
    
    def get_task(self, task_id: str) -> Optional[ScrapingTask]:
        """Get task by ID"""
        with self.lock:
            return self.tasks.get(task_id)
    
    def stop_task(self, task_id: str):
        """Stop a task"""
        with self.lock:
            task = self.tasks.get(task_id)
            if task:
                task.stop()
    
    def get_all_tasks(self):
        """Get all tasks"""
        with self.lock:
            return self.tasks.copy()
    
    def cleanup_old_tasks(self, max_age_hours=24):
        """Clean up old completed/failed tasks"""
        with self.lock:
            to_remove = []
            for task_id, task in self.tasks.items():
                if task.status in ['completed', 'failed', 'stopped']:
                    # Check age
                    if task.start_time:
                        age = datetime.now() - task.start_time
                        if age.total_seconds() > max_age_hours * 3600:
                            to_remove.append(task_id)
            
            for task_id in to_remove:
                del self.tasks[task_id]

def create_scraping_task(task_id, config):
    """Factory function to create scraping tasks"""
    return ScrapingTask(task_id, config)

# Add to background_tasks.py
import asyncio
from playwright.async_api import async_playwright

class RealGoogleMapsScraper:
    def __init__(self, task_id):
        self.task_id = task_id
        self.checkpoint_file = f"temp/checkpoint_{task_id}.csv"
    
    async def scrape_cards(self, search_url, max_results=50):
        """Actual Playwright scraping"""
        results = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                await page.goto(search_url, wait_until='networkidle')
                
                # Scroll and collect results
                for i in range(max_results // 10):
                    # Extract results
                    cards = await page.query_selector_all('a.hfpxzc')
                    for card in cards:
                        try:
                            name = await card.get_attribute('aria-label') or ''
                            href = await card.get_attribute('href') or ''
                            
                            results.append({
                                'name': name,
                                'google_maps_link': href,
                                'status': 'scraped'
                            })
                        except:
                            continue
                    
                    # Scroll down
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await asyncio.sleep(1)
                
                await browser.close()
                return results
                
            except Exception as e:
                await browser.close()
                raise e
