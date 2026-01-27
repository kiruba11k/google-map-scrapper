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
        
        # Create task-specific results file
        self.results_file = f"temp/results_{task_id}.csv"
        
    def run(self):
        """Main task execution method"""
        try:
            self.status = "running"
            self.start_time = datetime.now()
            
            if self.config['task_type'] == 'poi':
                self._run_poi_scraping()
            elif self.config['task_type'] == 'search':
                self._run_search_scraping()
            else:
                raise ValueError(f"Unknown task type: {self.config['task_type']}")
                
            if not self._stop_flag:
                self.status = "completed"
                self.progress = 1.0
                self.message = f"Task completed with {self.total_results} results"
        except Exception as e:
            self.status = "failed"
            self.message = f"Error: {str(e)}"
        finally:
            # Cleanup if stopped
            if self._stop_flag:
                self.status = "stopped"
                self.message = "Task stopped by user"
    
    def _run_poi_scraping(self):
        """Run POI radius scraping"""
        # Implementation of your POI scraping logic
        # This would use Playwright similarly to your original code
        # but optimized for background processing
        
        # Placeholder implementation
        self.message = "Starting POI scraping..."
        
        # Simulate progress for demo
        for i in range(1, 101):
            if self._stop_flag:
                break
            time.sleep(0.1)  # Simulated work
            self.progress = i / 100
            self.message = f"Scraping... {i}%"
            
        self.total_results = 100  # Placeholder
    
    def _run_search_scraping(self):
        """Run search query scraping"""
        # Implementation of your search scraping logic
        
        self.message = "Starting search scraping..."
        
        # Simulate progress for demo
        for i in range(1, 101):
            if self._stop_flag:
                break
            time.sleep(0.1)  # Simulated work
            self.progress = i / 100
            self.message = f"Scraping... {i}%"
            
        self.total_results = 150  # Placeholder
    
    def stop(self):
        """Stop the task"""
        self._stop_flag = True
    
    def get_status(self):
        """Get current task status"""
        return {
            'task_id': self.task_id,
            'status': self.status,
            'progress': self.progress,
            'message': self.message,
            'started_at': self.start_time.isoformat() if self.start_time else None,
            'total_results': self.total_results,
            'results_file': self.results_file
        }
    
    def get_results_file(self):
        """Get path to results file"""
        if os.path.exists(self.results_file):
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
