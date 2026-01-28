import os
import time
import re
import math
import urllib.parse
import pandas as pd
from datetime import datetime
from typing import Dict, Optional
import threading
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Optimized scraping logic
class OptimizedScraper:
    def __init__(self, base_dir, temp_dir, checkpoint_file):
        self.base_dir = base_dir
        self.temp_dir = temp_dir
        self.checkpoint_file = checkpoint_file
        
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
            print(f"Checkpoint saved: {self.checkpoint_file}, rows: {len(df)}")
        except Exception as e:
            print(f"Checkpoint save error: {e}")
    
    def build_search_url(self, query):
        """Build Google Maps search URL"""
        encoded = urllib.parse.quote_plus(query.strip())
        return f"https://www.google.com/maps/search/{encoded}"
    
    def normalize_maps_url(self, url):
        """Normalize Google Maps URL"""
        if not url:
            return ""
        return url.split("&")[0]

class RealGoogleMapsScraper:
    def __init__(self, task, base_dir, temp_dir, checkpoint_file):
        self.task = task
        self.task_id = task.task_id
        self.base_dir = base_dir
        self.temp_dir = temp_dir
        self.checkpoint_file = checkpoint_file
        
    def clean_text(self, text):
        if not text:
            return ""
        return re.sub(r'\s+', ' ', str(text)).strip()
    
    def safe_float(self, text):
        try:
            return float(str(text).strip())
        except:
            return None
    
    def safe_int(self, text):
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
            print(f"Checkpoint saved: {self.checkpoint_file}, rows: {len(df)}")
        except Exception as e:
            print(f"Checkpoint save error: {e}")
    
    def normalize_maps_url(self, url):
        """Normalize Google Maps URL"""
        if not url:
            return ""
        return url.split("&")[0]
    
    def scrape_cards_only(self, search_url, max_results=200, scroll_pause=1.0):
        """Actual card scraping from your original code"""
        rows = []
        seen_links = set()
        
        try:
            self.task.message = "Launching browser..."
            print("Launching Playwright browser...")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )
                context = browser.new_context(locale="en-US")
                page = context.new_page()
                page.set_default_timeout(90000)
                
                self.task.message = "Opening Google Maps search page..."
                print(f"Navigating to: {search_url}")
                page.goto(search_url, wait_until="domcontentloaded")
                
                try:
                    page.wait_for_selector('div[role="feed"]', timeout=60000)
                    self.task.message = "Results feed detected, starting to scroll..."
                except:
                    self.task.message = "Results feed not detected, trying to find results anyway..."
                
                feed = page.locator('div[role="feed"]').first
                
                last_count = 0
                stable_rounds = 0
                stable_limit = 6
                
                self.task.message = "Scrolling to load more results..."
                
                # Track scroll progress
                scroll_iterations = 0
                max_scroll_iterations = 100
                
                while scroll_iterations < max_scroll_iterations:
                    if self.task._stop_flag:
                        self.task.message = "Stopping scrolling..."
                        break
                        
                    cards = page.locator("a.hfpxzc")
                    count = cards.count()
                    
                    self.task.message = f"Loaded cards: {count}"
                    self.task.progress = min(0.4, count / max_results * 0.4)
                    
                    if count >= max_results:
                        self.task.message = f"Reached max results: {max_results}"
                        break
                    
                    if count == last_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                    
                    if stable_rounds >= stable_limit:
                        self.task.message = f"No new results for {stable_limit} scrolls, stopping..."
                        break
                    
                    last_count = count
                    
                    try:
                        feed.evaluate("(el) => el.scrollBy(0, el.scrollHeight)")
                    except:
                        page.mouse.wheel(0, 5000)
                    
                    time.sleep(scroll_pause)
                    scroll_iterations += 1
                
                cards = page.locator("a.hfpxzc")
                total = min(cards.count(), max_results)
                
                self.task.message = f"Extracting data from {total} cards..."
                print(f"Found {total} cards to scrape")
                
                for i in range(total):
                    if self.task._stop_flag:
                        self.task.message = "Stopping extraction..."
                        break
                        
                    # Update progress (40% to 90% for extraction)
                    self.task.progress = 0.4 + (i / total * 0.5)
                    
                    try:
                        card = cards.nth(i)
                        link = card.get_attribute("href") or ""
                        link = self.normalize_maps_url(link)
                        
                        if not link or link in seen_links:
                            continue
                        seen_links.add(link)
                        
                        # Try to get the name
                        name = ""
                        try:
                            name = card.get_attribute("aria-label") or ""
                        except:
                            pass
                        
                        # Try to get more details from the card
                        rating = None
                        reviews = None
                        category = ""
                        address_snippet = ""
                        
                        try:
                            # Get the card container
                            card_container = card.locator("xpath=ancestor::div[contains(@class, 'Nv2PK')]").first
                            
                            # Try to get rating
                            try:
                                rating_elem = card_container.locator("span.MW4etd").first
                                rating_text = rating_elem.inner_text(timeout=1000)
                                rating = self.safe_float(rating_text)
                            except:
                                pass
                            
                            # Try to get reviews
                            try:
                                reviews_elem = card_container.locator("span.UY7F9").first
                                reviews_text = reviews_elem.inner_text(timeout=1000)
                                reviews = self.safe_int(reviews_text)
                            except:
                                pass
                            
                            # Try to get category and address
                            try:
                                details_line = card_container.locator("div.W4Efsd").nth(1)
                                details_text = details_line.inner_text(timeout=1000)
                                details_text = self.clean_text(details_text)
                                
                                if "·" in details_text:
                                    parts = [p.strip() for p in details_text.split("·") if p.strip()]
                                    if len(parts) >= 1:
                                        category = parts[0]
                                    if len(parts) >= 2:
                                        address_snippet = parts[1]
                                else:
                                    category = details_text
                            except:
                                pass
                                
                        except Exception as e:
                            print(f"Error extracting card details: {e}")
                        
                        rows.append(
                            {
                                "name": self.clean_text(name),
                                "rating": rating,
                                "reviews": reviews,
                                "phone": "",
                                "industry": self.clean_text(category),
                                "full_address": self.clean_text(address_snippet),
                                "website": "",
                                "google_maps_link": link,
                                "status": "ok_card",
                            }
                        )
                        
                        # Auto-save every 10 rows
                        if len(rows) % 10 == 0:
                            temp_df = pd.DataFrame(rows)
                            self.save_checkpoint(temp_df)
                            self.task.message = f"Saved {len(rows)} results so far..."
                            print(f"Saved {len(rows)} results to checkpoint")
                        
                    except Exception as e:
                        print(f"Error processing card {i}: {e}")
                        rows.append(
                            {
                                "name": "",
                                "rating": None,
                                "reviews": None,
                                "phone": "",
                                "industry": "",
                                "full_address": "",
                                "website": "",
                                "google_maps_link": "",
                                "status": f"error: {str(e)[:100]}",
                            }
                        )
                
                browser.close()
            
            final_df = pd.DataFrame(rows)
            if not final_df.empty:
                final_df = final_df.drop_duplicates(subset=["google_maps_link"], keep="first")
            
            self.save_checkpoint(final_df)
            self.task.message = f"Card scraping complete: {len(final_df)} real results"
            print(f"Scraping completed. Total results: {len(final_df)}")
            
            return final_df
            
        except Exception as e:
            self.task.message = f"Error in card scraping: {str(e)[:200]}"
            print(f"Scraping error: {e}")
            raise e
    
    def scrape_deep(self, search_url, max_results=200, scroll_pause=1.0):
        """Deep scraping - cards + place details"""
        # Step 1: collect links fast
        self.task.message = "Step 1: Collecting place links..."
        cards_df = self.scrape_cards_only(
            search_url=search_url,
            max_results=max_results,
            scroll_pause=scroll_pause,
        )
        
        if cards_df.empty:
            self.task.message = "No links found to scrape details"
            return cards_df
        
        links = [x for x in cards_df["google_maps_link"].tolist() if x]
        links = list(dict.fromkeys(links))
        
        if not links:
            return cards_df
        
        rows = []
        self.task.message = f"Step 2: Scraping details from {len(links)} places..."
        print(f"Starting deep scrape of {len(links)} places")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(locale="en-US")
            page = context.new_page()
            page.set_default_timeout(120000)
            
            total = len(links)
            for idx, link in enumerate(links, start=1):
                if self.task._stop_flag:
                    self.task.message = "Stopping deep scraping..."
                    break
                
                # Update progress (50% to 100% for details)
                self.task.progress = 0.5 + (idx / total * 0.5)
                self.task.message = f"Scraping details: {idx} / {total}"
                
                # Scrape individual place
                item = self._scrape_place_details(page, link)
                rows.append(item)
                
                # Auto-save every 5 rows
                if len(rows) % 5 == 0:
                    temp_df = pd.DataFrame(rows)
                    self.save_checkpoint(temp_df)
                    self.task.message = f"Saved {len(rows)} detailed results..."
                
                time.sleep(1)  # Be respectful with requests
            
            browser.close()
        
        # Combine results
        if rows:
            detailed_df = pd.DataFrame(rows)
            final_df = pd.concat([cards_df, detailed_df]).drop_duplicates(subset=["google_maps_link"], keep="last")
        else:
            final_df = cards_df
        
        self.save_checkpoint(final_df)
        self.task.message = f"Deep scraping complete: {len(final_df)} results"
        
        return final_df
    
    def _scrape_place_details(self, page, place_url, retries=2):
        """Scrape individual place details"""
        place_url = self.normalize_maps_url(place_url)
        
        for attempt in range(retries + 1):
            try:
                page.goto(place_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)  # Wait for page to load
                
                name = ""
                rating = None
                reviews = None
                phone = ""
                industry = ""
                full_address = ""
                website = ""
                
                # Try to get name
                try:
                    name_elem = page.locator("h1.DUwDvf").first
                    name = name_elem.inner_text(timeout=5000)
                except:
                    try:
                        name_elem = page.locator("h1.fontHeadlineLarge").first
                        name = name_elem.inner_text(timeout=5000)
                    except:
                        pass
                
                # Try to get rating
                try:
                    rating_elem = page.locator("div.F7nice span.ceNzKf").first
                    rating_text = rating_elem.inner_text(timeout=3000)
                    rating = self.safe_float(rating_text)
                except:
                    pass
                
                # Try to get reviews
                try:
                    reviews_elem = page.locator("div.F7nice span:nth-child(2)").first
                    reviews_text = reviews_elem.inner_text(timeout=3000)
                    reviews = self.safe_int(reviews_text)
                except:
                    pass
                
                # Try to get industry/category
                try:
                    industry_elem = page.locator("button.DkEaL").first
                    industry = industry_elem.inner_text(timeout=3000)
                except:
                    pass
                
                # Try to get address
                try:
                    address_elem = page.locator('button[data-item-id="address"]').first
                    full_address = address_elem.inner_text(timeout=3000)
                except:
                    pass
                
                # Try to get phone
                try:
                    phone_elem = page.locator('button[data-item-id^="phone"]').first
                    phone = phone_elem.inner_text(timeout=3000)
                except:
                    pass
                
                # Try to get website
                try:
                    website_elem = page.locator('a[data-item-id="authority"]').first
                    website = website_elem.get_attribute("href") or ""
                except:
                    pass
                
                return {
                    "name": self.clean_text(name),
                    "rating": rating,
                    "reviews": reviews,
                    "phone": self.clean_text(phone),
                    "industry": self.clean_text(industry),
                    "full_address": self.clean_text(full_address),
                    "website": self.clean_text(website),
                    "google_maps_link": place_url,
                    "status": "ok_deep",
                }
                
            except PlaywrightTimeoutError:
                if attempt < retries:
                    time.sleep(2)
                    continue
                return {
                    "name": "",
                    "rating": None,
                    "reviews": None,
                    "phone": "",
                    "industry": "",
                    "full_address": "",
                    "website": "",
                    "google_maps_link": place_url,
                    "status": "timeout",
                }
            except Exception as e:
                if attempt < retries:
                    time.sleep(2)
                    continue
                return {
                    "name": "",
                    "rating": None,
                    "reviews": None,
                    "phone": "",
                    "industry": "",
                    "full_address": "",
                    "website": "",
                    "google_maps_link": place_url,
                    "status": f"error: {str(e)[:100]}",
                }

class ScrapingTask:
    def __init__(self, task_id, config, base_dir, temp_dir, checkpoint_file):
        self.task_id = task_id
        self.config = config
        self.base_dir = base_dir
        self.temp_dir = temp_dir
        self.checkpoint_file = checkpoint_file
        self.status = "pending"
        self.progress = 0.0
        self.message = "Initializing..."
        self.results_file = None
        self.start_time = None
        self.total_results = 0
        self.scraper = None  # Will be initialized in run()
        self._stop_flag = False
        
        # Create task-specific results file path
        self.results_file = os.path.join(self.temp_dir, f"results_{task_id}.csv")
        
    def run(self):
        """Main task execution method - NOW USING REAL SCRAPING"""
        try:
            self.status = "running"
            self.start_time = datetime.now()
            
            # Initialize REAL scraper (not simulation)
            self.scraper = RealGoogleMapsScraper(self, self.base_dir, self.temp_dir, self.checkpoint_file)
            
            if self.config['task_type'] == 'poi':
                results_df = self._run_poi_scraping_real()
            elif self.config['task_type'] == 'search':
                results_df = self._run_search_scraping_real()
            else:
                raise ValueError(f"Unknown task type: {self.config['task_type']}")
            
            self.total_results = len(results_df) if not results_df.empty else 0
            
            if not self._stop_flag:
                # Save results to file
                if not results_df.empty:
                    results_df.to_csv(self.results_file, index=False)
                    self.status = "completed"
                    self.progress = 1.0
                    self.message = f"Task completed with {self.total_results} REAL results"
                    print(f"Task {self.task_id} completed successfully with {self.total_results} results")
                else:
                    self.status = "completed"
                    self.progress = 1.0
                    self.message = "Task completed but no results found"
                    print(f"Task {self.task_id} completed with no results")
            else:
                self.status = "stopped"
                self.message = "Task stopped by user"
                    
        except Exception as e:
            self.status = "failed"
            self.message = f"Error: {str(e)}"
            print(f"Task {self.task_id} failed: {e}")
            
            # Create empty results file to prevent download errors
            empty_df = pd.DataFrame(columns=['name', 'rating', 'reviews', 'phone', 'industry', 
                                             'full_address', 'website', 'google_maps_link', 'status'])
            empty_df.to_csv(self.results_file, index=False)
    
    def _run_poi_scraping_real(self):
        """Run POI radius scraping with REAL Playwright"""
        self.message = "Starting REAL POI scraping..."
        print("Starting REAL POI scraping")
        
        # Get parameters from config
        poi_auto = self.config.get('auto_poi', True)
        manual_poi = self.config.get('custom_poi', '')
        lat = self.config.get('latitude', 12.971600)
        lon = self.config.get('longitude', 77.594600)
        max_results = self.config.get('max_results', 200)
        scroll_delay = self.config.get('scroll_delay', 1.0)
        mode = self.config.get('mode', 'fast')  # 'fast' or 'deep'
        
        # Determine POI list
        if poi_auto:
            poi_list = ["coaching centre", "tuition centre", "training institute", "academy", "institute"]
        else:
            poi_list = [x.strip() for x in manual_poi.split(",") if x.strip()]
        
        total_poi = len(poi_list)
        all_dfs = []
        
        for idx, poi in enumerate(poi_list, start=1):
            if self._stop_flag:
                break
            
            self.message = f"Scraping POI: {poi} ({idx}/{total_poi})"
            self.progress = (idx - 1) / total_poi * 0.5
            
            query = f"{poi} near {lat},{lon}"
            search_url = f"https://www.google.com/maps/search/{urllib.parse.quote_plus(query)}"
            
            print(f"Searching for: {query}")
            print(f"Search URL: {search_url}")
            
            if mode == 'fast':
                df = self.scraper.scrape_cards_only(
                    search_url=search_url,
                    max_results=max_results,
                    scroll_pause=scroll_delay,
                )
            else:
                df = self.scraper.scrape_deep(
                    search_url=search_url,
                    max_results=max_results,
                    scroll_pause=scroll_delay,
                )
            
            if not df.empty:
                df["poi_keyword"] = poi
                all_dfs.append(df)
            
            # Save intermediate checkpoint
            if all_dfs:
                temp_df = pd.concat(all_dfs, ignore_index=True)
                self.scraper.save_checkpoint(temp_df)
        
        # Combine all results
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=["google_maps_link"], keep="first")
            return final_df
        else:
            return pd.DataFrame()
    
    def _run_search_scraping_real(self):
        """Run search query scraping with REAL Playwright"""
        self.message = "Starting REAL search scraping..."
        print("Starting REAL search scraping")
        
        # Get parameters from config
        search_url = self.config.get('search_url', '')
        max_results = self.config.get('max_results', 200)
        scroll_delay = self.config.get('scroll_delay', 1.0)
        mode = self.config.get('mode', 'fast')  # 'fast' or 'deep'
        
        print(f"Search URL: {search_url}")
        print(f"Max results: {max_results}")
        print(f"Mode: {mode}")
        
        if mode == 'fast':
            return self.scraper.scrape_cards_only(
                search_url=search_url,
                max_results=max_results,
                scroll_pause=scroll_delay,
            )
        else:
            return self.scraper.scrape_deep(
                search_url=search_url,
                max_results=max_results,
                scroll_pause=scroll_delay,
            )
    
    def stop(self):
        """Stop the task"""
        self._stop_flag = True
        print(f"Stop requested for task {self.task_id}")
    
    def get_status(self):
        """Get current task status"""
        results_file_exists = os.path.exists(self.results_file) if self.results_file else False
        
        return {
            'task_id': self.task_id,
            'status': self.status,
            'progress': self.progress,
            'message': self.message,
            'started_at': self.start_time.isoformat() if self.start_time else None,
            'total_results': self.total_results,
            'results_file': self.results_file if results_file_exists else None,
            'results_file_exists': results_file_exists
        }
    
    def get_results_file(self):
        """Get path to results file"""
        if self.results_file and os.path.exists(self.results_file):
            return self.results_file
        return None

class TaskManager:
    """Manages background scraping tasks"""
    
    def __init__(self, base_dir, temp_dir, checkpoint_file):
        self.base_dir = base_dir
        self.temp_dir = temp_dir
        self.checkpoint_file = checkpoint_file
        self.tasks: Dict[str, ScrapingTask] = {}
        self.lock = threading.Lock()
    
    def add_task(self, task_id: str, task: ScrapingTask):
        """Add a new task"""
        with self.lock:
            self.tasks[task_id] = task
            print(f"Task added: {task_id}, total tasks: {len(self.tasks)}")
    
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
                print(f"Task {task_id} stopped")
    
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
                print(f"Cleaned up old task: {task_id}")

def create_scraping_task(task_id, config, base_dir, temp_dir, checkpoint_file):
    """Factory function to create scraping tasks"""
    return ScrapingTask(task_id, config, base_dir, temp_dir, checkpoint_file)
