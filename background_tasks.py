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

# ============================================================================
# REAL GOOGLE MAPS SCRAPER - NO SIMULATION
# ============================================================================

class GoogleMapsScraper:
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
    
    def normalize_maps_url(self, url):
        if not url:
            return ""
        return url.split("&")[0]
    
    def save_checkpoint(self, df):
        """Save checkpoint with minimal I/O"""
        try:
            if not df.empty:
                # Append to existing checkpoint to minimize writes
                if os.path.exists(self.checkpoint_file):
                    existing = pd.read_csv(self.checkpoint_file)
                    df = pd.concat([existing, df], ignore_index=True)
                    df = df.drop_duplicates(subset=['google_maps_link'], keep='last')
                df.to_csv(self.checkpoint_file, index=False)
                print(f"Checkpoint saved: {len(df)} rows")
        except Exception as e:
            print(f"Checkpoint save error: {e}")
    
    def build_search_url(self, query):
        """Build Google Maps search URL"""
        encoded = urllib.parse.quote_plus(query.strip())
        return f"https://www.google.com/maps/search/{encoded}"
    
    def scrape_cards_only(self, search_url, max_results=200, scroll_pause=1.0):
        """REAL scraping using the exact same selectors from your working Streamlit code"""
        print(f"Starting real scraping for: {search_url}")
        
        rows = []
        seen_links = set()
        
        try:
            self.task.message = "Launching browser..."
            print("Launching Playwright browser...")
            
            with sync_playwright() as p:
                # Use the same browser launch arguments as your working code
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )
                
                # Use the same context settings
                context = browser.new_context(locale="en-US")
                page = context.new_page()
                page.set_default_timeout(90000)
                
                self.task.message = "Opening Google Maps search page..."
                print(f"Navigating to: {search_url}")
                
                # Use the same navigation approach
                page.goto(search_url, wait_until="domcontentloaded")
                
                # Wait for the feed exactly like your working code
                try:
                    page.wait_for_selector('div[role="feed"]', timeout=60000)
                    print("Results feed detected")
                except:
                    self.task.message = "Results feed not detected, trying anyway..."
                    print("Results feed not detected, trying anyway...")
                
                # Get the feed element exactly like your working code
                feed = page.locator('div[role="feed"]').first
                
                # Use the exact same scrolling logic
                last_count = 0
                stable_rounds = 0
                stable_limit = 6
                
                self.task.message = "Scrolling until no new results appear..."
                print("Starting to scroll...")
                
                # Scroll until we have enough results or no new results
                while True:
                    # Count cards using the exact same selector
                    cards = page.locator("a.hfpxzc")
                    count = cards.count()
                    
                    self.task.message = f"Loaded cards: {count}"
                    print(f"Loaded cards: {count}")
                    
                    # Update progress based on actual count
                    if max_results > 0:
                        self.task.progress = min(0.7, count / max_results * 0.7)
                    
                    if count >= max_results:
                        break
                    
                    if count == last_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                    
                    if stable_rounds >= stable_limit:
                        break
                    
                    last_count = count
                    
                    # Scroll using the same method
                    try:
                        feed.evaluate("(el) => el.scrollBy(0, el.scrollHeight)")
                    except:
                        page.mouse.wheel(0, 5000)
                    
                    time.sleep(scroll_pause)
                
                # Get cards again after scrolling
                cards = page.locator("a.hfpxzc")
                total = min(cards.count(), max_results)
                
                self.task.message = f"Extracting data from {total} cards..."
                print(f"Extracting data from {total} cards...")
                
                # Extract data using the exact same logic as your working code
                for i in range(total):
                    if self.task._stop_flag:
                        self.task.message = "Stopping extraction..."
                        break
                    
                    # Update progress (70% to 100% for extraction)
                    self.task.progress = 0.7 + (i / total * 0.3)
                    
                    try:
                        card = cards.nth(i)
                        link = card.get_attribute("href") or ""
                        link = self.normalize_maps_url(link)
                        
                        if not link or link in seen_links:
                            continue
                        seen_links.add(link)
                        
                        # Get the container using the exact same selector
                        container = card.locator("xpath=ancestor::div[contains(@class,'Nv2PK')]").first
                        
                        # Extract name
                        name = ""
                        try:
                            name = container.locator("div.qBF1Pd").first.inner_text(timeout=2000)
                        except:
                            name = card.get_attribute("aria-label") or ""
                        
                        # Extract rating
                        rating = None
                        try:
                            rating_txt = container.locator("span.MW4etd").first.inner_text(timeout=2000)
                            rating = self.safe_float(rating_txt)
                        except:
                            rating = None
                        
                        # Extract reviews
                        reviews = None
                        try:
                            rev_txt = container.locator("span.UY7F9").first.inner_text(timeout=2000)
                            reviews = self.safe_int(rev_txt)
                        except:
                            reviews = None
                        
                        # Extract category and address
                        category = ""
                        address_snippet = ""
                        try:
                            line = container.locator("div.W4Efsd").nth(1).inner_text(timeout=2000)
                            line = self.clean_text(line)
                            if "·" in line:
                                parts = [p.strip() for p in line.split("·") if p.strip()]
                                if len(parts) >= 1:
                                    category = parts[0]
                                if len(parts) >= 2:
                                    address_snippet = parts[1]
                            else:
                                category = line
                        except:
                            category = ""
                            address_snippet = ""
                        
                        # Add to results
                        rows.append({
                            "name": self.clean_text(name),
                            "rating": rating,
                            "reviews": reviews,
                            "phone": "",
                            "industry": self.clean_text(category),
                            "full_address": self.clean_text(address_snippet),
                            "website": "",
                            "google_maps_link": link,
                            "status": "ok_card"
                        })
                        
                        # Auto-save every 20 rows
                        if len(rows) % 20 == 0:
                            temp_df = pd.DataFrame(rows).drop_duplicates(subset=["google_maps_link"], keep="first")
                            self.save_checkpoint(temp_df)
                            self.task.message = f"Saved {len(rows)} results so far..."
                            print(f"Saved {len(rows)} results to checkpoint")
                        
                    except Exception as e:
                        print(f"Error processing card {i}: {e}")
                        rows.append({
                            "name": "",
                            "rating": None,
                            "reviews": None,
                            "phone": "",
                            "industry": "",
                            "full_address": "",
                            "website": "",
                            "google_maps_link": "",
                            "status": f"error: {str(e)[:100]}"
                        })
                
                browser.close()
                print(f"Browser closed. Total results: {len(rows)}")
            
            # Create final DataFrame
            final_df = pd.DataFrame(rows)
            if not final_df.empty:
                final_df = final_df.drop_duplicates(subset=['google_maps_link'], keep='first')
            
            self.save_checkpoint(final_df)
            self.task.message = f"Scraping complete: {len(final_df)} real results"
            print(f"Final: {len(final_df)} unique results")
            
            return final_df
            
        except Exception as e:
            self.task.message = f"Scraping failed: {str(e)[:100]}"
            print(f"Scraping error: {e}")
            # Return empty dataframe on error
            return pd.DataFrame()
    
    def scrape_deep(self, search_url, max_results=200, scroll_pause=1.0):
        """Deep scrape - get basic info then visit each place"""
        print(f"Starting deep scrape for: {search_url}")
        
        # First get cards
        self.task.message = "Step 1: Getting place list..."
        cards_df = self.scrape_cards_only(search_url, max_results, scroll_pause)
        
        if cards_df.empty:
            self.task.message = "No cards found for deep scraping"
            return cards_df
        
        # Get unique links
        links = [link for link in cards_df['google_maps_link'].tolist() if link]
        links = list(dict.fromkeys(links))[:max_results]  # Remove duplicates and limit
        
        if not links:
            self.task.message = "No valid links found for deep scraping"
            return cards_df
        
        print(f"Found {len(links)} unique places for deep scraping")
        
        # Scrape each place
        detailed_rows = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            
            context = browser.new_context(locale="en-US")
            page = context.new_page()
            page.set_default_timeout(120000)
            
            for idx, link in enumerate(links, 1):
                if self.task._stop_flag:
                    break
                
                self.task.message = f"Deep scraping {idx}/{len(links)}"
                self.task.progress = 0.5 + (idx / len(links) * 0.5)
                
                try:
                    page.goto(link, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(1.2)  # Same wait as your working code
                    
                    # Extract detailed info
                    details = self._extract_place_details(page, link)
                    detailed_rows.append(details)
                    
                    # Auto-save every 10 rows
                    if len(detailed_rows) % 10 == 0:
                        temp_df = pd.DataFrame(detailed_rows)
                        self.save_checkpoint(temp_df)
                        print(f"Deep checkpoint: {len(detailed_rows)} places")
                    
                except Exception as e:
                    print(f"Error deep scraping {link}: {e}")
                    detailed_rows.append({
                        "name": "", "rating": None, "reviews": None, "phone": "",
                        "industry": "", "full_address": "", "website": "",
                        "google_maps_link": link, "status": f"error_deep: {str(e)[:50]}"
                    })
                
                # Same delay as your working code
                time.sleep(0.6)
            
            browser.close()
        
        # Merge card data with detailed data
        if detailed_rows:
            detailed_df = pd.DataFrame(detailed_rows)
            # Update cards_df with detailed info where available
            final_df = pd.concat([cards_df, detailed_df]).drop_duplicates(subset=["google_maps_link"], keep="last")
        else:
            final_df = cards_df
        
        self.save_checkpoint(final_df)
        return final_df
    
    def _extract_place_details(self, page, place_url, retries=2):
        """Extract details from a place page - same as your working code"""
        place_url = self.normalize_maps_url(place_url)
        
        for attempt in range(retries + 1):
            try:
                page.goto(place_url, wait_until="domcontentloaded", timeout=120000)
                time.sleep(1.2)
                
                name = ""
                rating = None
                reviews = None
                phone = ""
                industry = ""
                full_address = ""
                website = ""
                
                # Name
                try:
                    name = page.locator("h1.DUwDvf").first.inner_text(timeout=8000)
                except:
                    name = ""
                
                # Rating
                try:
                    rating_txt = page.locator("div.F7nice span.ceNzKf").first.inner_text(timeout=4000)
                    rating = self.safe_float(rating_txt)
                except:
                    rating = None
                
                # Reviews
                try:
                    rev_txt = page.locator("div.F7nice span:nth-child(2)").first.inner_text(timeout=4000)
                    reviews = self.safe_int(rev_txt)
                except:
                    reviews = None
                
                # Industry
                try:
                    industry = page.locator("button.DkEaL").first.inner_text(timeout=4000)
                except:
                    industry = ""
                
                # Address
                try:
                    full_address = page.locator('button[data-item-id="address"]').first.inner_text(timeout=4000)
                except:
                    full_address = ""
                
                # Phone
                try:
                    phone = page.locator('button[data-item-id^="phone"]').first.inner_text(timeout=4000)
                except:
                    phone = ""
                
                # Website
                try:
                    website = page.locator('a[data-item-id="authority"]').first.get_attribute("href") or ""
                except:
                    website = ""
                
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
                    "status": "timeout_place",
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
                    "status": f"error: {str(e)[:50]}",
                }

# ============================================================================
# TASK MANAGEMENT
# ============================================================================

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
        self.scraper = None
        self._stop_flag = False
        
        # Create task-specific results file path
        self.results_file = os.path.join(self.temp_dir, f"results_{task_id}.csv")
        os.makedirs(self.temp_dir, exist_ok=True)
    
    def run(self):
        """Main task execution method - USES REAL SCRAPING ONLY"""
        try:
            self.status = "running"
            self.start_time = datetime.now()
            self.message = "Starting real Google Maps scraping..."
            
            print(f"TASK STARTED: {self.task_id}")
            print(f"Config: {self.config}")
            
            # Initialize REAL scraper
            self.scraper = GoogleMapsScraper(self, self.base_dir, self.temp_dir, self.checkpoint_file)
            
            # Run based on task type
            if self.config['task_type'] == 'poi':
                results_df = self._run_poi_scraping_real()
            elif self.config['task_type'] == 'search':
                results_df = self._run_search_scraping_real()
            else:
                raise ValueError(f"Unknown task type: {self.config['task_type']}")
            
            self.total_results = len(results_df) if not results_df.empty else 0
            
            if not self._stop_flag:
                if not results_df.empty:
                    # Save final results
                    results_df.to_csv(self.results_file, index=False)
                    self.status = "completed"
                    self.progress = 1.0
                    self.message = f"Task completed! Found {self.total_results} REAL places"
                    print(f"TASK COMPLETED: {self.total_results} results saved")
                else:
                    self.status = "completed"
                    self.progress = 1.0
                    self.message = "Task completed but no results found"
                    print("TASK COMPLETED: No results found")
            else:
                self.status = "stopped"
                self.message = "Task stopped by user"
                print("TASK STOPPED BY USER")
                    
        except Exception as e:
            self.status = "failed"
            self.message = f"Error: {str(e)[:100]}"
            print(f"TASK FAILED: {e}")
            
            # Create empty results file
            empty_df = pd.DataFrame(columns=[
                'name', 'rating', 'reviews', 'phone', 'industry', 
                'full_address', 'website', 'google_maps_link', 'status'
            ])
            empty_df.to_csv(self.results_file, index=False)
    
    def _run_poi_scraping_real(self):
        """Run POI radius scraping - REAL ONLY"""
        print("STARTING POI SCRAPING")
        
        # Get parameters
        poi_auto = self.config.get('auto_poi', True)
        manual_poi = self.config.get('custom_poi', 'coaching centre, tuition centre')
        lat = self.config.get('latitude', 12.971600)
        lon = self.config.get('longitude', 77.594600)
        max_results = self.config.get('max_results', 50)
        scroll_delay = self.config.get('scroll_delay', 1.0)
        mode = self.config.get('mode', 'fast')
        
        # Determine POI list
        if poi_auto:
            poi_list = ["coaching centre", "tuition centre", "training institute", "academy", "institute"]
        else:
            poi_list = [x.strip() for x in manual_poi.split(",") if x.strip()]
        
        print(f"POIs: {poi_list}")
        print(f"Location: {lat}, {lon}")
        print(f"Mode: {mode}")
        
        all_dfs = []
        
        for idx, poi in enumerate(poi_list, 1):
            if self._stop_flag:
                break
            
            self.message = f"Searching: {poi} ({idx}/{len(poi_list)})"
            self.progress = (idx - 1) / len(poi_list) * 0.5
            
            query = f"{poi} near {lat},{lon}"
            search_url = self.scraper.build_search_url(query)
            
            print(f"POI {idx}/{len(poi_list)}: {poi}")
            print(f"URL: {search_url}")
            
            try:
                if mode == 'fast':
                    df = self.scraper.scrape_cards_only(search_url, max_results, scroll_delay)
                else:
                    df = self.scraper.scrape_deep(search_url, max_results, scroll_delay)
                
                if not df.empty:
                    df["poi_keyword"] = poi
                    all_dfs.append(df)
                    print(f"Found {len(df)} results for '{poi}'")
                else:
                    print(f"No results for '{poi}'")
                    
            except Exception as e:
                print(f"Error scraping '{poi}': {e}")
                import traceback
                traceback.print_exc()
            
            # Update progress between POIs
            self.progress = idx / len(poi_list) * 0.5
        
        # Combine all results
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=["google_maps_link"], keep="first")
            print(f"POI scraping complete: {len(final_df)} unique results")
            return final_df
        else:
            print("POI scraping: No results found")
            return pd.DataFrame()
    
    def _run_search_scraping_real(self):
        """Run search query scraping - REAL ONLY"""
        print("STARTING SEARCH SCRAPING")
        
        # Get parameters
        search_url = self.config.get('search_url', 'https://www.google.com/maps/search/coaching+centres+in+bangalore')
        max_results = self.config.get('max_results', 50)
        scroll_delay = self.config.get('scroll_delay', 1.0)
        mode = self.config.get('mode', 'fast')
        
        print(f"URL: {search_url}")
        print(f"Max results: {max_results}")
        print(f"Mode: {mode}")
        
        try:
            if mode == 'fast':
                return self.scraper.scrape_cards_only(search_url, max_results, scroll_delay)
            else:
                return self.scraper.scrape_deep(search_url, max_results, scroll_delay)
        except Exception as e:
            print(f"Search scraping error: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
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
