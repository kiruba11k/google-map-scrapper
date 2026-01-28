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
                print(f"✓ Checkpoint saved: {len(df)} rows")
        except Exception as e:
            print(f"Checkpoint save error: {e}")
    
    def build_search_url(self, query):
        """Build Google Maps search URL"""
        encoded = urllib.parse.quote_plus(query.strip())
        return f"https://www.google.com/maps/search/{encoded}"
    
    def scrape_cards_only(self, search_url, max_results=200, scroll_pause=1.0):
        """REAL scraping - opens browser and scrapes actual Google Maps"""
        print(f"\n STARTING REAL SCRAPING")
        print(f" Search URL: {search_url}")
        print(f" Max results: {max_results}")
        
        rows = []
        seen_links = set()
        
        try:
            self.task.message = " Launching browser..."
            print(" Launching Playwright browser...")
            
            # IMPORTANT: Increase timeout for Render's free tier
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-software-rasterizer",
                        "--disable-setuid-sandbox",
                        "--no-first-run",
                        "--no-zygote",
                        "--single-process",
                        "--disable-blink-features=AutomationControlled"
                    ],
                    timeout=180000  # 3 minutes timeout
                )
                
                context = browser.new_context(
                    locale="en-US",
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080}
                )
                
                page = context.new_page()
                page.set_default_timeout(120000)  # 2 minutes
                
                self.task.message = " Opening Google Maps..."
                print(f" Navigating to: {search_url}")
                
                try:
                    page.goto(search_url, wait_until="networkidle", timeout=120000)
                    time.sleep(3)  # Wait for page to load
                    
                    # Accept cookies if present
                    try:
                        accept_button = page.locator('button:has-text("Accept all")').first
                        if accept_button.is_visible(timeout=5000):
                            accept_button.click()
                            time.sleep(1)
                            print("✓ Accepted cookies")
                    except:
                        pass
                    
                except Exception as e:
                    print(f" Navigation warning: {e}")
                    page.goto(search_url, wait_until="load", timeout=120000)
                    time.sleep(3)
                
                # Wait for results
                self.task.message = " Waiting for results..."
                print(" Waiting for results feed...")
                
                try:
                    page.wait_for_selector('div[role="feed"]', timeout=30000)
                    print("✓ Results feed found")
                except Exception as e:
                    print(f" Results feed not found: {e}")
                    # Try alternative selectors
                    try:
                        page.wait_for_selector('div.m6QErb[aria-label]', timeout=10000)
                        print("✓ Alternative results container found")
                    except:
                        print(" No results container found, proceeding anyway")
                
                # Scroll to load more results
                self.task.message = " Scrolling to load results..."
                print(" Starting to scroll...")
                
                last_height = 0
                scroll_attempts = 0
                max_scroll_attempts = 30
                loaded_count = 0
                
                while scroll_attempts < max_scroll_attempts and len(rows) < max_results:
                    if self.task._stop_flag:
                        break
                    
                    # Count current cards
                    cards = page.locator('a[href*="/maps/place/"]')
                    current_count = cards.count()
                    
                    if current_count > loaded_count:
                        loaded_count = current_count
                        scroll_attempts = 0
                        self.task.message = f" Loaded: {current_count} places"
                        print(f" Loaded: {current_count} places")
                    else:
                        scroll_attempts += 1
                    
                    # Update progress
                    self.task.progress = min(0.7, len(rows) / max_results * 0.7)
                    
                    # Scroll down
                    try:
                        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                        time.sleep(scroll_pause)
                        
                        # Check if we reached bottom
                        new_height = page.evaluate('document.body.scrollHeight')
                        if new_height == last_height:
                            scroll_attempts += 1
                        last_height = new_height
                        
                    except Exception as e:
                        print(f" Scroll error: {e}")
                        break
                
                # Extract data from loaded cards
                self.task.message = f" Extracting data from {loaded_count} places..."
                print(f" Extracting data from {loaded_count} places...")
                
                for i in range(min(loaded_count, max_results)):
                    if self.task._stop_flag:
                        break
                    
                    try:
                        # Get card
                        card = cards.nth(i)
                        
                        # Get link
                        link = card.get_attribute('href')
                        if not link or link in seen_links:
                            continue
                        
                        full_link = f"https://www.google.com{link}" if link.startswith("/") else link
                        normalized_link = self.normalize_maps_url(full_link)
                        seen_links.add(normalized_link)
                        
                        # Get name from aria-label
                        name = ""
                        try:
                            name = card.get_attribute('aria-label') or ""
                            # Remove extra text like " · "
                            if " · " in name:
                                name = name.split(" · ")[0]
                        except:
                            pass
                        
                        # Try to get rating and reviews
                        rating = None
                        reviews = None
                        category = ""
                        address = ""
                        
                        # Get parent container for more details
                        try:
                            parent = card.locator('xpath=..').locator('xpath=..').locator('xpath=..')
                            
                            # Try to find rating
                            try:
                                rating_elem = parent.locator('span.MW4etd').first
                                if rating_elem.count() > 0:
                                    rating_text = rating_elem.text_content(timeout=1000)
                                    rating = self.safe_float(rating_text)
                            except:
                                pass
                            
                            # Try to find reviews
                            try:
                                reviews_elem = parent.locator('span.UY7F9').first
                                if reviews_elem.count() > 0:
                                    reviews_text = reviews_elem.text_content(timeout=1000)
                                    reviews = self.safe_int(reviews_text)
                            except:
                                pass
                            
                            # Try to get category and address
                            try:
                                details = parent.locator('div.W4Efsd').first
                                if details.count() > 0:
                                    details_text = details.text_content(timeout=1000)
                                    details_text = self.clean_text(details_text)
                                    
                                    if " · " in details_text:
                                        parts = details_text.split(" · ")
                                        if len(parts) >= 2:
                                            category = parts[0]
                                            address = parts[1]
                                    else:
                                        category = details_text
                            except:
                                pass
                                
                        except Exception as e:
                            print(f" Error getting details: {e}")
                        
                        # Add to results
                        rows.append({
                            "name": self.clean_text(name),
                            "rating": rating,
                            "reviews": reviews,
                            "phone": "",
                            "industry": self.clean_text(category),
                            "full_address": self.clean_text(address),
                            "website": "",
                            "google_maps_link": normalized_link,
                            "status": "ok_card"
                        })
                        
                        # Update progress
                        self.task.progress = 0.7 + (i / min(loaded_count, max_results) * 0.3)
                        self.task.message = f" Extracted: {len(rows)}/{min(loaded_count, max_results)}"
                        
                        # Save checkpoint every 10 rows
                        if len(rows) % 10 == 0 and rows:
                            temp_df = pd.DataFrame(rows)
                            self.save_checkpoint(temp_df)
                            print(f" Checkpoint: {len(rows)} rows saved")
                        
                    except Exception as e:
                        print(f" Error processing item {i}: {e}")
                        rows.append({
                            "name": "",
                            "rating": None,
                            "reviews": None,
                            "phone": "",
                            "industry": "",
                            "full_address": "",
                            "website": "",
                            "google_maps_link": "",
                            "status": f"error: {str(e)[:50]}"
                        })
                
                browser.close()
                print(f" Browser closed. Total results: {len(rows)}")
            
            # Create final DataFrame
            final_df = pd.DataFrame(rows)
            if not final_df.empty:
                final_df = final_df.drop_duplicates(subset=['google_maps_link'], keep='first')
            
            self.save_checkpoint(final_df)
            self.task.message = f" Scraping complete: {len(final_df)} real results"
            print(f" FINAL: {len(final_df)} unique results")
            
            return final_df
            
        except Exception as e:
            self.task.message = f" Scraping failed: {str(e)[:100]}"
            print(f" SCRAPING ERROR: {e}")
            # Return empty dataframe on error
            return pd.DataFrame()
    
    def scrape_deep(self, search_url, max_results=200, scroll_pause=1.0):
        """Deep scrape - get basic info then visit each place"""
        print(f"\n STARTING DEEP SCRAPE")
        
        # First get cards
        self.task.message = " Step 1: Getting place list..."
        cards_df = self.scrape_cards_only(search_url, max_results, scroll_pause)
        
        if cards_df.empty:
            return cards_df
        
        # Get unique links
        links = [link for link in cards_df['google_maps_link'].tolist() if link]
        links = list(dict.fromkeys(links))[:max_results]  # Remove duplicates and limit
        
        if not links:
            return cards_df
        
        print(f" Found {len(links)} unique places for deep scraping")
        
        # Scrape each place
        detailed_rows = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
                timeout=180000
            )
            
            context = browser.new_context(
                locale="en-US",
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            
            page = context.new_page()
            page.set_default_timeout(90000)
            
            for idx, link in enumerate(links, 1):
                if self.task._stop_flag:
                    break
                
                self.task.message = f" Deep scraping {idx}/{len(links)}"
                self.task.progress = idx / len(links)
                
                try:
                    page.goto(link, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(2)  # Wait for page
                    
                    # Extract detailed info
                    details = self._extract_place_details(page)
                    details['google_maps_link'] = link
                    details['status'] = 'ok_deep'
                    
                    detailed_rows.append(details)
                    
                    # Save checkpoint every 5 places
                    if idx % 5 == 0 and detailed_rows:
                        temp_df = pd.DataFrame(detailed_rows)
                        self.save_checkpoint(temp_df)
                        print(f" Deep checkpoint: {len(detailed_rows)} places")
                    
                except Exception as e:
                    print(f" Error deep scraping {link}: {e}")
                    detailed_rows.append({
                        "name": "", "rating": None, "reviews": None, "phone": "",
                        "industry": "", "full_address": "", "website": "",
                        "google_maps_link": link, "status": f"error_deep: {str(e)[:50]}"
                    })
                
                # Be respectful with delay
                time.sleep(1)
            
            browser.close()
        
        # Merge card data with detailed data
        if detailed_rows:
            detailed_df = pd.DataFrame(detailed_rows)
            # Update cards_df with detailed info
            for idx, row in detailed_df.iterrows():
                if row['google_maps_link']:
                    mask = cards_df['google_maps_link'] == row['google_maps_link']
                    if mask.any():
                        for col in ['name', 'rating', 'reviews', 'phone', 'industry', 'full_address', 'website']:
                            if pd.notna(row[col]) and row[col] != "":
                                cards_df.loc[mask, col] = row[col]
                        cards_df.loc[mask, 'status'] = row['status']
        
        self.save_checkpoint(cards_df)
        return cards_df
    
    def _extract_place_details(self, page):
        """Extract details from a place page"""
        details = {
            "name": "", "rating": None, "reviews": None, "phone": "",
            "industry": "", "full_address": "", "website": ""
        }
        
        try:
            # Name
            try:
                name_elem = page.locator('h1.DUwDvf, h1.fontHeadlineLarge').first
                details["name"] = name_elem.text_content(timeout=5000)
            except:
                pass
            
            # Rating
            try:
                rating_elem = page.locator('div.F7nice span.ceNzKf').first
                rating_text = rating_elem.text_content(timeout=3000)
                details["rating"] = self.safe_float(rating_text)
            except:
                pass
            
            # Reviews
            try:
                reviews_elem = page.locator('div.F7nice span:nth-child(2)').first
                reviews_text = reviews_elem.text_content(timeout=3000)
                details["reviews"] = self.safe_int(reviews_text)
            except:
                pass
            
            # Industry/Category
            try:
                category_elem = page.locator('button.DkEaL').first
                details["industry"] = category_elem.text_content(timeout=3000)
            except:
                pass
            
            # Address
            try:
                address_elem = page.locator('button[data-item-id="address"]').first
                details["full_address"] = address_elem.text_content(timeout=3000)
            except:
                pass
            
            # Phone
            try:
                phone_elem = page.locator('button[data-item-id^="phone"]').first
                details["phone"] = phone_elem.text_content(timeout=3000)
            except:
                pass
            
            # Website
            try:
                website_elem = page.locator('a[data-item-id="authority"]').first
                details["website"] = website_elem.get_attribute('href') or ""
            except:
                pass
                
        except Exception as e:
            print(f" Error extracting details: {e}")
        
        # Clean all text fields
        for key in ["name", "phone", "industry", "full_address", "website"]:
            details[key] = self.clean_text(details[key])
        
        return details

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
            self.message = " Starting real Google Maps scraping..."
            
            print(f"\n" + "="*60)
            print(f" TASK STARTED: {self.task_id}")
            print(f" Config: {self.config}")
            print("="*60)
            
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
                    self.message = f" Task completed! Found {self.total_results} REAL places"
                    print(f"TASK COMPLETED: {self.total_results} results saved to {self.results_file}")
                else:
                    self.status = "completed"
                    self.progress = 1.0
                    self.message = " Task completed but no results found"
                    print(" TASK COMPLETED: No results found")
            else:
                self.status = "stopped"
                self.message = "⏹️ Task stopped by user"
                print("⏹️ TASK STOPPED BY USER")
                    
        except Exception as e:
            self.status = "failed"
            self.message = f"Error: {str(e)[:100]}"
            print(f" TASK FAILED: {e}")
            
            # Create empty results file
            empty_df = pd.DataFrame(columns=[
                'name', 'rating', 'reviews', 'phone', 'industry', 
                'full_address', 'website', 'google_maps_link', 'status'
            ])
            empty_df.to_csv(self.results_file, index=False)
    
    def _run_poi_scraping_real(self):
        """Run POI radius scraping - REAL ONLY"""
        print(f"\n📍 STARTING POI SCRAPING (REAL)")
        
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
        
        print(f"📍 POIs: {poi_list}")
        print(f"📍 Location: {lat}, {lon}")
        print(f"📍 Mode: {mode}")
        
        all_dfs = []
        
        for idx, poi in enumerate(poi_list, 1):
            if self._stop_flag:
                break
            
            self.message = f" Searching: {poi} ({idx}/{len(poi_list)})"
            self.progress = (idx - 1) / len(poi_list) * 0.3
            
            query = f"{poi} near {lat},{lon}"
            search_url = self.scraper.build_search_url(query)
            
            print(f"\n POI {idx}/{len(poi_list)}: {poi}")
            print(f" URL: {search_url}")
            
            try:
                if mode == 'fast':
                    df = self.scraper.scrape_cards_only(search_url, max_results, scroll_delay)
                else:
                    df = self.scraper.scrape_deep(search_url, max_results, scroll_delay)
                
                if not df.empty:
                    df["poi_keyword"] = poi
                    all_dfs.append(df)
                    print(f" Found {len(df)} results for '{poi}'")
                else:
                    print(f" No results for '{poi}'")
                    
            except Exception as e:
                print(f" Error scraping '{poi}': {e}")
            
            # Update progress between POIs
            self.progress = idx / len(poi_list) * 0.3
        
        # Combine all results
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=["google_maps_link"], keep="first")
            print(f"\n POI scraping complete: {len(final_df)} unique results")
            return final_df
        else:
            print("\n POI scraping: No results found")
            return pd.DataFrame()
    
    def _run_search_scraping_real(self):
        """Run search query scraping - REAL ONLY"""
        print(f"\n STARTING SEARCH SCRAPING (REAL)")
        
        # Get parameters
        search_url = self.config.get('search_url', 'https://www.google.com/maps/search/coaching+centres+in+bangalore')
        max_results = self.config.get('max_results', 50)
        scroll_delay = self.config.get('scroll_delay', 1.0)
        mode = self.config.get('mode', 'fast')
        
        print(f" URL: {search_url}")
        print(f" Max results: {max_results}")
        print(f" Mode: {mode}")
        
        try:
            if mode == 'fast':
                return self.scraper.scrape_cards_only(search_url, max_results, scroll_delay)
            else:
                return self.scraper.scrape_deep(search_url, max_results, scroll_delay)
        except Exception as e:
            print(f" Search scraping error: {e}")
            return pd.DataFrame()
    
    def stop(self):
        """Stop the task"""
        self._stop_flag = True
        print(f"⏹️ Stop requested for task {self.task_id}")
    
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
            print(f"📥 Task added: {task_id}, total tasks: {len(self.tasks)}")
    
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
                print(f"⏹️ Task {task_id} stopped")
    
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
                print(f" Cleaned up old task: {task_id}")

def create_scraping_task(task_id, config, base_dir, temp_dir, checkpoint_file):
    """Factory function to create scraping tasks"""
    return ScrapingTask(task_id, config, base_dir, temp_dir, checkpoint_file)
