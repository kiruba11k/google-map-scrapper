import os
import time
import re
import math
import urllib.parse
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

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
    
    def normalize_maps_url(self, url):
        """Normalize Google Maps URL"""
        if not url:
            return ""
        return url.split("&")[0]

class RealGoogleMapsScraper(OptimizedScraper):
    def __init__(self, task):
        super().__init__()
        self.task = task
        self.task_id = task.task_id
        
    def scrape_cards_only(self, search_url, max_results=200, scroll_pause=1.0):
        """Actual card scraping from your original code"""
        rows = []
        seen_links = set()
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )
                context = browser.new_context(locale="en-US")
                page = context.new_page()
                page.set_default_timeout(90000)
                
                self.task.message = "Opening Google Maps search page..."
                page.goto(search_url, wait_until="domcontentloaded")
                
                try:
                    page.wait_for_selector('div[role="feed"]', timeout=60000)
                except:
                    self.task.message = "Results feed not detected, trying anyway..."
                
                feed = page.locator('div[role="feed"]').first
                
                last_count = 0
                stable_rounds = 0
                stable_limit = 6
                
                self.task.message = "Scrolling until no new results appear..."
                
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
                    self.task.progress = min(0.4, count / max_results * 0.4)  # 40% for scrolling
                    
                    if count >= max_results:
                        break
                    
                    if count == last_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                    
                    if stable_rounds >= stable_limit:
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
                
                for i in range(total):
                    if self.task._stop_flag:
                        self.task.message = "Stopping extraction..."
                        break
                        
                    # Update progress (40% to 90% for extraction)
                    self.task.progress = 0.4 + (i / total * 0.5)
                    
                    try:
                        card = cards.nth(i)
                        link = self.normalize_maps_url(card.get_attribute("href") or "")
                        if not link or link in seen_links:
                            continue
                        seen_links.add(link)
                        
                        container = card.locator("xpath=ancestor::div[contains(@class,'Nv2PK')]").first
                        
                        name = ""
                        rating = None
                        reviews = None
                        category = ""
                        address_snippet = ""
                        
                        try:
                            name = container.locator("div.qBF1Pd").first.inner_text(timeout=2000)
                        except:
                            name = card.get_attribute("aria-label") or ""
                        
                        try:
                            rating_txt = container.locator("span.MW4etd").first.inner_text(timeout=2000)
                            rating = self.safe_float(rating_txt)
                        except:
                            rating = None
                        
                        try:
                            rev_txt = container.locator("span.UY7F9").first.inner_text(timeout=2000)
                            reviews = self.safe_int(rev_txt)
                        except:
                            reviews = None
                        
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
                        
                        # Auto-save every 20 rows
                        if len(rows) % 20 == 0:
                            temp_df = pd.DataFrame(rows).drop_duplicates(subset=["google_maps_link"], keep="first")
                            self.save_checkpoint(temp_df)
                            self.task.message = f"Saved {len(rows)} results so far..."
                        
                    except Exception as e:
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
                                "status": f"error_card: {str(e)[:120]}",
                            }
                        )
                
                browser.close()
            
            final_df = pd.DataFrame(rows).drop_duplicates(subset=["google_maps_link"], keep="first")
            self.save_checkpoint(final_df)
            self.task.message = f"Card scraping complete: {len(final_df)} results"
            
            return final_df
            
        except Exception as e:
            self.task.message = f"Error in card scraping: {str(e)[:200]}"
            raise e
    
    def scrape_place_details(self, page, place_url, retries=2):
        """Scrape individual place details"""
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
                
                try:
                    name = page.locator("h1.DUwDvf").first.inner_text(timeout=8000)
                except:
                    name = ""
                
                try:
                    rating_txt = page.locator("div.F7nice span.ceNzKf").first.inner_text(timeout=4000)
                    rating = self.safe_float(rating_txt)
                except:
                    rating = None
                
                try:
                    rev_txt = page.locator("div.F7nice span:nth-child(2)").first.inner_text(timeout=4000)
                    reviews = self.safe_int(rev_txt)
                except:
                    reviews = None
                
                try:
                    industry = page.locator("button.DkEaL").first.inner_text(timeout=4000)
                except:
                    industry = ""
                
                try:
                    full_address = page.locator('button[data-item-id="address"]').first.inner_text(timeout=4000)
                except:
                    full_address = ""
                
                try:
                    phone = page.locator('button[data-item-id^="phone"]').first.inner_text(timeout=4000)
                except:
                    phone = ""
                
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
    
    def scrape_deep(self, search_url, max_results=200, scroll_pause=1.0):
        """Deep scraping - cards + place details"""
        # Step 1: collect links fast (50% progress)
        self.task.message = "Step 1: Collecting place links..."
        cards_df = self.scrape_cards_only(
            search_url=search_url,
            max_results=max_results,
            scroll_pause=scroll_pause,
        )
        
        links = [x for x in cards_df["google_maps_link"].tolist() if x]
        links = list(dict.fromkeys(links))
        
        if not links:
            self.task.message = "No links found to scrape details"
            return cards_df
        
        rows = []
        self.task.message = f"Step 2: Scraping details from {len(links)} places..."
        
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
                
                item = self.scrape_place_details(page, link, retries=2)
                rows.append(item)
                
                # Auto-save every 10 rows
                if len(rows) % 10 == 0:
                    temp_df = pd.DataFrame(rows).drop_duplicates(subset=["google_maps_link"], keep="first")
                    self.save_checkpoint(temp_df)
                    self.task.message = f"Saved {len(rows)} detailed results..."
                
                time.sleep(0.6)
            
            browser.close()
        
        # Combine card data with detailed data
        detailed_df = pd.DataFrame(rows).drop_duplicates(subset=["google_maps_link"], keep="first")
        
        # Update card data with detailed info where available
        final_df = pd.concat([cards_df, detailed_df]).drop_duplicates(subset=["google_maps_link"], keep="last")
        
        self.save_checkpoint(final_df)
        self.task.message = f"Deep scraping complete: {len(final_df)} results"
        
        return final_df

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
        self.scraper = None
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
            
            # Initialize scraper
            self.scraper = RealGoogleMapsScraper(self)
            
            if self.config['task_type'] == 'poi':
                results_df = self._run_poi_scraping()
            elif self.config['task_type'] == 'search':
                results_df = self._run_search_scraping()
            else:
                raise ValueError(f"Unknown task type: {self.config['task_type']}")
            
            self.total_results = len(results_df) if not results_df.empty else 0
            
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
    
    def _run_poi_scraping(self):
        """Run POI radius scraping with real Playwright"""
        poi_auto = self.config.get('auto_poi', True)
        manual_poi = self.config.get('custom_poi', '')
        lat = self.config.get('latitude', 12.971600)
        lon = self.config.get('longitude', 77.594600)
        max_results = self.config.get('max_results', 200)
        scroll_delay = self.config.get('scroll_delay', 1.0)
        mode = self.config.get('mode', 'fast')  # 'fast' or 'deep'
        
        if poi_auto:
            poi_list = ["coaching centre", "tuition centre", "training institute", "academy", "institute"]
        else:
            poi_list = [x.strip() for x in manual_poi.split(",") if x.strip()]
        
        all_dfs = []
        total_poi = len(poi_list)
        
        for idx, poi in enumerate(poi_list, start=1):
            if self._stop_flag:
                break
                
            self.message = f"Scraping POI: {poi} ({idx}/{total_poi})"
            self.progress = (idx - 1) / total_poi * 0.5  # First half for POI scraping
            
            query = f"{poi} near {lat},{lon}"
            search_url = self.scraper.build_search_url(query)
            
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
            
            df["poi_keyword"] = poi
            all_dfs.append(df)
        
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=["google_maps_link"], keep="first")
            return final_df
        else:
            return pd.DataFrame()
    
    def _run_search_scraping(self):
        """Run search query scraping with real Playwright"""
        search_url = self.config.get('search_url', '')
        max_results = self.config.get('max_results', 200)
        scroll_delay = self.config.get('scroll_delay', 1.0)
        mode = self.config.get('mode', 'fast')  # 'fast' or 'deep'
        
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
        self.message = "Stop requested..."
    
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
