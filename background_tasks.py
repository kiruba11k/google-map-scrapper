import os
import time
import re
import pandas as pd
from datetime import datetime
from typing import Dict, Optional
import threading
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import urllib.parse

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
            # Remove parentheses and non-numeric characters
            cleaned = re.sub(r'[^\d]', '', str(text))
            return int(cleaned) if cleaned else None
        except:
            return None
    
    def normalize_maps_url(self, url):
        if not url:
            return ""
        # Remove tracking parameters
        return url.split("?")[0] if "?" in url else url
    
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
    
    def extract_card_data(self, container, link):
        """Extract data from a single card using Chrome extension logic"""
        data = {
            "name": "",
            "rating": None,
            "reviews": None,
            "phone": "",
            "industry": "",
            "full_address": "",
            "website": "",
            "google_maps_link": link,
            "status": "ok"
        }
        
        try:
            # 1. Extract name (Title)
            title_elem = container.locator('.qBF1Pd, .fontHeadlineSmall').first
            if title_elem.count() > 0:
                data["name"] = self.clean_text(title_elem.text_content())
            
            # 2. Extract rating and reviews
            # Look for the rating span with class MW4etd
            rating_elem = container.locator('span.MW4etd').first
            if rating_elem.count() > 0:
                rating_text = rating_elem.text_content()
                data["rating"] = self.safe_float(rating_text)
            
            # Look for reviews span with class UY7F9
            reviews_elem = container.locator('span.UY7F9').first
            if reviews_elem.count() > 0:
                reviews_text = reviews_elem.text_content()
                # Remove parentheses
                reviews_text = reviews_text.replace('(', '').replace(')', '')
                data["reviews"] = self.safe_int(reviews_text)
            
            # 3. Extract industry and address from W4Efsd divs
            w4e_divs = container.locator('div.W4Efsd')
            
            # Get all W4Efsd divs text
            for i in range(w4e_divs.count()):
                try:
                    div = w4e_divs.nth(i)
                    text = div.text_content()
                    if not text:
                        continue
                    
                    # Check if this contains address/industry info
                    if "·" in text:
                        parts = [p.strip() for p in text.split("·") if p.strip()]
                        if len(parts) >= 2:
                            # First part is usually industry/category
                            if not data["industry"]:
                                data["industry"] = self.clean_text(parts[0])
                            # Second part might be address
                            if not data["full_address"]:
                                data["full_address"] = self.clean_text(parts[1])
                        elif len(parts) == 1:
                            if not data["industry"]:
                                data["industry"] = self.clean_text(parts[0])
                    
                    # Also look for address patterns
                    address_patterns = ['floor', 'rd', 'road', 'st', 'street', 'avenue', 'ave', 'lane', 'ln']
                    if any(pattern in text.lower() for pattern in address_patterns):
                        if not data["full_address"]:
                            data["full_address"] = self.clean_text(text)
                            
                except:
                    continue
            
            # 4. Extract phone number
            # Look for phone in UsdlK class or any numeric pattern
            phone_elem = container.locator('.UsdlK').first
            if phone_elem.count() > 0:
                data["phone"] = self.clean_text(phone_elem.text_content())
            else:
                # Try to find phone by regex in container text
                container_text = container.text_content()
                phone_regex = r'(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}'
                phone_match = re.search(phone_regex, container_text)
                if phone_match:
                    data["phone"] = phone_match.group(0)
            
            # 5. Extract website
            # Find all links and exclude Google Maps links
            all_links = container.locator('a[href]')
            for i in range(all_links.count()):
                try:
                    a = all_links.nth(i)
                    href = a.get_attribute('href')
                    if href and not href.startswith('https://www.google.com/maps/') and 'google.com' not in href:
                        data["website"] = href
                        break
                except:
                    continue
            
            # Clean up address (remove opening hours, etc.)
            if data["full_address"]:
                # Remove common patterns that are not part of address
                patterns_to_remove = [
                    r'Open\s+\d', r'Closed', r'Open\s+24\s+hours',
                    r'Closes\s+\d', r'Opens\s+\d', r'\d{1,2}:\d{2}\s*[ap]m'
                ]
                for pattern in patterns_to_remove:
                    data["full_address"] = re.sub(pattern, '', data["full_address"], flags=re.IGNORECASE)
                data["full_address"] = self.clean_text(data["full_address"])
            
        except Exception as e:
            print(f"Error extracting card data: {e}")
            data["status"] = f"error: {str(e)[:50]}"
        
        return data
    
    def scrape_cards_only(self, search_url, max_results=200, scroll_pause=1.0):
        """Main scraping function using Playwright"""
        print(f"Starting scraping for: {search_url}")
        
        rows = []
        seen_links = set()
        
        try:
            self.task.message = "Launching browser..."
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                )
                
                context = browser.new_context(
                    locale="en-US",
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080}
                )
                
                page = context.new_page()
                page.set_default_timeout(90000)
                
                self.task.message = "Opening Google Maps..."
                page.goto(search_url, wait_until="domcontentloaded")
                time.sleep(3)  # Wait for page to load
                
                # Accept cookies if present
                try:
                    accept_button = page.locator('button:has-text("Accept all"), button:has-text("I agree")').first
                    if accept_button.count() > 0 and accept_button.is_visible():
                        accept_button.click()
                        time.sleep(1)
                        print("Accepted cookies")
                except:
                    pass
                
                # Wait for results feed
                try:
                    page.wait_for_selector('div[role="feed"]', timeout=30000)
                    print("Results feed found")
                except:
                    print("Results feed not found, trying to find cards directly")
                
                # Scroll to load more results
                self.task.message = "Scrolling to load results..."
                
                last_height = 0
                scroll_attempts = 0
                max_scroll_attempts = 30
                total_cards = 0
                
                while scroll_attempts < max_scroll_attempts and total_cards < max_results:
                    # Scroll down
                    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    time.sleep(scroll_pause)
                    
                    # Count current cards
                    cards = page.locator('a.hfpxzc')
                    current_count = cards.count()
                    
                    if current_count > total_cards:
                        total_cards = current_count
                        scroll_attempts = 0
                        self.task.message = f"Loaded {total_cards} cards"
                        self.task.progress = min(0.5, total_cards / max_results * 0.5)
                    else:
                        scroll_attempts += 1
                    
                    # Check if we reached bottom
                    new_height = page.evaluate('document.body.scrollHeight')
                    if new_height == last_height:
                        scroll_attempts += 1
                    last_height = new_height
                    
                    if total_cards >= max_results:
                        break
                
                # Now extract data from cards
                self.task.message = f"Extracting data from {total_cards} cards..."
                
                cards = page.locator('a.hfpxzc')
                actual_count = min(cards.count(), max_results)
                
                for i in range(actual_count):
                    if self.task._stop_flag:
                        break
                    
                    try:
                        card = cards.nth(i)
                        link = card.get_attribute('href')
                        
                        if not link or link in seen_links:
                            continue
                        
                        seen_links.add(link)
                        
                        # Get the container (Nv2PK div)
                        container = card.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                        
                        if container.count() > 0:
                            # Extract data using Chrome extension logic
                            row_data = self.extract_card_data(container, link)
                            rows.append(row_data)
                        else:
                            # Fallback: just get basic info
                            name = card.get_attribute('aria-label') or ''
                            rows.append({
                                "name": self.clean_text(name),
                                "rating": None,
                                "reviews": None,
                                "phone": "",
                                "industry": "",
                                "full_address": "",
                                "website": "",
                                "google_maps_link": link,
                                "status": "no_container"
                            })
                        
                        # Update progress
                        self.task.progress = 0.5 + (i / actual_count * 0.5)
                        self.task.message = f"Extracted {len(rows)}/{actual_count} items"
                        
                        # Save checkpoint every 10 rows
                        if len(rows) % 10 == 0:
                            temp_df = pd.DataFrame(rows)
                            self.save_checkpoint(temp_df)
                            print(f"Checkpoint: {len(rows)} rows saved")
                        
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
                            "status": f"error: {str(e)[:50]}"
                        })
                
                browser.close()
            
            # Create final DataFrame
            final_df = pd.DataFrame(rows)
            if not final_df.empty:
                final_df = final_df.drop_duplicates(subset=['google_maps_link'], keep='first')
            
            self.save_checkpoint(final_df)
            self.task.message = f"Scraping complete: {len(final_df)} results"
            print(f"Scraped {len(final_df)} results")
            
            return final_df
            
        except Exception as e:
            self.task.message = f"Scraping failed: {str(e)}"
            print(f"Scraping error: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def scrape_deep(self, search_url, max_results=200, scroll_pause=1.0):
        """Deep scraping - visit each place page"""
        print(f"Starting deep scrape for: {search_url}")
        
        # First get basic info
        self.task.message = "Step 1: Getting basic info..."
        basic_df = self.scrape_cards_only(search_url, max_results, scroll_pause)
        
        if basic_df.empty:
            return basic_df
        
        # Get unique links
        links = [link for link in basic_df['google_maps_link'].tolist() if link]
        links = list(dict.fromkeys(links))[:max_results]
        
        if not links:
            return basic_df
        
        print(f"Found {len(links)} unique places for deep scraping")
        
        # Visit each place page
        detailed_rows = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            
            context = browser.new_context(
                locale="en-US",
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            
            page = context.new_page()
            page.set_default_timeout(60000)
            
            for idx, link in enumerate(links, 1):
                if self.task._stop_flag:
                    break
                
                self.task.message = f"Deep scraping {idx}/{len(links)}"
                self.task.progress = 0.5 + (idx / len(links) * 0.5)
                
                try:
                    page.goto(link, wait_until="domcontentloaded", timeout=30000)
                    time.sleep(2)
                    
                    # Extract detailed info
                    details = self._extract_place_details(page)
                    details['google_maps_link'] = link
                    details['status'] = 'ok_deep'
                    
                    detailed_rows.append(details)
                    
                    # Save checkpoint every 5 places
                    if idx % 5 == 0:
                        temp_df = pd.DataFrame(detailed_rows)
                        self.save_checkpoint(temp_df)
                    
                except Exception as e:
                    print(f"Error deep scraping {link}: {e}")
                    # Keep basic info
                    basic_row = basic_df[basic_df['google_maps_link'] == link].iloc[0].to_dict() if not basic_df[basic_df['google_maps_link'] == link].empty else {}
                    basic_row['status'] = f"error_deep: {str(e)[:50]}"
                    detailed_rows.append(basic_row)
                
                time.sleep(1)  # Be respectful
            
            browser.close()
        
        # Merge with basic data
        if detailed_rows:
            detailed_df = pd.DataFrame(detailed_rows)
            # Update basic data with detailed info
            for col in detailed_df.columns:
                basic_df[col] = detailed_df[col]
        
        return basic_df
    
    def _extract_place_details(self, page):
        """Extract details from a place page"""
        details = {
            "name": "", "rating": None, "reviews": None, "phone": "",
            "industry": "", "full_address": "", "website": ""
        }
        
        try:
            # Name
            name_elem = page.locator('h1.DUwDvf, h1.fontHeadlineLarge').first
            if name_elem.count() > 0:
                details["name"] = name_elem.text_content()
            
            # Rating
            rating_elem = page.locator('div.F7nice span.ceNzKf').first
            if rating_elem.count() > 0:
                rating_text = rating_elem.text_content()
                details["rating"] = self.safe_float(rating_text)
            
            # Reviews
            reviews_elem = page.locator('div.F7nice span:nth-child(2)').first
            if reviews_elem.count() > 0:
                reviews_text = reviews_elem.text_content()
                details["reviews"] = self.safe_int(reviews_text)
            
            # Industry
            industry_elem = page.locator('button.DkEaL').first
            if industry_elem.count() > 0:
                details["industry"] = industry_elem.text_content()
            
            # Address
            address_elem = page.locator('button[data-item-id="address"]').first
            if address_elem.count() > 0:
                details["full_address"] = address_elem.text_content()
            
            # Phone
            phone_elem = page.locator('button[data-item-id^="phone"]').first
            if phone_elem.count() > 0:
                details["phone"] = phone_elem.text_content()
            
            # Website
            website_elem = page.locator('a[data-item-id="authority"]').first
            if website_elem.count() > 0:
                details["website"] = website_elem.get_attribute('href') or ""
            
        except Exception as e:
            print(f"Error extracting place details: {e}")
        
        # Clean text fields
        for key in ["name", "phone", "industry", "full_address", "website"]:
            if key in details:
                details[key] = self.clean_text(details[key])
        
        return details

# ============================================================================
# TASK MANAGEMENT (same as before)
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
        """Main task execution method"""
        try:
            self.status = "running"
            self.start_time = datetime.now()
            self.message = "Starting Google Maps scraping..."
            
            print(f"TASK STARTED: {self.task_id}")
            print(f"Config: {self.config}")
            
            # Initialize scraper
            self.scraper = GoogleMapsScraper(self, self.base_dir, self.temp_dir, self.checkpoint_file)
            
            # Run based on task type
            if self.config['task_type'] == 'poi':
                results_df = self._run_poi_scraping()
            elif self.config['task_type'] == 'search':
                results_df = self._run_search_scraping()
            else:
                raise ValueError(f"Unknown task type: {self.config['task_type']}")
            
            self.total_results = len(results_df) if not results_df.empty else 0
            
            if not self._stop_flag:
                if not results_df.empty:
                    # Save final results
                    results_df.to_csv(self.results_file, index=False)
                    self.status = "completed"
                    self.progress = 1.0
                    self.message = f"Task completed! Found {self.total_results} places"
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
            import traceback
            traceback.print_exc()
            
            # Create empty results file
            empty_df = pd.DataFrame(columns=[
                'name', 'rating', 'reviews', 'phone', 'industry', 
                'full_address', 'website', 'google_maps_link', 'status'
            ])
            empty_df.to_csv(self.results_file, index=False)
    
    def _run_poi_scraping(self):
        """Run POI radius scraping"""
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
    
    def _run_search_scraping(self):
        """Run search query scraping"""
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
