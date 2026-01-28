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
    """REAL scraping using updated Google Maps selectors"""
    print(f"Starting real scraping for: {search_url}")
    
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
            
            context = browser.new_context(
                locale="en-US",
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            
            page = context.new_page()
            page.set_default_timeout(120000)
            
            self.task.message = "Opening Google Maps search page..."
            print(f"Navigating to: {search_url}")
            
            # Go to the search URL
            page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)  # Wait for page to load
            
            # Accept cookies if present
            try:
                accept_button = page.locator('button:has-text("Accept all")').first
                if accept_button.is_visible(timeout=3000):
                    accept_button.click()
                    time.sleep(1)
                    print("Accepted cookies")
            except:
                pass
            
            # Wait for results feed
            self.task.message = "Waiting for results..."
            print("Waiting for results feed...")
            
            try:
                # Try different selectors for results feed
                page.wait_for_selector('div[role="feed"], div[aria-label*="Results"], div.m6QErb[aria-label]', 
                                      timeout=30000)
                print("Results feed found")
            except:
                print("Results feed not found, trying to find cards directly")
            
            # Initialize variables for scrolling
            last_count = 0
            scroll_attempts = 0
            max_scroll_attempts = 50
            
            self.task.message = "Scrolling to load more results..."
            print("Starting to scroll...")
            
            # Scroll to load more results
            while scroll_attempts < max_scroll_attempts and len(rows) < max_results:
                if self.task._stop_flag:
                    break
                
                # Get current cards using multiple selectors
                cards = page.locator('a.hfpxzc, a[href*="/maps/place/"], div.Nv2PK a')
                current_count = cards.count()
                
                print(f"Found {current_count} cards")
                
                if current_count > 0:
                    # Update progress
                    progress_percent = min(0.7, len(rows) / max_results * 0.7)
                    self.task.progress = progress_percent
                    self.task.message = f"Found {current_count} places, extracted {len(rows)}"
                
                if current_count == last_count:
                    scroll_attempts += 1
                else:
                    scroll_attempts = 0
                    last_count = current_count
                
                if current_count >= max_results:
                    break
                
                # Scroll down
                try:
                    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    time.sleep(scroll_pause)
                except:
                    break
                
                # Check if we're at the bottom
                try:
                    new_height = page.evaluate('document.body.scrollHeight')
                    current_pos = page.evaluate('window.pageYOffset')
                    if new_height == current_pos:
                        scroll_attempts += 1
                except:
                    pass
            
            # Now extract data from cards
            self.task.message = f"Extracting data from {current_count} places..."
            print(f"Extracting data from {current_count} places...")
            
            # Get all cards again after scrolling
            cards = page.locator('a.hfpxzc, a[href*="/maps/place/"], div.Nv2PK a')
            final_count = min(cards.count(), max_results)
            
            for i in range(final_count):
                if self.task._stop_flag:
                    break
                
                try:
                    card = cards.nth(i)
                    
                    # Get the link
                    link = card.get_attribute('href')
                    if not link:
                        continue
                    
                    # Normalize the link
                    if link.startswith('/'):
                        full_link = f"https://www.google.com{link}"
                    else:
                        full_link = link
                    
                    normalized_link = self.normalize_maps_url(full_link)
                    
                    if normalized_link in seen_links:
                        continue
                    
                    seen_links.add(normalized_link)
                    
                    # Get name - try multiple selectors
                    name = ""
                    try:
                        name = card.get_attribute('aria-label') or ""
                    except:
                        pass
                    
                    if not name:
                        try:
                            # Try to get from the container
                            container = card.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                            name_elem = container.locator('div.qBF1Pd, div.fontHeadlineSmall, h1, h2, h3').first
                            name = name_elem.text_content(timeout=1000) or ""
                        except:
                            pass
                    
                    # Get rating
                    rating = None
                    try:
                        container = card.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                        rating_elem = container.locator('span.MW4etd, [aria-label*="stars"], .ZkP5Je span').first
                        rating_text = rating_elem.text_content(timeout=1000) or ""
                        rating = self.safe_float(rating_text)
                    except:
                        pass
                    
                    # Get reviews
                    reviews = None
                    try:
                        container = card.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                        reviews_elem = container.locator('span.UY7F9, [aria-label*="Reviews"], .ZkP5Je span:nth-child(2)').first
                        reviews_text = reviews_elem.text_content(timeout=1000) or ""
                        reviews = self.safe_int(reviews_text)
                    except:
                        pass
                    
                    # Get industry/category
                    industry = ""
                    try:
                        container = card.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                        industry_elem = container.locator('div.W4Efsd span:first-child, [class*="category"], [class*="type"]').first
                        industry_text = industry_elem.text_content(timeout=1000) or ""
                        industry = self.clean_text(industry_text)
                    except:
                        pass
                    
                    # Get address
                    address = ""
                    try:
                        container = card.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                        # Look for address in W4Efsd elements
                        w4e_elements = container.locator('div.W4Efsd')
                        for j in range(w4e_elements.count()):
                            try:
                                elem = w4e_elements.nth(j)
                                text = elem.text_content(timeout=500) or ""
                                if "floor" in text.lower() or "road" in text.lower() or "street" in text.lower() or "ave" in text.lower():
                                    address = self.clean_text(text)
                                    break
                            except:
                                pass
                    except:
                        pass
                    
                    # Add to results
                    rows.append({
                        "name": self.clean_text(name),
                        "rating": rating,
                        "reviews": reviews,
                        "phone": "",
                        "industry": industry,
                        "full_address": address,
                        "website": "",
                        "google_maps_link": normalized_link,
                        "status": "ok_card"
                    })
                    
                    # Update progress
                    self.task.progress = 0.7 + (i / final_count * 0.3)
                    self.task.message = f"Extracted {len(rows)}/{final_count} places"
                    
                    # Save checkpoint every 5 rows
                    if len(rows) % 5 == 0:
                        temp_df = pd.DataFrame(rows)
                        self.save_checkpoint(temp_df)
                        print(f"Checkpoint saved: {len(rows)} rows")
                    
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
        self.task.message = f"Scraping complete: {len(final_df)} real results"
        print(f"Final: {len(final_df)} unique results")
        
        return final_df
        
    except Exception as e:
        self.task.message = f"Scraping failed: {str(e)[:100]}"
        print(f"Scraping error: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def scrape_cards_only_direct(self, search_url, max_results=200, scroll_pause=1.0):
    """Direct scraping using the exact HTML structure from the example"""
    print(f"Starting direct scraping for: {search_url}")
    
    rows = []
    
    try:
        self.task.message = "Launching browser..."
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            
            page = context.new_page()
            page.set_default_timeout(60000)
            
            print(f"Going to: {search_url}")
            page.goto(search_url, wait_until="domcontentloaded")
            time.sleep(5)  # Wait longer for page to load
            
            # Check if we're on Google Maps
            if "google.com/maps" not in page.url:
                print(f"Not on Google Maps! Current URL: {page.url}")
                browser.close()
                return pd.DataFrame()
            
            # Look for the feed container
            feed_selector = 'div[role="feed"], div[aria-label*="Results"], div.m6QErb'
            try:
                page.wait_for_selector(feed_selector, timeout=10000)
                print("Found results feed")
            except:
                print("Could not find feed, trying to scrape anyway")
            
            # Scroll a few times to load results
            for i in range(10):
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                time.sleep(1)
                if self.task._stop_flag:
                    break
            
            # Now look for place cards - using the exact structure from your HTML
            place_cards = page.locator('div.Nv2PK, div[role="article"]')
            card_count = place_cards.count()
            
            print(f"Found {card_count} place cards")
            self.task.message = f"Found {card_count} place cards"
            
            for i in range(min(card_count, max_results)):
                if self.task._stop_flag:
                    break
                
                try:
                    card = place_cards.nth(i)
                    
                    # Get the link from anchor tag
                    link_elem = card.locator('a.hfpxzc').first
                    link = link_elem.get_attribute('href') if link_elem.count() > 0 else ""
                    
                    if not link:
                        continue
                    
                    # Get name
                    name_elem = card.locator('div.qBF1Pd, div.fontHeadlineSmall').first
                    name = name_elem.text_content() if name_elem.count() > 0 else ""
                    
                    # Get rating
                    rating_elem = card.locator('span.MW4etd').first
                    rating_text = rating_elem.text_content() if rating_elem.count() > 0 else ""
                    rating = self.safe_float(rating_text)
                    
                    # Get reviews
                    reviews_elem = card.locator('span.UY7F9').first
                    reviews_text = reviews_elem.text_content() if reviews_elem.count() > 0 else ""
                    reviews = self.safe_int(reviews_text)
                    
                    # Get industry and address from W4Efsd divs
                    industry = ""
                    address = ""
                    
                    w4e_divs = card.locator('div.W4Efsd')
                    if w4e_divs.count() >= 2:
                        # First W4Efsd might contain rating/reviews
                        # Second W4Efsd contains industry and address
                        details_div = w4e_divs.nth(1)
                        details_text = details_div.text_content() if details_div.count() > 0 else ""
                        
                        if "·" in details_text:
                            parts = details_text.split("·")
                            if len(parts) >= 2:
                                industry = self.clean_text(parts[0])
                                address = self.clean_text(parts[1])
                        else:
                            industry = self.clean_text(details_text)
                    
                    rows.append({
                        "name": self.clean_text(name),
                        "rating": rating,
                        "reviews": reviews,
                        "phone": "",
                        "industry": industry,
                        "full_address": address,
                        "website": "",
                        "google_maps_link": link,
                        "status": "ok_card"
                    })
                    
                    # Update progress
                    self.task.progress = i / min(card_count, max_results)
                    self.task.message = f"Processed {i+1}/{min(card_count, max_results)} cards"
                    
                except Exception as e:
                    print(f"Error with card {i}: {e}")
            
            browser.close()
        
        final_df = pd.DataFrame(rows)
        print(f"Scraped {len(final_df)} results")
        return final_df
        
    except Exception as e:
        print(f"Error in direct scraping: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()



def debug_page_structure(self, search_url):
    """Debug function to see what's actually on the page"""
    print(f"Debugging page: {search_url}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = context.new_page()
        
        page.goto(search_url, wait_until="domcontentloaded")
        time.sleep(5)
        
        print(f"Current URL: {page.url}")
        print(f"Page title: {page.title()}")
        
        # Check for common selectors
        selectors_to_check = [
            'div[role="feed"]',
            'div.Nv2PK',
            'a.hfpxzc',
            'div.qBF1Pd',
            'span.MW4etd',
            'span.UY7F9',
            'div.W4Efsd'
        ]
        
        for selector in selectors_to_check:
            count = page.locator(selector).count()
            print(f"{selector}: {count} found")
        
        # Take a screenshot for visual debugging
        page.screenshot(path="debug_page.png")
        print("Screenshot saved as debug_page.png")
        
        # Get some sample HTML
        sample_html = page.content()[:5000]  # First 5000 chars
        print(f"Sample HTML:\n{sample_html}")
        
        browser.close()

    
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
