import time
import re
import pandas as pd
import streamlit as st
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# -----------------------------
# Helpers
# -----------------------------
def clean_text(x):
    if not x:
        return ""
    x = re.sub(r"\s+", " ", str(x)).strip()
    return x

def safe_float(x):
    try:
        return float(str(x).strip())
    except:
        return None

def safe_int(x):
    try:
        return int(re.sub(r"[^\d]", "", str(x)))
    except:
        return None

def normalize_maps_url(url: str) -> str:
    if not url:
        return ""
    # Remove tracking params if any
    return url.split("&")[0]

def extract_phone(text: str):
    if not text:
        return ""
    # simple phone pattern for India / international
    m = re.search(r"(\+?\d[\d\s\-()]{7,}\d)", text)
    return clean_text(m.group(1)) if m else ""

def extract_website(text: str):
    if not text:
        return ""
    # detect website-like strings
    m = re.search(r"(https?://[^\s]+)", text)
    return clean_text(m.group(1)) if m else ""

# -----------------------------
# VERY FAST MODE (Cards Only)
# -----------------------------
def scrape_cards_only(search_url, max_results=100, scroll_pause=1.2, ui_status=None):
    rows = []
    seen_links = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(locale="en-US")
        page = context.new_page()

        page.set_default_timeout(90000)

        if ui_status:
            ui_status.info("Opening Google Maps search page...")

        page.goto(search_url, wait_until="domcontentloaded")

        # Wait for results panel
        try:
            page.wait_for_selector('div[role="feed"]', timeout=60000)
        except:
            if ui_status:
                ui_status.warning("Could not detect results feed. Trying anyway...")

        feed_selector = 'div[role="feed"]'
        feed = page.locator(feed_selector)

        # Scroll to load more
        last_count = 0
        same_count_rounds = 0

        if ui_status:
            ui_status.info("Scrolling to load results...")

        while True:
            cards = page.locator('a.hfpxzc')  # place links inside cards
            count = cards.count()

            if ui_status:
                ui_status.write(f"Loaded cards: {count}")

            if count >= max_results:
                break

            if count == last_count:
                same_count_rounds += 1
            else:
                same_count_rounds = 0

            if same_count_rounds >= 4:
                break

            last_count = count

            # Scroll feed
            try:
                feed.evaluate("(el) => el.scrollBy(0, el.scrollHeight)")
            except:
                page.mouse.wheel(0, 3000)

            time.sleep(scroll_pause)

        # Extract each card details
        cards = page.locator('a.hfpxzc')
        total = min(cards.count(), max_results)

        if ui_status:
            ui_status.success(f"Card scraped: {total}/{total}")

        for i in range(total):
            try:
                card = cards.nth(i)
                link = card.get_attribute("href") or ""
                link = normalize_maps_url(link)

                if not link or link in seen_links:
                    continue
                seen_links.add(link)

                # Card container is usually parent element
                container = card.locator("xpath=ancestor::div[contains(@class,'Nv2PK')]").first

                name = ""
                rating = ""
                reviews = ""
                category = ""
                address_snippet = ""

                # Name
                try:
                    name = container.locator("div.qBF1Pd").first.inner_text(timeout=2000)
                except:
                    try:
                        name = card.get_attribute("aria-label") or ""
                    except:
                        name = ""

                # Rating + reviews
                try:
                    rating = container.locator("span.MW4etd").first.inner_text(timeout=2000)
                except:
                    rating = ""

                try:
                    reviews = container.locator("span.UY7F9").first.inner_text(timeout=2000)
                    reviews = reviews.replace("(", "").replace(")", "")
                except:
                    reviews = ""

                # Category + address snippet line
                try:
                    # This line often contains: "Coaching center · Address..."
                    line = container.locator("div.W4Efsd").nth(1).inner_text(timeout=2000)
                    line = clean_text(line)
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
                        "name": clean_text(name),
                        "rating": safe_float(rating),
                        "reviews": safe_int(reviews),
                        "phone": "",
                        "industry": clean_text(category),
                        "full_address": clean_text(address_snippet),
                        "website": "",
                        "google_maps_link": link,
                        "status": "ok_card",
                    }
                )

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

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["google_maps_link"], keep="first")
    return df

# -----------------------------
# DEEP SCRAPE MODE (Open Each Place)
# -----------------------------
def scrape_place_details(page, place_url, retries=2):
    place_url = normalize_maps_url(place_url)
    if not place_url:
        return {
            "name": "",
            "rating": None,
            "reviews": None,
            "phone": "",
            "industry": "",
            "full_address": "",
            "website": "",
            "google_maps_link": "",
            "status": "missing_url",
        }

    for attempt in range(retries + 1):
        try:
            page.goto(place_url, wait_until="domcontentloaded", timeout=120000)
            time.sleep(1.2)

            # Name
            name = ""
            try:
                name = page.locator("h1.DUwDvf").first.inner_text(timeout=8000)
            except:
                name = ""

            # Rating
            rating = None
            try:
                rating_txt = page.locator("div.F7nice span.ceNzKf").first.inner_text(timeout=4000)
                rating = safe_float(rating_txt)
            except:
                rating = None

            # Reviews count
            reviews = None
            try:
                rev_txt = page.locator("div.F7nice span:nth-child(2)").first.inner_text(timeout=4000)
                reviews = safe_int(rev_txt)
            except:
                reviews = None

            # Category
            industry = ""
            try:
                industry = page.locator("button.DkEaL").first.inner_text(timeout=4000)
            except:
                industry = ""

            # Full address
            full_address = ""
            try:
                full_address = page.locator('button[data-item-id="address"]').first.inner_text(timeout=4000)
            except:
                full_address = ""

            # Phone
            phone = ""
            try:
                phone = page.locator('button[data-item-id^="phone"]').first.inner_text(timeout=4000)
            except:
                phone = ""

            # Website
            website = ""
            try:
                website = page.locator('a[data-item-id="authority"]').first.get_attribute("href")
            except:
                website = ""

            return {
                "name": clean_text(name),
                "rating": rating,
                "reviews": reviews,
                "phone": clean_text(phone),
                "industry": clean_text(industry),
                "full_address": clean_text(full_address),
                "website": clean_text(website),
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
            return {
                "name": "",
                "rating": None,
                "reviews": None,
                "phone": "",
                "industry": "",
                "full_address": "",
                "website": "",
                "google_maps_link": place_url,
                "status": f"error_place: {str(e)[:120]}",
            }

def scrape_deep(search_url, max_results=50, scroll_pause=1.2, ui_status=None):
    # Step 1: Get all place links quickly from cards
    cards_df = scrape_cards_only(
        search_url=search_url,
        max_results=max_results,
        scroll_pause=scroll_pause,
        ui_status=ui_status,
    )

    links = [x for x in cards_df["google_maps_link"].tolist() if x]
    links = list(dict.fromkeys(links))  # unique preserve order

    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(locale="en-US")
        page = context.new_page()
        page.set_default_timeout(120000)

        total = len(links)
        for idx, link in enumerate(links, start=1):
            if ui_status:
                ui_status.write(f"Scraping details: {idx} / {total}")

            item = scrape_place_details(page, link, retries=2)
            rows.append(item)

            # small delay to reduce blocking
            time.sleep(0.6)

        browser.close()

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["google_maps_link"], keep="first")
    return df

# -----------------------------
# STREAMLIT UI
# -----------------------------
st.set_page_config(page_title="Google Maps Scraper", layout="wide")

st.title(" Google Maps Scraper (Render Ready)")
st.caption("2 Modes: Very Fast (Cards) + Deep Scrape (Open each place)")

search_url_input = st.text_input(
    "Paste Google Maps Search URL",
    value="https://www.google.com/maps/search/jee+mains+coaching+centres+in+india/",
)

col1, col2, col3 = st.columns(3)

with col1:
    mode = st.selectbox("Scrape Mode", ["Very Fast (Cards)", "Deep Scrape (Accurate)"])

with col2:
    max_results = st.number_input("Max Results", min_value=1, max_value=5000, value=100)

with col3:
    scroll_pause = st.slider("Scroll Delay (seconds)", min_value=0.5, max_value=5.0, value=1.2, step=0.1)

status_box = st.empty()

if st.button(" Start Scraping"):
    if not search_url_input.strip():
        st.error("Please paste a valid Google Maps Search URL.")
        st.stop()

    status_box.info("Starting scrape...")

    try:
        if mode == "Very Fast (Cards)":
            df = scrape_cards_only(
                search_url=search_url_input.strip(),
                max_results=int(max_results),
                scroll_pause=float(scroll_pause),
                ui_status=status_box,
            )
        else:
            df = scrape_deep(
                search_url=search_url_input.strip(),
                max_results=int(max_results),
                scroll_pause=float(scroll_pause),
                ui_status=status_box,
            )

        if df.empty:
            st.warning("No data found. Try increasing scroll delay or max results.")
            st.stop()

        # reorder columns
        cols = [
            "name",
            "rating",
            "reviews",
            "phone",
            "industry",
            "full_address",
            "website",
            "google_maps_link",
            "status",
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = ""

        df = df[cols]

        st.success(f" Scraped rows: {len(df)}")
        st.dataframe(df, use_container_width=True)

        # TSV output
        tsv_data = df.to_csv(sep="\t", index=False)
        st.download_button(
            " Download TSV",
            data=tsv_data,
            file_name="google_maps_results.tsv",
            mime="text/tab-separated-values",
        )

        status_box.success("Done ")

    except Exception as e:
        st.error(f"Scraping failed: {e}")
        status_box.error("Failed ")
