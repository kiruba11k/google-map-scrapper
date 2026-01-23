import os
import time
import re
import math
import urllib.parse
import pandas as pd
import streamlit as st
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

st.set_page_config(page_title="Google Maps Scraper", layout="wide")
st.title("Google Maps Scraper")

# -----------------------------
# Session State Init
# -----------------------------
if "is_scraping" not in st.session_state:
    st.session_state.is_scraping = False

if "last_df" not in st.session_state:
    st.session_state.last_df = pd.DataFrame()

if "last_mode" not in st.session_state:
    st.session_state.last_mode = ""

if "last_source" not in st.session_state:
    st.session_state.last_source = ""

# -----------------------------
# Checkpoint Helpers
# -----------------------------
CHECKPOINT_FILE = "checkpoint_results.csv"

def save_checkpoint(df: pd.DataFrame):
    try:
        df.to_csv(CHECKPOINT_FILE, index=False)
    except:
        pass

def load_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            return pd.read_csv(CHECKPOINT_FILE)
    except:
        pass
    return pd.DataFrame()

# -----------------------------
# Helpers
# -----------------------------
def clean_text(x):
    if not x:
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()

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
    return url.split("&")[0]

def build_search_url(query: str):
    query = query.strip()
    encoded = urllib.parse.quote_plus(query)
    return f"https://www.google.com/maps/search/{encoded}"

# -----------------------------
# VERY FAST MODE (Cards)
# -----------------------------
def scrape_cards_only(search_url, max_results=200, scroll_pause=1.0, ui_status=None, ui_progress=None):
    rows = []
    seen_links = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(locale="en-US")
        page = context.new_page()
        page.set_default_timeout(90000)

        if ui_status:
            ui_status.info("Opening Google Maps search page...")
        page.goto(search_url, wait_until="domcontentloaded")

        try:
            page.wait_for_selector('div[role="feed"]', timeout=60000)
        except:
            if ui_status:
                ui_status.warning("Results feed not detected, trying anyway...")

        feed = page.locator('div[role="feed"]').first

        last_count = 0
        stable_rounds = 0
        stable_limit = 6

        if ui_status:
            ui_status.info("Scrolling until no new results appear...")

        while True:
            cards = page.locator("a.hfpxzc")
            count = cards.count()

            if ui_status:
                ui_status.write(f"Loaded cards: {count}")

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

        cards = page.locator("a.hfpxzc")
        total = min(cards.count(), max_results)

        if ui_status:
            ui_status.success(f"Card scraped: {total}/{total}")

        for i in range(total):
            if ui_progress:
                ui_progress.progress((i + 1) / total)

            try:
                card = cards.nth(i)
                link = normalize_maps_url(card.get_attribute("href") or "")
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
                    rating = safe_float(rating_txt)
                except:
                    rating = None

                try:
                    rev_txt = container.locator("span.UY7F9").first.inner_text(timeout=2000)
                    reviews = safe_int(rev_txt)
                except:
                    reviews = None

                try:
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
                        "rating": rating,
                        "reviews": reviews,
                        "phone": "",
                        "industry": clean_text(category),
                        "full_address": clean_text(address_snippet),
                        "website": "",
                        "google_maps_link": link,
                        "status": "ok_card",
                    }
                )

                # Auto-save every 20 rows
                if len(rows) % 20 == 0:
                    temp_df = pd.DataFrame(rows).drop_duplicates(subset=["google_maps_link"], keep="first")
                    save_checkpoint(temp_df)

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
    save_checkpoint(final_df)
    return final_df

# -----------------------------
# DEEP SCRAPE MODE (Open each place)
# -----------------------------
def scrape_place_details(page, place_url, retries=2):
    place_url = normalize_maps_url(place_url)

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
                rating = safe_float(rating_txt)
            except:
                rating = None

            try:
                rev_txt = page.locator("div.F7nice span:nth-child(2)").first.inner_text(timeout=4000)
                reviews = safe_int(rev_txt)
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

def scrape_deep(search_url, max_results=200, scroll_pause=1.0, ui_status=None, ui_progress=None):
    # Step 1: collect links fast
    cards_df = scrape_cards_only(
        search_url=search_url,
        max_results=max_results,
        scroll_pause=scroll_pause,
        ui_status=ui_status,
    )

    links = [x for x in cards_df["google_maps_link"].tolist() if x]
    links = list(dict.fromkeys(links))

    rows = []
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
            if ui_status:
                ui_status.write(f"Scraping details: {idx} / {total}")

            if ui_progress:
                ui_progress.progress(idx / total)

            item = scrape_place_details(page, link, retries=2)
            rows.append(item)

            # Auto-save every 10 rows
            if len(rows) % 10 == 0:
                temp_df = pd.DataFrame(rows).drop_duplicates(subset=["google_maps_link"], keep="first")
                save_checkpoint(temp_df)

            time.sleep(0.6)

        browser.close()

    final_df = pd.DataFrame(rows).drop_duplicates(subset=["google_maps_link"], keep="first")
    save_checkpoint(final_df)
    return final_df

# -----------------------------
# Run wrapper (locks UI)
# -----------------------------
def run_job(job_fn, *args, **kwargs):
    st.session_state.is_scraping = True
    try:
        df = job_fn(*args, **kwargs)
        st.session_state.last_df = df
        return df
    finally:
        st.session_state.is_scraping = False

# -----------------------------
# Tabs UI
# -----------------------------
tab_poi, tab_search, tab_recovery = st.tabs(["POI Radius Scraper", "Search Query Scraper", "Recovery"])

# -----------------------------
# POI TAB
# -----------------------------
with tab_poi:
    st.subheader("POI Radius Scraper")

    poi_auto = st.checkbox("Auto-detect POI keywords", value=True, disabled=st.session_state.is_scraping)
    manual_poi = st.text_input(
        "Custom POI Keywords (comma separated)",
        value="coaching centre, tuition centre, training institute, academy",
        disabled=st.session_state.is_scraping,
    )

    lat = st.number_input("Latitude", value=12.971600, format="%.6f", disabled=st.session_state.is_scraping)
    lon = st.number_input("Longitude", value=77.594600, format="%.6f", disabled=st.session_state.is_scraping)

    max_results = st.number_input("Max Results Per POI", min_value=10, max_value=5000, value=200, step=10,
                                  disabled=st.session_state.is_scraping)
    scroll_delay = st.slider("Scroll Delay", 0.5, 5.0, 1.0, 0.1, disabled=st.session_state.is_scraping)

    mode = st.selectbox(
        "Scrape Mode",
        ["Very Fast (Cards)", "Deep Scrape (Full Details)"],
        disabled=st.session_state.is_scraping,
        key="poi_mode",
    )

    start_poi = st.button("Start POI Radius Scrape", disabled=st.session_state.is_scraping)

    if start_poi:
        status_box = st.empty()
        progress_box = st.progress(0.0)

        if poi_auto:
            poi_list = ["coaching centre", "tuition centre", "training institute", "academy", "institute"]
        else:
            poi_list = [x.strip() for x in manual_poi.split(",") if x.strip()]

        all_dfs = []

        for idx, poi in enumerate(poi_list, start=1):
            status_box.info(f"Scraping POI: {poi} ({idx}/{len(poi_list)})")
            query = f"{poi} near {lat},{lon}"
            url = build_search_url(query)

            if "Very Fast" in mode:
                df = run_job(
                    scrape_cards_only,
                    url,
                    int(max_results),
                    float(scroll_delay),
                    status_box,
                    progress_box,
                )
            else:
                df = run_job(
                    scrape_deep,
                    url,
                    int(max_results),
                    float(scroll_delay),
                    status_box,
                    progress_box,
                )

            df["poi_keyword"] = poi
            all_dfs.append(df)

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=["google_maps_link"], keep="first")

        st.session_state.last_df = final_df
        st.success(f"Total unique rows scraped: {len(final_df)}")
        st.dataframe(final_df, use_container_width=True)

        st.download_button(
            "Download CSV",
            data=final_df.to_csv(index=False).encode("utf-8"),
            file_name="poi_radius_results.csv",
            mime="text/csv",
        )

# -----------------------------
# SEARCH TAB
# -----------------------------
with tab_search:
    st.subheader("Search Query Scraper")

    search_url = st.text_input(
        "Google Maps Search URL",
        value="https://www.google.com/maps/search/jee+mains+coaching+centres+in+india/",
        disabled=st.session_state.is_scraping,
    )

    max_results2 = st.number_input(
        "Max Results",
        min_value=10,
        max_value=5000,
        value=200,
        step=10,
        disabled=st.session_state.is_scraping,
    )

    scroll_delay2 = st.slider(
        "Scroll Delay (seconds)",
        0.5,
        5.0,
        1.0,
        0.1,
        disabled=st.session_state.is_scraping,
    )

    mode2 = st.selectbox(
        "Scrape Mode",
        ["Very Fast (Cards)", "Deep Scrape (Full Details)"],
        disabled=st.session_state.is_scraping,
        key="search_mode",
    )

    start_search = st.button("Start Search Scrape", disabled=st.session_state.is_scraping)

    if start_search:
        status_box2 = st.empty()
        progress_box2 = st.progress(0.0)

        if "Very Fast" in mode2:
            df2 = run_job(
                scrape_cards_only,
                search_url.strip(),
                int(max_results2),
                float(scroll_delay2),
                status_box2,
                progress_box2,
            )
        else:
            df2 = run_job(
                scrape_deep,
                search_url.strip(),
                int(max_results2),
                float(scroll_delay2),
                status_box2,
                progress_box2,
            )

        st.success(f"Scraped rows: {len(df2)}")
        st.dataframe(df2, use_container_width=True)

        st.download_button(
            "Download CSV",
            data=df2.to_csv(index=False).encode("utf-8"),
            file_name="search_query_results.csv",
            mime="text/csv",
        )

    if not st.session_state.last_df.empty:
        st.subheader("Last Output (Saved in Session)")
        st.dataframe(st.session_state.last_df, use_container_width=True)

# -----------------------------
# RECOVERY TAB
# -----------------------------
with tab_recovery:
    st.subheader("Recovery / Auto-Saved Data")

    checkpoint_df = load_checkpoint()
    if not checkpoint_df.empty:
        st.success(f"Recovered saved rows: {len(checkpoint_df)}")
        st.dataframe(checkpoint_df, use_container_width=True)

        st.download_button(
            "Download Last Auto-Saved CSV",
            data=checkpoint_df.to_csv(index=False).encode("utf-8"),
            file_name="checkpoint_results.csv",
            mime="text/csv",
        )
    else:
        st.info("No checkpoint saved yet.")
