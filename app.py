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

CHECKPOINT_FILE = "checkpoint_results.csv"

# -----------------------------
# Session State Init
# -----------------------------
if "is_scraping" not in st.session_state:
    st.session_state.is_scraping = False

if "last_df" not in st.session_state:
    st.session_state.last_df = pd.DataFrame()

# -----------------------------
# Checkpoint Helpers
# -----------------------------
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
# VERY FAST MODE
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
# UI (No rerun crash)
# -----------------------------
st.info("Tip: Do not switch tabs while scraping. The app is now locked during scraping.")

tab1, tab2 = st.tabs(["Search Query Scraper", "Recovery"])

with tab1:
    search_url = st.text_input(
        "Google Maps Search URL",
        value="https://www.google.com/maps/search/jee+mains+coaching+centres+in+india/",
        disabled=st.session_state.is_scraping,
    )

    max_results = st.number_input(
        "Max Results",
        min_value=10,
        max_value=5000,
        value=200,
        step=10,
        disabled=st.session_state.is_scraping,
    )

    scroll_delay = st.slider(
        "Scroll Delay (seconds)",
        0.5,
        5.0,
        1.0,
        0.1,
        disabled=st.session_state.is_scraping,
    )

    mode = st.selectbox(
        "Scrape Mode",
        ["Very Fast (Cards)"],
        disabled=st.session_state.is_scraping,
    )

    start = st.button("Start Scraping", disabled=st.session_state.is_scraping)

    if start:
        st.session_state.is_scraping = True
        status_box = st.empty()
        progress_box = st.progress(0.0)

        try:
            df = scrape_cards_only(
                search_url=search_url,
                max_results=int(max_results),
                scroll_pause=float(scroll_delay),
                ui_status=status_box,
                ui_progress=progress_box,
            )
            st.session_state.last_df = df

            st.success(f"Scraped rows: {len(df)}")
            st.dataframe(df, use_container_width=True)

            st.download_button(
                "Download CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="google_maps_results.csv",
                mime="text/csv",
            )

        except Exception as e:
            st.error(f"Scraping failed: {e}")

        st.session_state.is_scraping = False

    if not st.session_state.last_df.empty:
        st.subheader("Last Output")
        st.dataframe(st.session_state.last_df, use_container_width=True)

with tab2:
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
