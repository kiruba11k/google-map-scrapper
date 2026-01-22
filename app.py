import time
import json
import re
import urllib.parse
import pandas as pd
import streamlit as st
from playwright.sync_api import sync_playwright

st.set_page_config(page_title="Google Maps Scraper", layout="wide")
st.title("Google Maps Scraper")

def normalize_spaces(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def build_maps_search_url(query: str, location: str):
    full_query = f"{query} in {location}".strip()
    encoded = urllib.parse.quote_plus(full_query)
    return f"https://www.google.com/maps/search/{encoded}"

def safe_goto(page, url, timeout_ms=120000, retries=4):
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            return True
        except:
            time.sleep(2 + attempt)
    return False

def extract_place_links(page):
    cards = page.locator('a[href^="https://www.google.com/maps/place"]')
    links = []
    for i in range(cards.count()):
        href = cards.nth(i).get_attribute("href")
        if href and href not in links:
            links.append(href)
    return links

def strict_scroll_until_end(page, max_results: int, ui_status=None):
    delay = 0.7
    stable_cycles = 0
    stable_limit = 10
    last_count = 0
    all_links = []

    while True:
        current_links = extract_place_links(page)
        for l in current_links:
            if l not in all_links:
                all_links.append(l)

        current_count = len(all_links)

        if ui_status:
            ui_status.write(f"Loaded links: {current_count}")

        if current_count >= max_results:
            return all_links[:max_results]

        if current_count == last_count:
            stable_cycles += 1
        else:
            stable_cycles = 0
            last_count = current_count

        if stable_cycles >= stable_limit:
            return all_links

        page.mouse.wheel(0, 7000)
        time.sleep(delay)

def scrape_place_details(page, link: str):
    ok = safe_goto(page, link, timeout_ms=120000, retries=4)

    if not ok:
        return {
            "name": "",
            "rating": "",
            "reviews": "",
            "phone": "",
            "industry": "",
            "full_address": "",
            "website": "",
            "google_maps_link": link,
            "status": "failed_goto"
        }

    time.sleep(1.5)

    title = ""
    rating = ""
    reviews = ""
    address = ""
    website = ""
    phone = ""
    category = ""

    try:
        title = normalize_spaces(page.locator("h1").first.inner_text(timeout=7000))
    except:
        title = ""

    try:
        aria = page.locator('div[role="img"]').first.get_attribute("aria-label")
        if aria and "stars" in aria:
            rating = aria.split(" ")[0]
    except:
        rating = ""

    try:
        reviews_btn = page.locator('button[jsaction*="reviews"]').first
        reviews_text = normalize_spaces(reviews_btn.inner_text(timeout=7000))
        nums = re.findall(r"[\d,]+", reviews_text)
        reviews = nums[0] if nums else ""
    except:
        reviews = ""

    # Full details extraction (no fast mode)
    info_buttons = page.locator('button[data-item-id]')
    for i in range(info_buttons.count()):
        item = info_buttons.nth(i)
        data_id = item.get_attribute("data-item-id") or ""
        try:
            txt = normalize_spaces(item.inner_text(timeout=3000))
        except:
            txt = ""

        if data_id == "address":
            address = txt
        elif data_id == "authority":
            website = txt
        elif data_id == "phone:tel":
            phone = txt
        elif data_id == "category":
            category = txt

    status = "ok" if title else "partial"

    return {
        "name": title,
        "rating": rating,
        "reviews": reviews,
        "phone": phone,
        "industry": category,
        "full_address": address,
        "website": website,
        "google_maps_link": link,
        "status": status
    }

def scrape_google_maps_all(search_url: str, max_results: int, ui_progress=None, ui_status=None):
    final_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Speed improvement (safe): block heavy resources
        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "font", "media"]
            else route.continue_()
        )

        if ui_status:
            ui_status.write("Opening search page...")

        ok = safe_goto(page, search_url, timeout_ms=150000, retries=4)
        if not ok:
            browser.close()
            return []

        time.sleep(2)

        if ui_status:
            ui_status.write("Scrolling results until end...")

        links = strict_scroll_until_end(page, max_results=max_results, ui_status=ui_status)

        if ui_status:
            ui_status.write(f"Total links collected: {len(links)}")
            ui_status.write("Scraping all place details...")

        total = len(links)

        for idx, link in enumerate(links, start=1):
            row = scrape_place_details(page, link)
            final_rows.append(row)

            if ui_progress:
                ui_progress.progress(idx / total)

            if ui_status:
                ui_status.write(f"Scraped {idx}/{total} | Status: {row.get('status')}")

            # Small delay to reduce blocking
            time.sleep(0.6)

        browser.close()

    return final_rows


tab1, tab2 = st.tabs(["Agent Mode", "Search Query Mode"])

with tab1:
    st.subheader("Agent Mode")
    query = st.text_input("Business Query", value="software company", key="agent_query")
    location = st.text_input("Location", value="Whitefield Bangalore", key="agent_location")
    max_results = st.slider("Max Results", 10, 1000, 200, 10, key="agent_max_results")

    start_agent = st.button("Start Scraping (Agent Mode)", key="agent_start")

    if start_agent:
        search_url = build_maps_search_url(query=query, location=location)
        st.write("Generated Search URL:")
        st.code(search_url)

        progress = st.progress(0.0)
        status = st.empty()

        data = scrape_google_maps_all(
            search_url=search_url,
            max_results=max_results,
            ui_progress=progress,
            ui_status=status
        )

        if not data:
            st.error("No data scraped. Try again.")
        else:
            df = pd.DataFrame(data)
            st.success(f"Scraped total rows: {len(df)}")
            st.dataframe(df, use_container_width=True)

            st.download_button(
                "Download CSV (All Rows)",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="google_maps_all_results.csv",
                mime="text/csv"
            )

with tab2:
    st.subheader("Search Query Mode")
    search_url_input = st.text_input(
        "Google Maps Search URL",
        value="https://www.google.com/maps/search/software+company+in+whitefield",
        key="search_url_mode"
    )

    max_results2 = st.slider("Max Results", 10, 2000, 300, 10, key="search_max_results")

    start_search = st.button("Start Scraping (Search Query Mode)", key="search_start")

    if start_search:
        if "/maps/search" not in search_url_input:
            st.error("Please paste a valid Google Maps Search URL containing /maps/search")
        else:
            st.write("Using Search URL:")
            st.code(search_url_input)

            progress2 = st.progress(0.0)
            status2 = st.empty()

            data2 = scrape_google_maps_all(
                search_url=search_url_input.strip(),
                max_results=max_results2,
                ui_progress=progress2,
                ui_status=status2
            )

            if not data2:
                st.error("No data scraped. Try again.")
            else:
                df2 = pd.DataFrame(data2)
                st.success(f"Scraped total rows: {len(df2)}")
                st.dataframe(df2, use_container_width=True)

                st.download_button(
                    "Download CSV (All Rows)",
                    data=df2.to_csv(index=False).encode("utf-8"),
                    file_name="google_maps_all_results.csv",
                    mime="text/csv"
                )
