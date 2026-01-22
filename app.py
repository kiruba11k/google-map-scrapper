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

def extract_place_links(page):
    cards = page.locator('a[href^="https://www.google.com/maps/place"]')
    links = []
    for i in range(cards.count()):
        href = cards.nth(i).get_attribute("href")
        if href and href not in links:
            links.append(href)
    return links

def strict_scroll_until_end(page, mode: str, max_results: int, progress_callback=None):
    delay = 1.4 if mode == "safe" else 0.6

    stable_cycles = 0
    stable_limit = 6
    last_count = 0

    all_links = []

    while True:
        current_links = extract_place_links(page)
        for l in current_links:
            if l not in all_links:
                all_links.append(l)

        current_count = len(all_links)

        if progress_callback:
            progress_callback(current_count)

        if current_count >= max_results:
            return all_links[:max_results]

        if current_count == last_count:
            stable_cycles += 1
        else:
            stable_cycles = 0
            last_count = current_count

        if stable_cycles >= stable_limit:
            return all_links

        page.mouse.wheel(0, 5000)
        time.sleep(delay)

def scrape_place_details(page, link: str, mode: str):
    per_place_delay = 2.0 if mode == "safe" else 0.8
    goto_timeout = 180000 if mode == "safe" else 90000

    def safe_goto(url):
        # Retry 2 times
        for _ in range(2):
            try:
                page.goto(url, timeout=goto_timeout, wait_until="domcontentloaded")
                return True
            except:
                time.sleep(2)
        return False

    ok = safe_goto(link)
    if not ok:
        # Return partial row instead of crashing whole scraping
        return {
            "name": "",
            "rating": "",
            "reviews": "",
            "phone": "",
            "industry": "",
            "full_address": "",
            "website": "",
            "google_maps_link": link,
            "error": "goto_timeout"
        }

    time.sleep(per_place_delay)

    title = ""
    rating = ""
    reviews = ""
    address = ""
    website = ""
    phone = ""
    category = ""

    try:
        title = normalize_spaces(page.locator("h1").first.inner_text(timeout=5000))
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
        reviews_text = normalize_spaces(reviews_btn.inner_text(timeout=5000))
        nums = re.findall(r"[\d,]+", reviews_text)
        reviews = nums[0] if nums else ""
    except:
        reviews = ""

    info_buttons = page.locator('button[data-item-id]')
    for i in range(info_buttons.count()):
        item = info_buttons.nth(i)
        data_id = item.get_attribute("data-item-id") or ""
        try:
            txt = normalize_spaces(item.inner_text(timeout=2000))
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

    return {
        "name": title,
        "rating": rating,
        "reviews": reviews,
        "phone": phone,
        "industry": category,
        "full_address": address,
        "website": website,
        "google_maps_link": link
    }

def scrape_google_maps_strict(search_url: str, mode: str, max_results: int, ui_progress=None, ui_status=None):
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "font", "media"] else route.continue_())


        if ui_status:
            ui_status.write("Opening Google Maps search URL...")

        page.goto(search_url, timeout=120000, wait_until="domcontentloaded")
        time.sleep(3)

        if ui_status:
            ui_status.write("Scrolling results until end or limit reached...")

        def progress_cb(count):
            if ui_progress:
                pct = min(count / max_results, 1.0)
                ui_progress.progress(pct)
            if ui_status:
                ui_status.write(f"Loaded listings: {count}")

        links = strict_scroll_until_end(
            page=page,
            mode=mode,
            max_results=max_results,
            progress_callback=progress_cb
        )

        if ui_status:
            ui_status.write(f"Total unique listings collected: {len(links)}")
            ui_status.write("Scraping place details...")

        for idx, link in enumerate(links, start=1):
            item = scrape_place_details(page, link, mode=mode)
            results.append(item)

            if ui_progress:
                pct = min(idx / max_results, 1.0)
                ui_progress.progress(pct)

            if ui_status:
                ui_status.write(f"Scraped details: {idx} / {len(links)}")

        browser.close()

    return results


tab1, tab2 = st.tabs(["Agent Mode", "Search Query Mode"])

with tab1:
    st.subheader("Agent Mode")

    query = st.text_input("Business Query", value="software company", key="agent_query")
    location = st.text_input("Location", value="Whitefield Bangalore", key="agent_location")
    mode = st.selectbox("Mode", ["safe", "fast"], index=0, key="agent_mode")
    max_results = st.slider("Max Results", 10, 300, 80, 10, key="agent_max_results")

    start_agent = st.button("Start Scraping (Agent Mode)", key="agent_start")

    if start_agent:
        search_url = build_maps_search_url(query=query, location=location)
        st.write("Generated Search URL:")
        st.code(search_url)

        progress = st.progress(0.0)
        status = st.empty()

        data = scrape_google_maps_strict(
            search_url=search_url,
            mode=mode,
            max_results=max_results,
            ui_progress=progress,
            ui_status=status
        )

        if not data:
            st.error("No results scraped. Try Safe mode or increase Max Results.")
        else:
            df = pd.DataFrame(data)
            st.success(f"Scraped {len(df)} results")
            st.dataframe(df, use_container_width=True)

            st.download_button(
                "Download CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="google_maps_agent_results.csv",
                mime="text/csv"
            )

with tab2:
    st.subheader("Search Query Mode")

    st.write("Paste Google Maps Search URL and scrape until end automatically.")

    search_url_input = st.text_input(
        "Google Maps Search URL",
        value="https://www.google.com/maps/search/software+company+in+whitefield",
        key="search_url_mode"
    )

    mode2 = st.selectbox("Mode", ["safe", "fast"], index=0, key="search_mode")
    max_results2 = st.slider("Max Results", 10, 1000, 200, 10, key="search_max_results")

    start_search = st.button("Start Scraping (Search Query Mode)", key="search_start")

    if start_search:
        if "/maps/search" not in search_url_input:
            st.error("Please paste a valid Google Maps Search URL containing /maps/search")
        else:
            st.write("Using Search URL:")
            st.code(search_url_input)

            progress2 = st.progress(0.0)
            status2 = st.empty()

            data2 = scrape_google_maps_strict(
                search_url=search_url_input.strip(),
                mode=mode2,
                max_results=max_results2,
                ui_progress=progress2,
                ui_status=status2
            )

            if not data2:
                st.error("No results scraped. Try Safe mode and increase Max Results.")
            else:
                df2 = pd.DataFrame(data2)
                st.success(f"Scraped {len(df2)} results")
                st.dataframe(df2, use_container_width=True)

                st.download_button(
                    "Download CSV",
                    data=df2.to_csv(index=False).encode("utf-8"),
                    file_name="google_maps_search_query_results.csv",
                    mime="text/csv"
                )

                st.download_button(
                    "Download TSV",
                    data=df2.to_csv(index=False, sep="\t").encode("utf-8"),
                    file_name="google_maps_search_query_results.tsv",
                    mime="text/tab-separated-values"
                )

                st.download_button(
                    "Download JSON",
                    data=json.dumps(data2, ensure_ascii=False, indent=2).encode("utf-8"),
                    file_name="google_maps_search_query_results.json",
                    mime="application/json"
                )
