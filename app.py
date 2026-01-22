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

def safe_goto(page, url, timeout_ms=120000, retries=3):
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            return True
        except:
            time.sleep(1.5 + attempt)
    return False

def scroll_until_end(page, max_results: int, ui_status=None):
    delay = 0.5
    stable_cycles = 0
    stable_limit = 10
    last_count = 0
    all_links = []

    while True:
        links = extract_place_links(page)
        for l in links:
            if l not in all_links:
                all_links.append(l)

        current_count = len(all_links)

        if ui_status:
            ui_status.write(f"Loaded cards: {current_count}")

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

def extract_place_links(page):
    cards = page.locator('a[href^="https://www.google.com/maps/place"]')
    links = []
    for i in range(cards.count()):
        href = cards.nth(i).get_attribute("href")
        if href and href not in links:
            links.append(href)
    return links

def extract_cards_data_from_results(page):
    """
    Very Fast mode:
    Scrape directly from visible result cards without opening each place.
    """
    cards = page.locator('a[href^="https://www.google.com/maps/place"]')
    rows = []

    for i in range(cards.count()):
        href = cards.nth(i).get_attribute("href")
        if not href:
            continue

        # container card
        container = cards.nth(i).locator(
            "xpath=ancestor::div[contains(@jsaction,'mouseover:pane')]"
        ).first

        name = rating = reviews = address = category = ""

        try:
            name = normalize_spaces(container.locator(".fontHeadlineSmall").first.inner_text(timeout=1000))
        except:
            name = ""

        try:
            aria = container.locator('[role="img"]').first.get_attribute("aria-label")
            if aria and "stars" in aria:
                rating = aria.split(" ")[0]
                parts = aria.split()
                if len(parts) >= 3:
                    reviews = parts[2].replace("(", "").replace(")", "")
        except:
            rating, reviews = "", ""

        # category + address are mixed in card text
        try:
            text_blob = normalize_spaces(container.inner_text(timeout=1000))
            # usually category appears after rating/reviews
            # address often contains digits
            addr_match = re.search(r"\d+[^|•\n]+", text_blob)
            if addr_match:
                address = normalize_spaces(addr_match.group(0))
        except:
            address = ""

        rows.append({
            "name": name,
            "rating": rating,
            "reviews": reviews,
            "phone": "",
            "industry": category,
            "full_address": address,
            "website": "",
            "google_maps_link": href,
            "status": "ok_card"
        })

    # Deduplicate by link
    unique = {}
    for r in rows:
        unique[r["google_maps_link"]] = r
    return list(unique.values())

def scrape_place_details(page, link: str):
    """
    Deep mode:
    Open each place link and extract full details.
    """
    ok = safe_goto(page, link, timeout_ms=120000, retries=3)
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

    time.sleep(1.2)

    title = rating = reviews = address = website = phone = category = ""

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

    return {
        "name": title,
        "rating": rating,
        "reviews": reviews,
        "phone": phone,
        "industry": category,
        "full_address": address,
        "website": website,
        "google_maps_link": link,
        "status": "ok_deep"
    }

def run_scraper(search_url: str, max_results: int, mode: str, ui_progress=None, ui_status=None):
    """
    mode = "very_fast" or "deep"
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Block heavy resources to speed up
        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "font", "media"]
            else route.continue_()
        )

        if ui_status:
            ui_status.write("Opening Google Maps search page...")

        ok = safe_goto(page, search_url, timeout_ms=150000, retries=3)
        if not ok:
            browser.close()
            return []

        time.sleep(2)

        if ui_status:
            ui_status.write("Scrolling results until end...")

        scroll_until_end(page, max_results=max_results, ui_status=ui_status)

        if mode == "very_fast":
            if ui_status:
                ui_status.write("Extracting data from cards (Very Fast)...")

            rows = extract_cards_data_from_results(page)
            rows = rows[:max_results]

            browser.close()
            return rows

        # Deep mode
        if ui_status:
            ui_status.write("Collecting place links for Deep Scrape...")

        links = extract_place_links(page)
        links = links[:max_results]

        results = []
        total = len(links)

        if ui_status:
            ui_status.write(f"Deep scraping {total} places...")

        for idx, link in enumerate(links, start=1):
            row = scrape_place_details(page, link)
            results.append(row)

            if ui_progress:
                ui_progress.progress(idx / total)

            if ui_status:
                ui_status.write(f"Scraped {idx}/{total}")

        browser.close()
        return results


tab1, tab2 = st.tabs(["Agent Mode", "Search Query Mode"])

with tab1:
    st.subheader("Agent Mode")
    query = st.text_input("Business Query", value="software company")
    location = st.text_input("Location", value="Whitefield Bangalore")
    max_results = st.slider("Max Results", 10, 1000, 200, 10)

    scrape_mode = st.selectbox("Scrape Mode", ["Very Fast (Cards)", "Deep Scrape (Full Details)"])

    start_agent = st.button("Start Scraping")

    if start_agent:
        search_url = build_maps_search_url(query=query, location=location)
        st.write("Generated Search URL:")
        st.code(search_url)

        progress = st.progress(0.0)
        status = st.empty()

        mode = "very_fast" if "Very Fast" in scrape_mode else "deep"

        data = run_scraper(
            search_url=search_url,
            max_results=max_results,
            mode=mode,
            ui_progress=progress,
            ui_status=status
        )

        df = pd.DataFrame(data)
        st.success(f"Scraped rows: {len(df)}")
        st.dataframe(df, use_container_width=True)

        st.download_button(
            "Download CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="google_maps_results.csv",
            mime="text/csv"
        )

with tab2:
    st.subheader("Search Query Mode")
    search_url_input = st.text_input(
        "Google Maps Search URL",
        value="https://www.google.com/maps/search/software+company+in+whitefield"
    )

    max_results2 = st.slider("Max Results (Search Mode)", 10, 2000, 300, 10)

    scrape_mode2 = st.selectbox(
        "Scrape Mode (Search Mode)",
        ["Very Fast (Cards)", "Deep Scrape (Full Details)"],
        key="mode2"
    )

    start_search = st.button("Start Scraping (Search URL)")

    if start_search:
        if "/maps/search" not in search_url_input:
            st.error("Please paste a valid Google Maps Search URL containing /maps/search")
        else:
            st.write("Using Search URL:")
            st.code(search_url_input)

            progress2 = st.progress(0.0)
            status2 = st.empty()

            mode = "very_fast" if "Very Fast" in scrape_mode2 else "deep"

            data2 = run_scraper(
                search_url=search_url_input.strip(),
                max_results=max_results2,
                mode=mode,
                ui_progress=progress2,
                ui_status=status2
            )

            df2 = pd.DataFrame(data2)
            st.success(f"Scraped rows: {len(df2)}")
            st.dataframe(df2, use_container_width=True)

            st.download_button(
                "Download CSV",
                data=df2.to_csv(index=False).encode("utf-8"),
                file_name="google_maps_results.csv",
                mime="text/csv"
            )
