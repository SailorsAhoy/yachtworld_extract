import os
import csv
import time
import random
import cloudscraper
from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
from urllib.parse import urljoin, urlparse
import threading
from concurrent.futures import ThreadPoolExecutor
import json

# -------------------------------
# Enhanced User-Agents with rotation
# -------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", 
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0"
]

# Referrer rotation
REFERRERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    "https://yachtworld.com/",
    "https://www.yachtworld.com/boats/",
    ""
]

# -------------------------------
# CSV output file
# -------------------------------
OUTPUT_FILE = "Yachtworld_Extract.csv"

# -------------------------------
# Column Groups (UNCHANGED - DO NOT TOUCH)
# -------------------------------
DATAGROUP_A = [
    "id", "origin", "title", "location", "price_eu", "price_us", "Broker",
    "boat_type", "manufacturer", "boatmodel", "description", "other_details"
]

DATAGROUP_B = [
    "engine", "power", "engine_hours", "class", "length", "year", "model", "capacity"
]

DATAGROUP_C = [
    "Electrical Equipment", "Electronics", "Inside Equipment", "Outside Equipment", "Rigging", "Sails"
]

DATAGROUP_D = [
    "Engine_Make", "Engine Model", "Engine Year", "Total Power", "Engine Hours",
    "Engine Type", "Drive Type", "Fuel Type", "Propeller Type", "Propeller Material"
]

DATAGROUP_E = [
    "Length Overall", "Max Draft", "Beam", "Length at Waterline", "Windlass", "Liferaft Capacity",
    "Electrical Circuit", "Hull Material", "Fresh Water Tank", "Fuel Tank", "Holding Tank",
    "Guest Cabins", "Guest Heads", "Cruising Speed", "Max Speed", "Range", "Gross Tonnage",
    "Crew Cabins", "Crew Heads"
]

DATAGROUP_F = []
for i in range(1, 76):
    DATAGROUP_F.append(f"IMAGE_{i}")
    DATAGROUP_F.append(f"IMAGE_{i}_ALT")

ALL_COLUMNS = DATAGROUP_A + DATAGROUP_B + DATAGROUP_C + DATAGROUP_D + DATAGROUP_E + DATAGROUP_F

# Session pool for rotation
session_pool = []
session_lock = threading.Lock()
current_session_index = 0

def create_session_pool(pool_size=5):
    """Create a pool of sessions with different configurations."""
    global session_pool
    session_pool = []
    
    for i in range(pool_size):
        ua = random.choice(USER_AGENTS)
        session = cloudscraper.create_scraper(
            browser={
                'browser': random.choice(['chrome', 'firefox']),
                'platform': random.choice(['windows', 'darwin', 'linux']),
                'mobile': False
            }
        )
        
        # Randomize headers per session
        session.headers.update({
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': random.choice(['en-US,en;q=0.9', 'en-GB,en;q=0.9', 'en-US,en;q=0.8,fr;q=0.6']),
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': str(random.randint(0, 1)),
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': random.choice(['cross-site', 'same-origin', 'none']),
            'Sec-Fetch-User': '?1',
            'Cache-Control': random.choice(['max-age=0', 'no-cache', 'no-store']),
            'Referer': random.choice(REFERRERS)
        })
        
        session_pool.append(session)
    
    print(f"🔄 Created session pool with {pool_size} sessions")

def get_session():
    """Get next session from the pool with rotation."""
    global current_session_index
    with session_lock:
        session = session_pool[current_session_index]
        current_session_index = (current_session_index + 1) % len(session_pool)
        
        # Occasionally refresh headers
        if random.random() < 0.2:
            session.headers['User-Agent'] = random.choice(USER_AGENTS)
            session.headers['Referer'] = random.choice(REFERRERS)
        
        return session

def fetch(url: str, retries: int = 8) -> BeautifulSoup:
    """Ultimate anti-blocking fetch with advanced techniques."""
    
    # Initialize session pool if not done
    if not session_pool:
        create_session_pool(8)
    
    for attempt in range(retries):
        try:
            # Get session from pool
            session = get_session()
            
            # Progressive delay strategy
            base_delay = random.uniform(4, 10)
            if attempt > 0:
                base_delay *= (1.8 ** min(attempt, 4))  # Cap exponential growth
            
            # Add jitter to avoid detection patterns
            jitter = random.uniform(-0.5, 0.5) * base_delay
            total_delay = max(1, base_delay + jitter)
            
            if attempt > 0:
                print(f"Fetching {url} (attempt {attempt + 1}/{retries}), waiting {total_delay:.1f}s...")
            
            time.sleep(total_delay)
            
            # Randomize request parameters
            timeout = random.randint(45, 75)
            
            # Add occasional pre-request delay for very suspicious attempts
            if attempt > 3:
                extra_delay = random.uniform(10, 25)
                print(f"🐌 Extra cooling period: {extra_delay:.1f}s")
                time.sleep(extra_delay)
            
            # Make request
            response = session.get(
                url, 
                timeout=timeout, 
                allow_redirects=True,
                stream=False
            )
            
            # Handle response codes
            if response.status_code == 200:
                return BeautifulSoup(response.text, "lxml")
                
            elif response.status_code in (403, 429):
                # Escalating wait times for blocking
                if attempt < 3:
                    wait_time = random.uniform(30, 60) * (attempt + 1)
                else:
                    wait_time = random.uniform(90, 180) * (attempt - 2)
                    
                print(f"🚫 {response.status_code} received. Extended wait: {wait_time:.1f}s")
                
                # Recreate session pool on persistent blocks
                if attempt > 4:
                    print("🔄 Recreating session pool due to persistent blocking...")
                    create_session_pool(10)
                
                time.sleep(wait_time)
                continue
                
            elif response.status_code == 503:
                wait_time = random.uniform(60, 120) * (attempt + 1)
                print(f"🔧 Service unavailable (503). Waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
                
            elif response.status_code == 404:
                print(f"📴 Page not found (404): {url}")
                return None
                
            elif response.status_code in (301, 302, 307, 308):
                print(f"🔀 Redirect {response.status_code}, following...")
                continue
                
            else:
                response.raise_for_status()
                
        except requests.exceptions.Timeout:
            wait_time = random.uniform(15, 30) * (attempt + 1)
            print(f"⏰ Timeout error. Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
            
        except requests.exceptions.ConnectionError:
            wait_time = random.uniform(20, 40) * (attempt + 1)
            print(f"🔌 Connection error. Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
            
        except Exception as e:
            wait_time = random.uniform(10, 30) * (attempt + 1)
            print(f"❗ Unexpected error: {e}. Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
    
    print(f"💀 Failed to fetch {url} after {retries} attempts")
    return None

# ================================
# EXTRACTION SECTIONS - UNCHANGED
# ================================

def parse_boat(url: str) -> dict | None:
    """Parse one boat page into a dictionary row. Returns None if skipped."""
    soup = fetch(url)
    if soup is None:
        return None

    # Initialize row with all columns set to Null
    row = {col: "Null" for col in ALL_COLUMNS}
    row["origin"] = url

    # -------------------------------
    # Breadcrumbs
    # -------------------------------
    breadcrumb = soup.find("ul", class_="breadcrumb")
    if not breadcrumb:
        return None

    items = breadcrumb.find_all("li")
    if len(items) < 4:
        return None

    third_li = items[2].get_text(strip=True)
    if "Power" in third_li:
        return None
    if "Sail" not in third_li:
        return None

    row["boat_type"] = items[3].get_text(strip=True) if len(items) > 3 else "Null"

    if len(items) > 4 and items[4].find("a"):
        row["manufacturer"] = items[4].find("a").get("title", "Null")
    else:
        row["manufacturer"] = "Null"

    row["boatmodel"] = items[5].get_text(strip=True) if len(items) > 5 else "Null"

    # SECTION A EXTENSION
    # -------------------------------
    title_tag = soup.find("h1")
    if title_tag:
        row["title"] = title_tag.get_text(strip=True)

    location_tag = soup.find("p", class_="style-module_content__tmQCh style-module_content-6__CzZ47")
    if location_tag:
        row["location"] = location_tag.get_text(strip=True)

    price_section = soup.find("div", class_="style-module_priceSection__wa5Pn style-module_tppPriceSection__7x-f4")
    if price_section:
        price_tag = price_section.find("p")
        if price_tag:
            price_text = price_tag.get_text(strip=True)
            parts = price_text.split()
            for part in parts:
                if part.startswith("US$") or part.startswith("(US$"):
                    row["price_us"] = part.strip("()")
                else:
                    row["price_eu"] = part

    # -------------------------------
    #   Details (DATAGROUP_B)
    # -------------------------------
    details_div = soup.find("div", class_="details")
    if details_div:
        sub_div = details_div.find("div", class_="style-module_boatDetails__2wKB2")
        if sub_div:
            mapping = {
                "Engine": "engine",
                "Total Power": "power",
                "Engine Hours": "engine_hours",
                "Class": "class",
                "Length": "length",
                "Year": "year",
                "Model": "model",
                "Capacity": "capacity",
            }
            for h3 in sub_div.find_all("h3", class_="style-module_title__QGET2 style-module_title-9__QvhIY"):
                header_text = h3.get_text(strip=True)
                if header_text in mapping:
                    p_tag = h3.find_next("p", class_="style-module_content__tmQCh style-module_content-15__m8Mqo")
                    if p_tag:
                        row[mapping[header_text]] = p_tag.get_text(strip=True)

    # -------------------------------
    # Description
    # -------------------------------
    desc_summary = soup.find("summary", string="Description")
    if desc_summary:
        desc_div = desc_summary.find_next("div", class_="data-html-inner-wrapper")
        if desc_div:
            row["description"] = desc_div.get_text(" ", strip=True)

    # -------------------------------
    # Other Details
    # -------------------------------
    other_summary = soup.find("summary", string="Other Details")
    if other_summary:
        other_div = other_summary.find_next("div", class_="data-html")
        if other_div:
            content = other_div.decode_contents()
            row["other_details"] = " ".join(content.split())
    else:
        row["other_details"] = "Null"

    # -------------------------------
    # Features (6 sections)
    # -------------------------------
    features_summary = soup.find("summary", string="Features")
    if features_summary:
        section = features_summary.find_next("section", class_="data-details-wrapper")
        if section:
            for cell in section.find_all("div", class_="data-details-cell"):
                header = cell.find("h4")
                content = cell.find("div", class_="data-details-cell-content")
                if header and content:
                    header_text = header.get_text(strip=True)
                    if header_text in DATAGROUP_C:
                        items = [
                            span.get_text(strip=True).replace(":", "")
                            for span in content.find_all("span", class_="null")
                        ]
                        if items:
                            row[header_text] = " | ".join(items)

    # -------------------------------
    # Propulsion
    # -------------------------------
    propulsion_summary = soup.find("summary", string="Propulsion")
    if propulsion_summary:
        content = propulsion_summary.find_next("div", class_="data-details-cell-content")
        if content:
            label_map = {
                "Engine Make": "Engine_Make",
                "Engine Model": "Engine Model",
                "Engine Year": "Engine Year",
                "Total Power": "Total Power",
                "Engine Hours": "Engine Hours",
                "Engine Type": "Engine Type",
                "Drive Type": "Drive Type",
                "Fuel Type": "Fuel Type",
                "Propeller Type": "Propeller Type",
                "Propeller Material": "Propeller Material",
            }
            for p in content.find_all("p"):
                label = p.find("span", class_="null")
                val = p.find("span", class_="cell-content-value")
                if label and val:
                    key = label.get_text(strip=True).replace(":", "")
                    if key in label_map:
                        row[label_map[key]] = val.get_text(strip=True)

    # -------------------------------
    # Specifications
    # -------------------------------
    spec_summary = soup.find("summary", string="Specifications")
    if spec_summary:
        section = spec_summary.find_next("section", class_="data-details-wrapper")
        if section:
            for cell in section.find_all("div", class_="data-details-cell"):
                content = cell.find("div", class_="data-details-cell-content")
                if not content:
                    continue
                for p in content.find_all("p"):
                    label = p.find("span", class_="null")
                    val = p.find("span", class_="cell-content-value")
                    if label:
                        key = label.get_text(strip=True).replace(":", "")
                        if key in DATAGROUP_E:
                            row[key] = val.get_text(strip=True) if val else "Null"
        else:
            for col in DATAGROUP_E:
                row[col] = "Null"
    else:
        for col in DATAGROUP_E:
            row[col] = "Null"

    # -------------------------------
    # Broker
    # -------------------------------
    broker_div = soup.find("div", class_="style-module_listedByText__u6Ijx")
    if broker_div:
        h3 = broker_div.find_next("h3")
        if h3:
            row["Broker"] = h3.get_text(strip=True)

    # -------------------------------
    # Images (dynamic)
    # -------------------------------
    gallery = soup.find("div", class_="embla__container")
    if gallery:
        slides = gallery.find_all("div", class_="embla__slide")
        for i, slide in enumerate(slides, 1):
            if i > 75:
                break
            img = slide.find("img")
            if img and img.get("src"):
                row[f"IMAGE_{i}"] = img["src"]
                row[f"IMAGE_{i}_ALT"] = img.get("alt", "")

    # Ensure all missing columns are explicitly set to Null
    for col in ALL_COLUMNS:
        if col not in row:
            row[col] = "Null"

    return row


def main():
    print("🚀 Starting ULTIMATE YachtWorld scraper with advanced anti-blocking...")
    
    with open("source.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        urls = [row["url"] for row in reader]

    print(f"📊 Found {len(urls)} URLs to scrape")

    start_index = 0
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
            try:
                last_row = list(csv.DictReader(f))
                if last_row:
                    last_url = last_row[-1].get("origin", "")
                    if last_url and last_url in urls:
                        start_index = urls.index(last_url) + 1
                        print(f"📍 Resuming from index {start_index}")
            except:
                print("⚠️ Could not read existing file, starting fresh")

    # Initialize session pool
    create_session_pool(10)
    
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALL_COLUMNS)
        if start_index == 0:
            writer.writeheader()

        successful_scrapes = 0
        failed_scrapes = 0
        
        for i, url in enumerate(tqdm(urls[start_index:], desc="Scraping boats", unit="boat"), start_index):
            row = parse_boat(url)
            if row is None:
                failed_scrapes += 1
                continue
                
            # Ensure all columns exist
            for col in ALL_COLUMNS:
                if col not in row:
                    row[col] = "Null"
                    
            writer.writerow(row)
            f.flush()
            successful_scrapes += 1
            
            # Dynamic delay based on success/failure ratio
            base_delay = 8 if failed_scrapes > successful_scrapes else 5
            delay = random.uniform(base_delay, base_delay + 5)
            time.sleep(delay)
            
            # Status update every 5 successful scrapes
            if successful_scrapes % 5 == 0:
                success_rate = successful_scrapes / (successful_scrapes + failed_scrapes) * 100 if (successful_scrapes + failed_scrapes) > 0 else 0
                print(f"✅ Progress: {successful_scrapes} successful, {failed_scrapes} failed (Success rate: {success_rate:.1f}%)")

    print(f"🎉 Scraping completed! Successfully processed {successful_scrapes} boats, {failed_scrapes} failed")


if __name__ == "__main__":
    main()