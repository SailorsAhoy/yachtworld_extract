import os
import csv
import time
import random
import cloudscraper
from bs4 import BeautifulSoup
from tqdm import tqdm

# -------------------------------
# User-Agents for rotation
# -------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile Safari/604.1",
]

# -------------------------------
# CSV output file
# -------------------------------
OUTPUT_FILE = "Yachtworld_Extract.csv"

# -------------------------------
# Column Groups
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


def fetch(url: str, retries: int = 5) -> BeautifulSoup:
    """Fetch URL with Cloudscraper + retry logic + rotating UA."""
    for attempt in range(retries):
        ua = random.choice(USER_AGENTS)
        scraper = cloudscraper.create_scraper(browser={"custom": ua})
        try:
            r = scraper.get(url, timeout=30)
            if r.status_code in (403, 429):  # Forbidden or Too Many Requests
                wait = 5 * (attempt + 1)
                print(f"⚠️ {r.status_code} received. Retrying in {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"⚠️ Error {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise Exception(f"Failed to fetch {url} after {retries} attempts")


def parse_boat(url: str) -> dict | None:
    """Parse one boat page into a dictionary row. Returns None if skipped."""
    soup = fetch(url)

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
    with open("source.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        urls = [row["url"] for row in reader]

    start_index = 0
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
            last_row = list(csv.DictReader(f))
            if last_row:
                last_url = None
                for col in ALL_COLUMNS:
                    if col in last_row[-1]:
                        if last_row[-1][col] != "Null":
                            last_url = last_row[-1][col]
                            break
                if last_url and last_url in urls:
                    start_index = urls.index(last_url) + 1

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALL_COLUMNS)
        if start_index == 0:
            writer.writeheader()

        for url in tqdm(urls[start_index:], desc="Scraping boats", unit="boat"):
            row = parse_boat(url)
            if row is None:
                continue
            for col in ALL_COLUMNS:
                if col not in row:
                    row[col] = "Null"
            writer.writerow(row)
            f.flush()
            time.sleep(random.uniform(1, 3))


if __name__ == "__main__":
    main()
