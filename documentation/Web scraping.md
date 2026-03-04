```markdown
# SUUMO Used-Condos Scraper (Tokyo wards) — Documentation

## What this script does
This Python script crawls SUUMO search results for **中古マンション (used condos)** in Tokyo and collects:
1) **Listing metadata** (price, layout, area, year built, floors, address, station + walk minutes, etc.)
2) **Listing photos** (downloads images to disk + records image URLs and filenames in a CSV)

It is **restartable**: it won’t re-scrape listings already saved in `listings.csv`, and it won’t re-download images already recorded in `images.csv`.

---

## High-level workflow
1. Start at the SUUMO results page defined in `URL`.
2. Extract all detail listing URLs from the results page.
3. For each listing URL:
   - Fetch the listing page HTML
   - Parse listing attributes (price, layout, area, year, floors, address, station, etc.)
   - Extract image URLs from HTML (and optionally probe additional “photo/gallery” pages)
   - Download images to `raw_data/suumo_images/<source_id>/...`
   - Append one row to `raw_data/listings.csv`
   - Append one row per image to `raw_data/images.csv`
4. Go to the “next” results page and repeat until hitting `MAX_LISTINGS` or no more pages.

---

## Outputs (files & folders)

### 1) Listings CSV
**Path:** `raw_data/listings.csv`

**Columns**
- `source_id` — unique ID for the listing (derived from SUUMO `/nc_<id>/` when possible; otherwise hash)
- `url` — canonical listing URL
- `price_man_yen` — price in **万円** as an integer (supports 億 + 万円 formats)
- `layout` — e.g., `2LDK`, `3LDK`, `1R`, etc.
- `area_sqm` — float area in square meters (㎡)
- `year_built` — integer year (e.g., 2008)
- `floor_number` — integer floor the unit is on (if detected)
- `floors_total` — integer building floors (if detected)
- `address` — listing address / location text
- `nearest_station` — station name (e.g., `渋谷駅`)
- `walk_minutes` — integer minutes to station (if detected)
- `image_count` — number of images downloaded *in this run* for that listing

> Note: `image_count` reflects images downloaded/recorded by the script, not necessarily the total available on SUUMO.

### 2) Images CSV
**Path:** `raw_data/images.csv`

**Columns**
- `source_id` — matches listing `source_id`
- `listing_url` — listing page URL where images were collected
- `image_url` — the actual image URL (as found)
- `image_name` — local filename saved under the listing’s folder

### 3) Image folder structure
**Root:** `raw_data/suumo_images/`

Images are stored under:
```

raw_data/suumo_images/<source_id>/<source_id>_<hash>.<ext>

````

Filename is deterministic per `(source_id, image_url)`:
- `<hash>` = first 10 chars of SHA1(image_url)
- extension inferred from URL path; defaults to `.jpg` if unknown

---

## How to run

### Requirements
Install dependencies:
- `requests`
- `beautifulsoup4`
- `lxml` (recommended; script falls back to `html.parser` if unavailable)

Example:
```bash
pip install requests beautifulsoup4 lxml
````

### Run the scraper

From the project root:

```bash
python3 path/to/this_script.py
```

The script will create:

* `raw_data/listings.csv`
* `raw_data/images.csv`
* `raw_data/suumo_images/`

---

## Key configuration (edit near top of script)

### Target URL

`URL` is the SUUMO search URL.
It currently targets **Tokyo used condos** and multiple `sc=...` area codes (wards).
To scrape a different area/category, replace the URL.

### Output paths

* `OUTPUT_DIR = "raw_data/"`
* `LISTINGS_CSV = raw_data/listings.csv`
* `IMAGES_CSV = raw_data/images.csv`
* `IMAGE_ROOT = raw_data/suumo_images`

### Crawl limits & behavior

* `MAX_LISTINGS` — maximum number of listings to scrape per run
* `MAX_WORKERS` — number of concurrent listing workers (default `1` = slower but safer)
* `DELAY_SECONDS` — delay between results pages
* Retry behavior:

  * `RETRY_STATUS_CODES = {429, 500, 502, 503, 504}`
  * `RETRY_ATTEMPTS`, `RETRY_BASE_SLEEP_SECONDS` (exponential backoff + jitter)

### Image probing behavior

* `MAX_IMAGE_PROBE_URLS`

  * `0` = only extract images from main listing HTML
  * `>0` = probe additional “photo/gallery/media” pages or data URLs (more requests; usually more images)

---

## How it avoids duplicates (restart-safe)

### Listing dedupe

On startup, it reads existing `raw_data/listings.csv` and collects prior `source_id`s.
If a listing’s `source_id` is already present, it will be skipped.

### Image dedupe

On startup, it reads existing `raw_data/images.csv` and collects prior `(source_id, image_url)` pairs.
If the same image is encountered again, it will not be downloaded again.

---

## Parsing details (what it tries to extract)

### Price (`price_man_yen`)

Parses formats like:

* `4980万円`
* `1億2000万円`

Stored as integer **in 万円**:

* `4980万円` → `4980`
* `1億2000万円` → `22000` (because 1億 = 10000万円)

### Layout (`layout`)

Detects common layouts like:

* `1R`, `1K`, `1DK`, `1LDK`, `2LDK`, etc.
* `ワンルーム`
* tries to keep `+S` patterns when present

### Area (`area_sqm`)

Parses `専有面積` in:

* `㎡`, `m2`, `m²`, `平米`

### Year built (`year_built`)

Parses year from `築年月` / `完成時期（築年月）` as `YYYY`.

### Floors (`floor_number`, `floors_total`)

Attempts to parse:

* unit floor (所在階)
* building total floors (地上○階 / ○階建)
  Best effort; may be `None` if not found.

### Nearest station & walk minutes

Looks for patterns like:

* `「渋谷」徒歩8分` → `渋谷駅`, `8`

---

## Known limitations / gotchas

* SUUMO HTML structure can change; parsing is “best effort.”
* Some image URLs may be dynamic/protected; downloads can fail.
* Higher concurrency (`MAX_WORKERS > 1`) increases risk of 429 rate limits.
* `image_count` is the number of images successfully downloaded for that listing **in this run**.

---

## Tips (common operations)

### Quick test run (fewer listings)

```python
MAX_LISTINGS = 20
```

### Try to capture more images per listing

```python
MAX_IMAGE_PROBE_URLS = 8
```

### If you’re getting blocked (429)

* Keep `MAX_WORKERS = 1`
* Increase delays/backoff:

```python
DELAY_SECONDS = 3.0
RETRY_BASE_SLEEP_SECONDS = 3.0
```

### Start from scratch

Delete:

* `raw_data/listings.csv`
* `raw_data/images.csv`
* `raw_data/suumo_images/`
  Then rerun.

---

## Entry point

The script runs via:

```python
if __name__ == "__main__":
    crawl()
```

`crawl()` is the main orchestrator: initializes outputs, loads dedupe state, iterates result pages, and schedules listing scraping tasks.

```
```
