1. What Is a Web Scraper?
Imagine you want to collect information from hundreds of web pages — prices, descriptions, photos — but doing it by hand would take weeks. A web scraper is a program that does this automatically: it opens web pages just like a browser does, reads the content, and saves the pieces you care about into a file.

This specific scraper targets SUUMO, Japan's largest real estate website. It automatically visits thousands of Tokyo condo listings and collects:
Listing details — price, layout (LDK), floor, area, year built, address, nearest station
Interior photos — kitchen, bathroom, bedroom, living room, etc. (exterior and floor-plan photos are excluded)

💡 Note: The scraper does not modify or submit anything on the website. It only reads pages, exactly like a human would — just much faster.


2. Key Concepts You Need to Know
Before diving into the steps, here are a few technical words used throughout this document:

Term
What it means
HTML
The raw text that makes up a web page. It tells the browser what to show and where to put things.
URL
A web address, like https://suumo.jp/…. Every page and every image has its own URL.
HTTP request
When the scraper 'visits' a page, it sends an HTTP request — basically saying 'please send me that page'.
CSV
A spreadsheet-like text file where each row is one record and columns are separated by commas. Opens directly in Excel or Google Sheets.
GCS / bucket
Google Cloud Storage. A bucket is like a folder in the cloud where files (images) are stored.
source_id
A unique number that identifies each individual listing, extracted from the SUUMO URL.
Batch
A chunk of ~500 listings processed in one run. The full dataset is split into 3 batches.
Checkpoint
A saved note of where the scraper stopped, so it can resume from the same place next time.
regex
A pattern-matching language used to find specific pieces of text inside HTML, like prices or image URLs.
BeautifulSoup
A Python library that makes it easy to search and navigate HTML, like a map for web pages.


3. What the Scraper Does — High-Level Overview
The scraper follows the same pattern a human researcher would use, just automated:

Step 1 — Open the SUUMO search results page (like searching 'used condos, Tokyo').
Step 2 — Collect all the links to individual listings shown on that page.
Step 3 — Go to the next page of results and collect more links.
Step 4 — Visit each listing link, read the details (price, area, etc.), and find all interior photos.
Step 5 — Download the photos and save everything to files.
Step 6 — Save a checkpoint so that if the run is interrupted, it can continue later without repeating work.

This is repeated across 3 batches of ~500 listings each, covering all ~25,000 Tokyo listings in total.

4. Step-by-Step Code Walkthrough
4.1  Starting the Program
When you run the script from the command line, Python reads the arguments you provide (like --batch 1 or --gcs-bucket my-bucket) and calls the main() function.

# Run batch 1, store images in Google Cloud Storage:
python suumo_scraper.py --batch 1 --gcs-bucket my-suumo-bucket

# Test with just 1 results page, save locally:
python suumo_scraper.py --pages 1 --out-dir test_out --img-dir test_images


The main() function figures out which pages to scrape and where to save things, then hands off control to the crawl() function.

4.2  Batch System
Because the full dataset has ~25,000 listings with ~100,000 images, it would be impractical to scrape everything in a single run. The batch system splits the work into manageable chunks:

Batch 1 — scrapes listings 1 to 500 (25 results pages × 20 listings/page)
Batch 2 — reads the checkpoint left by Batch 1 and continues from listing 501
Batch 3 — continues from Batch 2's checkpoint and runs until the last listing

💡 Note: After each batch finishes, a hidden file called .checkpoint_next_url.txt is saved in the output folder. The next batch reads this file automatically to know where to pick up.


4.3  Fetching the Search Results Page
The crawl() function starts by opening the SUUMO search results page — the same URL you would type into a browser. Internally it calls fetch_html(), which sends an HTTP request and gets back the raw HTML text of the page.

Think of this as the scraper opening a browser tab and waiting for the page to load. Instead of rendering it visually, it just receives the raw HTML code.

The hardcoded starting URL covers all 23 Tokyo wards and filters for used condos (中古マンション) with any price and size. This URL is stored in the SUUMO_URL constant near the bottom of the file.

4.4  Collecting Listing Links
The function extract_detail_links() scans the HTML of the results page and collects all links that point to individual property listings. It recognises two URL patterns:

/ms/chuko/…/nc_XXXXXX/ — the standard used-condo detail page format
/jj/bukken/shosai/?jnc=XXXXXX — the general property search detail page format

Each results page typically shows 20 listings. The scraper repeats this for as many results pages as the current batch needs, following the 'next page' button each time.

The next_page_url() function looks for the '次へ' (next) link in the HTML to find the URL of the next results page.

4.5  Visiting Each Listing and Extracting Data
For each collected listing URL, the scraper calls scrape_listing(), which:

Opens the listing page by fetching its HTML.
Parses the listing details by calling parse_fields().
Finds all interior photos using two parallel methods.
Downloads and saves the photos.
Writes one row to listings.csv and one row per image to images.csv.

4.6  Parsing Listing Details
The parse_fields() function reads the structured data from each listing page. SUUMO displays property details in HTML tables and lists (<table>, <dl>), which the code scans for key–value pairs.

For each field, the code first tries to find a label like '価格' (price) or '専有面積' (exclusive area) in those table cells, then reads the value next to it. If that fails, it falls back to searching the entire page text with a regex pattern.

Here is what each field does:

Term
What it means
price_man_yen
The asking price in 万円 (10,000 yen units). Handles formats like '5,480万円' or '1億2,000万円'.
layout
Room layout code like '3LDK' or '1K'. Extracted from the 間取り field.
area_sqm
Exclusive floor area in square metres from the 専有面積 field.
year_built
The year the building was completed, from 築年月 (year/month built).
floor_number
Which floor the unit is on (e.g. 5 for the 5th floor).
floors_total
How many floors the building has in total.
address
The street address of the property.
nearest_station
The closest train station name, extracted from the 交通 (transport) section.
walk_minutes
How many minutes on foot from the nearest station.
interior_image_count
How many interior photos were successfully downloaded for this listing.


4.7  Finding Interior Photos
Finding photos is the most complex part of the scraper because SUUMO uses JavaScript and lazy-loading, which means images are not always directly visible in the raw HTML. The scraper uses two parallel strategies:

Strategy A — DOM Extraction
The function extract_dom_candidates() uses BeautifulSoup to navigate the HTML structure (the Document Object Model, or DOM). It searches for any tag that might contain an image URL:
<img src='...'> — the standard image tag
data-src, data-lazy, data-original — attributes used by lazy-loading libraries
data-srcset — responsive image definitions

For each image URL found, it also captures the surrounding context text — the alt text, nearby headings, and parent element labels. This context is used later to decide whether the photo is of an interior room.

Strategy B — Regex Extraction
The function extract_regex_candidates() searches the raw HTML text directly using regular expressions. Some image URLs are embedded inside JavaScript code or JSON data blobs that BeautifulSoup cannot easily parse. The regex patterns look for any string that looks like an image URL:

# Matches any URL ending in .jpg, .png, .webp, etc.
_RAW_IMG_URL = re.compile(r'(?:https?:)?//[^"\'>\s]+?\.(?:jpg|jpeg|png|webp)[^"\'>\s]*')

# Matches SUUMO's specific image path pattern
_SUUMO_GAZO  = re.compile(r'[^"\'>\s]*?/front/gazo/[^"\'>\s]+')


Probing Additional Pages
Some listings link to separate photo gallery pages. The function collect_all_candidates() also tries common sub-URLs like /photo/, /images/, /gallery/ to find additional photos that are not on the main listing page.

4.8  Filtering: Interior Photos Only
Not all photos on a listing page are useful. SUUMO listings also include floor plans, building exteriors, lobby photos, area maps, and neighbourhood photos. The function select_interior_images() filters them out.

It uses two lists:
Allow list — Japanese and English words that indicate an interior photo: リビング (living room), キッチン (kitchen), 浴室 (bathroom), 玄関 (entrance), etc.
Block list — words that indicate a non-interior photo: 間取り図 (floor plan), 外観 (exterior), ロビー (lobby), 地図 (map), etc.

The context text captured alongside each image URL (from Step 4.7) is checked against both lists. If the context contains a blocked word and no interior word, the image is dropped. If it matches an interior word, the image is kept and given a specific tag.

The possible tags assigned to kept images are:

living, dining, kitchen, bedroom, room
bathroom, washroom, toilet, entrance, closet, storage
interior — used when a photo is clearly inside but doesn't match a specific room
other_room — explicitly labelled as a room other than the living room

4.9  Downloading Photos
Once the interior photos are selected, download_image() downloads each one at its original resolution — no resizing or compression is applied.

The function tries up to 3 times if a download fails. It also checks that the response actually contains image data (not an error page or HTML document).

Each image is saved with a filename in the format <source_id>_<url_hash>.<ext>. For example: 12345678_a1b2c3d4e5f6g7h8.jpg. The URL hash ensures that the same image is never downloaded twice even if the scraper runs again.

If a Google Cloud Storage bucket is provided, the image is uploaded directly to GCS without being saved to local disk first. Otherwise it is saved to the local img-dir folder.

4.10  Saving the Data
After each listing is fully processed, the scraper appends one row to each CSV file. This append-as-you-go approach means that if the scraper crashes, all work up to that point is already saved — the scraper can resume from the checkpoint and skip listings it already processed.

5. Output Files
After running the scraper you will find the following files:

File / Folder
Where it lives
What is inside
batch1/listings.csv
Your computer (local)
One row per listing with all structured fields
batch1/images.csv
Your computer (local)
One row per image with the GCS path or local path
batch1/.checkpoint_next_url.txt
Your computer (local)
The URL where Batch 2 should start from
gs://bucket/suumo_images/<source_id>/<id>_<hash>.jpg
Google Cloud Storage
The actual image files at original resolution


5.1  listings.csv Column Reference

Term
What it means
source
Always 'suumo' — identifies which website the data came from
source_id
The unique numeric ID of the listing (from the URL)
url
The full URL of the listing detail page
price_man_yen
Asking price in 万円 (multiply by 10,000 for yen, by ~67 for USD)
layout
Room layout: e.g. 3LDK, 1K, 2SLDK
area_sqm
Exclusive floor area in square metres
year_built
Year the building was completed
floor_number
Floor the unit is on
floors_total
Total number of floors in the building
address
Street address
nearest_station
Closest train/subway station
walk_minutes
Walking time to nearest station in minutes
interior_image_count
Number of interior photos successfully downloaded


5.2  images.csv Column Reference

Term
What it means
source
Always 'suumo'
source_id
Links back to the listing in listings.csv
listing_url
URL of the listing page this image came from
image_url
Original URL of the image on SUUMO's servers
image_tag
Room type label: kitchen, bathroom, living, etc.
image_path
Where the image was saved: a gs:// URI or a local file path
image_sha256
A fingerprint of the file (empty when stored in GCS)


6. Politeness — Not Overloading the Server
Good scraping practice means not sending too many requests too quickly, which could slow down the website for real users. The scraper includes deliberate pauses between requests:

polite_sleep(base=1.5, jitter=0.9) — waits ~1.5 seconds between listing requests, with a small random extra delay to avoid patterns that look automated
polite_sleep(0.12, 0.14) — shorter pause between photo page probes

The random component (jitter) makes the traffic pattern look more human-like and less like a bot hammering the server at perfectly regular intervals.

7. Resume and Deduplication
Two mechanisms ensure work is never repeated:

7.1  Checkpoint File
At the end of each batch run, the URL of the next results page is written to .checkpoint_next_url.txt inside the batch output folder. When the next batch starts, it reads this file and begins fetching from that URL instead of the beginning.

7.2  Already-scraped IDs
When the scraper starts (or resumes), it reads listings.csv and builds a set of all source_id values already recorded. Any listing whose ID is already in that set is silently skipped. This means if Batch 2 is interrupted halfway, rerunning it will skip the first 250 already-done listings and continue from where it stopped.

7.3  Image deduplication
Each image filename is derived from a hash of its original URL (SHA-256). If the same image URL appears multiple times, the hash will be identical and the file will only be downloaded once.

8. Google Cloud Storage Integration
When you pass a bucket name with --gcs-bucket, the scraper uploads each image directly to GCS instead of saving it to your local disk. This avoids filling your computer's hard drive with ~60 GB of photos.

The GCSStore class handles all communication with GCS:

upload_from_bytes() — uploads image data directly from memory (no temporary file on disk)
exists() — checks if an image is already in the bucket before trying to re-upload
blob_name() — constructs the path inside the bucket: suumo_images/<source_id>/<id>_<hash>.jpg

The image_path column in images.csv will contain a gs:// URI like:

gs://my-suumo-bucket/suumo_images/12345678/12345678_a1b2c3d4e5f6g7h8.jpg


This URI can be used directly in Python, BigQuery, Vertex AI, or any Google Cloud service to access the image.

9. How to Run the Scraper
9.1  Prerequisites
Python 3.9 or later installed on your Mac
Install required libraries: pip install requests beautifulsoup4 lxml tqdm google-cloud-storage
Authenticate with Google Cloud: gcloud auth application-default login
Create a GCS bucket: gcloud storage buckets create gs://your-bucket --location=asia-northeast1

9.2  Test Run (1 page, local)
python suumo_scraper.py --pages 1 --out-dir test_out --img-dir test_images

This scrapes ~20 listings and saves everything locally. Check test_out/listings.csv to verify data is being collected correctly.

9.3  Full Run with GCS
# Run all three batches in sequence:
python suumo_scraper.py --batch 1 --gcs-bucket your-bucket-name
python suumo_scraper.py --batch 2 --gcs-bucket your-bucket-name
python suumo_scraper.py --batch 3 --gcs-bucket your-bucket-name

Each batch takes approximately 3–5 hours. They can be run on separate days — the checkpoint system ensures continuity.

9.4  Command-Line Options Reference

Term
What it means
--batch N
Shortcut for --pages 25 --out-dir batchN --img-dir imagesN. Use 1, 2, or 3.
--pages N
How many results pages to scrape in this run (20 listings per page).
--out-dir PATH
Folder where listings.csv, images.csv, and the checkpoint file are saved.
--img-dir PATH
Folder where images are saved locally (not used when --gcs-bucket is set).
--gcs-bucket NAME
Name of the GCS bucket to upload images to.
--gcs-prefix PATH
Subfolder inside the bucket (default: suumo_images).
--delay N
Seconds to wait between listing requests (default: 1.5).
--no-resume
Wipe the output folder and start completely fresh.
--start-page N
Start from page N instead of page 1 (rarely needed; use checkpoints instead).


10. Code Structure at a Glance
The script is a single Python file. Its sections are:

Term
What it means
Constants & lists
INTERIOR_LABELS, BLOCK_LABELS, IMAGE_TAG_RULES — the allow/block word lists and image tag mappings
Data models
ListingRow and ImageRow dataclasses — define exactly which fields go into each CSV
Utility helpers
polite_sleep, decode_js, sha256_file, safe_filename — small reusable functions
GCSStore class
All Google Cloud Storage upload and existence-check logic
HTTP helpers
fetch_html, download_image — all network communication
URL filtering
looks_like_photo, is_listing_photo — decide if a URL is a real property photo
Image URL extraction
extract_dom_candidates, extract_regex_candidates — find image URLs in HTML
Section classification
classify_label, extract_tag, select_interior_images — filter and tag images
Listing page parsing
extract_detail_links, _source_id, _canon_url — navigate search results
Field parsing
parse_price, parse_layout, parse_area … parse_fields — extract structured data
Photo page discovery
candidate_photo_pages, candidate_data_urls — find hidden image galleries
Multi-source aggregation
collect_all_candidates — orchestrate all discovery methods together
CSV helpers
_ensure_header, _append_row — safe append-only CSV writing
Per-listing scrape
scrape_listing — the main function called for each individual property
Crawl loop
crawl — the outer loop that drives everything: pages → listings → images
main / CLI
argument parsing, batch shortcuts, checkpoint loading, startup banner


11. Common Issues and Solutions

Term
What it means
0 listings found
The URL pattern on SUUMO may have changed. Try opening the search URL in a browser and check if links still contain /ms/chuko/ or /jj/bukken/shosai/.
0 interior images per listing
SUUMO may be using a new JavaScript lazy-loading method. Inspect a listing page in your browser's developer tools (F12 → Network → Images) to see image URL patterns.
HTTP 403 / 429 errors
The scraper is being rate-limited. Increase --delay to 3.0 or more.
GCS permission denied
Run gcloud auth application-default login again, or check that your GCS bucket exists.
Disk full
If saving locally, images can easily exceed 20 GB per batch. Use --gcs-bucket to save to the cloud instead.
Checkpoint not found
Make sure you run batches in order (1 → 2 → 3) and that the batch1/ folder exists before starting batch 2.
Module not found error
Run: pip install requests beautifulsoup4 lxml tqdm google-cloud-storage
