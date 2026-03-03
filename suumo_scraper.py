#!/usr/bin/env python3

import csv
import hashlib
import html as html_lib
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter


URL = (
    "https://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13"
    "&jspIdFlg=patternShikugun"
    "&sc=13101&sc=13102&sc=13103&sc=13104&sc=13105&sc=13113"
    "&sc=13106&sc=13107&sc=13108&sc=13118&sc=13121&sc=13122&sc=13123"
    "&sc=13109&sc=13110&sc=13111&sc=13112&sc=13114&sc=13115&sc=13120"
    "&sc=13116&sc=13117&sc=13119"
    "&kb=1&kt=9999999&mb=0&mt=9999999"
    "&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999&srch_navi=1"
)

OUTPUT_DIR = "raw_data/"
LISTINGS_CSV = os.path.join(OUTPUT_DIR, "listings.csv")
IMAGES_CSV = os.path.join(OUTPUT_DIR, "images.csv")
IMAGE_ROOT = os.path.join("raw_data", "suumo_images")

LISTINGS_COLUMNS = [
    "source_id",
    "url",
    "price_man_yen",
    "layout",
    "area_sqm",
    "year_built",
    "floor_number",
    "floors_total",
    "address",
    "nearest_station",
    "walk_minutes",
    "image_count",
]
IMAGES_COLUMNS = ["source_id", "listing_url", "image_url", "image_name"]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

DELAY_SECONDS = 2.0
MAX_WORKERS = 1
MAX_LISTINGS = 1000
MAX_IMAGE_PROBE_URLS = 0
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
RETRY_ATTEMPTS = 6
RETRY_BASE_SLEEP_SECONDS = 2.5

FW_DIGITS = str.maketrans("０１２３４５６７８９，．", "0123456789,.")
IMG_EXT_RE = re.compile(r"\.(jpg|jpeg|png|webp|avif)(?:$|[?&])", re.I)
RAW_IMG_URL_RE = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]+?\.(?:jpg|jpeg|png|webp|avif)[^"\'<>\s]*', re.I)
RAW_SUUMO_GAZO_RE = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]*?/front/gazo/[^"\'<>\s]+', re.I)
RAW_SUUMO_MEDIA_RE = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]*?/(?:photo|photos|image|images|gallery|media)/[^"\'<>\s]*', re.I)
RAW_PATTERNS = (RAW_IMG_URL_RE, RAW_SUUMO_GAZO_RE, RAW_SUUMO_MEDIA_RE)
NON_PHOTO_HINTS = (
    "logo",
    "icon",
    "sprite",
    "banner",
    "button",
    "favicon",
    "apple-touch-icon",
    "qr",
    "map",
    "avatar",
    "/common/",
    "/parts/",
    "resizeimage",
)
SUUMO_MEDIA_PATH_HINTS = (
    "/front/gazo/",
    "/photo/",
    "/photos/",
    "/image/",
    "/images/",
    "/gallery/",
    "/media/",
)
_THREAD_LOCAL = local()


def norm_ws(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def norm_num(text):
    return (text or "").translate(FW_DIGITS)


def make_soup(html):
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def image_filename_from_url(sid, image_url):
    suffix = hashlib.sha1(image_url.encode()).hexdigest()[:10]
    ext = os.path.splitext(urlparse(image_url).path)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".avif"):
        ext = ".jpg"
    return f"{sid}_{suffix}{ext}"


def migrate_images_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = (row.get("source_id") or "").strip()
            listing_url = (row.get("listing_url") or "").strip()
            image_url = (row.get("image_url") or "").strip()
            image_name = (row.get("image_name") or "").strip()
            if not image_name and sid and image_url:
                image_name = image_filename_from_url(sid, image_url)
            rows.append(
                {
                    "source_id": sid,
                    "listing_url": listing_url,
                    "image_url": image_url,
                    "image_name": image_name,
                }
            )

    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=IMAGES_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, path)


def ensure_csv(path, columns):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, newline="", encoding="utf-8") as f:
            header = next(csv.reader(f), [])
        if header != columns:
            if path == IMAGES_CSV and {"source_id", "listing_url", "image_url"}.issubset(set(header)):
                migrate_images_csv(path)
                return
            raise ValueError(f"{path} header mismatch. Expected {columns}, got {header}")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=columns).writeheader()


def append_rows(path, columns, rows):
    if not rows:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=columns).writerows(rows)


def load_existing_source_ids():
    out = set()
    if os.path.exists(LISTINGS_CSV):
        with open(LISTINGS_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sid = (row.get("source_id") or "").strip()
                if sid:
                    out.add(sid)
    return out


def load_existing_image_keys():
    out = set()
    if os.path.exists(IMAGES_CSV):
        with open(IMAGES_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sid = (row.get("source_id") or "").strip()
                image_url = (row.get("image_url") or "").strip()
                if sid and image_url:
                    out.add((sid, image_url))
    return out


def build_session():
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ja,en-US;q=0.8,en;q=0.6"})
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS * 4, pool_maxsize=MAX_WORKERS * 4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def worker_session():
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = build_session()
        _THREAD_LOCAL.session = session
    return session


def _retry_sleep_seconds(attempt, base_sleep):
    return base_sleep * (2 ** (attempt - 1)) + random.uniform(0.0, 0.5)


def fetch_html(session, url, attempts=RETRY_ATTEMPTS, base_sleep=RETRY_BASE_SLEEP_SECONDS):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            r = session.get(url, timeout=30)

            if r.status_code in RETRY_STATUS_CODES:
                if attempt >= attempts:
                    r.raise_for_status()

                wait_s = _retry_sleep_seconds(attempt, base_sleep)
                retry_after = r.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_s = max(wait_s, float(retry_after))

                print(
                    f"[retry] status={r.status_code} attempt={attempt}/{attempts} "
                    f"wait={wait_s:.1f}s url={url}"
                )
                time.sleep(wait_s)
                continue

            r.raise_for_status()
            return r.text

        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt >= attempts:
                raise

            wait_s = _retry_sleep_seconds(attempt, base_sleep)
            print(
                f"[retry] network_error attempt={attempt}/{attempts} "
                f"wait={wait_s:.1f}s url={url}"
            )
            time.sleep(wait_s)

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to fetch URL after retries: {url}")


def source_id(url):
    m = re.search(r"/nc_(\d+)/", url)
    return m.group(1) if m else hashlib.md5(url.encode()).hexdigest()[:12]


def canon_listing_url(url):
    p = urlparse(url)
    path = p.path or "/"
    if "/ms/chuko/" in path and "/nc_" in path:
        if not path.endswith("/"):
            path += "/"
        return p._replace(path=path, query="", fragment="").geturl()
    return url.split("#")[0]


def detail_links(soup, base_url):
    links = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/ms/chuko/" in href and "/nc_" in href:
            links.add(canon_listing_url(urljoin(base_url, href)))
    return sorted(links)


def next_page(soup, base_url):
    a = soup.find("a", rel=lambda v: v and "next" in v)
    if a and a.get("href"):
        return urljoin(base_url, a["href"])
    for a in soup.select("a[href]"):
        if norm_ws(a.get_text()) == "次へ":
            return urljoin(base_url, a["href"])
    return None


def pairs_from_page(soup):
    out = []
    for tr in soup.select("tr"):
        th, td = tr.find("th"), tr.find("td")
        if th and td:
            k = norm_ws(th.get_text(" ", strip=True))
            v = norm_ws(td.get_text(" ", strip=True))
            if k and v:
                out.append((k, v))
    for dl in soup.select("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        if len(dts) == len(dds):
            for dt, dd in zip(dts, dds):
                k = norm_ws(dt.get_text(" ", strip=True))
                v = norm_ws(dd.get_text(" ", strip=True))
                if k and v:
                    out.append((k, v))
    return out


def first_value(pairs, *keys):
    for key in keys:
        for k, v in pairs:
            if key in k:
                return v
    return ""


def parse_price(raw):
    s = norm_num(norm_ws(raw)).replace(",", "")
    m = re.search(r"(?:(\d+(?:\.\d+)?)億)?(\d+(?:\.\d+)?)?\s*万円", s)
    if not m:
        return None
    return int(round(float(m.group(1) or 0) * 10000 + float(m.group(2) or 0)))


def parse_layout(raw):
    s = norm_ws(raw).upper().replace("＋", "+")
    m = re.search(r"((?:\d+|ワンルーム)\s*(?:S?LDK|SDK|DK|K|R)(?:\+\s*\d*S)?(?:\+\s*S)?)", s)
    return norm_ws(m.group(1)).replace(" ", "") if m else None


def parse_area(raw):
    s = norm_num(norm_ws(raw))
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(?:m\^\{2\}|m2|m²|㎡|平米)", s, re.I)
    return float(m.group(1)) if m else None


def parse_year(raw):
    s = norm_num(norm_ws(raw))
    m = re.search(r"((?:19|20)\d{2})\s*年", s)
    return int(m.group(1)) if m else None


def parse_floors(raw):
    s = norm_num(norm_ws(raw))
    floor_number = floors_total = None
    for pat in (r"所在階[^0-9]*(\d+)\s*階", r"(\d+)\s*階部分", r"(\d+)\s*階\s*/"):
        m = re.search(pat, s)
        if m:
            floor_number = int(m.group(1))
            break
    m = re.search(r"地上\s*(\d+)\s*階(?:建)?", s)
    if m:
        floors_total = int(m.group(1))
    else:
        m = re.search(r"(\d+)\s*階建", s)
        if m:
            floors_total = int(m.group(1))
        else:
            all_candidates = [int(x) for x in re.findall(r"(\d+)\s*階", s)]
            if all_candidates:
                floors_total = max(all_candidates)

    if floor_number is None and floors_total is not None:
        m = re.search(r"(\d+)\s*階", s)
        if m:
            candidate = int(m.group(1))
            if candidate <= floors_total:
                floor_number = candidate
    return floor_number, floors_total


def parse_listing_fields(soup):
    pairs = pairs_from_page(soup)
    text_flat = norm_ws(soup.get_text(" ", strip=True))
    text_lines = soup.get_text("\n", strip=True)

    price_raw = first_value(pairs, "価格")
    if not price_raw:
        m = re.search(r"価格[^0-9０-９]{0,20}([0-9０-９,，\.．]+(?:億[0-9０-９,，\.．]*)?万円)", text_flat)
        price_raw = m.group(1) if m else ""

    layout_raw = first_value(pairs, "間取り")
    if not layout_raw:
        m = re.search(
            r"間取り[^A-Za-z0-9０-９]{0,20}([0-9０-９]+(?:\+S)?(?:SLDK|LDK|SDK|DK|K|R)|ワンルーム)",
            text_flat,
            re.IGNORECASE,
        )
        layout_raw = m.group(1) if m else ""

    area_raw = first_value(pairs, "専有面積")
    if not area_raw:
        m = re.search(
            r"専有面積[^0-9０-９]{0,20}([0-9０-９,，\.．]+\s*(?:m\^\{2\}|m2|m²|㎡|平米))",
            text_flat,
            re.IGNORECASE,
        )
        area_raw = m.group(1) if m else ""

    year_raw = first_value(pairs, "完成時期（築年月）", "築年月")
    if not year_raw:
        m = re.search(r"(?:完成時期（築年月）|築年月)[^0-9]{0,20}((?:19|20)\d{2}\s*年)", text_flat)
        year_raw = m.group(1) if m else ""

    floor_raw = first_value(pairs, "所在階/構造・階建", "所在階")
    floor_number, floors_total = parse_floors(floor_raw)
    if floor_number is None and floors_total is None:
        m = re.search(r"所在階/構造・階建[^。]{0,80}", text_flat)
        if m:
            floor_number, floors_total = parse_floors(m.group(0))

    address = first_value(pairs, "住所", "所在地") or None
    if not address:
        m = re.search(r"住所\s*[\n\r]+(.+)", text_lines)
        if m:
            address = re.split(r"\n|交通|周辺環境|関連リンク", m.group(1).strip())[0].strip()
        else:
            m = re.search(r"所在地\s*[\n\r]+(.+)", text_lines)
            if m:
                address = re.split(r"\n|交通|周辺環境|関連リンク", m.group(1).strip())[0].strip()

    nearest_station = walk_minutes = None
    m = re.search(r"「([^」]{1,40})」\s*(?:徒歩|歩)\s*(\d{1,3})\s*分", text_flat)
    if m:
        nearest_station = f"{m.group(1)}駅"
        walk_minutes = int(m.group(2))

    return {
        "price_man_yen": parse_price(price_raw),
        "layout": parse_layout(layout_raw),
        "area_sqm": parse_area(area_raw),
        "year_built": parse_year(year_raw),
        "floor_number": floor_number,
        "floors_total": floors_total,
        "address": address,
        "nearest_station": nearest_station,
        "walk_minutes": walk_minutes,
    }


def decode_js_escapes(raw):
    if not raw:
        return ""
    s = html_lib.unescape(raw).replace("\\/", "/")
    s = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), s)
    return s


def normalize_image_url(raw, base_url):
    raw = decode_js_escapes((raw or "").strip())
    if not raw or raw.startswith(("data:", "javascript:", "mailto:")):
        return None
    if raw.startswith("//"):
        raw = f"https:{raw}"
    u = urljoin(base_url, raw).split("#")[0]
    return u if u.startswith(("http://", "https://")) else None


def is_probable_suumo_media_url(url):
    u = unquote((url or "")).lower()
    return ("suumo.jp" in u or "suumo.com" in u) and any(h in u for h in SUUMO_MEDIA_PATH_HINTS)


def looks_like_photo(url):
    u = unquote((url or "")).lower()
    if not u:
        return False
    has_ext = bool(IMG_EXT_RE.search(u))
    has_media_hint = any(k in u for k in ("gazo", "image", "photo", "gallery", "media"))
    if not has_ext and not has_media_hint:
        return False
    if ("suumo.jp" in u or "suumo.com" in u) and not has_ext and not is_probable_suumo_media_url(url):
        return False
    return not any(h in u for h in NON_PHOTO_HINTS)


def extract_media_urls_from_text(raw_text, base_url):
    text = decode_js_escapes(raw_text or "")
    if not text:
        return []

    out = []
    seen = set()

    direct = normalize_image_url(text, base_url)
    if direct and looks_like_photo(direct):
        seen.add(direct)
        out.append(direct)

    for pat in RAW_PATTERNS:
        for m in pat.finditer(text):
            u = normalize_image_url(m.group(0), base_url)
            if u and u not in seen and looks_like_photo(u):
                seen.add(u)
                out.append(u)
    return out


def extract_image_urls(detail_html, base_url):
    soup = make_soup(detail_html)
    out = []
    seen = set()

    def add(url):
        if url and url not in seen:
            seen.add(url)
            out.append(url)

    selector = (
        "img, source, [data-src], [data-original], [data-lazy], [data-lazy-src], "
        "[data-srcset], [data-image], [data-img], [style]"
    )
    for node in soup.select(selector):
        for attr in ("src", "data-src", "data-original", "data-lazy", "data-lazy-src", "data-image", "data-img"):
            v = node.get(attr)
            if v:
                for u in extract_media_urls_from_text(str(v), base_url):
                    add(u)

        for attr in ("srcset", "data-srcset"):
            v = node.get(attr)
            if not v:
                continue
            for part in str(v).split(","):
                token = part.strip().split(" ")[0]
                if token:
                    for u in extract_media_urls_from_text(token, base_url):
                        add(u)

        style = node.get("style", "")
        if style:
            for m in re.finditer(r"url\((['\"]?)([^)'\"]+)\1\)", style):
                for u in extract_media_urls_from_text(m.group(2), base_url):
                    add(u)

        for raw_v in getattr(node, "attrs", {}).values():
            values = raw_v if isinstance(raw_v, list) else [raw_v]
            for item in values:
                for u in extract_media_urls_from_text(str(item), base_url):
                    add(u)

    decoded = decode_js_escapes(detail_html)
    for pat in RAW_PATTERNS:
        for m in pat.finditer(decoded):
            u = normalize_image_url(m.group(0), base_url)
            if u and looks_like_photo(u):
                add(u)

    return out


def candidate_photo_pages(detail_html, detail_url):
    parsed = urlparse(detail_url)
    path = parsed.path
    m = re.search(r"/nc_\d+/", path)
    base_path = path[:m.end()] if m else (path if path.endswith("/") else f"{path}/")

    guesses = [f"{base_path}{x}/" for x in ("photo", "photos", "image", "images", "gallery", "shashin")]
    out = []
    seen = {detail_url}

    for p in guesses:
        u = parsed._replace(path=p, query="", fragment="").geturl()
        if u not in seen:
            seen.add(u)
            out.append(u)

    soup = make_soup(detail_html)
    for node in soup.select("a[href], iframe[src], link[href]"):
        href = (node.get("href") or node.get("src") or "").strip()
        if not href:
            continue
        text = norm_ws(node.get_text(" ", strip=True)) if hasattr(node, "get_text") else ""
        href_l = href.lower()
        if not any(k in href_l for k in ("photo", "gallery", "image", "shashin")) and not any(
            k in text for k in ("写真", "フォト", "画像")
        ):
            continue
        abs_u = urljoin(detail_url, href).split("#")[0]
        if "/nc_" in abs_u and abs_u not in seen:
            seen.add(abs_u)
            out.append(abs_u)
    return out


def candidate_data_urls(detail_html, detail_url, sid, max_urls=8):
    out = []
    seen = set()
    decoded = decode_js_escapes(detail_html)

    def maybe_add(raw_u):
        if len(out) >= max_urls:
            return
        abs_u = urljoin(detail_url, raw_u).split("#")[0]
        if not abs_u.startswith(("http://", "https://")):
            return
        host = (urlparse(abs_u).netloc or "").lower()
        if "suumo.jp" not in host and "suumo.com" not in host:
            return
        ul = abs_u.lower()
        if sid not in ul and f"nc_{sid}" not in ul and not any(k in ul for k in ("photo", "image", "gallery", "media", "json", "api")):
            return
        if abs_u not in seen:
            seen.add(abs_u)
            out.append(abs_u)

    soup = make_soup(detail_html)
    for node in soup.select("script[src], link[href], iframe[src]"):
        raw_u = (node.get("src") or node.get("href") or "").strip()
        if raw_u:
            maybe_add(raw_u)

    for m in re.finditer(r'https?://[^"\'<>\s]+', decoded):
        maybe_add(m.group(0))
    for m in re.finditer(r'/[^"\'<>\s]*(?:nc_\d+|photo|image|gallery|media|api|json)[^"\'<>\s]*', decoded, re.IGNORECASE):
        maybe_add(m.group(0))
    return out


def collect_all_image_urls(session, detail_url, detail_html, sid, max_probe_urls=8):
    urls = []
    seen_urls = set()

    def add_many(found):
        for u in found:
            if u and u not in seen_urls:
                seen_urls.add(u)
                urls.append(u)

    add_many(extract_image_urls(detail_html, detail_url))

    queue = []
    seen_probe = {detail_url}

    def enqueue(candidates):
        for u in candidates:
            if len(queue) >= max_probe_urls:
                break
            if u and u not in seen_probe and u not in queue:
                queue.append(u)

    enqueue(candidate_photo_pages(detail_html, detail_url))
    enqueue(candidate_data_urls(detail_html, detail_url, sid, max_urls=max_probe_urls))

    while queue and (len(seen_probe) - 1) < max_probe_urls:
        probe_url = queue.pop(0)
        if probe_url in seen_probe:
            continue
        seen_probe.add(probe_url)
        try:
            probe_html = fetch_html(session, probe_url)
        except Exception:
            continue

        add_many(extract_image_urls(probe_html, probe_url))
        enqueue(candidate_data_urls(probe_html, probe_url, sid, max_urls=4))
        if "/nc_" in probe_url:
            enqueue(candidate_photo_pages(probe_html, probe_url))

    return urls


def image_ext(url):
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    return ext if ext in (".jpg", ".jpeg", ".png", ".webp", ".avif") else ".jpg"


def download_image(session, image_url, out_path, referer):
    try:
        r = session.get(image_url, timeout=40, stream=True, headers={"Referer": referer})
        r.raise_for_status()
        ct = (r.headers.get("Content-Type") or "").lower()
        if ct and "image" not in ct and "octet-stream" not in ct:
            return False
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1 << 16):
                if chunk:
                    f.write(chunk)
        return True
    except Exception:
        return False


def format_elapsed(start_ts):
    secs = max(0, int(time.time() - start_ts))
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def scrape_listing(listing_url, existing_image_keys_snapshot):
    session = worker_session()
    html = fetch_html(session, listing_url)
    soup = make_soup(html)

    sid = source_id(listing_url)
    fields = parse_listing_fields(soup)
    image_urls = collect_all_image_urls(
        session, listing_url, html, sid, max_probe_urls=MAX_IMAGE_PROBE_URLS
    )

    listing_image_dir = os.path.join(IMAGE_ROOT, sid)
    os.makedirs(listing_image_dir, exist_ok=True)

    image_rows = []
    image_count = 0

    for image_url in image_urls:
        if (sid, image_url) in existing_image_keys_snapshot:
            continue

        filename = image_filename_from_url(sid, image_url)
        path = os.path.join(listing_image_dir, filename)

        if not os.path.exists(path) and not download_image(session, image_url, path, listing_url):
            continue

        image_rows.append(
            {
                "source_id": sid,
                "listing_url": listing_url,
                "image_url": image_url,
                "image_name": filename,
            }
        )
        image_count += 1

    listing_row = {
        "source_id": sid,
        "url": listing_url,
        "price_man_yen": fields["price_man_yen"],
        "layout": fields["layout"],
        "area_sqm": fields["area_sqm"],
        "year_built": fields["year_built"],
        "floor_number": fields["floor_number"],
        "floors_total": fields["floors_total"],
        "address": fields["address"],
        "nearest_station": fields["nearest_station"],
        "walk_minutes": fields["walk_minutes"],
        "image_count": image_count,
    }
    return listing_row, image_rows


def crawl():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(IMAGE_ROOT, exist_ok=True)
    ensure_csv(LISTINGS_CSV, LISTINGS_COLUMNS)
    ensure_csv(IMAGES_CSV, IMAGES_COLUMNS)

    existing_source_ids = load_existing_source_ids()
    existing_image_keys = load_existing_image_keys()

    page_session = build_session()
    current = URL
    page_num = 0
    scraped_this_run = 0
    images_added_this_run = 0
    started_at = time.time()

    print(f"[start] target={MAX_LISTINGS} listings, workers={MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        while current and scraped_this_run < MAX_LISTINGS:
            page_num += 1
            print(f"[page {page_num}] fetching results page (elapsed {format_elapsed(started_at)})")
            try:
                html = fetch_html(page_session, current, attempts=max(RETRY_ATTEMPTS, 6), base_sleep=1.5)
            except Exception as exc:
                print(f"[page {page_num}] failed after retries: {exc}")
                print("[stop] ending run early; rerun later to continue.")
                break
            soup = make_soup(html)

            candidates = [u for u in detail_links(soup, current) if source_id(u) not in existing_source_ids]
            remaining = MAX_LISTINGS - scraped_this_run
            if remaining <= 0:
                break
            candidates = candidates[:remaining]
            print(f"[page {page_num}] candidates={len(candidates)} remaining_target={remaining}")
            existing_image_keys_snapshot = set(existing_image_keys)

            listing_rows = []
            image_rows = []

            futures = [pool.submit(scrape_listing, url, existing_image_keys_snapshot) for url in candidates]
            for fut in as_completed(futures):
                try:
                    listing_row, scraped_image_rows = fut.result()
                except Exception:
                    continue

                sid = listing_row["source_id"]
                if sid in existing_source_ids:
                    continue
                existing_source_ids.add(sid)
                listing_rows.append(listing_row)
                scraped_this_run += 1

                for row in scraped_image_rows:
                    key = (row["source_id"], row["image_url"])
                    if key in existing_image_keys:
                        continue
                    existing_image_keys.add(key)
                    image_rows.append(row)
                    images_added_this_run += 1

                print(
                    f"[progress] listings={scraped_this_run}/{MAX_LISTINGS} "
                    f"images={images_added_this_run} elapsed={format_elapsed(started_at)}"
                )

            append_rows(LISTINGS_CSV, LISTINGS_COLUMNS, listing_rows)
            append_rows(IMAGES_CSV, IMAGES_COLUMNS, image_rows)

            if scraped_this_run >= MAX_LISTINGS:
                break

            current = next_page(soup, current)
            if current and DELAY_SECONDS > 0:
                time.sleep(DELAY_SECONDS)

    print(
        f"[done] listings={scraped_this_run} images={images_added_this_run} "
        f"elapsed={format_elapsed(started_at)} pages={page_num}"
    )


if __name__ == "__main__":
    crawl()
