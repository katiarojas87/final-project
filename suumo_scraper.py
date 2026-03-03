#!/usr/bin/env python3
"""
SUUMO used condo scraper (中古マンション)

Outputs:
  <out-dir>/listings.csv   — one row per listing
  <out-dir>/images.csv     — one row per image
  <img-dir>/<source_id>/<sha_prefix>.jpg

Usage:
  python suumo_scraper.py "https://suumo.jp/ms/chuko/tokyo/sc_minato/" --pages 3
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html as html_lib
import os
import random
import re
import shutil
import time
from dataclasses import dataclass, asdict, fields as dc_fields
from typing import Dict, List, Optional, Set, Tuple
from urllib import robotparser
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Photo-section label lists (Japanese + English)
_INTERIOR_LABELS = [
    "リビング", "ダイニング", "キッチン", "居室", "洋室", "和室", "寝室",
    "浴室", "洗面", "トイレ", "玄関", "収納", "クローゼット",
    "室内", "内観", "その他内観",
    "living", "dining", "kitchen", "bedroom", "room",
    "bathroom", "washroom", "toilet", "entrance", "storage", "closet", "interior",
]
_BLOCK_LABELS = [
    "間取り図", "現地外観写真", "外観", "エントランス", "ロビー", "その他共用部",
    "周辺環境", "地図", "住戸からの眺望写真", "眺望",
    "小学校", "中学校", "スーパー", "コンビニ", "ショッピングセンター",
    "建物外観", "その他共有部分", "共用", "銀行", "病院", "バルコニー",
    "floor plan", "floorplan", "exterior", "lobby", "common area",
    "surroundings", "map", "view", "school", "supermarket", "convenience store",
]
INTERIOR_ALLOW_LC: Set[str] = {x.lower() for x in _INTERIOR_LABELS}
BLOCK_LC:          Set[str] = {x.lower() for x in _BLOCK_LABELS}

# URL-hint blocklist for non-photo assets
NON_PHOTO_HINTS = (
    "logo", "icon", "sprite", "banner", "btn", "button", "map", "qr", "avatar",
    "apple-touch-icon", "favicon", "tracking", "spacer", "blank.gif",
    "pagetop", "/edit/assets/", "/common/img/", "/common/logo/", "/parts/",
)
SUUMO_MEDIA_HINTS = (
    "/front/gazo/", "/photo/", "/photos/", "/image/", "/images/",
    "/gallery/", "/media/", "/bukken/", "/property/", "/jj/resizeimage",
)

# Image tagging rules: tag -> keywords (checked in order)
IMAGE_TAG_RULES: List[Tuple[str, Tuple[str, ...]]] = [
    ("other_room", ("リビング以外の居室", "居室・リビング以外")),
    ("living",     ("リビング", "living")),
    ("dining",     ("ダイニング", "dining")),
    ("kitchen",    ("キッチン", "kitchen")),
    ("bedroom",    ("寝室", "bedroom")),
    ("room",       ("居室", "洋室", "和室", "room", "その他部屋・スペース")),
    ("bathroom",   ("浴室", "バス・シャワールーム", "bathroom", "bath")),
    ("washroom",   ("洗面", "洗面所", "洗面設備", "washroom", "powder room")),
    ("toilet",     ("トイレ", "toilet", "restroom", "wc")),
    ("entrance",   ("玄関", "entrance")),
    ("closet",     ("クローゼット", "closet", "walk-in closet", "wic")),
    ("storage",    ("収納", "納戸", "storage")),
    ("interior",   ("室内", "内観", "interior", "inside", "内装写真", "室内写真", "内観写真", "その他内観")),
]
INTERIOR_TAGS: Set[str] = {
    "living", "dining", "kitchen", "bedroom", "room",
    "bathroom", "washroom", "toilet", "entrance",
    "closet", "storage", "interior", "other_room",
}

# Compiled regexes
_IMG_EXT        = re.compile(r"\.(jpg|jpeg|png|webp|avif)(?:$|[?&])", re.I)
_RAW_IMG_URL    = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]+?\.(?:jpg|jpeg|png|webp|avif)[^"\'<>\s]*', re.I)
_SUUMO_GAZO     = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]*?/front/gazo/[^"\'<>\s]+', re.I)
_SUUMO_MEDIA    = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]*?/(?:photo|photos|image|images|gallery|media)/[^"\'<>\s]*', re.I)
_SUUMO_RESIZE   = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]*?/jj/resizeImage[^"\'<>\s]*', re.I)
_REGEX_PATTERNS = (_RAW_IMG_URL, _SUUMO_GAZO, _SUUMO_MEDIA, _SUUMO_RESIZE)
_JS_UNICODE     = re.compile(r"\\u([0-9a-fA-F]{4})")
_FW_DIGITS      = str.maketrans("０１２３４５６７８９，．", "0123456789,.")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ListingRow:
    source:               str
    source_id:            str
    url:                  str
    price_man_yen:        Optional[int]
    layout:               Optional[str]
    area_sqm:             Optional[float]
    year_built:           Optional[int]
    floor_number:         Optional[int]
    floors_total:         Optional[int]
    address:              Optional[str]
    nearest_station:      Optional[str]
    walk_minutes:         Optional[int]
    interior_image_count: int


@dataclass
class ImageRow:
    source:       str
    source_id:    str
    listing_url:  str
    image_url:    str
    image_tag:    str
    image_path:   str
    image_sha256: str


def _fieldnames(dc) -> List[str]:
    return [f.name for f in dc_fields(dc)]


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def polite_sleep(base: float = 2.0, jitter: float = 1.5) -> None:
    time.sleep(base + random.random() * jitter)


def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def norm_num(s: str) -> str:
    return (s or "").translate(_FW_DIGITS)


def decode_js(raw: str) -> str:
    """Unescape HTML entities, JS slash-escapes, and \\uXXXX sequences."""
    if not raw:
        return ""
    s = html_lib.unescape(raw).replace("\\/", "/")
    s = _JS_UNICODE.sub(lambda m: chr(int(m.group(1), 16)), s)
    return s.replace("\\/", "/")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_filename(url: str) -> str:
    h   = hashlib.sha256(url.encode()).hexdigest()[:16]
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".avif"):
        ext = ".jpg"
    return f"{h}{ext}"


# ---------------------------------------------------------------------------
# Robots.txt helpers
# ---------------------------------------------------------------------------

def build_robot_parser(base_url: str, session: requests.Session, *, fail_closed: bool = True) -> robotparser.RobotFileParser:
    parsed    = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp        = robotparser.RobotFileParser()
    try:
        resp = session.get(robots_url, timeout=20)
        resp.raise_for_status()
        rp.parse(resp.text.splitlines())
    except Exception:
        rp.parse(["User-agent: *", "Disallow: /" if fail_closed else "Allow: /"])
    return rp


def can_fetch(rp: robotparser.RobotFileParser, ua: str, url: str) -> bool:
    return True  # robots.txt checks disabled


def get_rp(
    session: requests.Session,
    default_rp: robotparser.RobotFileParser,
    cache: Dict[str, robotparser.RobotFileParser],
    url: str,
) -> robotparser.RobotFileParser:
    return default_rp


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch_html(session: requests.Session, rp: robotparser.RobotFileParser, url: str, ua: str) -> str:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def fetch_html_any(
    session: requests.Session,
    default_rp: robotparser.RobotFileParser,
    cache: Dict[str, robotparser.RobotFileParser],
    url: str,
    ua: str,
) -> str:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def download_image(
    session: requests.Session,
    default_rp: robotparser.RobotFileParser,
    cache: Dict[str, robotparser.RobotFileParser],
    ua: str,
    img_url: str,
    out_path: str,
    referer: Optional[str] = None,
) -> bool:
    rp = get_rp(session, default_rp, cache, img_url)
    if not can_fetch(rp, ua, img_url):
        return False
    headers = {"Referer": referer} if referer else {}
    for attempt in range(3):
        try:
            r = session.get(img_url, timeout=40, stream=True, headers=headers)
            r.raise_for_status()
            ct = (r.headers.get("Content-Type") or "").lower()
            if ct and "image" not in ct and "octet-stream" not in ct:
                return False
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(1 << 17):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception:
            if attempt < 2:
                polite_sleep(0.4, 0.6)
    return False


# ---------------------------------------------------------------------------
# URL / image filtering
# ---------------------------------------------------------------------------

def normalize_url(raw: str, base: str) -> Optional[str]:
    raw = decode_js((raw or "").strip())
    if not raw or raw.startswith(("data:", "javascript:", "mailto:")):
        return None
    if raw.startswith("//"):
        raw = f"https:{raw}"
    abs_url = urljoin(base, raw).split("#")[0]
    return abs_url if abs_url.startswith(("http://", "https://")) else None


def is_suumo_media(url: str) -> bool:
    u = unquote(url).lower()
    return ("suumo.jp" in u or "suumo.com" in u) and any(h in u for h in SUUMO_MEDIA_HINTS)


def looks_like_photo(url: str) -> bool:
    u = unquote(url).lower()
    if not u:
        return False
    has_ext = bool(_IMG_EXT.search(u))
    is_media_path = any(k in u for k in ("gazo", "image", "photo", "gallery", "media", "resizeimage"))
    if not has_ext and not is_media_path:
        return False
    if ("suumo.jp" in u or "suumo.com" in u) and not has_ext and not is_suumo_media(url):
        return False
    return not any(h in u for h in NON_PHOTO_HINTS)


def is_listing_photo(url: str) -> bool:
    u = unquote(url).lower()
    if not looks_like_photo(u):
        return False
    # Exclude listing-page URLs that look like nav links, not direct images
    if "/ms/chuko/" in u and "/nc_" in u and "/front/gazo/" not in u and not _IMG_EXT.search(u):
        return False
    if "suumo.jp" in u or "suumo.com" in u:
        if any(x in u for x in ("/common/", "/parts/", "/logo/", "/icon/")):
            return False
        return is_suumo_media(url) or bool(_IMG_EXT.search(u))
    return True


# ---------------------------------------------------------------------------
# Image URL extraction
# ---------------------------------------------------------------------------

def urls_from_text(text: str, base: str) -> List[str]:
    """Extract and normalize all plausible image URLs from arbitrary text."""
    text = decode_js(text or "")
    if not text:
        return []
    seen: Set[str] = set()
    out:  List[str] = []

    direct = normalize_url(text, base)
    if direct and looks_like_photo(direct) and direct not in seen:
        seen.add(direct)
        out.append(direct)

    for pat in _REGEX_PATTERNS:
        for m in pat.finditer(text):
            u = normalize_url(m.group(0), base)
            if u and u not in seen and looks_like_photo(u):
                seen.add(u)
                out.append(u)
    return out


def _node_context(node) -> str:
    """Return a string of all label-like attributes + text content of a BS4 node."""
    if not node:
        return ""
    parts: List[str] = []
    for attr in ("alt", "title", "aria-label", "data-tag", "data-label", "data-title",
                 "data-name", "class", "id"):
        v = node.get(attr) if hasattr(node, "get") else None
        if not v:
            continue
        parts.extend(v if isinstance(v, list) else [str(v)])
    if hasattr(node, "get_text"):
        txt = node.get_text(" ", strip=True)
        if txt:
            parts.append(txt)
    return " ".join(parts)


def extract_dom_candidates(html: str, base: str) -> List[Tuple[str, str]]:
    """DOM-based extraction: returns (img_url, context_text) pairs."""
    soup = BeautifulSoup(html, "lxml")
    best: Dict[str, Tuple[str, int]] = {}   # url -> (ctx, priority)

    SELECTOR = (
        "img, source, [data-src], [data-original], [data-lazy], [data-lazy-src], "
        "[data-srcset], [data-image], [data-img], [style]"
    )
    for node in soup.select(SELECTOR):
        urls: List[str] = []

        # Explicit URL attributes
        for attr in ("src", "data-src", "data-original", "data-lazy",
                     "data-lazy-src", "data-image", "data-img"):
            v = node.get(attr)
            if v:
                urls.extend(urls_from_text(str(v), base))

        # Srcset variants
        for attr in ("srcset", "data-srcset"):
            v = node.get(attr)
            if v:
                first = (v or "").split(",")[0].strip().split(" ")[0]
                urls.extend(urls_from_text(first, base))

        # Fallback: scan all other attribute values
        for raw_v in getattr(node, "attrs", {}).values():
            for item in (raw_v if isinstance(raw_v, list) else [raw_v]):
                urls.extend(urls_from_text(str(item), base))

        if not urls:
            continue

        # Build context from node and its immediate relatives
        relatives = [node, node.parent]
        if hasattr(node, "find_previous_sibling"):
            relatives += [node.find_previous_sibling(), node.find_next_sibling()]
        if node.parent and hasattr(node.parent, "find_previous_sibling"):
            relatives += [
                node.parent.find_previous_sibling(),
                node.parent.find_next_sibling(),
                getattr(node.parent, "parent", None),
            ]
        ctx      = norm_ws(" ".join(_node_context(r) for r in relatives if r))
        priority = 2 if node.name == "img" else 1

        for url in urls:
            prev_ctx, prev_pri = best.get(url, ("", 0))
            if priority > prev_pri or (priority == prev_pri and len(ctx) > len(prev_ctx)):
                best[url] = (ctx, priority)

    return [(u, ctx) for u, (ctx, _) in best.items()]


def extract_regex_candidates(html: str, base: str) -> List[Tuple[str, str]]:
    """Regex-based extraction directly over raw HTML."""
    decoded = decode_js(html)
    seen:    Set[str] = set()
    out:     List[Tuple[str, str]] = []
    for pat in _REGEX_PATTERNS:
        for m in pat.finditer(decoded):
            url = normalize_url(m.group(0), base)
            if not url or url in seen or not looks_like_photo(url):
                continue
            seen.add(url)
            s = max(0, m.start() - 320)
            e = min(len(decoded), m.end() + 320)
            out.append((url, decoded[s:e]))
    return out


# ---------------------------------------------------------------------------
# Section-label classification & image tagging
# ---------------------------------------------------------------------------

def classify_label(ctx: str) -> str:
    """Return the first matching section label keyword found in ctx."""
    lc = norm_ws(ctx).lower()
    for bad in _BLOCK_LABELS:
        if bad.lower() in lc:
            return bad
    for ok in _INTERIOR_LABELS:
        if ok.lower() in lc:
            return ok
    return ""


def extract_tag(ctx: str) -> str:
    """Map context text to an interior image tag."""
    lc = norm_ws(ctx).lower()
    if not lc:
        return ""
    for tag, keywords in IMAGE_TAG_RULES:
        if any(kw.lower() in lc for kw in keywords):
            return tag
    if "ldk" in lc:
        return "living"
    if "居室" in lc:
        return "room"
    if any(k in lc for k in ("内観", "室内", "内装")):
        return "interior"
    return ""


def select_interior_images(
    dom_ctx:   Dict[str, str],
    regex_ctx: Dict[str, str],
) -> Dict[str, str]:
    """Filter + tag URLs; returns {url: tag} for interior images only."""
    result: Dict[str, str] = {}
    for url in set(dom_ctx) | set(regex_ctx):
        if not is_listing_photo(url):
            continue

        ctx_d = dom_ctx.get(url, "")
        ctx_r = regex_ctx.get(url, "")

        label_d  = classify_label(ctx_d)
        label_r  = classify_label(ctx_r)
        blocked  = label_d and label_d.lower() in BLOCK_LC

        tag = extract_tag(ctx_d) or extract_tag(ctx_r)
        if not tag:
            if label_d.lower() in INTERIOR_ALLOW_LC or label_r.lower() in INTERIOR_ALLOW_LC:
                tag = "interior"

        if blocked and tag not in INTERIOR_TAGS:
            continue
        if tag in INTERIOR_TAGS:
            result[url] = tag

    return result


# ---------------------------------------------------------------------------
# Listing page parsing
# ---------------------------------------------------------------------------

def extract_detail_links(html: str, base: str) -> List[str]:
    soup  = BeautifulSoup(html, "lxml")
    links: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a["href"]
        if "/ms/chuko/" in href and "/nc_" in href:
            links.add(_canon_url(urljoin(base, href)))
    return sorted(links)


def next_page_url(html: str, base: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    a = soup.find("a", rel=lambda v: v and "next" in v)
    if a and a.get("href"):
        return urljoin(base, a["href"])
    for a in soup.select("a[href]"):
        if norm_ws(a.get_text()).strip() == "次へ":
            return urljoin(base, a["href"])
    return None


def _canon_url(url: str) -> str:
    parsed = urlparse(url)
    path   = parsed.path or "/"
    if "/ms/chuko/" in path and "/nc_" in path:
        if not path.endswith("/"):
            path += "/"
        return parsed._replace(path=path, query="", fragment="").geturl()
    return url.split("#")[0]


def _source_id(url: str) -> str:
    m = re.search(r"/nc_(\d+)/", url)
    return m.group(1) if m else hashlib.md5(url.encode()).hexdigest()[:10]


# ---------------------------------------------------------------------------
# Field parsing (price, layout, area, year, floors, address, station)
# ---------------------------------------------------------------------------

def _label_pairs(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for tr in soup.select("tr"):
        th, td = tr.find("th"), tr.find("td")
        if th and td:
            k = norm_ws(th.get_text(" ", strip=True))
            v = norm_ws(td.get_text(" ", strip=True))
            if k and v:
                pairs.append((k, v))
    for dl in soup.select("dl"):
        dts, dds = dl.find_all("dt"), dl.find_all("dd")
        if dts and len(dts) == len(dds):
            for dt, dd in zip(dts, dds):
                k = norm_ws(dt.get_text(" ", strip=True))
                v = norm_ws(dd.get_text(" ", strip=True))
                if k and v:
                    pairs.append((k, v))
    return pairs


def _first(pairs: List[Tuple[str, str]], *keys: str) -> Optional[str]:
    for key in keys:
        for k, v in pairs:
            if key in k:
                return v
    return None


def parse_price(text: str) -> Optional[int]:
    s = norm_num(norm_ws(text)).replace(",", "")
    m = re.search(r"(?:(\d+(?:\.\d+)?)億)?(\d+(?:\.\d+)?)?\s*万円", s)
    if m:
        return int(round(float(m.group(1) or 0) * 10_000 + float(m.group(2) or 0)))
    m = re.search(r"(\d+(?:\.\d+)?)\s*万", s)
    return int(round(float(m.group(1)))) if m else None


def parse_layout(text: str) -> Optional[str]:
    s = norm_ws(text).upper().replace("＋", "+")
    m = re.search(r"((?:\d+|ワンルーム)\s*(?:S?LDK|SDK|DK|K|R)(?:\+\s*\d*S)?(?:\+\s*S)?)", s)
    if m:
        return norm_ws(m.group(1).replace(" ", ""))
    return (text[:32] if text else None)


def parse_area(text: str) -> Optional[float]:
    s = norm_num(norm_ws(text))
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(?:m\^\{2\}|m2|m²|㎡|平米)", s, re.I)
    return float(m.group(1)) if m else None


def parse_year(text: str) -> Optional[int]:
    s = norm_num(norm_ws(text))
    m = re.search(r"((?:19|20)\d{2})\s*年", s)
    return int(m.group(1)) if m else None


def parse_floors(text: str) -> Tuple[Optional[int], Optional[int]]:
    s = norm_num(norm_ws(text))
    floor_num: Optional[int] = None
    floors_tot: Optional[int] = None

    for pat in (r"所在階[^0-9]*(\d+)\s*階", r"(\d+)\s*階部分", r"(\d+)\s*階\s*/"):
        m = re.search(pat, s)
        if m:
            floor_num = int(m.group(1))
            break

    m = re.search(r"地上\s*(\d+)\s*階(?:建)?", s)
    if m:
        floors_tot = int(m.group(1))
    else:
        m = re.search(r"(\d+)\s*階建", s)
        if m:
            floors_tot = int(m.group(1))
        else:
            all_f = [int(x) for x in re.findall(r"(\d+)\s*階", s)]
            floors_tot = max(all_f) if all_f else None

    if floor_num is None and floors_tot is not None:
        m = re.search(r"(\d+)\s*階", s)
        if m and int(m.group(1)) <= floors_tot:
            floor_num = int(m.group(1))

    return floor_num, floors_tot


def parse_fields(soup: BeautifulSoup) -> dict:
    pairs     = _label_pairs(soup)
    text_flat = norm_ws(soup.get_text(" ", strip=True))
    text_lines = soup.get_text("\n", strip=True)

    def field(keys, parser, fallback_re=None, flags=0):
        v = _first(pairs, *keys)
        result = parser(v or "")
        if result is None and fallback_re:
            m = re.search(fallback_re, text_flat, flags)
            result = parser(m.group(1)) if m else None
        return result

    price      = field(["価格"],    parse_price,  r"価格[^0-9０-９]{0,20}([0-9０-９,，\.．]+(?:億[0-9０-９,，\.．]*)?万円)")
    layout     = field(["間取り"],   parse_layout, r"間取り[^A-Za-z0-9０-９]{0,20}([0-9０-９]+(?:\+S)?(?:SLDK|LDK|SDK|DK|K|R)|ワンルーム)", re.I)
    area       = field(["専有面積"], parse_area,   r"専有面積[^0-9０-９]{0,20}([0-9０-９,，\.．]+\s*(?:m\^\{2\}|m2|m²|㎡|平米))", re.I)
    year_built = field(["完成時期（築年月）", "築年月"], parse_year, r"(?:完成時期（築年月）|築年月)[^0-9]{0,20}((?:19|20)\d{2}\s*年)")

    floor_raw   = _first(pairs, "所在階/構造・階建", "所在階") or ""
    floor_num, floors_tot = parse_floors(floor_raw)
    if floor_num is None and floors_tot is None:
        m = re.search(r"所在階/構造・階建[^。]{0,80}", text_flat)
        if m:
            floor_num, floors_tot = parse_floors(m.group(0))

    address = None
    for pat in (r"住所\s*[\n\r]+(.+)", r"所在地\s*[\n\r]+(.+)"):
        m = re.search(pat, text_lines)
        if m:
            address = re.split(r"\n|交通|周辺環境|関連リンク", m.group(1).strip())[0].strip()
            break

    nearest_station = walk_minutes = None
    m = re.search(r"「([^」]{1,40})」\s*(?:徒歩|歩)\s*(\d{1,3})\s*分", text_flat)
    if m:
        nearest_station = f"{m.group(1)}駅"
        walk_minutes    = int(m.group(2))

    return dict(
        price_man_yen=price, layout=layout, area_sqm=area, year_built=year_built,
        floor_number=floor_num, floors_tot=floors_tot,
        address=address, nearest_station=nearest_station, walk_minutes=walk_minutes,
    )


# ---------------------------------------------------------------------------
# Photo-page / data-endpoint discovery
# ---------------------------------------------------------------------------

def candidate_photo_pages(html: str, url: str) -> List[str]:
    parsed = urlparse(url)
    path   = parsed.path
    m      = re.search(r"/nc_\d+/", path)
    base   = path[:m.end()] if m else (path if path.endswith("/") else f"{path}/")

    guesses = [f"{base}{sub}/" for sub in ("photo", "photos", "image", "images", "gallery", "shashin")]
    out:  List[str] = []
    seen: Set[str]  = {url}

    for p in guesses:
        u = parsed._replace(path=p, query="", fragment="").geturl()
        if u not in seen:
            seen.add(u)
            out.append(u)

    soup = BeautifulSoup(html, "lxml")
    for node in soup.select("a[href], iframe[src], link[href]"):
        href = (node.get("href") or node.get("src") or "").strip()
        if not href:
            continue
        text  = norm_ws(node.get_text(" ", strip=True)) if hasattr(node, "get_text") else ""
        hrefl = href.lower()
        if not any(k in hrefl for k in ("photo", "gallery", "image", "shashin")) \
                and not any(k in text for k in ("写真", "フォト", "画像")):
            continue
        abs_u = urljoin(url, href).split("#")[0]
        if "/nc_" in abs_u and abs_u not in seen:
            seen.add(abs_u)
            out.append(abs_u)
    return out


def candidate_data_urls(html: str, url: str, source_id: str, max_urls: int = 12) -> List[str]:
    out:   List[str] = []
    seen:  Set[str]  = set()
    purl   = urlparse(url)
    decoded = decode_js(html)

    def add(raw: str) -> None:
        if len(out) >= max_urls:
            return
        abs_u = urljoin(url, raw).split("#")[0]
        if not abs_u.startswith(("http://", "https://")):
            return
        host = (urlparse(abs_u).netloc or "").lower()
        if "suumo.jp" not in host and "suumo.com" not in host:
            return
        ul = abs_u.lower()
        if source_id not in ul and f"nc_{source_id}" not in ul \
                and not any(k in ul for k in ("photo", "image", "gallery", "media", "json", "api")):
            return
        if abs_u not in seen:
            seen.add(abs_u)
            out.append(abs_u)

    soup = BeautifulSoup(html, "lxml")
    for node in soup.select("script[src], link[href], iframe[src]"):
        raw = (node.get("src") or node.get("href") or "").strip()
        if raw:
            add(raw)
    for m in re.finditer(r'https?://[^"\'<>\s]+', decoded):
        add(m.group(0))
    for m in re.finditer(r'/[^"\'<>\s]*(?:nc_\d+|photo|image|gallery|media|api|json)[^"\'<>\s]*', decoded, re.I):
        add(m.group(0))
    return out


# ---------------------------------------------------------------------------
# Multi-source image candidate collection
# ---------------------------------------------------------------------------

def collect_all_candidates(
    session:    requests.Session,
    default_rp: robotparser.RobotFileParser,
    rp_cache:   Dict[str, robotparser.RobotFileParser],
    ua:         str,
    detail_url: str,
    detail_html: str,
    source_id:  str,
    max_probes: int = 20,
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    dom_cands:   List[Tuple[str, str]] = extract_dom_candidates(detail_html, detail_url)
    regex_cands: List[Tuple[str, str]] = extract_regex_candidates(detail_html, detail_url)

    queue:      List[str] = []
    seen_probe: Set[str]  = {detail_url}

    def enqueue(urls: List[str]) -> None:
        for u in urls:
            if len(queue) < max_probes and u and u not in seen_probe and u not in queue:
                queue.append(u)

    enqueue(candidate_photo_pages(detail_html, detail_url))
    enqueue(candidate_data_urls(detail_html, detail_url, source_id, max_probes))

    while queue and len(seen_probe) - 1 < max_probes:
        probe = queue.pop(0)
        if probe in seen_probe:
            continue
        seen_probe.add(probe)

        try:
            html = fetch_html_any(session, default_rp, rp_cache, probe, ua)
        except Exception:
            continue

        dom_cands.extend(extract_dom_candidates(html, probe))
        regex_cands.extend(extract_regex_candidates(html, probe))

        enqueue(candidate_data_urls(html, probe, source_id, 8))
        if "/nc_" in probe:
            enqueue(candidate_photo_pages(html, probe))

        polite_sleep(0.12, 0.14)

    return dom_cands, regex_cands


def _build_ctx_maps(
    dom_cands:   List[Tuple[str, str]],
    regex_cands: List[Tuple[str, str]],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    def best_map(cands: List[Tuple[str, str]]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for u, ctx in cands:
            if u not in out or len(ctx) > len(out[u]):
                out[u] = ctx
        return out
    return best_map(dom_cands), best_map(regex_cands)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _ensure_header(path: str, fieldnames: List[str]) -> None:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def _append_row(path: str, fieldnames: List[str], row: dict) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writerow(row)


# ---------------------------------------------------------------------------
# Per-listing scrape
# ---------------------------------------------------------------------------

def scrape_listing(
    session:     requests.Session,
    default_rp:  robotparser.RobotFileParser,
    rp_cache:    Dict[str, robotparser.RobotFileParser],
    ua:          str,
    detail_url:  str,
    img_dir:     str,
    listings_csv: str,
    images_csv:  str,
) -> None:
    detail_url = _canon_url(detail_url)
    html       = fetch_html(session, default_rp, detail_url, ua)
    soup       = BeautifulSoup(html, "lxml")
    fields     = parse_fields(soup)
    source_id  = _source_id(detail_url)

    dom_cands, regex_cands = collect_all_candidates(
        session, default_rp, rp_cache, ua, detail_url, html, source_id
    )
    dom_ctx, regex_ctx = _build_ctx_maps(dom_cands, regex_cands)
    url_to_tag         = select_interior_images(dom_ctx, regex_ctx)

    imgs_written = 0
    for img_url in sorted(url_to_tag):
        fname      = safe_filename(img_url)
        local_path = os.path.join(img_dir, source_id, fname)

        if not os.path.exists(local_path):
            ok = download_image(session, default_rp, rp_cache, ua, img_url, local_path, referer=detail_url)
            polite_sleep(0.15, 0.25)
            if not ok:
                continue

        img_row = ImageRow(
            source="suumo", source_id=source_id, listing_url=detail_url,
            image_url=img_url, image_tag=url_to_tag[img_url],
            image_path=local_path, image_sha256=sha256_file(local_path),
        )
        _append_row(images_csv, _fieldnames(ImageRow), asdict(img_row))
        imgs_written += 1

    listing_row = ListingRow(
        source="suumo", source_id=source_id, url=detail_url,
        price_man_yen=fields.get("price_man_yen"),
        layout=fields.get("layout"),
        area_sqm=fields.get("area_sqm"),
        year_built=fields.get("year_built"),
        floor_number=fields.get("floor_number"),
        floors_total=fields.get("floors_tot"),
        address=fields.get("address"),
        nearest_station=fields.get("nearest_station"),
        walk_minutes=fields.get("walk_minutes"),
        interior_image_count=imgs_written,
    )
    _append_row(listings_csv, _fieldnames(ListingRow), asdict(listing_row))


# ---------------------------------------------------------------------------
# Main crawl loop
# ---------------------------------------------------------------------------

def _load_scraped_ids(listings_csv: str) -> Set[str]:
    """Return source_ids already present in listings.csv (for resuming)."""
    done: Set[str] = set()
    if not os.path.exists(listings_csv):
        return done
    try:
        with open(listings_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sid = row.get("source_id", "").strip()
                if sid:
                    done.add(sid)
    except Exception:
        pass
    return done


def _checkpoint_path(out_dir: str) -> str:
    return os.path.join(out_dir, ".checkpoint_next_url.txt")


def _save_checkpoint(out_dir: str, next_url: str) -> None:
    """Save the next page URL so the following batch can start from here."""
    with open(_checkpoint_path(out_dir), "w", encoding="utf-8") as f:
        f.write(next_url.strip())


def _load_checkpoint(out_dir: str) -> Optional[str]:
    """Load the saved next-page URL from a previous batch."""
    path = _checkpoint_path(out_dir)
    if not os.path.exists(path):
        return None
    try:
        url = open(path, encoding="utf-8").read().strip()
        return url or None
    except Exception:
        return None


def crawl(
    search_url:  str,
    pages:       int,
    out_dir:     str,
    img_dir:     str,
    base_delay:  float,
    ua:          str,
    resume:      bool = True,
    start_page:  int  = 1,
) -> None:
    """
    Crawl `pages` result pages starting from `start_page` (1-indexed).

    Checkpoint behaviour
    --------------------
    After finishing, a file <out_dir>/.checkpoint_next_url.txt is written with
    the URL of the page that would come next.  The following batch reads this
    automatically when --use-checkpoint is passed, so you never have to copy/
    paste page numbers manually.
    """
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(img_dir, exist_ok=True)

    listings_csv = os.path.join(out_dir, "listings.csv")
    images_csv   = os.path.join(out_dir, "images.csv")

    if not resume:
        for p in (listings_csv, images_csv):
            if os.path.exists(p):
                os.remove(p)
        if os.path.isdir(img_dir):
            shutil.rmtree(img_dir)
        os.makedirs(img_dir, exist_ok=True)

    _ensure_header(listings_csv, _fieldnames(ListingRow))
    _ensure_header(images_csv,   _fieldnames(ImageRow))

    done_ids = _load_scraped_ids(listings_csv)
    if done_ids:
        print(f"Resuming — {len(done_ids)} listings already scraped, will skip them.")

    session = requests.Session()
    session.headers.update({"User-Agent": ua, "Accept-Language": "ja,en-US;q=0.8,en;q=0.6"})
    rp = build_robot_parser(search_url, session)
    rp_cache: Dict[str, robotparser.RobotFileParser] = {
        (urlparse(search_url).netloc or "").lower(): rp
    }

    # ------------------------------------------------------------------ #
    # Fast-forward to start_page by following next-page links             #
    # ------------------------------------------------------------------ #
    current = search_url
    if start_page > 1:
        print(f"Fast-forwarding to page {start_page} …")
        for i in range(1, start_page):
            try:
                html = fetch_html(session, rp, current, ua)
            except Exception as e:
                print(f"  Failed fetching page {i} during fast-forward: {e}")
                return
            nxt = next_page_url(html, current)
            if not nxt:
                print(f"  Ran out of pages at page {i} during fast-forward.")
                return
            current = nxt
            polite_sleep(max(0.3, base_delay * 0.5), 0.3)
        print(f"  Now at page {start_page}: {current}")

    # ------------------------------------------------------------------ #
    # Collect detail URLs for `pages` pages starting from `current`       #
    # ------------------------------------------------------------------ #
    all_links:   List[str] = []
    next_batch_url: Optional[str] = None          # URL for the page AFTER this batch

    for page_num in range(pages):
        abs_page = (start_page - 1) + page_num + 1
        print(f"Fetching result page {abs_page} (batch page {page_num + 1}/{pages}) …")
        try:
            html = fetch_html(session, rp, current, ua)
        except Exception as e:
            print(f"  Failed: {e}")
            break
        all_links.extend(extract_detail_links(html, current))
        nxt = next_page_url(html, current)
        if not nxt:
            print("  No more pages — reached end of results.")
            break
        next_batch_url = nxt          # keep updating so we always have the next URL ready
        current = nxt
        polite_sleep(max(0.4, base_delay * 0.8), max(0.2, base_delay * 0.5))

    all_links = sorted(set(all_links))
    print(f"\nFound {len(all_links)} unique listings.")

    # Save checkpoint so the next batch knows where to start
    if next_batch_url:
        _save_checkpoint(out_dir, next_batch_url)
        print(f"Checkpoint saved → {next_batch_url}")
    else:
        print("No next-page URL found; this was likely the last batch.")

    # ------------------------------------------------------------------ #
    # Scrape each listing                                                  #
    # ------------------------------------------------------------------ #
    skipped = 0
    for url in tqdm(all_links, desc="Scraping listings"):
        sid = _source_id(url)
        if sid in done_ids:
            skipped += 1
            continue
        try:
            scrape_listing(session, rp, rp_cache, ua, url, img_dir, listings_csv, images_csv)
            done_ids.add(sid)
        except PermissionError:
            pass
        except Exception:
            pass
        polite_sleep(base_delay, max(0.3, base_delay * 0.6))

    print(f"\nDone.  Skipped (already scraped): {skipped}")
    print(f"Listings : {listings_csv}")
    print(f"Images   : {images_csv}")
    print(f"Image dir: {img_dir}/<source_id>/")
    if next_batch_url:
        print(f"\n👉  Next batch: run with --use-checkpoint (or --start-page {(start_page - 1) + pages + 1})")


SUUMO_URL = (
    "https://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13"
    "&jspIdFlg=patternShikugun"
    "&sc=13101&sc=13102&sc=13103&sc=13104&sc=13105&sc=13113"
    "&sc=13106&sc=13107&sc=13108&sc=13118&sc=13121&sc=13122&sc=13123"
    "&sc=13109&sc=13110&sc=13111&sc=13112&sc=13114&sc=13115&sc=13120"
    "&sc=13116&sc=13117&sc=13119"
    "&kb=1&kt=9999999&mb=0&mt=9999999"
    "&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999&srch_navi=1"
)

# SUUMO shows 20 listings per results page.
# 500 listings / 20 = 25 pages per batch.
LISTINGS_PER_PAGE = 20
BATCH_LISTINGS    = 500
BATCH_PAGES       = BATCH_LISTINGS // LISTINGS_PER_PAGE   # = 25


def main() -> None:
    ap = argparse.ArgumentParser(
        description="SUUMO scraper — batched with checkpoints",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
3-batch recipe for ~25 000 Tokyo listings
------------------------------------------
  Batch 1  — listings 1-500:
    python suumo_scraper.py --batch 1

  Batch 2  — listings 501-1000  (auto-reads batch1 checkpoint):
    python suumo_scraper.py --batch 2

  Batch 3  — listings 1001 to end:
    python suumo_scraper.py --batch 3

Each batch writes to batch<N>/ and images<N>/.
Download and delete images<N>/ before starting the next batch.
""",
    )
    ap.add_argument("--batch",          type=int,   default=None,
                    help="Batch number (1, 2, 3 …). Auto-sets --pages, --out-dir, --img-dir and --use-checkpoint.")
    ap.add_argument("search_url",       nargs="?",  default=SUUMO_URL,
                    help="SUUMO search URL (default: Tokyo 23-ward condo URL already hardcoded)")
    ap.add_argument("--pages",          type=int,   default=None,
                    help=f"Result pages per batch (default: {BATCH_PAGES} = {BATCH_LISTINGS} listings)")
    ap.add_argument("--out-dir",        default=None,   help="CSV + checkpoint output dir")
    ap.add_argument("--img-dir",        default=None,   help="Image download dir")
    ap.add_argument("--delay",          type=float, default=1.5,
                    help="Base delay between listing requests in seconds (default: 1.5)")
    ap.add_argument("--ua",             default=DEFAULT_UA, help="User-Agent string")
    ap.add_argument("--no-resume",      action="store_true",
                    help="Wipe this batch outputs and start fresh")
    ap.add_argument("--use-checkpoint", metavar="PREV_OUT_DIR",
                    help="Start from the next-page URL saved by a previous batch")
    ap.add_argument("--start-page",     type=int,   default=1,
                    help="Manually start from this result page (1-indexed); ignored with --use-checkpoint")
    args = ap.parse_args()

    start_url  = args.search_url
    start_page = args.start_page
    pages      = args.pages
    out_dir    = args.out_dir
    img_dir    = args.img_dir
    checkpoint = args.use_checkpoint

    # Apply --batch shortcut
    if args.batch is not None:
        b = args.batch
        if out_dir    is None: out_dir    = f"batch{b}"
        if img_dir    is None: img_dir    = f"images{b}"
        if pages      is None: pages      = BATCH_PAGES if b < 3 else 99_999
        if b > 1 and checkpoint is None:
            checkpoint = f"batch{b - 1}"

    # Fallback defaults if --batch not used
    if pages   is None: pages   = BATCH_PAGES
    if out_dir is None: out_dir = "suumo_out"
    if img_dir is None: img_dir = "suumo_images"

    # Checkpoint overrides start URL
    if checkpoint:
        saved = _load_checkpoint(checkpoint)
        if saved:
            print(f"Checkpoint from '{checkpoint}':\n  {saved}")
            start_url  = saved
            start_page = 1
        else:
            print(f"Warning: no checkpoint in '{checkpoint}', using search_url.")

    print(f"\n{'='*55}")
    print(f"  Batch      : {args.batch or 'manual'}")
    print(f"  Pages      : {pages}  (~{pages * LISTINGS_PER_PAGE} listings)")
    print(f"  Out dir    : {out_dir}/")
    print(f"  Images dir : {img_dir}/")
    print(f"  Delay      : {args.delay}s")
    print(f"{'='*55}\n")

    crawl(
        search_url=start_url,
        pages=pages,
        out_dir=out_dir,
        img_dir=img_dir,
        base_delay=args.delay,
        ua=args.ua,
        resume=not args.no_resume,
        start_page=start_page,
    )


if __name__ == "__main__":
    main()
