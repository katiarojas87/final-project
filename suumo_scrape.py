#!/usr/bin/env python3
"""
SUUMO used condo scraper (中古マンション) - saves everything to files and associates images per listing.

Outputs:
- listings.csv  (one row per listing)
- images.csv    (one row per image; includes listing source_id + image_tag)
- images stored in: <img-dir>/<source_id>/<sha>.jpg
- outputs are rewritten each run (no stale rows/files)

Fields:
- price_man_yen (万円)
- layout (間取り)
- area_sqm (専有面積)
- year_built (築年月 -> year)
- floor_number (所在階)
- floors_total (階建)
- address
- nearest_station
- walk_minutes
- interior images only (heuristic by section label)
"""

import argparse
import csv
import html as html_lib
import hashlib
import os
import random
import re
import shutil
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib import robotparser

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Allowlist interior photo section labels, blocklist non-interior sections
INTERIOR_SECTION_ALLOW = [
    "リビング", "ダイニング", "キッチン", "居室", "洋室", "和室", "寝室",
    "浴室", "洗面", "トイレ", "玄関", "収納", "クローゼット",
    "室内", "内観", "その他内観",
    "living", "dining", "kitchen", "bedroom", "room",
    "bathroom", "washroom", "toilet", "entrance", "storage", "closet", "interior",
]
SECTION_BLOCK = [
    "間取り図", "現地外観写真", "外観", "エントランス", "ロビー", "その他共用部",
    "周辺環境", "地図", "住戸からの眺望写真", "眺望",
    "小学校", "中学校", "スーパー", "コンビニ", "ショッピングセンター",
    "建物外観", "その他共有部分", "共用",
    "銀行", "病院", "バルコニー",
    "floor plan", "floorplan", "exterior", "lobby", "common area",
    "surroundings", "map", "view", "school", "supermarket", "convenience store",
]
INTERIOR_SECTION_ALLOW_LC = {x.lower() for x in INTERIOR_SECTION_ALLOW}
SECTION_BLOCK_LC = {x.lower() for x in SECTION_BLOCK}

IMG_EXT_RE = re.compile(r"\.(jpg|jpeg|png|webp|avif)(?:$|[?&])", re.IGNORECASE)
RAW_IMG_URL_RE = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]+?\.(?:jpg|jpeg|png|webp|avif)[^"\'<>\s]*', re.IGNORECASE)
RAW_SUUMO_GAZO_RE = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]*?/front/gazo/[^"\'<>\s]+', re.IGNORECASE)
RAW_SUUMO_MEDIA_RE = re.compile(
    r'(?:(?:https?:)?//|/)[^"\'<>\s]*?/(?:photo|photos|image|images|gallery|media)/[^"\'<>\s]*',
    re.IGNORECASE
)
RAW_SUUMO_RESIZE_RE = re.compile(r'(?:(?:https?:)?//|/)[^"\'<>\s]*?/jj/resizeImage[^"\'<>\s]*', re.IGNORECASE)
NON_PHOTO_HINTS = [
    "logo", "icon", "sprite", "banner", "btn", "button", "map", "qr", "avatar",
    "apple-touch-icon", "favicon", "tracking", "spacer", "blank.gif",
    "pagetop", "/edit/assets/", "/common/img/", "/common/logo/", "/parts/",
]
SUUMO_MEDIA_PATH_HINTS = (
    "/front/gazo/", "/photo/", "/photos/", "/image/", "/images/", "/gallery/", "/media/",
    "/bukken/", "/property/", "/jj/resizeimage",
)
FW_NUM_TRANS = str.maketrans("０１２３４５６７８９，．", "0123456789,.")
IMAGE_TAG_RULES = [
    ("other_room", ("リビング以外の居室", "rooms other than the living room", "居室・リビング以外")),
    ("living", ("リビング", "living")),
    ("dining", ("ダイニング", "dining")),
    ("kitchen", ("キッチン", "kitchen")),
    ("bedroom", ("寝室", "bedroom")),
    ("room", ("居室", "洋室", "和室", "room", "その他部屋・スペース")),
    ("bathroom", ("浴室", "バス・シャワールーム", "bathroom", "bath")),
    ("washroom", ("洗面", "洗面所", "洗面設備", "washroom", "powder room")),
    ("toilet", ("トイレ", "toilet", "restroom", "wc")),
    ("entrance", ("玄関", "entrance")),
    ("closet", ("クローゼット", "closet", "walk-in closet", "wic")),
    ("storage", ("収納", "納戸", "storage")),
    ("interior", ("室内", "内観", "interior", "inside")),
    ("interior", ("内装写真", "室内写真", "内観写真", "その他内観")),
]
INTERIOR_IMAGE_TAGS = {
    "living", "dining", "kitchen", "bedroom", "room",
    "bathroom", "washroom", "toilet", "entrance",
    "closet", "storage", "interior", "other_room",
}


@dataclass
class ListingRow:
    source: str
    source_id: str
    url: str
    price_man_yen: Optional[int]
    layout: Optional[str]
    area_sqm: Optional[float]
    year_built: Optional[int]
    floor_number: Optional[int]
    floors_total: Optional[int]
    address: Optional[str]
    nearest_station: Optional[str]
    walk_minutes: Optional[int]
    interior_image_count: int


@dataclass
class ImageRow:
    source: str
    source_id: str
    listing_url: str
    image_url: str
    image_tag: str
    image_path: str
    image_sha256: str


def polite_sleep(base: float = 2.0, jitter: float = 1.5) -> None:
    time.sleep(base + random.random() * jitter)


def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def build_robot_parser(
    base_url: str,
    session: requests.Session,
    fail_closed: bool = True
) -> robotparser.RobotFileParser:
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        resp = session.get(robots_url, timeout=20)
        resp.raise_for_status()
        rp.parse(resp.text.splitlines())
    except Exception:
        if fail_closed:
            rp.parse(["User-agent: *", "Disallow: /"])
        else:
            rp.parse(["User-agent: *", "Allow: /"])
    return rp


def can_fetch(rp: robotparser.RobotFileParser, ua: str, url: str) -> bool:
    try:
        return rp.can_fetch(ua, url)
    except Exception:
        return False


def fetch_html(session: requests.Session, rp: robotparser.RobotFileParser, url: str, ua: str) -> str:
    if not can_fetch(rp, ua, url):
        raise PermissionError(f"Blocked by robots.txt: {url}")
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def fetch_html_any_host(
    session: requests.Session,
    default_rp: robotparser.RobotFileParser,
    robots_by_host: Dict[str, robotparser.RobotFileParser],
    url: str,
    ua: str,
) -> str:
    rp = get_parser_for_url(session, default_rp, robots_by_host, url)
    if not can_fetch(rp, ua, url):
        raise PermissionError(f"Blocked by robots.txt: {url}")
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def extract_detail_links(list_html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(list_html, "lxml")
    links: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/ms/chuko/" in href and "/nc_" in href:
            links.add(canonicalize_listing_url(urljoin(base_url, href)))
    return sorted(links)


def next_page_url(list_html: str, base_url: str) -> Optional[str]:
    soup = BeautifulSoup(list_html, "lxml")
    a = soup.find("a", rel=lambda v: v and "next" in v)
    if a and a.get("href"):
        return urljoin(base_url, a["href"])
    for a in soup.select("a[href]"):
        if (a.get_text() or "").strip() == "次へ":
            return urljoin(base_url, a["href"])
    return None


def extract_source_id(detail_url: str) -> str:
    m = re.search(r"/nc_(\d+)/", detail_url)
    if m:
        return m.group(1)
    return hashlib.md5(detail_url.encode("utf-8")).hexdigest()[:10]


def canonicalize_listing_url(url: str) -> str:
    """
    Normalize SUUMO listing URLs to avoid tracking query variants such as ?fmlg=bo001.
    """
    parsed = urlparse(url)
    path = parsed.path or "/"
    if "/ms/chuko/" in path and "/nc_" in path:
        if not path.endswith("/"):
            path = f"{path}/"
        return parsed._replace(path=path, query="", fragment="").geturl()
    return url.split("#")[0]


def normalize_num_text(s: str) -> str:
    return (s or "").translate(FW_NUM_TRANS)


def collect_label_values(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []

    for tr in soup.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        k = norm_ws(th.get_text(" ", strip=True))
        v = norm_ws(td.get_text(" ", strip=True))
        if k and v:
            pairs.append((k, v))

    for dl in soup.select("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        if not dts or len(dts) != len(dds):
            continue
        for dt, dd in zip(dts, dds):
            k = norm_ws(dt.get_text(" ", strip=True))
            v = norm_ws(dd.get_text(" ", strip=True))
            if k and v:
                pairs.append((k, v))

    return pairs


def first_value_for_keys(pairs: List[Tuple[str, str]], keys: List[str]) -> Optional[str]:
    for key in keys:
        for k, v in pairs:
            if key in k:
                return v
    return None


def parse_price_man_yen(text: str) -> Optional[int]:
    s = normalize_num_text(norm_ws(text))
    if not s:
        return None
    s = s.replace(",", "")

    m = re.search(r"(?:(\d+(?:\.\d+)?)億)?(\d+(?:\.\d+)?)?\s*万円", s)
    if m:
        oku = float(m.group(1)) if m.group(1) else 0.0
        man = float(m.group(2)) if m.group(2) else 0.0
        return int(round(oku * 10000 + man))

    m = re.search(r"(\d+(?:\.\d+)?)\s*万", s)
    if m:
        return int(round(float(m.group(1))))

    return None


def parse_layout(text: str) -> Optional[str]:
    s = norm_ws(text)
    if not s:
        return None
    s_u = s.upper().replace("＋", "+")
    m = re.search(r"((?:\d+|ワンルーム)\s*(?:S?LDK|SDK|DK|K|R)(?:\+\s*\d*S)?(?:\+\s*S)?)", s_u)
    if m:
        return norm_ws(m.group(1).replace(" ", ""))
    return s if len(s) <= 32 else s[:32]


def parse_area_sqm(text: str) -> Optional[float]:
    s = normalize_num_text(norm_ws(text))
    if not s:
        return None
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(?:m\^\{2\}|m2|m²|㎡|平米)", s, re.IGNORECASE)
    if m:
        return float(m.group(1))
    return None


def parse_year_built(text: str) -> Optional[int]:
    s = normalize_num_text(norm_ws(text))
    if not s:
        return None
    m = re.search(r"((?:19|20)\d{2})\s*年", s)
    if m:
        return int(m.group(1))
    return None


def parse_floors(text: str) -> Tuple[Optional[int], Optional[int]]:
    s = normalize_num_text(norm_ws(text))
    if not s:
        return None, None

    floor_number: Optional[int] = None
    floors_total: Optional[int] = None

    for pat in (
        r"所在階[^0-9]*(\d+)\s*階",
        r"(\d+)\s*階部分",
        r"(\d+)\s*階\s*/",
    ):
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


def parse_fields(soup: BeautifulSoup) -> dict:
    """
    Regex parsing over the detail page text.
    Handles SUUMO area format like 27.44m^{2}.
    """
    text_lines = soup.get_text("\n", strip=True)
    text_flat = norm_ws(soup.get_text(" ", strip=True))
    pairs = collect_label_values(soup)
    out = {}

    price_text = first_value_for_keys(pairs, ["価格"])
    out["price_man_yen"] = parse_price_man_yen(price_text or "")
    if out["price_man_yen"] is None:
        m = re.search(r"価格[^0-9０-９]{0,20}([0-9０-９,，\.．]+(?:億[0-9０-９,，\.．]*)?万円)", text_flat)
        out["price_man_yen"] = parse_price_man_yen(m.group(1)) if m else None

    layout_text = first_value_for_keys(pairs, ["間取り"])
    out["layout"] = parse_layout(layout_text or "")
    if out["layout"] is None:
        m = re.search(r"間取り[^A-Za-z0-9０-９]{0,20}([0-9０-９]+(?:\+S)?(?:SLDK|LDK|SDK|DK|K|R)|ワンルーム)", text_flat, re.IGNORECASE)
        out["layout"] = parse_layout(m.group(1)) if m else None

    area_text = first_value_for_keys(pairs, ["専有面積"])
    out["area_sqm"] = parse_area_sqm(area_text or "")
    if out["area_sqm"] is None:
        m = re.search(r"専有面積[^0-9０-９]{0,20}([0-9０-９,，\.．]+\s*(?:m\^\{2\}|m2|m²|㎡|平米))", text_flat, re.IGNORECASE)
        out["area_sqm"] = parse_area_sqm(m.group(1)) if m else None

    built_text = first_value_for_keys(pairs, ["完成時期（築年月）", "築年月"])
    out["year_built"] = parse_year_built(built_text or "")
    if out["year_built"] is None:
        m = re.search(r"(?:完成時期（築年月）|築年月)[^0-9]{0,20}((?:19|20)\d{2}\s*年)", text_flat)
        out["year_built"] = parse_year_built(m.group(1)) if m else None

    floor_text = first_value_for_keys(pairs, ["所在階/構造・階建", "所在階"])
    floor_number, floors_total = parse_floors(floor_text or "")
    if floor_number is None and floors_total is None:
        m = re.search(r"所在階/構造・階建[^。]{0,80}", text_flat)
        if m:
            floor_number, floors_total = parse_floors(m.group(0))
    out["floor_number"] = floor_number
    out["floors_total"] = floors_total

    m = re.search(r"住所\s*[\n\r]+(.+)", text_lines)
    if m:
        out["address"] = re.split(r"\n|交通|周辺環境|関連リンク", m.group(1).strip())[0].strip()
    else:
        m = re.search(r"所在地\s*[\n\r]+(.+)", text_lines)
        out["address"] = re.split(r"\n|交通|周辺環境", m.group(1).strip())[0].strip() if m else None

    # 交通: ＪＲ山手線「田町」歩9分 -> 田町駅, 9
    m = re.search(r"「([^」]{1,40})」\s*(?:徒歩|歩)\s*(\d{1,3})\s*分", text_flat)
    if m:
        out["nearest_station"] = f"{m.group(1)}駅"
        out["walk_minutes"] = int(m.group(2))
    else:
        out["nearest_station"] = None
        out["walk_minutes"] = None

    return out


def classify_section_label(context_text: str) -> str:
    """
    SUUMO often has the photo section label (キッチン/浴室/間取り図 etc.) near the image.
    We infer it by scanning nearby text.
    """
    ctx = norm_ws(context_text).lower()
    for bad in SECTION_BLOCK:
        if bad.lower() in ctx:
            return bad
    for ok in INTERIOR_SECTION_ALLOW:
        if ok.lower() in ctx:
            return ok
    return ""


def is_interior_section(label: str) -> bool:
    if not label:
        return False
    lc = label.lower()
    if lc in SECTION_BLOCK_LC:
        return False
    return lc in INTERIOR_SECTION_ALLOW_LC


def extract_image_tag(context_text: str) -> str:
    ctx = norm_ws(context_text).lower()
    if not ctx:
        return ""
    for tag, keywords in IMAGE_TAG_RULES:
        for kw in keywords:
            if kw.lower() in ctx:
                return tag

    if "ldk" in ctx:
        return "living"
    if "居室" in ctx:
        return "room"
    if "内観" in ctx or "室内" in ctx or "内装" in ctx:
        return "interior"
    return ""


def tag_from_section_label(label: str) -> str:
    if not label:
        return ""
    tag = extract_image_tag(label)
    if tag:
        return tag
    if label.lower() in INTERIOR_SECTION_ALLOW_LC:
        return "interior"
    return ""


def first_srcset_url(srcset: str) -> Optional[str]:
    for part in (srcset or "").split(","):
        token = part.strip().split(" ")[0]
        if token:
            return token
    return None


def normalize_image_url(raw_url: str, base_url: str) -> Optional[str]:
    raw = (raw_url or "").strip()
    if not raw or raw.startswith(("data:", "javascript:", "mailto:")):
        return None
    raw = decode_js_escapes(raw)
    if raw.startswith("//"):
        raw = f"https:{raw}"
    abs_url = urljoin(base_url, raw).split("#")[0]
    return abs_url if abs_url.startswith(("http://", "https://")) else None


def decode_js_escapes(raw: str) -> str:
    if not raw:
        return ""
    s = html_lib.unescape(raw)
    s = s.replace("\\/", "/")
    s = re.sub(r"\\u002[fF]", "/", s)
    s = re.sub(r"\\x2[fF]", "/", s)
    # Decode generic JS unicode escapes so context labels like \u30ad\u30c3\u30c1\u30f3
    # become readable and taggable.
    s = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), s)
    s = s.replace("\\/", "/")
    return s


def is_probable_suumo_media_url(url: str) -> bool:
    u = unquote((url or "")).lower()
    if not u:
        return False
    if "suumo.jp" not in u and "suumo.com" not in u:
        return False
    return any(h in u for h in SUUMO_MEDIA_PATH_HINTS)


def looks_like_photo(url: str) -> bool:
    u = unquote((url or "")).lower()
    if not u:
        return False
    if not IMG_EXT_RE.search(u) and "gazo" not in u and "image" not in u and "photo" not in u and "gallery" not in u and "media" not in u and "resizeimage" not in u:
        return False
    if ("suumo.jp" in u or "suumo.com" in u) and not IMG_EXT_RE.search(u):
        if not is_probable_suumo_media_url(u):
            return False
    return not any(hint in u for hint in NON_PHOTO_HINTS)


def is_listing_photo_url(url: str) -> bool:
    u = unquote((url or "")).lower()
    if not looks_like_photo(u):
        return False
    if "/ms/chuko/" in u and "/nc_" in u and "/front/gazo/" not in u and not IMG_EXT_RE.search(u):
        # Listing/detail/photo pages are not direct image files.
        return False
    if "suumo.jp" in u or "suumo.com" in u:
        if any(x in u for x in ("/common/", "/parts/", "/logo/", "/icon/")):
            return False
        if is_probable_suumo_media_url(u):
            return True
        # Last resort for SUUMO-hosted direct image files.
        return bool(IMG_EXT_RE.search(u))
    return True


def context_from_node(node) -> str:
    if not node:
        return ""
    parts: List[str] = []
    for attr in ("alt", "title", "aria-label", "data-tag", "data-label", "data-title", "data-name", "class", "id"):
        v = node.get(attr) if hasattr(node, "get") else None
        if not v:
            continue
        if isinstance(v, list):
            parts.extend(str(x) for x in v)
        else:
            parts.append(str(v))
    if hasattr(node, "get_text"):
        txt = node.get_text(" ", strip=True)
        if txt:
            parts.append(txt)
    return " ".join(parts)


def extract_media_urls_from_text(raw_text: str, base_url: str) -> List[str]:
    text = decode_js_escapes(raw_text or "")
    if not text:
        return []

    out: List[str] = []
    seen: Set[str] = set()
    patterns = (RAW_IMG_URL_RE, RAW_SUUMO_GAZO_RE, RAW_SUUMO_MEDIA_RE, RAW_SUUMO_RESIZE_RE)

    # If the full value itself is a URL-ish token, test it first.
    direct = normalize_image_url(text, base_url)
    if direct and looks_like_photo(direct):
        seen.add(direct)
        out.append(direct)

    for pat in patterns:
        for m in pat.finditer(text):
            u = normalize_image_url(m.group(0), base_url)
            if not u or u in seen or not looks_like_photo(u):
                continue
            seen.add(u)
            out.append(u)
    return out


def extract_image_candidates(detail_html: str, base_url: str) -> List[Tuple[str, str]]:
    """
    Return list of (img_url, nearby_context_text) from DOM image tags.
    """
    soup = BeautifulSoup(detail_html, "lxml")
    out_by_url: Dict[str, Tuple[str, int]] = {}

    selector = (
        "img, source, [data-src], [data-original], [data-lazy], [data-lazy-src], "
        "[data-srcset], [data-image], [data-img], [style]"
    )
    for img in soup.select(selector):
        urls: List[str] = []

        # Common explicit URL-bearing attributes.
        for attr in ("src", "data-src", "data-original", "data-lazy", "data-lazy-src", "srcset", "data-srcset", "data-image", "data-img"):
            v = img.get(attr)
            if not v:
                continue
            if "srcset" in attr:
                first = first_srcset_url(v)
                if first:
                    urls.extend(extract_media_urls_from_text(first, base_url))
                continue
            urls.extend(extract_media_urls_from_text(str(v), base_url))

        # Fallback: scan all attribute values (covers custom lazy-load attrs and style urls()).
        for _, raw_v in getattr(img, "attrs", {}).items():
            if isinstance(raw_v, list):
                for item in raw_v:
                    urls.extend(extract_media_urls_from_text(str(item), base_url))
            else:
                urls.extend(extract_media_urls_from_text(str(raw_v), base_url))

        if not urls:
            continue

        ctx_parts = [context_from_node(img)]
        if hasattr(img, "find_previous_sibling"):
            prev_sib = img.find_previous_sibling()
            if prev_sib is not None:
                ctx_parts.append(context_from_node(prev_sib))
            next_sib = img.find_next_sibling()
            if next_sib is not None:
                ctx_parts.append(context_from_node(next_sib))
        if img.parent:
            ctx_parts.append(context_from_node(img.parent))
            if hasattr(img.parent, "find_previous_sibling"):
                prev_parent_sib = img.parent.find_previous_sibling()
                if prev_parent_sib is not None:
                    ctx_parts.append(context_from_node(prev_parent_sib))
                next_parent_sib = img.parent.find_next_sibling()
                if next_parent_sib is not None:
                    ctx_parts.append(context_from_node(next_parent_sib))
            if img.parent.parent:
                ctx_parts.append(context_from_node(img.parent.parent))

        ctx = norm_ws(" ".join(ctx_parts))
        node_priority = 2 if img.name == "img" else 1

        for url in urls:
            if url not in out_by_url:
                out_by_url[url] = (ctx, node_priority)
                continue

            prev_ctx, prev_priority = out_by_url[url]
            if node_priority > prev_priority or (node_priority == prev_priority and len(ctx) > len(prev_ctx)):
                out_by_url[url] = (ctx, node_priority)

    return [(u, ctx) for u, (ctx, _) in out_by_url.items()]


def extract_regex_image_candidates(detail_html: str, base_url: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    seen: Set[str] = set()
    decoded = decode_js_escapes(detail_html)

    for pat in (RAW_IMG_URL_RE, RAW_SUUMO_GAZO_RE, RAW_SUUMO_MEDIA_RE, RAW_SUUMO_RESIZE_RE):
        for m in pat.finditer(decoded):
            url = normalize_image_url(m.group(0), base_url)
            if not url or url in seen or not looks_like_photo(url):
                continue
            seen.add(url)
            # Keep local script context, but avoid over-including unrelated section labels.
            start = max(0, m.start() - 320)
            end = min(len(decoded), m.end() + 320)
            out.append((url, decoded[start:end]))
    return out


def build_context_maps(
    candidates: List[Tuple[str, str]],
    regex_candidates: List[Tuple[str, str]],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    dom_context_by_url: Dict[str, str] = {}
    regex_context_by_url: Dict[str, str] = {}
    for u, ctx in candidates:
        if u not in dom_context_by_url or len(ctx) > len(dom_context_by_url[u]):
            dom_context_by_url[u] = ctx
    for u, ctx in regex_candidates:
        if u not in regex_context_by_url or len(ctx) > len(regex_context_by_url[u]):
            regex_context_by_url[u] = ctx
    return dom_context_by_url, regex_context_by_url


def select_interior_listing_images(
    dom_context_by_url: Dict[str, str],
    regex_context_by_url: Dict[str, str],
) -> Dict[str, str]:
    selected_url_to_tag: Dict[str, str] = {}
    all_urls = set(dom_context_by_url.keys()) | set(regex_context_by_url.keys())
    for u in all_urls:
        if not is_listing_photo_url(u):
            continue

        ctx_dom = dom_context_by_url.get(u, "")
        ctx_regex = regex_context_by_url.get(u, "")
        label_dom = classify_section_label(ctx_dom) if ctx_dom else ""
        label_regex = classify_section_label(ctx_regex) if ctx_regex else ""
        blocked_dom = bool(label_dom and label_dom.lower() in SECTION_BLOCK_LC)

        # Prefer DOM tag signals; use regex context only as fallback.
        image_tag = extract_image_tag(ctx_dom)
        if not image_tag and ctx_regex:
            image_tag = extract_image_tag(ctx_regex)
        if not image_tag:
            image_tag = tag_from_section_label(label_dom) or tag_from_section_label(label_regex)
        if not image_tag and (is_interior_section(label_dom) or is_interior_section(label_regex)):
            image_tag = "interior"

        # If explicit DOM block labels are present and no interior signal exists, drop.
        if blocked_dom and image_tag not in INTERIOR_IMAGE_TAGS:
            continue

        if image_tag in INTERIOR_IMAGE_TAGS:
            selected_url_to_tag[u] = image_tag

    return selected_url_to_tag


def candidate_photo_pages(detail_html: str, detail_url: str) -> List[str]:
    parsed = urlparse(detail_url)
    path = parsed.path
    m = re.search(r"/nc_\d+/", path)
    if m:
        base_path = path[:m.end()]
    else:
        base_path = path if path.endswith("/") else f"{path}/"

    guessed_paths = [
        f"{base_path}photo/",
        f"{base_path}photos/",
        f"{base_path}image/",
        f"{base_path}images/",
        f"{base_path}gallery/",
        f"{base_path}shashin/",
    ]

    out: List[str] = []
    seen: Set[str] = set()

    for p in guessed_paths:
        u = parsed._replace(path=p, query="", fragment="").geturl()
        if u not in seen and u != detail_url:
            seen.add(u)
            out.append(u)

    soup = BeautifulSoup(detail_html, "lxml")
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
        if "/nc_" not in abs_u:
            continue
        if abs_u not in seen and abs_u != detail_url:
            seen.add(abs_u)
            out.append(abs_u)
    return out


def candidate_data_urls(detail_html: str, detail_url: str, source_id: str, max_urls: int = 12) -> List[str]:
    """
    Extract likely listing-specific JSON/JS/data endpoints that may contain photo URLs.
    """
    out: List[str] = []
    seen: Set[str] = set()
    parsed_detail = urlparse(detail_url)
    decoded = decode_js_escapes(detail_html)

    def maybe_add(raw_u: str) -> None:
        if len(out) >= max_urls:
            return
        abs_u = urljoin(detail_url, raw_u).split("#")[0]
        if not abs_u.startswith(("http://", "https://")):
            return
        ul = abs_u.lower()
        host = (urlparse(abs_u).netloc or "").lower()
        if "suumo.jp" not in host and "suumo.com" not in host:
            return
        if parsed_detail.netloc and host != parsed_detail.netloc and "suumo.com" not in host:
            return
        if source_id not in ul and f"nc_{source_id}" not in ul and not any(
            k in ul for k in ("photo", "image", "gallery", "media", "json", "api")
        ):
            return
        if abs_u in seen:
            return
        seen.add(abs_u)
        out.append(abs_u)

    soup = BeautifulSoup(detail_html, "lxml")
    for node in soup.select("script[src], link[href], iframe[src]"):
        raw_u = (node.get("src") or node.get("href") or "").strip()
        if raw_u:
            maybe_add(raw_u)

    for m in re.finditer(r'https?://[^"\'<>\s]+', decoded):
        maybe_add(m.group(0))
    for m in re.finditer(r'/[^"\'<>\s]*(?:nc_\d+|photo|image|gallery|media|api|json)[^"\'<>\s]*', decoded, re.IGNORECASE):
        maybe_add(m.group(0))

    return out


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_filename(url: str) -> str:
    # stable name based on URL hash
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".avif"):
        ext = ".jpg"
    return f"{h}{ext}"


def get_parser_for_url(
    session: requests.Session,
    default_rp: robotparser.RobotFileParser,
    robots_by_host: Dict[str, robotparser.RobotFileParser],
    url: str,
) -> robotparser.RobotFileParser:
    host = (urlparse(url).netloc or "").lower()
    if not host:
        return default_rp
    if host not in robots_by_host:
        # For image hosts, fail-open if robots.txt cannot be fetched due transient errors.
        robots_by_host[host] = build_robot_parser(url, session, fail_closed=False)
    return robots_by_host[host]


def download_image(
    session: requests.Session,
    default_rp: robotparser.RobotFileParser,
    robots_by_host: Dict[str, robotparser.RobotFileParser],
    ua: str,
    img_url: str,
    out_path: str,
    referer: Optional[str] = None,
) -> bool:
    rp = get_parser_for_url(session, default_rp, robots_by_host, img_url)
    if not can_fetch(rp, ua, img_url):
        return False
    for attempt in range(3):
        try:
            headers = {"Referer": referer} if referer else None
            r = session.get(img_url, timeout=40, stream=True, headers=headers)
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()
            if ctype and "image" not in ctype and "octet-stream" not in ctype:
                return False
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception:
            if attempt < 2:
                polite_sleep(base=0.4, jitter=0.6)
                continue
            return False


def ensure_csv_header(path: str, fieldnames: List[str]) -> None:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


def append_csv(path: str, fieldnames: List[str], row: dict) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(row)


def collect_all_listing_image_candidates(
    session: requests.Session,
    rp: robotparser.RobotFileParser,
    robots_by_host: Dict[str, robotparser.RobotFileParser],
    ua: str,
    detail_url: str,
    detail_html: str,
    source_id: str,
    max_probe_urls: int = 20,
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    Gather image candidates from the detail page and additional listing-related sources
    (photo pages + data endpoints). This runs before filtering so we keep all possible
    listing images and only then apply relevance tags.
    """
    candidates = extract_image_candidates(detail_html, detail_url)
    regex_candidates = extract_regex_image_candidates(detail_html, detail_url)

    queue: List[str] = []
    seen_probe: Set[str] = {detail_url}

    def enqueue(urls: List[str]) -> None:
        for u in urls:
            if len(queue) >= max_probe_urls:
                break
            if not u or u in seen_probe or u in queue:
                continue
            queue.append(u)

    enqueue(candidate_photo_pages(detail_html, detail_url))
    enqueue(candidate_data_urls(detail_html, detail_url, source_id, max_urls=max_probe_urls))

    while queue and (len(seen_probe) - 1) < max_probe_urls:
        probe_url = queue.pop(0)
        if probe_url in seen_probe:
            continue
        seen_probe.add(probe_url)

        try:
            probe_html = fetch_html_any_host(session, rp, robots_by_host, probe_url, ua)
        except PermissionError:
            continue
        except Exception:
            continue

        candidates.extend(extract_image_candidates(probe_html, probe_url))
        regex_candidates.extend(extract_regex_image_candidates(probe_html, probe_url))

        # One extra discovery layer from probe pages catches embedded JSON/media endpoints.
        enqueue(candidate_data_urls(probe_html, probe_url, source_id, max_urls=8))
        if "/nc_" in probe_url:
            enqueue(candidate_photo_pages(probe_html, probe_url))

        polite_sleep(base=0.12, jitter=0.14)

    return candidates, regex_candidates


def scrape_listing_and_save(
    session: requests.Session,
    rp: robotparser.RobotFileParser,
    robots_by_host: Dict[str, robotparser.RobotFileParser],
    ua: str,
    detail_url: str,
    img_dir: str,
    listings_csv: str,
    images_csv: str,
) -> None:
    detail_url = canonicalize_listing_url(detail_url)
    html = fetch_html(session, rp, detail_url, ua)
    soup = BeautifulSoup(html, "lxml")
    fields = parse_fields(soup)

    source_id = extract_source_id(detail_url)

    # Collect candidates from all known listing sources first, then keep only relevant tags.
    candidates, regex_candidates = collect_all_listing_image_candidates(
        session, rp, robots_by_host, ua, detail_url, html, source_id
    )
    dom_context_by_url, regex_context_by_url = build_context_maps(candidates, regex_candidates)
    selected_url_to_tag = select_interior_listing_images(dom_context_by_url, regex_context_by_url)

    interior_urls = sorted(selected_url_to_tag.keys())

    # Download images + write images.csv rows
    image_rows_written = 0
    for img_url in interior_urls:
        fname = safe_filename(img_url)
        local_path = os.path.join(img_dir, source_id, fname)

        if not os.path.exists(local_path):
            ok = download_image(
                session, rp, robots_by_host, ua, img_url, local_path, referer=detail_url
            )
            polite_sleep(base=0.15, jitter=0.25)
            if not ok:
                continue

        img_hash = sha256_file(local_path)

        img_row = ImageRow(
            source="suumo",
            source_id=source_id,
            listing_url=detail_url,
            image_url=img_url,
            image_tag=selected_url_to_tag.get(img_url, ""),
            image_path=local_path,
            image_sha256=img_hash,
        )
        append_csv(images_csv, list(asdict(img_row).keys()), asdict(img_row))
        image_rows_written += 1

    # Write listing row (includes interior_image_count)
    listing_row = ListingRow(
        source="suumo",
        source_id=source_id,
        url=detail_url,
        price_man_yen=fields.get("price_man_yen"),
        layout=fields.get("layout"),
        area_sqm=fields.get("area_sqm"),
        year_built=fields.get("year_built"),
        floor_number=fields.get("floor_number"),
        floors_total=fields.get("floors_total"),
        address=fields.get("address"),
        nearest_station=fields.get("nearest_station"),
        walk_minutes=fields.get("walk_minutes"),
        interior_image_count=image_rows_written,
    )
    append_csv(listings_csv, list(asdict(listing_row).keys()), asdict(listing_row))


def crawl(
    search_url: str,
    pages: int,
    out_dir: str,
    img_dir: str,
    base_delay: float,
    ua: str
) -> None:
    os.makedirs(out_dir, exist_ok=True)

    listings_csv = os.path.join(out_dir, "listings.csv")
    images_csv = os.path.join(out_dir, "images.csv")

    # Rewrite outputs each run so stale records/files are not mixed in.
    for p in (listings_csv, images_csv):
        if os.path.exists(p):
            os.remove(p)
    if os.path.isdir(img_dir):
        shutil.rmtree(img_dir)
    os.makedirs(img_dir, exist_ok=True)

    # Write fresh headers.
    ensure_csv_header(listings_csv, list(asdict(ListingRow(
        source="", source_id="", url="", price_man_yen=None, layout=None, area_sqm=None,
        year_built=None, floor_number=None, floors_total=None, address=None,
        nearest_station=None, walk_minutes=None, interior_image_count=0
    )).keys()))
    ensure_csv_header(images_csv, list(asdict(ImageRow(
        source="", source_id="", listing_url="", image_url="", image_tag="", image_path="", image_sha256=""
    )).keys()))

    session = requests.Session()
    session.headers.update({"User-Agent": ua, "Accept-Language": "ja,en-US;q=0.8,en;q=0.6"})
    rp = build_robot_parser(search_url, session)
    robots_by_host: Dict[str, robotparser.RobotFileParser] = {(urlparse(search_url).netloc or "").lower(): rp}

    # Collect detail URLs
    current = search_url
    all_detail_links: List[str] = []
    for _ in range(pages):
        html = fetch_html(session, rp, current, ua)
        all_detail_links.extend(extract_detail_links(html, current))

        nxt = next_page_url(html, current)
        if not nxt:
            break
        current = nxt
        polite_sleep(base=max(0.4, base_delay * 0.8), jitter=max(0.2, base_delay * 0.5))

    all_detail_links = sorted(set(all_detail_links))

    # Scrape each detail URL and save immediately (safe for long runs)
    for url in tqdm(all_detail_links, desc="Scraping listings"):
        try:
            scrape_listing_and_save(session, rp, robots_by_host, ua, url, img_dir, listings_csv, images_csv)
        except PermissionError:
            # blocked by robots.txt; skip
            pass
        except Exception:
            # log-less skip by default
            pass

        polite_sleep(base=base_delay, jitter=max(0.3, base_delay * 0.6))

    print(f"Saved listings to: {listings_csv}")
    print(f"Saved images manifest to: {images_csv}")
    print(f"Saved images under: {img_dir}/<source_id>/")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("search_url", help="SUUMO results URL (list page) OR any page with links to /nc_")
    ap.add_argument("--pages", type=int, default=1, help="How many result pages to traverse")
    ap.add_argument("--out-dir", default="suumo_out", help="Directory to store CSV outputs")
    ap.add_argument("--img-dir", default="suumo_images_interior", help="Directory to store downloaded images")
    ap.add_argument("--delay", type=float, default=1.0, help="Base delay (seconds) between listing requests")
    ap.add_argument("--ua", default=DEFAULT_UA, help="User-Agent string")
    args = ap.parse_args()

    crawl(args.search_url, args.pages, args.out_dir, args.img_dir, args.delay, args.ua)


if __name__ == "__main__":
    main()
    
