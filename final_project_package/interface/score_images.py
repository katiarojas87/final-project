"""
score_images.py
---------------
Rebuilds the scoring_dict column from scratch using claude-haiku-4-5-20251001.

- Drops any existing scoring_dict (old CLIP scores are discarded)
- Reads  : data_dump/images_cleaned.csv
- Writes : data_dump/image_claude.csv

Usage (run from your project root):
    python score_images.py

Requirements:
    pip install anthropic pandas tqdm

Environment:
    ANTHROPIC_API_KEY must be set.

Resume behaviour:
    If data_dump/image_claude.csv already exists, rows whose scoring_dict
    already contains all 4 keys are skipped. Safe to restart after interruption.
    Progress is checkpointed to disk after every 5 000 images.
"""

import ast
import base64
import json
import os
import re
import time
from pathlib import Path

import anthropic
import pandas as pd
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_CSV  = "../../data_dump/images_cleaned.csv"
OUTPUT_CSV = "../../data_dump/image_claude.csv"
BATCH_SIZE = 5_000
MODEL      = "claude-haiku-4-5-20251001"
MAX_RETRIES = 3
RETRY_DELAY = 5   # seconds between retries

REQUIRED_KEYS = {"luxury", "condition", "brightness", "spaciousness"}

DEFAULT_FLOOR_PLAN = {
    "luxury": -1000,
    "condition": -1000,
    "brightness": -1000,
    "spaciousness": -1000,
}

VISION_PROMPT = """Assess this room image. Reply ONLY with valid JSON, no markdown:
{
  "luxury": <float 0.0 to 1.0>,
  "condition": <float 0.0 to 1.0>,
  "brightness": <float 0.0 to 1.0>,
  "spaciousness": <float 0.0 to 1.0>
}

Guidelines:
- luxury:
0.0-0.2 = builder-grade laminate, hollow-core doors, vinyl flooring, basic white fixtures, stock cabinets, no architectural detail,
0.3-0.5 = mid-range finishes, ceramic tile, standard granite counters, basic stainless appliances, simple crown molding,
0.6-0.8 = premium hardwood floors, quartz countertops, custom cabinetry, designer light fixtures, high-end appliances (Sub-Zero, Wolf),
0.9-1.0 = marble/travertine surfaces, bespoke millwork, coffered ceilings, statement chandelier, integrated smart home, herringbone parquet, designer fixtures (Waterworks, Restoration Hardware)

- condition:
0.0 = severe damage (peeling paint, water stains, cracked tiles, mold),
1.0 = pristine/immaculate (spotless, freshly renovated, zero blemishes)

- brightness:
0.0-0.2 = no windows visible, artificial light only, dim overhead bulb, dark corners, heavy blackout curtains,
0.3-0.5 = some natural light, partially shaded, small windows, moderate artificial lighting,
0.6-0.8 = good natural light, large windows, bright and airy feel, south-facing exposure,
0.9-1.0 = floor-to-ceiling windows, sun-drenched, flooded with natural light, skylights, panoramic glass, glowing interior

- spaciousness:
0.0-0.2 = cramped, cluttered, low ceiling, furniture blocking pathways, no circulation space, tight corridors,
0.3-0.5 = average room size, standard ceiling height, functional but not generous, modest proportions,
0.6-0.8 = open floor plan, generous proportions, good circulation, 10ft+ ceilings, minimal visual clutter,
0.9-1.0 = grand, voluminous, double-height ceilings, sweeping open plan, loft-like, unobstructed sightlines, expansive
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_image_b64(path: str) -> tuple:
    ext = Path(path).suffix.lower()
    media_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png",  ".gif": "image/gif",
        ".webp": "image/webp",
    }
    media_type = media_map.get(ext, "image/jpeg")
    with open(path, "rb") as f:
        return base64.standard_b64encode(f.read()).decode("utf-8"), media_type


def is_complete(val) -> bool:
    """True only when the cell already holds all 4 Claude-generated keys."""
    if val is None or val == "":
        return False
    try:
        if isinstance(val, float) and pd.isna(val):
            return False
    except Exception:
        pass
    try:
        d = val if isinstance(val, dict) else ast.literal_eval(str(val))
        return isinstance(d, dict) and REQUIRED_KEYS.issubset(d.keys())
    except Exception:
        return False


def parse_response(text: str) -> dict:
    text = re.sub(r"```[a-z]*", "", text).strip("`").strip()
    return json.loads(text)


def score_image(client, image_path: str) -> dict:
    data, media_type = load_image_b64(image_path)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=256,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": data,
                        }},
                        {"type": "text", "text": VISION_PROMPT},
                    ],
                }],
            )
            result = parse_response(response.content[0].text)
            for k in REQUIRED_KEYS:
                result.setdefault(k, -999)
            return result
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"\n  WARNING: Parse error on {image_path} (attempt {attempt}): {e}")
        except anthropic.RateLimitError:
            wait = RETRY_DELAY * attempt
            print(f"\n  Rate limited - waiting {wait}s ...")
            time.sleep(wait)
        except Exception as e:
            print(f"\n  ERROR on {image_path} (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return {k: -999 for k in REQUIRED_KEYS}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY environment variable is not set.")

    client = anthropic.Anthropic(api_key=api_key)

    # ── Load source and drop old CLIP scoring_dict ──
    print(f"Loading {INPUT_CSV} ...")
    df_source = pd.read_csv(INPUT_CSV)
    df_source.drop(columns=["scoring_dict"], errors="ignore", inplace=True)
    print(f"  {len(df_source):,} rows loaded  |  old scoring_dict column dropped.")

    # ── Load or initialise output CSV ──
    output_path = Path(OUTPUT_CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        print(f"Resuming from existing {OUTPUT_CSV} ...")
        df = pd.read_csv(OUTPUT_CSV)
        # Safety: re-align if row count drifted
        if len(df) != len(df_source):
            print("  Row count mismatch - rebuilding output from source.")
            df = df_source.copy()
            df["scoring_dict"] = None
    else:
        print("No existing output found - starting fresh.")
        df = df_source.copy()
        df["scoring_dict"] = None

    # ── Apply floor-plan defaults & collect rows that still need scoring ──
    needs_scoring = []
    floor_plan_count = 0

    for idx, row in df.iterrows():
        room_type = str(row.get("room_type", "")).strip().lower()
        if room_type == "floor plan":
            df.at[idx, "scoring_dict"] = json.dumps(DEFAULT_FLOOR_PLAN)
            floor_plan_count += 1
        elif not is_complete(row.get("scoring_dict")):
            needs_scoring.append(idx)

    print(f"  {floor_plan_count:,} floor-plan rows -> default -1000 (no API call).")
    print(f"  {len(needs_scoring):,} images queued for Claude scoring.")

    if not needs_scoring:
        print("Nothing to do - all images already scored.")
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Saved -> {OUTPUT_CSV}")
        return

    # ── Process in batches of BATCH_SIZE ──
    total_batches = (len(needs_scoring) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num in range(total_batches):
        batch_indices = needs_scoring[batch_num * BATCH_SIZE : (batch_num + 1) * BATCH_SIZE]
        print(f"\nBatch {batch_num + 1}/{total_batches}  ({len(batch_indices):,} images)")

        for idx in tqdm(batch_indices, unit="img", dynamic_ncols=True):
            image_path = str(df.at[idx, "image_path"])

            if not Path(image_path).exists():
                # File missing on disk - store sentinel, do not crash
                df.at[idx, "scoring_dict"] = json.dumps({k: -998 for k in REQUIRED_KEYS})
                continue

            df.at[idx, "scoring_dict"] = json.dumps(score_image(client, image_path))

        # ── Checkpoint after every batch ──
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"  Checkpoint saved -> {OUTPUT_CSV}  (batch {batch_num + 1}/{total_batches} done)")

    print(f"\nDone! Full results written to {OUTPUT_CSV}")
    print(f"Sentinel legend:  -1000 = floor plan  |  -998 = file not found  |  -999 = API error")


if __name__ == "__main__":
    main()
