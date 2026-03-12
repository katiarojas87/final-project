"""
score_images.py
---------------
Scores property images using claude-haiku-4-5-20251001.

HOW IT WORKS:
  1. First run: scores the first 1000 images (test batch).
     → Pauses and asks you to confirm before continuing.
  2. On confirmation (or on subsequent runs): processes remaining images
     in 5,000-image batches, with a pause between each batch.

Output: data_dump/image_claude.csv  (single file, safe to resume at any time)

Usage (run from the project root folder):
    python score_images.py

    # To skip the pause and run fully automated (e.g. in a tmux/nohup session):
    python score_images.py --auto

Requirements:
    pip install anthropic pandas tqdm python-dotenv

Environment:
    ANTHROPIC_API_KEY must be set (or present in a .env at project root).

Sentinel legend:
    -1000 = floor plan (no API call made)
    -998  = file not found on disk
    -997  = Claude rejected image (BadRequestError)
    -996  = corrupt / non-image file
    -999  = API error after all retries
"""

import argparse
import ast
import base64
import json
import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
import anthropic
import pandas as pd
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────
_ROOT            = Path(__file__).resolve().parents[2]
INPUT_CSV        = _ROOT / "data_dump/images_cleaned.csv"
OUTPUT_CSV       = _ROOT / "data_dump/image_claude.csv"

OLD_PATH_PREFIX  = "/Users/kaatsandoval/code/katiarojas87/final-project/raw_data/suumo_images/"
NEW_PATH_PREFIX  = str(_ROOT / "raw_data/suumo_images") + "/"

TEST_BATCH_SIZE  = 1_000    # images scored before first pause
BATCH_SIZE       = 5_000    # images per subsequent batch
CHECKPOINT_EVERY = 500      # save to disk every N completed images
WORKERS          = 5        # parallel threads (reduce if you hit 429s)
MODEL            = "claude-haiku-4-5-20251001"
MAX_RETRIES      = 6
BASE_RETRY_DELAY = 60       # seconds; doubles each retry

REQUIRED_KEYS = {"luxury", "condition", "brightness", "spaciousness"}

DEFAULT_FLOOR_PLAN = {
    "luxury":       -1000,
    "condition":    -1000,
    "brightness":   -1000,
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
0.0-0.2 = builder-grade laminate, hollow-core doors, vinyl flooring, basic white fixtures, stock cabinets, no architectural detail
0.3-0.5 = mid-range finishes, ceramic tile, standard granite counters, basic stainless appliances, simple crown molding
0.6-0.8 = premium hardwood floors, quartz countertops, custom cabinetry, designer light fixtures, high-end appliances (Sub-Zero, Wolf)
0.9-1.0 = marble/travertine surfaces, bespoke millwork, coffered ceilings, statement chandelier, integrated smart home, herringbone parquet, designer fixtures

- condition:
0.0 = severe damage (peeling paint, water stains, cracked tiles, mold)
1.0 = pristine/immaculate (spotless, freshly renovated, zero blemishes)

- brightness:
0.0-0.2 = no windows visible, artificial light only, dim overhead bulb, dark corners, heavy blackout curtains
0.3-0.5 = some natural light, partially shaded, small windows, moderate artificial lighting
0.6-0.8 = good natural light, large windows, bright and airy feel, south-facing exposure
0.9-1.0 = floor-to-ceiling windows, sun-drenched, flooded with natural light, skylights, panoramic glass

- spaciousness:
0.0-0.2 = cramped, cluttered, low ceiling, furniture blocking pathways, no circulation space
0.3-0.5 = average room size, standard ceiling height, functional but not generous
0.6-0.8 = open floor plan, generous proportions, good circulation, 10ft+ ceilings, minimal clutter
0.9-1.0 = grand, voluminous, double-height ceilings, sweeping open plan, loft-like, expansive
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def resolve_path(raw_path: str) -> Path:
    raw_path = str(raw_path)
    if raw_path.startswith(OLD_PATH_PREFIX):
        raw_path = NEW_PATH_PREFIX + raw_path[len(OLD_PATH_PREFIX):]
    return Path(raw_path)


def validate_image(path: Path) -> bool:
    MAGIC = [b"\xff\xd8", b"\x89PNG", b"GIF8", b"RIFF"]
    with open(path, "rb") as f:
        header = f.read(4)
    return any(header[:len(k)] == k for k in MAGIC)


def load_image_b64(path: Path) -> tuple:
    ext = path.suffix.lower()
    media_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png",  ".gif": "image/gif",
        ".webp": "image/webp",
    }
    media_type = media_map.get(ext, "image/jpeg")
    with open(path, "rb") as f:
        return base64.standard_b64encode(f.read()).decode("utf-8"), media_type


def is_complete(val) -> bool:
    """True only when the cell holds all 4 keys with at least one real (>=0) score."""
    if val is None or val == "":
        return False
    try:
        if isinstance(val, float) and pd.isna(val):
            return False
    except Exception:
        pass
    try:
        d = val if isinstance(val, dict) else ast.literal_eval(str(val))
        if not isinstance(d, dict) or not REQUIRED_KEYS.issubset(d.keys()):
            return False
        return any(v >= 0 for v in d.values())
    except Exception:
        return False


def parse_response(text: str) -> dict:
    text = re.sub(r"```[a-z]*", "", text).strip("`").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[^{}]+\}", text, re.DOTALL)
    if match:
        return json.loads(match.group())
    raise ValueError(f"No JSON object found in response: {text!r}")


def score_image(client: anthropic.Anthropic, image_path: Path) -> dict:
    data, media_type = load_image_b64(image_path)

    for attempt in range(1, MAX_RETRIES + 1):
        wait = BASE_RETRY_DELAY * (2 ** (attempt - 1))
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

        except (json.JSONDecodeError, ValueError) as e:
            print(f"\n  WARNING: Parse error on {image_path.name} "
                  f"(attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2)

        except anthropic.BadRequestError as e:
            print(f"\n  SKIPPED {image_path.name}: BadRequestError - {e}")
            return {k: -997 for k in REQUIRED_KEYS}

        except (anthropic.RateLimitError, anthropic.APIStatusError) as e:
            print(f"\n  API error (attempt {attempt}/{MAX_RETRIES}): "
                  f"{type(e).__name__} - waiting {wait}s ...")
            if attempt < MAX_RETRIES:
                time.sleep(wait)

        except anthropic.APIConnectionError as e:
            print(f"\n  Connection error (attempt {attempt}/{MAX_RETRIES}): "
                  f"{e} - waiting {wait}s ...")
            if attempt < MAX_RETRIES:
                time.sleep(wait)

        except Exception as e:
            print(f"\n  Unexpected error on {image_path.name} "
                  f"(attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(wait)

    return {k: -999 for k in REQUIRED_KEYS}


def process_one(args) -> tuple:
    client, idx, raw_path = args
    image_path = resolve_path(raw_path)

    if not image_path.exists():
        print(f"\n  NOT FOUND: {image_path}")
        return idx, json.dumps({k: -998 for k in REQUIRED_KEYS})

    if not validate_image(image_path):
        print(f"\n  CORRUPT/HTML: {image_path.name}")
        return idx, json.dumps({k: -996 for k in REQUIRED_KEYS})

    return idx, json.dumps(score_image(client, image_path))


def run_batch(df, indices, clients, label):
    """Score a list of row indices and save checkpoints to OUTPUT_CSV."""
    checkpoint_lock = threading.Lock()
    scored_since_checkpoint = [0]

    tasks = [
        (clients[i % WORKERS], idx, str(df.at[idx, "image_path"]))
        for i, idx in enumerate(indices)
    ]

    print(f"\n{'='*60}")
    print(f"  {label}: {len(tasks):,} images")
    print(f"{'='*60}\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process_one, t): t for t in tasks}

        with tqdm(total=len(tasks), unit="img", dynamic_ncols=True) as pbar:
            for future in as_completed(futures):
                idx, result_json = future.result()

                with checkpoint_lock:
                    df.at[idx, "scoring_dict"] = result_json
                    scored_since_checkpoint[0] += 1

                    if scored_since_checkpoint[0] >= CHECKPOINT_EVERY:
                        df.to_csv(OUTPUT_CSV, index=False)
                        scored_since_checkpoint[0] = 0

                pbar.update(1)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n  ✓ {label} complete. Saved -> {OUTPUT_CSV}")


def ask_continue(prompt: str) -> bool:
    """Prompt user to continue. Returns True if yes."""
    while True:
        answer = input(f"\n{prompt} [y/n]: ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("  Please enter y or n.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--auto", action="store_true",
        help="Skip all prompts and run all batches automatically (e.g. for tmux/nohup)"
    )
    args = parser.parse_args()

    # Load .env
    load_dotenv(dotenv_path=next(
        (p / ".env" for p in [Path(__file__).resolve(), *Path(__file__).resolve().parents]
         if (p / ".env").exists()),
        None
    ))

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY is not set.")

    # ── API sanity check ─────────────────────────────────────────────────────
    print("Checking Anthropic API availability...")
    test_client = anthropic.Anthropic(api_key=api_key)
    try:
        test_client.messages.create(
            model=MODEL, max_tokens=10,
            messages=[{"role": "user", "content": "test"}]
        )
        print("✓ Anthropic API is working.\n")
    except Exception as e:
        print(f"\n❌ Anthropic API not usable:\n{e}")
        print("\nCheck your credits or billing settings.")
        return

    clients = [anthropic.Anthropic(api_key=api_key) for _ in range(WORKERS)]

    # ── Load source CSV ───────────────────────────────────────────────────────
    print(f"Loading {INPUT_CSV} ...")
    df_source = pd.read_csv(INPUT_CSV)
    df_source.drop(columns=["scoring_dict"], errors="ignore", inplace=True)
    print(f"  {len(df_source):,} rows loaded.")

    # ── Load or initialise output CSV ─────────────────────────────────────────
    output_path = Path(OUTPUT_CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        print(f"Resuming from existing {OUTPUT_CSV} ...")
        df = pd.read_csv(OUTPUT_CSV)
        if len(df) != len(df_source):
            print("  Row-count mismatch — rebuilding output from source.")
            df = df_source.copy()
            df["scoring_dict"] = None
    else:
        print("No existing output found — starting fresh.")
        df = df_source.copy()
        df["scoring_dict"] = None

    # ── Apply floor-plan defaults & find unscored rows ────────────────────────
    needs_scoring = []
    floor_plan_count = 0

    for idx, row in df.iterrows():
        room_type = str(row.get("room_type", "")).strip().lower()
        if room_type == "floor plan":
            df.at[idx, "scoring_dict"] = json.dumps(DEFAULT_FLOOR_PLAN)
            floor_plan_count += 1
        elif not is_complete(row.get("scoring_dict")):
            needs_scoring.append(idx)

    print(f"  {floor_plan_count:,} floor-plan rows → default -1000 (no API call).")
    print(f"  {len(needs_scoring):,} images need scoring.")

    if not needs_scoring:
        print("\nNothing to do — all images already scored.")
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Saved → {OUTPUT_CSV}")
        return

    # ── Split into test batch + remaining 5k batches ──────────────────────────
    already_done = len(df_source) - floor_plan_count - len(needs_scoring)
    print(f"  {already_done:,} images already scored (will be skipped).\n")

    # Determine if the test batch has already been completed
    # (i.e. we have more than TEST_BATCH_SIZE scored non-floor-plan rows)
    test_already_done = already_done >= TEST_BATCH_SIZE

    batches = []

    if not test_already_done:
        # How many of the test batch remain?
        test_remaining = TEST_BATCH_SIZE - already_done
        test_indices = needs_scoring[:test_remaining]
        remaining_indices = needs_scoring[test_remaining:]
        batches.append(("Test batch (first 1,000)", test_indices))
    else:
        remaining_indices = needs_scoring
        print("✓ Test batch of 1,000 already complete — skipping to main batches.\n")

    # Split remaining into 5k chunks
    for i in range(0, len(remaining_indices), BATCH_SIZE):
        chunk = remaining_indices[i: i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        label = f"Batch {batch_num} (images {i+1:,}–{i+len(chunk):,} of remaining)"
        batches.append((label, chunk))

    print(f"Plan: {len(batches)} batch(es) to run:\n")
    for label, indices in batches:
        print(f"  • {label}: {len(indices):,} images")

    # ── Run batches ───────────────────────────────────────────────────────────
    for batch_num, (label, indices) in enumerate(batches):
        if not indices:
            print(f"\n  Skipping empty batch: {label}")
            continue

        run_batch(df, indices, clients, label)

        # After the test batch, always pause (unless --auto)
        is_test_batch = (batch_num == 0 and not test_already_done)
        is_last_batch = (batch_num == len(batches) - 1)

        if is_last_batch:
            break  # no pause needed after the final batch

        if args.auto:
            print(f"\n  --auto flag set, continuing to next batch automatically...")
            time.sleep(2)  # brief pause to avoid hammering the API
        else:
            if is_test_batch:
                prompt = (
                    f"✅ Test batch complete!\n"
                    f"   Check {OUTPUT_CSV} to review the results.\n"
                    f"   Continue with the remaining batches?"
                )
            else:
                prompt = f"   Continue with the next batch?"

            if not ask_continue(prompt):
                print("\n  Stopping. Run the script again to resume from where you left off.")
                return

    print(f"\n{'='*60}")
    print(f"  🎉 All done! Results written to {OUTPUT_CSV}")
    print(f"{'='*60}")
    print("Sentinel legend:  -1000=floor plan  |  -998=not found  |  -997=Claude rejected  |  -996=corrupt  |  -999=API error")


if __name__ == "__main__":
    main()
