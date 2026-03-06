"""
Room Image Labeling Tool
Reads a Google Sheet, displays images for labeling, saves CSV and updates the sheet.
"""

import csv
import io
import os

import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, render_template, request, jsonify, send_file

app = Flask(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
SHEET_ID         = "13OR-j9nP97eo_4d5sLoErXxvANB6gJUzBdCaIcIzDxE"
CREDENTIALS_FILE = "secrets/credentials.json"
OUTPUT_FILE      = "assessment_output.csv"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

LABEL_COLS = ["RoomType", "Luxury", "Brightness", "Condition"]


# ── Google Sheets helpers ─────────────────────────────────────────────────────

def get_sheet():
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1


def fetch_sheet_data():
    sheet    = get_sheet()
    records  = sheet.get_all_records()
    headers  = sheet.row_values(1)
    return records, headers


def col_index(headers, name):
    """Return 1-based column index, adding the column to the sheet if missing."""
    if name in headers:
        return headers.index(name) + 1
    sheet = get_sheet()
    headers.append(name)
    sheet.update_cell(1, len(headers), name)
    return len(headers)


def push_row_to_sheet(row_number_1based, row_data, headers):
    """Write the 4 label columns for a single data row back to the sheet."""
    sheet = get_sheet()
    for col_name in LABEL_COLS:
        col_idx = col_index(headers, col_name)
        sheet.update_cell(row_number_1based, col_idx, row_data.get(col_name, ""))


# ── Local CSV helpers ─────────────────────────────────────────────────────────

def load_output():
    labeled = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = row.get("image_url", "").strip()
                if key:
                    labeled[key] = row
    return labeled


def save_output(rows, fieldnames):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    try:
        rows, headers = fetch_sheet_data()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    fieldnames = list(headers)
    for col in LABEL_COLS:
        if col not in fieldnames:
            fieldnames.append(col)

    room_type_labels = sorted(
        set(r.get("room_type", "").strip() for r in rows if r.get("room_type", "").strip())
    )

    labeled = load_output()
    merged  = []
    for row in rows:
        key = row.get("image_url", "").strip()
        if key and key in labeled:
            merged.append({**row, **{c: labeled[key].get(c, "") for c in LABEL_COLS}})
        else:
            for col in LABEL_COLS:
                row.setdefault(col, "")
            merged.append(row)

    first_unlabeled = next(
        (i for i, r in enumerate(merged) if not r.get("RoomType", "").strip()),
        None
    )

    return jsonify({
        "rows":             merged,
        "fieldnames":       fieldnames,
        "room_type_labels": room_type_labels,
        "first_unlabeled":  first_unlabeled,
    })


@app.route("/api/save", methods=["POST"])
def api_save():
    data       = request.get_json()
    rows       = data.get("rows", [])
    fieldnames = data.get("fieldnames", [])
    updates    = data.get("updates", [])

    if not rows or not fieldnames:
        return jsonify({"error": "No data provided"}), 400

    # 1 — Save local CSV
    try:
        save_output(rows, fieldnames)
    except Exception as e:
        return jsonify({"error": f"CSV save failed: {e}"}), 500

    # 2 — Push changed rows back to Google Sheet
    if updates:
        try:
            _, headers = fetch_sheet_data()
            for col in LABEL_COLS:
                col_index(headers, col)
            for update in updates:
                sheet_row = update["row_index"] + 2  # +2 for header row + 1-based index
                push_row_to_sheet(sheet_row, update["row_data"], headers)
        except Exception as e:
            return jsonify({"error": f"Sheet update failed: {e}"}), 500

    return jsonify({"ok": True, "saved": len(rows), "sheet_updated": len(updates)})


@app.route("/api/download")
def api_download():
    if not os.path.exists(OUTPUT_FILE):
        return jsonify({"error": "No output file yet"}), 404
    return send_file(
        OUTPUT_FILE,
        mimetype="text/csv",
        as_attachment=True,
        download_name="assessment_output.csv",
    )


@app.route("/api/test-write")
def test_write():
    try:
        sheet   = get_sheet()
        headers = sheet.row_values(1)
        col_idx = col_index(headers, "RoomType")
        sheet.update_cell(2, col_idx, "TEST")
        return jsonify({"ok": True, "message": "Wrote TEST to row 2 RoomType"})
    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == "__main__":
    app.run(debug=True, port=5050)
