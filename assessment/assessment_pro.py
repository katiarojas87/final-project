"""
Room Assessment Pro
Compares manual labels vs Claude Vision scores. Saves validations to CSV + Google Sheet.
"""
import csv, os, traceback
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, render_template, request, jsonify, send_file

app = Flask(__name__, template_folder="templates")

SHEET_ID         = "13OR-j9nP97eo_4d5sLoErXxvANB6gJUzBdCaIcIzDxE"
CREDENTIALS_FILE = "secrets/credentials.json"
SCORES_PKL       = "assessment_output_v3.pkl"
OUTPUT_FILE      = "assessment_output_pro.csv"
SCOPES           = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
VAL_COLS         = ["val_lux", "val_bright", "val_cond"]


def get_sheet():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds).open_by_key(SHEET_ID).sheet1


def ensure_col(sheet, headers, name):
    if name not in headers:
        headers.append(name)
        sheet.update_cell(1, len(headers), name)
    return headers.index(name) + 1


def push_to_sheet(updates):
    sheet   = get_sheet()
    headers = sheet.row_values(1)
    for u in updates:
        row = u["row_index"] + 2
        for col in VAL_COLS:
            sheet.update_cell(row, ensure_col(sheet, headers, col), u["row_data"].get(col, ""))


def safe_float(val):
    try:
        v = float(val)
        return None if (v != v or v == -1000) else round(v, 4)  # nan check
    except Exception:
        return None


def load_data():
    if not os.path.exists(SCORES_PKL):
        raise FileNotFoundError(f"{SCORES_PKL} not found — run the scoring notebook first.")

    df = pd.read_pickle(SCORES_PKL)
    df.columns = [c.strip() for c in df.columns]

    saved = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                k = r.get("image_url", "").strip()
                if k:
                    saved[k] = {c: r.get(c, "") for c in VAL_COLS}

    rows = []
    for _, r in df.iterrows():
        key = str(r.get("image_url", "")).strip()
        row = {
            "image_url":    key,
            "image_path":   str(r.get("image_path", "")).strip(),
            "room_type":    str(r.get("room_type",  "")).strip(),
            "manual_lux":   str(r.get("Luxury",     "")).strip(),
            "manual_bright":str(r.get("Brightness", "")).strip(),
            "manual_cond":  str(r.get("Condition",  "")).strip(),
            "score_lux":    safe_float(r.get("score_lux")),
            "score_bright": safe_float(r.get("score_bright")),
            "score_cond":   safe_float(r.get("score_cond")),
        }
        vals = saved.get(key, {c: "" for c in VAL_COLS})
        row.update(vals)
        rows.append(row)
    return rows


def save_data(rows):
    fields = ["image_url","image_path","room_type",
              "manual_lux","manual_bright","manual_cond",
              "score_lux","score_bright","score_cond"] + VAL_COLS
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


@app.route("/")
def index():
    return render_template("index_pro.html")


@app.route("/api/data")
def api_data():
    try:
        rows = load_data()
        first = next((i for i, r in enumerate(rows)
                      if not r.get("val_lux") and r.get("score_lux") is not None), 0)
        return jsonify({
            "rows":      rows,
            "total":     len(rows),
            "validated": sum(1 for r in rows if r.get("val_lux")),
            "first":     first,
        })
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/save", methods=["POST"])
def api_save():
    data    = request.get_json()
    rows    = data.get("rows", [])
    updates = data.get("updates", [])
    if not rows:
        return jsonify({"error": "No data"}), 400
    try:
        save_data(rows)
    except Exception as e:
        return jsonify({"error": f"CSV: {e}"}), 500
    if updates:
        try:
            push_to_sheet(updates)
        except Exception as e:
            return jsonify({"error": f"Sheet: {e}"}), 500
    return jsonify({"ok": True, "saved": len(rows), "sheet_updated": len(updates)})


@app.route("/api/download")
def api_download():
    if not os.path.exists(OUTPUT_FILE):
        return jsonify({"error": "No file yet"}), 404
    return send_file(OUTPUT_FILE, mimetype="text/csv", as_attachment=True,
                     download_name="assessment_output_pro.csv")


@app.route("/api/debug")
def api_debug():
    try:
        rows = load_data()
        return jsonify({"ok": True, "count": len(rows), "cols": list(rows[0].keys()), "sample": rows[0]})
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()})


if __name__ == "__main__":
    app.run(debug=True, port=5051)
