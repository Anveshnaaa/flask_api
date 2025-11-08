import os
import time
import logging
from flask import Flask, request, jsonify
import pandas as pd
from werkzeug.exceptions import BadRequest

DATA_PATH = os.path.join("data", "friends_data.csv")

# Logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# CSV helpers
def load_csv():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError("CSV not found.")
    df = pd.read_csv(DATA_PATH, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    if "id" not in df.columns or "first_name" not in df.columns or "last_name" not in df.columns:
        raise ValueError("CSV must contain id, first_name, last_name columns.")
    return df

def save_csv(df):
    df.to_csv(DATA_PATH, index=False)

# Pagination
def parse_pagination():
    try:
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 10))
    except ValueError:
        raise BadRequest("page and per_page must be integers.")
    return max(page, 1), max(per_page, 1)

def paginate(df, page, per_page):
    total = len(df)
    start = (page - 1) * per_page
    end = start + per_page
    items = df.iloc[start:end].to_dict(orient="records")
    meta = {
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": (total + per_page - 1) // per_page,
    }
    return items, meta

# Request logging
@app.after_request
def log_request(response):
    logger.info("%s %s -> %s", request.method, request.path, response.status_code)
    return response

# Routes 
@app.get("/")
def home():
    try:
        return jsonify({"home": "It works, ready to check endpoints"}), 200
    except:
        logger.exception("GET / failed")
        return jsonify({"error": "Internal server error"}), 500

@app.get("/characters")
def list_characters():
    try:
        page, per_page = parse_pagination()
        df = load_csv()
        items, meta = paginate(df, page, per_page)
        return jsonify({"data": items, "meta": meta}), 200
    
    except Exception:
        logger.exception("GET /characters failed")
        return jsonify({"error": "Internal server error"}), 500
    


@app.get("/characters/search")
def search_characters():
    try:
        first = request.args.get("first_name", "").lower()
        last = request.args.get("last_name", "").lower()
        if not first and not last:
            return jsonify({"error": "Provide first_name or last_name"}), 400

        df = load_csv()
        if first:
            df = df[df["first_name"].str.lower().str.contains(first)]
        if last:
            df = df[df["last_name"].str.lower().str.contains(last)]

        return jsonify({"data": df.to_dict(orient="records"), "count": len(df)}), 200
    except Exception:
        logger.exception("GET /characters/search failed")
        return jsonify({"error": "Internal server error"}), 500

@app.put("/characters/<id>")
def update_character(id):
    try:
        data = request.get_json(silent=True) or {}
        df = load_csv()
        if id not in df["id"].values:
            return jsonify({"error": "Character not found"}), 404

        for col, val in data.items():
            if col in df.columns and col != "id":
                df.loc[df["id"] == id, col] = str(val)

        save_csv(df)
        updated = df[df["id"] == id].iloc[0].to_dict()
        return jsonify({"data": updated}), 200
    except Exception:
        logger.exception("PUT /characters failed")
        return jsonify({"error": "Internal server error"}), 500

@app.delete("/characters/<id>")
def delete_character(id):
    try:
        df = load_csv()
        if id not in df["id"].values:
            return jsonify({"error": "Character not found"}), 404

        df = df[df["id"] != id]
        save_csv(df)
        return ("", 204)
    except Exception:
        logger.exception("DELETE /characters failed")
        return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(404)
def not_found(_):
    return jsonify({"error": "Route not found"}), 404

if __name__ == "__main__":
    app.run(port=5000, debug=False)

