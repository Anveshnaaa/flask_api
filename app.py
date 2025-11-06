import os
import uuid
import logging
from flask import Flask, request, jsonify
import pandas as pd
from filelock import FileLock, Timeout

DATA_PATH = os.path.join("data", "friends_data.csv")
LOCK_PATH = DATA_PATH + ".lock"
LOCK_TIMEOUT_SEC = 5

# --- Logging setup ---
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


#  CSV Module 
def _load_csv():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError("CSV not found at data/friends_data.csv")

    df = pd.read_csv(DATA_PATH, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]

    # Ensure id exists
    if "id" not in df.columns:
        df.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df))])
    else:
        df["id"] = df["id"].apply(lambda x: x if str(x).strip() else str(uuid.uuid4()))
        df["id"] = df["id"].astype(str)

    # TopytohEnsure first and last name exist
    if "first_name" not in df.columns or "last_name" not in df.columns:
        raise ValueError("CSV must contain first_name and last_name columns.")

    # Remove duplicate ids
    df = df.drop_duplicates(subset=["id"], keep="first")
    return df


def _atomic_write(df):
    tmp = DATA_PATH + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, DATA_PATH)


def read_csv_safely():
    with FileLock(LOCK_PATH, timeout=LOCK_TIMEOUT_SEC):
        return _load_csv()


def write_csv_safely(df):
    with FileLock(LOCK_PATH, timeout=LOCK_TIMEOUT_SEC):
        _atomic_write(df)


# --- Pagination ---
def paginate(df, page, per_page):
    total = len(df)
    page = max(page, 1)
    per_page = max(per_page, 1)
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


# --- Routes ---

@app.get("/characters")
def list_characters():
    try:
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 10))
        df = read_csv_safely()
        items, meta = paginate(df, page, per_page)
        return jsonify({"data": items, "meta": meta}), 200
    except Exception as e:
        logger.exception("Error in GET /characters")
        return jsonify({"error": str(e)}), 500


@app.get("/characters/search")
def search_characters():
    try:
        first = request.args.get("first_name", "").lower()
        last = request.args.get("last_name", "").lower()

        if not first and not last:
            return jsonify({"error": "Provide first_name or last_name"}), 400

        df = read_csv_safely()

        mask = pd.Series([True] * len(df))
        if first:
            mask &= df["first_name"].str.lower().str.contains(first)
        if last:
            mask &= df["last_name"].str.lower().str.contains(last)

        results = df[mask].to_dict(orient="records")
        return jsonify({"data": results, "count": len(results)}), 200
    except Exception as e:
        logger.exception("Error in GET /characters/search")
        return jsonify({"error": str(e)}), 500


@app.put("/characters/<id>")
def update_character(id):
    try:
        payload = request.get_json(silent=True) or {}
        if not payload:
            return jsonify({"error": "JSON body required"}), 400

        df = read_csv_safely()
        if id not in set(df["id"]):
            return jsonify({"error": "Character not found"}), 404

        for column, value in payload.items():
            if column != "id" and column in df.columns:
                df.loc[df["id"] == id, column] = str(value)

        write_csv_safely(df)
        updated = df[df["id"] == id].iloc[0].to_dict()
        return jsonify({"data": updated}), 200
    except Exception as e:
        logger.exception("Error in PUT /characters/<id>")
        return jsonify({"error": str(e)}), 500


@app.delete("/characters/<id>")
def delete_character(id):
    try:
        df = read_csv_safely()
        if id not in set(df["id"]):
            return jsonify({"error": "Character not found"}), 404

        df = df[df["id"] != id]
        write_csv_safely(df)
        return jsonify({"message": "Deleted"}), 200
    except Exception as e:
        logger.exception("Error in DELETE /characters/<id>")
        return jsonify({"error": str(e)}), 500


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Route not found"}), 404


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=False)
