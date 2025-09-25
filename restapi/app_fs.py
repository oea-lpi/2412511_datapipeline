# app_fs.py
import json, os, re, hashlib, tempfile
from datetime import datetime, timezone
from glob import glob

import requests
from flask import Flask, request, jsonify, abort

from werkzeug.exceptions import HTTPException
import traceback

app = Flask(__name__)

STORAGE_DIR = (os.getenv("API_STORAGE") or "/data").strip()
HOST = os.getenv("API_HOST", "0.0.0.0")
PORT = int(os.getenv("API_PORT", "8000"))

URL_RE = re.compile(r"^https?://", re.IGNORECASE)

@app.errorhandler(Exception)
def json_errors(e):
    # Known HTTP errors (abort(...), 404, etc.)
    if isinstance(e, HTTPException):
        return jsonify(
            error=e.name,
            status=e.code,
            message=e.description
        ), e.code
    # Unexpected exceptions → 500 with message (and log the stack)
    app.logger.error("Unhandled exception:\n%s", traceback.format_exc())
    return jsonify(
        error="Internal Server Error",
        status=500,
        message=str(e)
    ), 500

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def ensure_store():
    os.makedirs(STORAGE_DIR, exist_ok=True)

def make_event_id(metadata_url: str, meta: dict) -> str:
    payload = {
        "metadata_url": metadata_url.strip(),
        "message_type": meta.get("message_type"),
        "created_at": meta.get("created_at") or meta.get("creation_timestamp"),
        "data_url": meta.get("data_url"),
        "version": meta.get("version"),
    }
    h = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return h[:16]  # kurze, stabile ID

def event_path(event_id: str) -> str:
    return os.path.join(STORAGE_DIR, f"event_{event_id}.json")

def atomic_write_json(path: str, obj: dict):
    dirpath = os.path.dirname(path)
    os.makedirs(dirpath, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
        os.replace(tmp, path)  # atomic on same filesystem
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def fetch_metadata(metadata_url: str) -> dict:
    if not URL_RE.match(metadata_url):
        abort(400, description="metadata_url muss mit http(s):// beginnen.")
    try:
        resp = requests.get(metadata_url, timeout=30)
    except requests.Timeout:
        abort(504, description="Timeout beim Abruf der Metadaten-URL.")
    except requests.ConnectionError as e:
        abort(502, description=f"Metadaten-URL nicht erreichbar: {e}")
    except requests.RequestException as e:
        abort(502, description=f"HTTP-Fehler beim Abruf der Metadaten: {e}")

    if resp.status_code != 200:
        abort(502, description=f"Metadaten HTTP {resp.status_code} von {metadata_url}.")

    try:
        meta = resp.json()
    except ValueError:
        abort(502, description="Metadaten-Response ist kein gültiges JSON.")

    if "data_url" not in meta:
        abort(400, description="Pflichtfeld 'data_url' fehlt in Metadaten.")
    if not any(k in meta for k in ("created_at", "creation_timestamp")):
        abort(400, description="Zeitstempel fehlt (created_at / creation_timestamp).")
    return meta

@app.get("/health")
def health():
    return jsonify(status="ok", time=iso_now())

@app.post("/event")
def receive_event():
    if not request.is_json:
        abort(400, description="Erwarte application/json")
    body = request.get_json(silent=True) or {}
    metadata_url = body.get("url") or body.get("metadata_url")
    if not metadata_url:
        abort(400, description='Feld "url" (oder "metadata_url") fehlt')

    ensure_store()
    meta = fetch_metadata(metadata_url)
    event_id = make_event_id(metadata_url, meta)
    path = event_path(event_id)

    record = {
        "event_id": event_id,
        "metadata_url": metadata_url,
        "metadata": meta,
        "stored_at": iso_now(),
        "schema": "lpi-event-v1",
    }

    # Idempotenz: nur schreiben, wenn Datei noch nicht existiert
    if not os.path.exists(path):
        atomic_write_json(path, record)
        # optional: "latest" Pointer aktualisieren
        atomic_write_json(os.path.join(STORAGE_DIR, "latest.json"), record)

    return jsonify(timestamp=iso_now(), status="accepted", event_id=event_id), 200

@app.get("/event")
def get_last_event():
    ensure_store()
    latest = os.path.join(STORAGE_DIR, "latest.json")
    if os.path.exists(latest):
        with open(latest, "r", encoding="utf-8") as f:
            return jsonify(json.load(f))
    # Fallback: neuestes Event über mtime suchen
    files = sorted(glob(os.path.join(STORAGE_DIR, "event_*.json")), key=os.path.getmtime, reverse=True)
    if not files:
        abort(404, description="Kein Event gespeichert")
    with open(files[0], "r", encoding="utf-8") as f:
        return jsonify(json.load(f))

@app.post("/notify")
def notify_sensical():
    """
    Body JSON (either pass both or set env vars, see below):
      {
        "event_url": "http://<sensical-host-or-ip>:18989/event",   # where to POST
        "metadata_url": "https://<your-public-base>/mock-metadata" # what URL they should GET
      }

    If 'metadata_url' is omitted, we try to build it from PUBLIC_BASE_URL + '/mock-metadata'.
    If 'event_url' is omitted, we read SENSICAL_EVENT_URL from env.
    """
    if not request.is_json:
        abort(400, description="Erwarte application/json")

    body = request.get_json(silent=True) or {}

    event_url = body.get("event_url") or os.getenv("SENSICAL_EVENT_URL")
    metadata_url = body.get("metadata_url")
    if not metadata_url:
        base = os.getenv("PUBLIC_BASE_URL")  # e.g. https://<your-ngrok>.ngrok-free.dev
        if not base:
            abort(400, description="metadata_url fehlt und PUBLIC_BASE_URL ist nicht gesetzt.")
        metadata_url = base.rstrip("/") + "/mock-metadata"

    if not event_url:
        abort(400, description="event_url fehlt (oder SENSICAL_EVENT_URL ist nicht gesetzt).")

    try:
        resp = requests.post(event_url, json={"url": metadata_url}, timeout=30)
        status = resp.status_code
        text = resp.text[:1000]
    except requests.Timeout:
        abort(504, description=f"Timeout beim POST an {event_url}")
    except requests.RequestException as e:
        abort(502, description=f"POST an {event_url} fehlgeschlagen: {e}")

    return jsonify(
        sent_to=event_url,
        payload={"url": metadata_url},
        response_status=status,
        response_text=text
    ), 200

@app.get("/events/<event_id>")
def get_event(event_id: str):
    path = event_path(event_id)
    if not os.path.exists(path):
        abort(404, description="Unbekannte Event-ID")
    with open(path, "r", encoding="utf-8") as f:
        return jsonify(json.load(f))
    
@app.get("/mock-metadata")
def mock_metadata():
    return jsonify({
        "created_at": iso_now(),
        "message_type": "GADS",
        "version": "v1.0.0",
        "data_url": "http://example.com/dummy.parquet",
        "metadata_url": request.url
    })

RECEIVED = []
@app.post("/event_sink")
def event_sink():
    data = request.get_json(silent=True) or {}
    RECEIVED.append({"at": iso_now(), "body": data})
    return jsonify(ok=True, received=len(RECEIVED))
@app.get("/event_sink")
def event_sink_list():
    return jsonify(RECEIVED)

if __name__ == "__main__":
    ensure_store()
    app.run(host=HOST, port=PORT, debug=True)
