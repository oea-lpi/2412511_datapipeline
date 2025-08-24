# app.py
from flask import Flask, request, jsonify, abort
import hmac, hashlib, os, threading, queue, json

# ---- Settings (set with environment variables in production) ----
SHARED_SECRET = os.environ.get("WEBHOOK_SECRET", "change-me")  # set a strong secret
MAX_BYTES = int(os.environ.get("MAX_BYTES", "16384"))          # 16 KB, adjust as needed
REQUIRE_TOKEN = os.environ.get("REQUIRE_TOKEN", "1") == "1"
TOKEN_HEADER = "X-Webhook-Token"
SIGNATURE_HEADER = "X-Signature"                               # HMAC-SHA256 hex digest

# ---- App & background worker ----
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_BYTES                    # hard request cap

jobs = queue.Queue()

def worker():
    while True:
        payload = jobs.get()
        try:
            # TODO: put your background logic here
            # e.g., write to a DB, post to Redis, trigger another service...
            print("Processing:", payload)
        finally:
            jobs.task_done()

threading.Thread(target=worker, daemon=True).start()

# ---- Helpers ----
def verify_signature(raw_body: bytes, signature_hex: str) -> bool:
    if not signature_hex:
        return False
    mac = hmac.new(SHARED_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature_hex)

# ---- Endpoint ----
@app.post("/webhook")
def webhook():
    # content-type check (many providers send application/json)
    if request.mimetype != "application/json":
        abort(415, description="JSON only")

    # optional fixed token (simple auth)
    if REQUIRE_TOKEN:
        token = request.headers.get(TOKEN_HEADER, "")
        if token != SHARED_SECRET:
            abort(401)

    # signature verification (recommended)
    raw = request.get_data(cache=False, as_text=False)
    if not verify_signature(raw, request.headers.get(SIGNATURE_HEADER, "")):
        abort(401, description="Bad signature")

    # parse JSON safely
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        abort(400, description="Invalid JSON")

    # enqueue for background processing; reply fast
    jobs.put(data)
    return jsonify({"ok": True}), 200

@app.get("/")
def health():
    return "OK", 200

if __name__ == "__main__":
    # Dev only. In production run via gunicorn: gunicorn -w 2 -b 0.0.0.0:8000 app:app
    app.run(host="0.0.0.0", port=8000)
