from sms77api.Sms77api import Sms77api
from flask import Flask, request, jsonify
import time
import threading
import json
import os


api_key = os.getenv("test_key")
app = Flask(__name__)
api = Sms77api(api_key=api_key)

CALLER_NUMBER_1 = os.getenv("caller_number")
CALLER_NUMBER_2 = ""
CALLER_NUMBER_3 = ""
TARGET_PHONE_NUMBER = os.getenv("target_number")
MESSAGE = "Test Message! Just a test Message!"

def make_call_basic():
    print("Sending basic voice call...")
    response = api.voice(
        to=TARGET_PHONE_NUMBER,
        text=MESSAGE,
        params={"ringtime": 45, "foreign_id": "control_center_hamm", "from": CALLER_NUMBER_1}
    )
    print("Call response:", response)

def retry_call():
    print("Waiting 3 minutes before retry...")
    time.sleep(180)
    make_call_basic()

@app.route("/start", methods=["GET"])
def start_call():
    make_call_basic()
    return "Call sent", 200

@app.route("/webhook/voice", methods=["POST"])
def handle_webhook():
    data = request.json
    """    
    status = data.get("data", {}).get("status")
    print("Webhook received. Call status:", status)

    if status in ("no-answer", "busy", "rejected"):
        threading.Thread(target=retry_call).start()
    elif status in ("in-progress", "completed"):
        print("Call answered.")
    else:
        print(f"Unknown status: {status}")"""

    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    return jsonify(success=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
