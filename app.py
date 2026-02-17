import os
import logging
import time
from flask import Flask, render_template, request, jsonify
from genie_client import GenieClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID")
genie = GenieClient(space_id=GENIE_SPACE_ID) if GENIE_SPACE_ID else None


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/ask", methods=["POST"])
def ask():
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500

    data = request.get_json()
    question = data.get("question", "").strip()
    if not question:
        return jsonify({"success": False, "error": "No question provided"}), 400

    conversation_id = data.get("conversation_id")

    if conversation_id:
        result = genie.continue_conversation(conversation_id, question)
    else:
        result = genie.ask(question)

    return jsonify({
        "success": result.success,
        "response": result.raw_response,
        "sql_query": result.sql_query,
        "elapsed_seconds": result.elapsed_seconds,
        "error": result.error,
        "conversation_id": result.conversation_id,
    })


@app.route("/api/conversations")
def list_conversations():
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500

    conversations = genie.list_conversations()
    return jsonify({"success": True, "conversations": conversations})


@app.route("/api/conversations/<conversation_id>/messages")
def get_conversation_messages(conversation_id):
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500

    messages, error = genie.get_conversation_messages(conversation_id)
    if error:
        return jsonify({"success": False, "error": error})
    return jsonify({"success": True, "messages": messages})


if __name__ == "__main__":
    port = int(os.environ.get("FLASK_PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
