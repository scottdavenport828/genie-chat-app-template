import os
import logging
import time
from flask import Flask, render_template, request, jsonify
from genie_client import GenieClient
from conversation_store import ConversationStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")
CONVERSATION_TABLE = os.environ.get("CONVERSATION_TABLE")
conv_store = ConversationStore(WAREHOUSE_ID, CONVERSATION_TABLE) if WAREHOUSE_ID and CONVERSATION_TABLE else None


def get_genie_client():
    """Create a GenieClient using the app service principal's default auth.

    The Genie API requires an OAuth scope named 'genie', but the Databricks
    Apps platform only offers 'dashboards.genie' as a user_api_scope — the two
    don't match, so the forwarded user token cannot call the Genie API.
    Until Databricks resolves this, all Genie calls go through the SP.
    """
    if not GENIE_SPACE_ID:
        return None
    return GenieClient(space_id=GENIE_SPACE_ID)


@app.route("/")
def index():
    user_email = request.headers.get("X-Forwarded-Email", "")
    user_name = request.headers.get("X-Forwarded-Preferred-Username", "")
    return render_template("index.html", user_email=user_email, user_name=user_name)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/ask", methods=["POST"])
def ask():
    genie = get_genie_client()
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

    # Record new conversation ownership
    if result.success and result.conversation_id and not conversation_id:
        user_email = request.headers.get("X-Forwarded-Email", "anonymous")
        if conv_store:
            try:
                conv_store.record(user_email, result.conversation_id)
            except Exception as e:
                logger.warning(f"Failed to record conversation ownership: {e}")

    return jsonify({
        "success": result.success,
        "response": result.raw_response,
        "sql_query": result.sql_query,
        "elapsed_seconds": result.elapsed_seconds,
        "error": result.error,
        "conversation_id": result.conversation_id,
        "message_id": result.message_id,
        "followup": result.followup,
    })


@app.route("/api/conversations")
def list_conversations():
    genie = get_genie_client()
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500

    all_conversations = genie.list_conversations()
    if conv_store:
        user_email = request.headers.get("X-Forwarded-Email", "anonymous")
        user_ids = conv_store.get_ids(user_email)
        conversations = [c for c in all_conversations if c["id"] in user_ids]
    else:
        conversations = all_conversations
    return jsonify({"success": True, "conversations": conversations})


@app.route("/api/conversations/<conversation_id>/messages/<message_id>/result")
def get_query_result(conversation_id, message_id):
    genie = get_genie_client()
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500
    result = genie.get_query_result(conversation_id, message_id)
    if "error" in result:
        return jsonify({"success": False, "error": result["error"]})
    return jsonify({"success": True, **result})


@app.route("/api/feedback", methods=["POST"])
def send_feedback():
    genie = get_genie_client()
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500
    data = request.get_json()
    conversation_id = data.get("conversation_id")
    message_id = data.get("message_id")
    rating = data.get("rating")
    if not all([conversation_id, message_id, rating]):
        return jsonify({"success": False, "error": "Missing required fields"}), 400
    success = genie.send_feedback(conversation_id, message_id, rating)
    return jsonify({"success": success})


@app.route("/api/conversations/<conversation_id>", methods=["DELETE"])
def delete_conversation(conversation_id):
    genie = get_genie_client()
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500
    success = genie.delete_conversation(conversation_id)
    if not success:
        return jsonify({"success": False, "error": "Failed to delete conversation"})
    if conv_store:
        user_email = request.headers.get("X-Forwarded-Email", "anonymous")
        try:
            conv_store.remove(user_email, conversation_id)
        except Exception as e:
            logger.warning(f"Failed to remove conversation mapping: {e}")
    return jsonify({"success": True})


@app.route("/api/conversations/<conversation_id>/messages")
def get_conversation_messages(conversation_id):
    genie = get_genie_client()
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500

    messages, error = genie.get_conversation_messages(conversation_id)
    if error:
        return jsonify({"success": False, "error": error})
    return jsonify({"success": True, "messages": messages})


if __name__ == "__main__":
    port = int(os.environ.get("FLASK_PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
