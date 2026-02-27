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

    if result.success and result.conversation_id:
        user_email = request.headers.get("X-Forwarded-Email", "anonymous")
        if conv_store:
            try:
                if not conversation_id:
                    # New conversation — record ownership and store title
                    conv_store.record(user_email, result.conversation_id, title=question[:60])
                else:
                    # Follow-up message — bump updated_at so sidebar order stays current
                    conv_store.touch(user_email, result.conversation_id)
            except Exception as e:
                logger.warning(f"Failed to update conversation store: {e}")

    return jsonify({
        "success": result.success,
        "response": result.raw_response,
        "sql_query": result.sql_query,
        "elapsed_seconds": result.elapsed_seconds,
        "error": result.error,
        "conversation_id": result.conversation_id,
        "message_id": result.message_id,
    })


@app.route("/api/conversations")
def list_conversations():
    genie = get_genie_client()
    if not genie:
        return jsonify({"success": False, "error": "GENIE_SPACE_ID not configured"}), 500

    if conv_store:
        user_email = request.headers.get("X-Forwarded-Email", "anonymous")
        # Delta table is the authoritative source — every conversation the user
        # has ever started is here regardless of Genie API pagination limits.
        delta_conversations = conv_store.get_conversations(user_email)

        # Enrich with live Genie metadata (updated_at, authoritative title) where
        # available, but never drop a conversation just because Genie didn't return it.
        try:
            genie_lookup = {c["id"]: c for c in genie.list_conversations()}
        except Exception as e:
            logger.warning(f"Could not fetch Genie conversation list for enrichment: {e}")
            genie_lookup = {}

        conversations = []
        for conv in delta_conversations:
            genie_data = genie_lookup.get(conv["id"], {})
            conversations.append({
                "id": conv["id"],
                "title": genie_data.get("title") or conv["title"],
                "created_at": conv["created_at"],
                # Prefer Genie's updated_at; fall back to Delta's own updated_at
                # so the sidebar always has a meaningful recency timestamp.
                "updated_at": genie_data.get("updated_at") or conv.get("updated_at"),
            })
    else:
        conversations = genie.list_conversations()

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
