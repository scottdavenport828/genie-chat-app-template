"""
Genie API Client

Wrapper for the Databricks Genie API to ask natural language questions
and extract structured responses.

Best Practices Implemented (per Databricks docs):
- Exponential backoff for polling (1s initial, 60s max)
- Retry logic with exponential backoff for transient failures
- 10-minute timeout (600 seconds) as recommended
- Handles all terminal states: COMPLETED, FAILED, CANCELLED
- Comprehensive logging of API responses
"""

import time
import re
import random
import logging
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class GenieMessageStatus(Enum):
    """Status of a Genie message/query."""
    PENDING = "PENDING"
    EXECUTING = "EXECUTING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_SUCCESS_STATES = ["COMPLETED"]
TERMINAL_FAILURE_STATES = ["FAILED", "CANCELLED", "CANCELED", "ABORTED"]


@dataclass
class GenieResult:
    """Structured result from a Genie query."""
    success: bool
    raw_response: str
    query_result: Optional[List[Dict[str, Any]]] = None
    sql_query: Optional[str] = None
    error: Optional[str] = None
    elapsed_seconds: Optional[float] = None
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    def get_numeric_value(self) -> Optional[float]:
        """Extract a single numeric value from the response."""
        if self.query_result and len(self.query_result) > 0:
            first_row = self.query_result[0]
            for value in first_row.values():
                if isinstance(value, (int, float)):
                    return float(value)
                if isinstance(value, str):
                    try:
                        return float(value.replace(',', '').replace('%', ''))
                    except ValueError:
                        continue

        if self.raw_response:
            numbers = re.findall(r'[-+]?\d*\.?\d+', self.raw_response)
            if numbers:
                return float(numbers[0])

        return None


class GenieClient:
    """Client for interacting with Databricks Genie API."""

    INITIAL_POLL_INTERVAL = 1.0
    MAX_POLL_INTERVAL = 60.0
    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 1.0

    RETRYABLE_ERRORS = [
        "connection", "timeout", "rate limit", "429",
        "500", "502", "503", "504", "temporarily unavailable",
    ]

    FOLLOW_UP_SETTLE_SECONDS = 3.0       # Seconds between each check
    FOLLOW_UP_INITIAL_STABLE_CHECKS = 3  # Checks before declaring stable (no follow-up seen yet) = 9s
    FOLLOW_UP_CHAIN_STABLE_CHECKS = 6    # Checks after a follow-up completed (watch for chains) = 18s
    MAX_FOLLOW_UP_CYCLES = 20            # Overall cap

    def __init__(
        self,
        space_id: str,
        timeout_seconds: int = 600,
        initial_poll_interval: float = 1.0,
        max_poll_interval: float = 60.0,
        max_retries: int = 3,
        user_token: Optional[str] = None,
        host: Optional[str] = None
    ):
        self.space_id = space_id
        self.timeout_seconds = timeout_seconds
        self.initial_poll_interval = initial_poll_interval
        self.max_poll_interval = max_poll_interval
        self.max_retries = max_retries
        self._user_token = user_token
        self._host = host
        self._client = None

    @property
    def client(self):
        """Lazy initialization of Databricks SDK client."""
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            if self._user_token and self._host:
                self._client = WorkspaceClient(token=self._user_token, host=self._host)
                logger.debug("WorkspaceClient initialized with user token")
            else:
                self._client = WorkspaceClient()
                logger.debug("WorkspaceClient initialized with default auth")
        return self._client

    def _get_next_poll_interval(self, current_interval: float) -> float:
        return min(current_interval * 2, self.max_poll_interval)

    def _is_retryable_error(self, error: Exception) -> bool:
        error_str = str(error).lower()
        return any(indicator in error_str for indicator in self.RETRYABLE_ERRORS)

    def _retry_with_backoff(self, func: Callable, operation_name: str = "operation") -> Any:
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                return func()
            except Exception as e:
                last_exception = e

                if not self._is_retryable_error(e):
                    logger.error(f"{operation_name} failed with non-retryable error: {e}")
                    raise

                if attempt < self.max_retries - 1:
                    wait_time = (self.RETRY_BASE_DELAY * (2 ** attempt)) + random.uniform(0, 1)
                    logger.warning(f"{operation_name} failed (attempt {attempt + 1}): {e}. Retrying...")
                    time.sleep(wait_time)

        raise last_exception

    def _get_status_string(self, message) -> str:
        if hasattr(message, 'status'):
            status = message.status
            if hasattr(status, 'value'):
                return str(status.value).upper()
            return str(status).upper()
        return "UNKNOWN"

    def _is_terminal_success(self, status: str) -> bool:
        return any(s in status for s in TERMINAL_SUCCESS_STATES)

    def _is_terminal_failure(self, status: str) -> bool:
        return any(s in status for s in TERMINAL_FAILURE_STATES)

    def _poll_for_result(self, conversation_id: str, message_id: str, start_time: float,
                         _check_follow_ups: bool = True) -> GenieResult:
        """Poll a message until it reaches a terminal state and extract the result."""
        current_poll_interval = self.initial_poll_interval

        while time.time() - start_time < self.timeout_seconds:
            def get_msg():
                return self.client.genie.get_message(
                    space_id=self.space_id,
                    conversation_id=conversation_id,
                    message_id=message_id
                )

            try:
                message = self._retry_with_backoff(get_msg, "get_message")
            except Exception as poll_error:
                logger.warning(f"Poll failed: {poll_error}")
                time.sleep(current_poll_interval)
                current_poll_interval = self._get_next_poll_interval(current_poll_interval)
                continue

            status = self._get_status_string(message)
            elapsed = time.time() - start_time

            if self._is_terminal_success(status):
                if _check_follow_ups:
                    final = self._wait_for_final_message(conversation_id, message_id, start_time)
                    if final is not None:
                        return final

                result = self._extract_result(message)
                result.elapsed_seconds = elapsed
                result.conversation_id = conversation_id
                result.message_id = message_id
                logger.info(f"Query completed in {elapsed:.1f}s")
                return result

            if self._is_terminal_failure(status):
                error_msg = getattr(message, 'error', f'Query {status}')
                return GenieResult(
                    success=False,
                    raw_response="",
                    error=str(error_msg),
                    elapsed_seconds=elapsed,
                    conversation_id=conversation_id,
                    message_id=message_id
                )

            time.sleep(current_poll_interval)
            current_poll_interval = self._get_next_poll_interval(current_poll_interval)

        elapsed = time.time() - start_time
        return GenieResult(
            success=False,
            raw_response="",
            error=f"Query timed out after {elapsed:.0f} seconds.",
            elapsed_seconds=elapsed,
            conversation_id=conversation_id,
            message_id=message_id
        )

    def _wait_for_final_message(self, conversation_id: str, last_known_id: str,
                                start_time: float) -> Optional[GenieResult]:
        """Check if Genie spawned follow-up messages with a refined answer.

        Returns the final GenieResult if a newer message was found, or None if
        the original message is already the final answer.
        """
        original_id = last_known_id
        consecutive_stable = 0
        for cycle in range(self.MAX_FOLLOW_UP_CYCLES):
            if time.time() - start_time >= self.timeout_seconds:
                logger.warning("Timeout reached while checking for follow-up messages")
                break

            time.sleep(self.FOLLOW_UP_SETTLE_SECONDS)

            try:
                def list_msgs():
                    return self.client.genie.list_conversation_messages(
                        space_id=self.space_id,
                        conversation_id=conversation_id,
                    )
                response = self._retry_with_backoff(list_msgs, "list_conversation_messages (follow-up)")
            except Exception as e:
                logger.warning(f"Failed to list conversation messages for follow-up check: {e}")
                break

            items = response.messages if hasattr(response, 'messages') else response
            if not items:
                break

            # Find the latest message by last_updated_timestamp
            latest_msg = max(
                items,
                key=lambda m: getattr(m, 'last_updated_timestamp', 0) or 0,
            )
            latest_id = getattr(latest_msg, 'message_id', None) or getattr(latest_msg, 'id', None)

            if latest_id == last_known_id:
                consecutive_stable += 1
                # Short wait if no follow-up seen yet; longer wait after a follow-up completed
                required = (self.FOLLOW_UP_INITIAL_STABLE_CHECKS
                            if last_known_id == original_id
                            else self.FOLLOW_UP_CHAIN_STABLE_CHECKS)
                if consecutive_stable >= required:
                    logger.debug(f"Follow-up cycle {cycle + 1}: conversation stabilized after "
                                 f"{consecutive_stable} consecutive checks")
                    return None
                logger.debug(f"Follow-up cycle {cycle + 1}: no new message yet "
                             f"({consecutive_stable}/{required} stable checks)")
                continue

            # New message appeared — reset stable counter
            consecutive_stable = 0
            status = self._get_status_string(latest_msg)
            logger.info(f"Follow-up message detected: {latest_id} (status={status}, cycle={cycle + 1})")

            if self._is_terminal_failure(status):
                logger.warning(f"Follow-up message {latest_id} failed ({status}), using original result")
                return None

            if self._is_terminal_success(status):
                # Completed — but loop once more to confirm no further follow-ups
                last_known_id = latest_id
                continue

            # Still processing — poll it to completion (without recursing into follow-up checks)
            result = self._poll_for_result(conversation_id, latest_id, start_time,
                                           _check_follow_ups=False)
            if not result.success:
                logger.warning(f"Follow-up message {latest_id} failed during polling, using original result")
                return None
            # Polled to completion; loop again to check for yet another follow-up
            last_known_id = latest_id

        # If we got here via a completed follow-up (last_known_id changed), extract that result
        if last_known_id != original_id:
            # Re-fetch the final message to extract its result
            try:
                def get_final():
                    return self.client.genie.get_message(
                        space_id=self.space_id,
                        conversation_id=conversation_id,
                        message_id=last_known_id,
                    )
                final_msg = self._retry_with_backoff(get_final, "get_message (final)")
                result = self._extract_result(final_msg)
                result.elapsed_seconds = time.time() - start_time
                result.conversation_id = conversation_id
                result.message_id = last_known_id
                logger.info(f"Returning follow-up message {last_known_id} as final answer")
                return result
            except Exception as e:
                logger.warning(f"Failed to fetch final follow-up message: {e}")

        return None

    def ask(self, question: str) -> GenieResult:
        """Ask Genie a question by starting a new conversation."""
        logger.info(f"Asking Genie: {question[:100]}...")
        start_time = time.time()

        try:
            def start_conv():
                return self.client.genie.start_conversation(
                    space_id=self.space_id,
                    content=question
                )

            conversation = self._retry_with_backoff(start_conv, "start_conversation")
            conversation_id = conversation.conversation_id
            message_id = conversation.message_id
            logger.info(f"Started conversation {conversation_id}, message {message_id}")

            return self._poll_for_result(conversation_id, message_id, start_time)

        except Exception as e:
            elapsed = time.time() - start_time
            logger.exception(f"Error querying Genie: {e}")
            return GenieResult(
                success=False,
                raw_response="",
                error=str(e),
                elapsed_seconds=elapsed
            )

    def continue_conversation(self, conversation_id: str, question: str) -> GenieResult:
        """Continue an existing Genie conversation with a follow-up question."""
        logger.info(f"Continuing conversation {conversation_id}: {question[:100]}...")
        start_time = time.time()

        try:
            def create_msg():
                return self.client.genie.create_message(
                    space_id=self.space_id,
                    conversation_id=conversation_id,
                    content=question
                )

            wait_resp = self._retry_with_backoff(create_msg, "create_message")
            message_id = wait_resp.message_id
            logger.info(f"Created message {message_id} in conversation {conversation_id}")

            return self._poll_for_result(conversation_id, message_id, start_time)

        except Exception as e:
            elapsed = time.time() - start_time
            logger.exception(f"Error continuing conversation: {e}")
            return GenieResult(
                success=False,
                raw_response="",
                error=str(e),
                elapsed_seconds=elapsed,
                conversation_id=conversation_id
            )

    def list_conversations(self) -> List[Dict[str, Any]]:
        """List recent Genie conversations for this space."""
        try:
            def list_convs():
                return self.client.genie.list_conversations(space_id=self.space_id)

            response = self._retry_with_backoff(list_convs, "list_conversations")

            conversations = []
            items = response.conversations if hasattr(response, 'conversations') else response
            if items:
                for conv in items:
                    conversations.append({
                        "id": getattr(conv, 'conversation_id', None) or conv.id,
                        "title": getattr(conv, 'title', 'Untitled'),
                        "created_at": getattr(conv, 'created_timestamp', None),
                        "updated_at": getattr(conv, 'last_updated_timestamp', None),
                    })
            return conversations

        except Exception as e:
            logger.exception(f"Error listing conversations: {e}")
            return []

    def get_query_result(self, conversation_id: str, message_id: str) -> Dict[str, Any]:
        """Get the query result data (columns + rows) for a completed message.

        Always returns a dict. On success it contains columns/rows/total_rows.
        On failure it contains an "error" key describing what went wrong.

        The Genie get_message_query_result API often returns data_array=None.
        When that happens we fall back to the Statement Execution API using
        the statement_id from the response.
        """
        try:
            def get_result():
                return self.client.genie.get_message_query_result(
                    space_id=self.space_id, conversation_id=conversation_id, message_id=message_id
                )
            response = self._retry_with_backoff(get_result, "get_message_query_result")

            stmt = response.statement_response
            if not stmt:
                logger.warning("statement_response is None")
                return {"error": "statement_response is None — query may still be executing"}
            if not stmt.manifest:
                logger.warning("statement_response.manifest is None")
                return {"error": "manifest is None — no schema returned"}

            columns = [
                {"name": c.name, "type": str(c.type_name.value) if c.type_name else "STRING"}
                for c in (stmt.manifest.schema.columns or [])
            ]

            # Try inline data first
            rows = stmt.result.data_array if stmt.result else None
            if rows is not None:
                return {
                    "columns": columns,
                    "rows": rows,
                    "total_rows": stmt.manifest.total_row_count,
                }

            # No inline data — could be an empty result set or a large result needing fallback
            statement_id = getattr(stmt, 'statement_id', None)
            if not statement_id:
                # No fallback available — treat as empty result (query succeeded with 0 rows)
                logger.info("data_array is None with no statement_id — returning empty result set")
                return {
                    "columns": columns,
                    "rows": [],
                    "total_rows": 0,
                }

            logger.info(f"Falling back to statement_execution API (statement_id={statement_id})")
            return self._fetch_statement_result(statement_id, columns)

        except Exception as e:
            logger.exception(f"Error getting query result: {e}")
            return {"error": f"Exception: {e}"}

    def _fetch_statement_result(self, statement_id: str, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Fetch result data from the Statement Execution API."""
        try:
            def get_stmt():
                return self.client.statement_execution.get_statement(statement_id)
            result = self._retry_with_backoff(get_stmt, "get_statement")

            status = result.status
            if status and status.state:
                state_str = str(status.state.value) if hasattr(status.state, 'value') else str(status.state)
                if state_str != "SUCCEEDED":
                    err = getattr(status, 'error', None)
                    logger.warning(f"Statement {statement_id} state={state_str}, error={err}")
                    return {"error": f"Statement not succeeded (state={state_str})"}

            manifest = result.manifest
            if manifest and manifest.schema and manifest.schema.columns:
                columns = [
                    {"name": c.name, "type": str(c.type_name.value) if c.type_name else "STRING"}
                    for c in manifest.schema.columns
                ]

            rows = result.result.data_array if result.result else None
            if rows is None:
                # Query succeeded but returned no rows — treat as empty result set
                logger.info(f"Statement {statement_id}: data_array is None — returning empty result set")
                rows = []

            total_rows = manifest.total_row_count if manifest else len(rows)
            return {
                "columns": columns,
                "rows": rows,
                "total_rows": total_rows,
            }
        except Exception as e:
            logger.exception(f"Error fetching statement result: {e}")
            return {"error": f"Statement fetch failed: {e}"}

    def send_feedback(self, conversation_id: str, message_id: str, rating: str) -> bool:
        """Send thumbs up/down feedback on a Genie message."""
        try:
            from databricks.sdk.service.dashboards import GenieFeedbackRating
            rating_enum = GenieFeedbackRating.POSITIVE if rating == "positive" else GenieFeedbackRating.NEGATIVE

            def send_fb():
                return self.client.genie.send_message_feedback(
                    space_id=self.space_id, conversation_id=conversation_id,
                    message_id=message_id, rating=rating_enum
                )
            self._retry_with_backoff(send_fb, "send_message_feedback")
            return True
        except Exception as e:
            logger.exception(f"Error sending feedback: {e}")
            return False

    def delete_conversation(self, conversation_id: str) -> bool:
        """Delete a conversation from the Genie space."""
        try:
            def delete_conv():
                return self.client.genie.delete_conversation(
                    space_id=self.space_id, conversation_id=conversation_id
                )
            self._retry_with_backoff(delete_conv, "delete_conversation")
            return True
        except Exception as e:
            logger.exception(f"Error deleting conversation: {e}")
            return False

    def get_conversation_messages(self, conversation_id: str):
        """Get all messages in a conversation with extracted results.

        Returns:
            Tuple of (messages_list, error_string_or_None)
        """
        try:
            def list_msgs():
                return self.client.genie.list_conversation_messages(
                    space_id=self.space_id,
                    conversation_id=conversation_id
                )

            response = self._retry_with_backoff(list_msgs, "list_conversation_messages")

            messages = []
            items = response.messages if hasattr(response, 'messages') else response
            if items:
                logger.info(f"Processing {len(items)} raw GenieMessages for conversation {conversation_id}")
                for msg in items:
                    # 1. User message (always present when content exists)
                    if hasattr(msg, 'content') and msg.content:
                        messages.append({
                            "role": "user",
                            "content": str(msg.content),
                            "sql_query": None,
                            "timestamp": getattr(msg, 'created_timestamp', None),
                        })

                    # 2. Assistant response (from attachments on completed messages)
                    if hasattr(msg, 'attachments') and msg.attachments:
                        result = self._extract_result(msg)
                        if result.raw_response or result.sql_query:
                            messages.append({
                                "role": "assistant",
                                "content": result.raw_response or "(Query executed)",
                                "sql_query": result.sql_query,
                                "message_id": getattr(msg, 'message_id', None) or getattr(msg, 'id', None),
                                "timestamp": getattr(msg, 'last_updated_timestamp', None),
                            })

            # Sort by timestamp for correct ordering
            messages.sort(key=lambda m: m.get("timestamp") or 0)

            user_count = sum(1 for m in messages if m["role"] == "user")
            asst_count = sum(1 for m in messages if m["role"] == "assistant")
            logger.info(f"Extracted {user_count} user + {asst_count} assistant messages")
            return messages, None

        except Exception as e:
            logger.exception(f"Error getting conversation messages: {e}")
            return [], str(e)

    def _extract_result(self, message) -> GenieResult:
        """Extract structured result from a completed Genie message."""
        try:
            query_texts = []   # text from attachments that have a SQL query (= the answer)
            other_texts = []   # text from attachments without a query (= follow-up question)
            sql_query = None

            if hasattr(message, 'attachments') and message.attachments:
                for attachment in message.attachments:
                    has_query = hasattr(attachment, 'query') and attachment.query
                    if has_query:
                        if hasattr(attachment.query, 'query'):
                            sql_query = attachment.query.query
                    if hasattr(attachment, 'text') and hasattr(attachment.text, 'content'):
                        if has_query:
                            query_texts.append(attachment.text.content)
                        else:
                            other_texts.append(attachment.text.content)

            # Answer first, follow-up question(s) after
            raw_response = "\n".join(query_texts + other_texts)

            return GenieResult(
                success=True,
                raw_response=raw_response.strip(),
                sql_query=sql_query,
            )

        except Exception as e:
            logger.warning(f"Error extracting result: {e}")
            return GenieResult(
                success=True,
                raw_response=str(message),
                error=f"Partial extraction: {e}"
            )
