"""
Conversation Store

Tracks user-to-conversation ownership in a Delta table.  All Genie API
calls go through the service principal (SP), so we need a side-channel
to remember which user started which conversation.

Uses the SQL Statement Execution API (via the Databricks SDK) so it works
from any environment where the SP has warehouse access.
"""

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, StatementState

logger = logging.getLogger(__name__)


class ConversationStore:
    """Tracks user -> conversation ownership in Delta (persistent)."""

    def __init__(self, warehouse_id: str, table_name: str):
        self._warehouse_id = warehouse_id
        self._table = table_name
        self._client = None
        self._table_ready = False

    @property
    def client(self):
        if self._client is None:
            self._client = WorkspaceClient()
        return self._client

    def _execute(self, statement: str, parameters=None):
        """Execute a SQL statement and return the response."""
        try:
            resp = self.client.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=statement,
                parameters=parameters,
                disposition=Disposition.INLINE,
                wait_timeout="30s",
            )
            if resp.status and resp.status.state == StatementState.FAILED:
                logger.error(f"SQL failed: {resp.status.error}")
                return None
            return resp
        except Exception as e:
            logger.exception(f"Statement execution error: {e}")
            return None

    def _ensure_table(self):
        """Verify the table exists (must be pre-created by an admin)."""
        if self._table_ready:
            return
        resp = self._execute(f"SELECT 1 FROM {self._table} LIMIT 0")
        if resp is not None:
            self._table_ready = True
        else:
            logger.warning(
                f"Table {self._table} is not accessible. "
                "An admin must create the schema and table, then grant "
                "USE SCHEMA + SELECT + MODIFY to the app service principal."
            )

    def record(self, user_email: str, conversation_id: str, title: str = None):
        """Record a new user -> conversation mapping, with an optional title.

        Sets both created_at and updated_at to the current timestamp on insert.
        """
        self._ensure_table()
        from databricks.sdk.service.sql import StatementParameterListItem

        self._execute(
            f"INSERT INTO {self._table} (user_email, conversation_id, title, created_at, updated_at) "
            "VALUES (:email, :conv_id, :title, current_timestamp(), current_timestamp())",
            parameters=[
                StatementParameterListItem(name="email", value=user_email),
                StatementParameterListItem(name="conv_id", value=conversation_id),
                StatementParameterListItem(name="title", value=title or ""),
            ],
        )
        logger.info(f"Recorded conversation {conversation_id} for {user_email}")

    def touch(self, user_email: str, conversation_id: str):
        """Update updated_at to now for an existing conversation.

        Called on every follow-up message so the sidebar sorts most-recently
        active conversations to the top.
        """
        self._ensure_table()
        from databricks.sdk.service.sql import StatementParameterListItem

        self._execute(
            f"UPDATE {self._table} SET updated_at = current_timestamp() "
            "WHERE user_email = :email AND conversation_id = :conv_id",
            parameters=[
                StatementParameterListItem(name="email", value=user_email),
                StatementParameterListItem(name="conv_id", value=conversation_id),
            ],
        )
        logger.debug(f"Touched conversation {conversation_id} for {user_email}")

    def get_conversations(self, user_email: str) -> list:
        """Return all conversations for a user with stored metadata from Delta.

        This is the authoritative listing source — it always returns every
        conversation the user has ever started, regardless of what the Genie
        API happens to return in a given list_conversations call.

        Results are ordered by updated_at descending so the most recently
        active conversation appears first in the sidebar.
        """
        self._ensure_table()
        from databricks.sdk.service.sql import StatementParameterListItem

        resp = self._execute(
            f"SELECT conversation_id, title, created_at, updated_at FROM {self._table} "
            "WHERE user_email = :email ORDER BY updated_at DESC",
            parameters=[
                StatementParameterListItem(name="email", value=user_email),
            ],
        )
        conversations = []
        if resp and resp.result and resp.result.data_array:
            for row in resp.result.data_array:
                conversations.append({
                    "id": row[0],
                    "title": row[1] or "Untitled",
                    "created_at": row[2],
                    "updated_at": row[3],
                })
        return conversations

    def get_ids(self, user_email: str) -> set:
        """Return conversation IDs for a user (always reads from Delta)."""
        return {c["id"] for c in self.get_conversations(user_email)}

    def remove(self, user_email: str, conversation_id: str):
        """Remove a conversation mapping from Delta."""
        self._ensure_table()
        from databricks.sdk.service.sql import StatementParameterListItem

        self._execute(
            f"DELETE FROM {self._table} WHERE user_email = :email AND conversation_id = :conv_id",
            parameters=[
                StatementParameterListItem(name="email", value=user_email),
                StatementParameterListItem(name="conv_id", value=conversation_id),
            ],
        )
        logger.info(f"Removed conversation {conversation_id} for {user_email}")
