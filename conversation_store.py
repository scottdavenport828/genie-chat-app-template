"""
Conversation Store

Tracks user-to-conversation ownership in a Delta table with an in-memory
cache layer.  All Genie API calls go through the service principal (SP),
so we need a side-channel to remember which user started which conversation.

Uses the SQL Statement Execution API (via the Databricks SDK) so it works
from any environment where the SP has warehouse access.
"""

import logging
import threading
from collections import defaultdict

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, StatementState

logger = logging.getLogger(__name__)


class ConversationStore:
    """Tracks user -> conversation ownership in Delta (persistent) with in-memory cache."""

    def __init__(self, warehouse_id: str, table_name: str):
        self._warehouse_id = warehouse_id
        self._table = table_name
        self._cache = defaultdict(set)  # user_email -> {conversation_ids}
        self._cache_loaded = set()  # emails whose data has been loaded from Delta
        self._lock = threading.Lock()
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
        """Create the schema and table if they don't exist yet."""
        if self._table_ready:
            return
        parts = self._table.split(".")
        if len(parts) == 3:
            catalog, schema, _ = parts
            self._execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "  user_email STRING NOT NULL,"
            "  conversation_id STRING NOT NULL,"
            "  created_at TIMESTAMP DEFAULT current_timestamp()"
            ")"
        )
        self._table_ready = True

    def record(self, user_email: str, conversation_id: str):
        """Record a new user -> conversation mapping."""
        self._ensure_table()
        from databricks.sdk.service.sql import StatementParameterListItem

        self._execute(
            f"INSERT INTO {self._table} (user_email, conversation_id) VALUES (:email, :conv_id)",
            parameters=[
                StatementParameterListItem(name="email", value=user_email),
                StatementParameterListItem(name="conv_id", value=conversation_id),
            ],
        )
        with self._lock:
            self._cache[user_email].add(conversation_id)
        logger.info(f"Recorded conversation {conversation_id} for {user_email}")

    def get_ids(self, user_email: str) -> set:
        """Return conversation IDs for a user. Cache-first, lazy-load from Delta."""
        with self._lock:
            if user_email in self._cache_loaded:
                return set(self._cache[user_email])

        # Cache miss — load from Delta
        self._ensure_table()
        from databricks.sdk.service.sql import StatementParameterListItem

        resp = self._execute(
            f"SELECT conversation_id FROM {self._table} WHERE user_email = :email",
            parameters=[
                StatementParameterListItem(name="email", value=user_email),
            ],
        )
        ids = set()
        if resp and resp.result and resp.result.data_array:
            ids = {row[0] for row in resp.result.data_array}

        with self._lock:
            self._cache[user_email] = ids
            self._cache_loaded.add(user_email)
        return set(ids)

    def remove(self, user_email: str, conversation_id: str):
        """Remove a conversation mapping from Delta and the cache."""
        self._ensure_table()
        from databricks.sdk.service.sql import StatementParameterListItem

        self._execute(
            f"DELETE FROM {self._table} WHERE user_email = :email AND conversation_id = :conv_id",
            parameters=[
                StatementParameterListItem(name="email", value=user_email),
                StatementParameterListItem(name="conv_id", value=conversation_id),
            ],
        )
        with self._lock:
            self._cache[user_email].discard(conversation_id)
        logger.info(f"Removed conversation {conversation_id} for {user_email}")
