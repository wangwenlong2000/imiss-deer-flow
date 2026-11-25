"""Durable, tenant-scoped mobile conversation bindings.

Each operation owns a short SQLite transaction. Network calls must happen after
the connection closes, so concurrent Gateway requests never hold a write lock
while waiting for LangGraph. Reserving a deterministic thread ID lets competing
requests recover from an upstream create conflict and from process restarts.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from uuid import NAMESPACE_URL, uuid5

from deerflow.config.paths import get_paths


def get_or_create_conversation_thread(
    *, org_id: str, user_id: str, conversation_id: str
) -> str:
    """Atomically reserve one thread for an organization/user/conversation.

    The empty organization is reserved for unauthenticated development calls;
    it cannot collide with a verified IAM organization.
    """
    if not conversation_id.strip():
        raise ValueError("conversation_id is required")
    if org_id and (not org_id.strip() or not user_id.strip()):
        raise ValueError(
            "Authenticated conversations require organization and user IDs"
        )

    database = get_paths().base_dir / "gateway.sqlite3"
    database.parent.mkdir(parents=True, exist_ok=True)
    proposed_thread_id = str(
        uuid5(
            NAMESPACE_URL,
            "deerflow:conversation:v1:"
            + json.dumps([org_id, user_id, conversation_id], ensure_ascii=False),
        )
    )
    with closing(sqlite3.connect(database, timeout=5)) as connection:
        with connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS mobile_conversations (
                    org_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL UNIQUE,
                    PRIMARY KEY (org_id, user_id, conversation_id)
                )"""
            )
            connection.execute(
                """INSERT INTO mobile_conversations (org_id, user_id, conversation_id, thread_id)
                VALUES (?, ?, ?, ?) ON CONFLICT (org_id, user_id, conversation_id) DO NOTHING""",
                (org_id, user_id, conversation_id, proposed_thread_id),
            )
            record = connection.execute(
                "SELECT thread_id FROM mobile_conversations WHERE org_id = ? AND user_id = ? AND conversation_id = ?",
                (org_id, user_id, conversation_id),
            ).fetchone()
    if record is None:
        raise RuntimeError("Conversation thread reservation was not persisted")
    return str(record[0])


__all__ = ["get_or_create_conversation_thread"]
