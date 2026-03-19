from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _init_db(self) -> None:
        conn = self._connect()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topics (
                topic_id TEXT PRIMARY KEY,
                group_id TEXT NOT NULL,
                type TEXT,
                create_time TEXT,
                create_time_iso TEXT,
                text TEXT,
                answer_text TEXT,
                owner_name TEXT,
                like_count INTEGER NOT NULL DEFAULT 0,
                comment_count INTEGER NOT NULL DEFAULT 0,
                liked INTEGER NOT NULL DEFAULT 0,
                topic_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_sync_state (
                group_id TEXT PRIMARY KEY,
                latest_topic_id TEXT,
                latest_create_time TEXT,
                latest_create_time_iso TEXT,
                last_synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                document_id TEXT PRIMARY KEY,
                group_id TEXT NOT NULL,
                topic_id TEXT NOT NULL,
                file_id TEXT NOT NULL,
                name TEXT NOT NULL,
                download_url TEXT,
                content_type TEXT,
                size INTEGER,
                local_path TEXT NOT NULL,
                extraction_status TEXT NOT NULL,
                extracted_text TEXT,
                document_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS filtered_topics (
                topic_id TEXT PRIMARY KEY,
                group_id TEXT NOT NULL,
                create_time_iso TEXT,
                text TEXT,
                topic_json TEXT NOT NULL,
                filtered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_topics_group_time "
            "ON topics(group_id, create_time_iso DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_documents_group "
            "ON documents(group_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_filtered_topics_group "
            "ON filtered_topics(group_id, create_time_iso DESC)"
        )
        conn.commit()

    def upsert_topics(self, topics: list[dict[str, Any]], group_id: str) -> int:
        saved_count = 0
        conn = self._connect()
        for topic in topics:
            topic_id = topic.get("topic_id")
            if topic_id is None:
                continue
            conn.execute(
                """
                INSERT INTO topics (
                    topic_id, group_id, type, create_time, create_time_iso, text,
                    answer_text, owner_name, like_count, comment_count, liked, topic_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(topic_id) DO UPDATE SET
                    group_id=excluded.group_id,
                    type=excluded.type,
                    create_time=excluded.create_time,
                    create_time_iso=excluded.create_time_iso,
                    text=excluded.text,
                    answer_text=excluded.answer_text,
                    owner_name=excluded.owner_name,
                    like_count=excluded.like_count,
                    comment_count=excluded.comment_count,
                    liked=excluded.liked,
                    topic_json=excluded.topic_json,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    str(topic_id),
                    group_id,
                    topic.get("type"),
                    topic.get("create_time"),
                    topic.get("create_time_iso"),
                    topic.get("text"),
                    topic.get("answer_text"),
                    (topic.get("owner") or {}).get("name"),
                    topic.get("like_count", 0),
                    topic.get("comment_count", 0),
                    1 if topic.get("liked") else 0,
                    json.dumps(topic, ensure_ascii=False),
                ),
            )
            saved_count += 1
        conn.commit()
        return saved_count

    def upsert_filtered_topics(self, topics: list[dict[str, Any]], group_id: str) -> int:
        saved_count = 0
        conn = self._connect()
        for topic in topics:
            topic_id = topic.get("topic_id")
            if topic_id is None:
                continue
            conn.execute(
                """
                INSERT INTO filtered_topics (
                    topic_id, group_id, create_time_iso, text, topic_json
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(topic_id) DO UPDATE SET
                    group_id=excluded.group_id,
                    create_time_iso=excluded.create_time_iso,
                    text=excluded.text,
                    topic_json=excluded.topic_json,
                    filtered_at=CURRENT_TIMESTAMP
                """,
                (
                    str(topic_id),
                    group_id,
                    topic.get("create_time_iso"),
                    topic.get("text"),
                    json.dumps(topic, ensure_ascii=False),
                ),
            )
            saved_count += 1
        conn.commit()
        return saved_count

    def list_topics(
        self,
        limit: int = 20,
        offset: int = 0,
        group_id: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT topic_json
            FROM topics
        """
        params: list[Any] = []
        if group_id:
            query += " WHERE group_id = ?"
            params.append(group_id)
        query += " ORDER BY datetime(create_time_iso) DESC, topic_id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        conn = self._connect()
        rows = conn.execute(query, params).fetchall()
        return [json.loads(row["topic_json"]) for row in rows]

    def topic_exists(self, topic_id: str | int, group_id: str) -> bool:
        conn = self._connect()
        row = conn.execute(
            "SELECT 1 FROM topics WHERE topic_id = ? AND group_id = ? LIMIT 1",
            (str(topic_id), group_id),
        ).fetchone()
        return row is not None

    def topic_ids_exist(self, topic_ids: list[str], group_id: str) -> set[str]:
        if not topic_ids:
            return set()
        conn = self._connect()
        placeholders = ",".join("?" for _ in topic_ids)
        rows = conn.execute(
            f"SELECT topic_id FROM topics WHERE group_id = ? AND topic_id IN ({placeholders})",
            [group_id, *topic_ids],
        ).fetchall()
        return {row["topic_id"] for row in rows}

    def get_latest_topic_marker(self, group_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        row = conn.execute(
            """
            SELECT latest_topic_id, latest_create_time, latest_create_time_iso, last_synced_at
            FROM group_sync_state
            WHERE group_id = ?
            """,
            (group_id,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def update_group_sync_state(
        self,
        group_id: str,
        latest_topic_id: str | int | None,
        latest_create_time: str | None,
        latest_create_time_iso: str | None,
    ) -> None:
        conn = self._connect()
        conn.execute(
            """
            INSERT INTO group_sync_state (
                group_id,
                latest_topic_id,
                latest_create_time,
                latest_create_time_iso,
                last_synced_at
            ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(group_id) DO UPDATE SET
                latest_topic_id=excluded.latest_topic_id,
                latest_create_time=excluded.latest_create_time,
                latest_create_time_iso=excluded.latest_create_time_iso,
                last_synced_at=CURRENT_TIMESTAMP
            """,
            (
                group_id,
                str(latest_topic_id) if latest_topic_id is not None else None,
                latest_create_time,
                latest_create_time_iso,
            ),
        )
        conn.commit()

    def get_all_sync_states(self) -> list[dict[str, Any]]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT group_id, latest_topic_id, latest_create_time_iso, last_synced_at "
            "FROM group_sync_state ORDER BY last_synced_at DESC"
        ).fetchall()
        return [dict(row) for row in rows]

    def upsert_documents(self, documents: list[dict[str, Any]]) -> int:
        saved_count = 0
        conn = self._connect()
        for document in documents:
            document_id = document.get("document_id")
            if not document_id:
                continue
            conn.execute(
                """
                INSERT INTO documents (
                    document_id, group_id, topic_id, file_id, name, download_url,
                    content_type, size, local_path, extraction_status, extracted_text, document_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(document_id) DO UPDATE SET
                    group_id=excluded.group_id,
                    topic_id=excluded.topic_id,
                    file_id=excluded.file_id,
                    name=excluded.name,
                    download_url=excluded.download_url,
                    content_type=excluded.content_type,
                    size=excluded.size,
                    local_path=excluded.local_path,
                    extraction_status=excluded.extraction_status,
                    extracted_text=excluded.extracted_text,
                    document_json=excluded.document_json,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    str(document_id),
                    document.get("group_id"),
                    str(document.get("topic_id")),
                    str(document.get("file_id")),
                    document.get("name"),
                    document.get("download_url"),
                    document.get("content_type"),
                    document.get("size"),
                    document.get("local_path"),
                    document.get("extraction_status"),
                    document.get("extracted_text"),
                    json.dumps(document, ensure_ascii=False),
                ),
            )
            saved_count += 1
        conn.commit()
        return saved_count

    def list_documents(
        self,
        limit: int = 20,
        offset: int = 0,
        group_id: str | None = None,
        topic_id: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT document_json
            FROM documents
        """
        clauses: list[str] = []
        params: list[Any] = []
        if group_id:
            clauses.append("group_id = ?")
            params.append(group_id)
        if topic_id:
            clauses.append("topic_id = ?")
            params.append(topic_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, document_id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        conn = self._connect()
        rows = conn.execute(query, params).fetchall()
        return [json.loads(row["document_json"]) for row in rows]

    def search_documents(
        self,
        query_text: str,
        limit: int = 20,
        offset: int = 0,
        group_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_query = query_text.strip()
        if not normalized_query:
            return []

        query = """
            SELECT document_json, extracted_text
            FROM documents
            WHERE extracted_text IS NOT NULL
              AND extracted_text != ''
              AND lower(extracted_text) LIKE lower(?)
        """
        params: list[Any] = [f"%{normalized_query}%"]
        if group_id:
            query += " AND group_id = ?"
            params.append(group_id)
        query += " ORDER BY updated_at DESC, document_id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        conn = self._connect()
        rows = conn.execute(query, params).fetchall()

        results: list[dict[str, Any]] = []
        for row in rows:
            document = json.loads(row["document_json"])
            document["match_preview"] = self._build_match_preview(
                text=row["extracted_text"] or "",
                query_text=normalized_query,
            )
            results.append(document)
        return results

    def _build_match_preview(self, text: str, query_text: str, radius: int = 80) -> str:
        if not text:
            return ""

        lower_text = text.lower()
        lower_query = query_text.lower()
        index = lower_text.find(lower_query)
        if index == -1:
            snippet = text[: radius * 2]
            return snippet.strip()

        start = max(0, index - radius)
        end = min(len(text), index + len(query_text) + radius)
        snippet = text[start:end].strip()
        if start > 0:
            snippet = "..." + snippet
        if end < len(text):
            snippet = snippet + "..."
        return snippet
