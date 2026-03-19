import unittest
from unittest.mock import patch
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from app.config import Settings
import app.main as main_module
from app.main import app
from app.services.sqlite_store import SQLiteStore


class FetchPostsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app, raise_server_exceptions=False)
        self.temp_dir = TemporaryDirectory()
        self.db_path = f"{self.temp_dir.name}/test.db"
        self.docs_path = f"{self.temp_dir.name}/docs"
        self._store = SQLiteStore(self.db_path)
        # Override the global store
        self._original_store = main_module._store
        main_module._store = self._store

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
        main_module._store = self._original_store
        self._store.close()
        self.temp_dir.cleanup()

    def override_settings(self) -> Settings:
        return Settings(
            zsxq_access_token="token",
            group_id="group-id",
            sqlite_db_path=self.db_path,
            docs_storage_path=self.docs_path,
            sync_interval_seconds=0,
            request_delay_seconds=0,
        )

    def test_fetch_posts_returns_cleaned_payload(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings

        with patch("app.main.ZsxqScraper.fetch_posts") as mock_fetch_posts:
            mock_fetch_posts.return_value = {
                "group_id": "group-id",
                "count": 1,
                "has_more": False,
                "next_end_time": "2024-01-01T10:00:00.000+0800",
                "topics": [{"topic_id": 1, "text": "hello"}],
            }

            response = self.client.post("/api/v1/fetch_posts?count=1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 1)
        self.assertEqual(response.json()["topics"][0]["topic_id"], 1)

    def test_fetch_all_posts_returns_paginated_payload(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings

        with patch("app.main.ZsxqScraper.fetch_all_posts") as mock_fetch_all_posts:
            mock_fetch_all_posts.return_value = {
                "group_id": "group-id",
                "count": 2,
                "page_size": 1,
                "pages_fetched": 2,
                "next_end_time": "2024-01-01T09:00:00.000+0800",
                "topics": [{"topic_id": 1}, {"topic_id": 2}],
            }

            response = self.client.post("/api/v1/fetch_all_posts?page_size=1&max_pages=2")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["pages_fetched"], 2)
        self.assertEqual(len(response.json()["topics"]), 2)

    def test_fetch_posts_supports_group_id_override(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings

        with patch("app.main.ZsxqScraper.fetch_posts") as mock_fetch_posts:
            mock_fetch_posts.return_value = {
                "group_id": "group-override",
                "count": 1,
                "has_more": False,
                "next_end_time": None,
                "topics": [{"topic_id": 1, "text": "hello"}],
            }

            response = self.client.post("/api/v1/fetch_posts?group_id=group-override")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["group_id"], "group-override")

    def test_fetch_posts_persist_true_saves_to_sqlite(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings

        with patch("app.main.ZsxqScraper.fetch_posts") as mock_fetch_posts:
            mock_fetch_posts.return_value = {
                "group_id": "group-id",
                "count": 1,
                "has_more": False,
                "next_end_time": None,
                "topics": [
                    {
                        "topic_id": 1,
                        "type": "talk",
                        "create_time": "2024-01-01T10:00:00.000+0800",
                        "create_time_iso": "2024-01-01T02:00:00+00:00",
                        "text": "hello",
                        "answer_text": "",
                        "owner": {"name": "alice"},
                        "like_count": 0,
                        "comment_count": 0,
                        "liked": False,
                    }
                ],
            }
            with patch("app.main.ZsxqScraper.filter_promotional_topics") as mock_filter_promotional_topics:
                mock_filter_promotional_topics.return_value = (
                    mock_fetch_posts.return_value["topics"],
                    [],
                )
                with patch("app.main.DocumentIngestor.ingest_topic_documents", return_value=[]):
                    response = self.client.post("/api/v1/fetch_posts?count=1&persist=true")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["persisted"]["saved_count"], 1)
        self.assertEqual(response.json()["persisted"]["documents_saved_count"], 0)

        topics = self._store.list_topics(limit=10, offset=0, group_id="group-id")
        self.assertEqual(len(topics), 1)
        self.assertEqual(topics[0]["topic_id"], 1)

    def test_list_topics_returns_saved_topics(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings
        self._store.upsert_topics(
            topics=[
                {
                    "topic_id": 2,
                    "type": "talk",
                    "create_time": "2024-01-01T10:00:00.000+0800",
                    "create_time_iso": "2024-01-01T02:00:00+00:00",
                    "text": "saved",
                    "answer_text": "",
                    "owner": {"name": "bob"},
                    "like_count": 0,
                    "comment_count": 0,
                    "liked": False,
                }
            ],
            group_id="group-id",
        )

        response = self.client.get("/api/v1/topics?limit=10&offset=0")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 1)
        self.assertEqual(response.json()["topics"][0]["topic_id"], 2)

    def test_list_groups_returns_joined_groups(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings

        with patch("app.main.ZsxqScraper.list_groups") as mock_list_groups:
            mock_list_groups.return_value = {
                "count": 1,
                "has_more": False,
                "next_end_time": None,
                "groups": [{"group_id": 1, "name": "Alpha"}],
            }

            response = self.client.get("/api/v1/groups?count=1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 1)
        self.assertEqual(response.json()["groups"][0]["name"], "Alpha")

    def test_list_all_groups_returns_paginated_payload(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings

        with patch("app.main.ZsxqScraper.fetch_all_groups") as mock_fetch_all_groups:
            mock_fetch_all_groups.return_value = {
                "count": 2,
                "page_size": 1,
                "pages_fetched": 2,
                "next_end_time": None,
                "groups": [{"group_id": 1}, {"group_id": 2}],
            }

            response = self.client.get("/api/v1/groups/all?page_size=1&max_pages=2")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["pages_fetched"], 2)
        self.assertEqual(len(response.json()["groups"]), 2)

    def test_sync_group_posts_returns_incremental_sync_payload(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings

        with patch("app.main.ZsxqScraper.sync_group_posts") as mock_sync_group_posts:
            mock_sync_group_posts.return_value = {
                "group_id": "group-id",
                "new_topics_count": 3,
                "filtered_topics_count": 1,
                "saved_count": 3,
                "documents_saved_count": 2,
                "pages_fetched": 1,
                "page_size": 20,
                "stopped_on_known_topic": True,
                "latest_marker_before_sync": {"latest_topic_id": "100"},
                "latest_marker_after_sync": {"latest_topic_id": "103"},
            }

            response = self.client.post("/api/v1/sync_group_posts?page_size=20")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["saved_count"], 3)
        self.assertEqual(response.json()["documents_saved_count"], 2)
        self.assertTrue(response.json()["stopped_on_known_topic"])
        self.assertEqual(response.json()["filtered_topics_count"], 1)

    def test_sync_all_groups_posts_returns_aggregated_payload(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings

        with patch("app.main.ZsxqScraper.sync_all_groups_posts") as mock_sync_all_groups_posts:
            mock_sync_all_groups_posts.return_value = {
                "groups_count": 2,
                "group_page_size": 20,
                "group_pages_fetched": 1,
                "topic_page_size": 20,
                "topic_max_pages": 10,
                "new_topics_count": 5,
                "filtered_topics_count": 2,
                "saved_count": 5,
                "documents_saved_count": 4,
                "results": [],
            }

            response = self.client.post("/api/v1/sync_all_groups_posts")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["groups_count"], 2)
        self.assertEqual(response.json()["saved_count"], 5)
        self.assertEqual(response.json()["documents_saved_count"], 4)
        self.assertEqual(response.json()["filtered_topics_count"], 2)

    def test_list_documents_returns_saved_documents(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings
        self._store.upsert_documents(
            [
                {
                    "document_id": "group-id:2:file-1",
                    "group_id": "group-id",
                    "topic_id": "2",
                    "file_id": "file-1",
                    "name": "notes.txt",
                    "download_url": "https://example.com/notes.txt",
                    "content_type": "text/plain",
                    "size": 12,
                    "local_path": f"{self.docs_path}/group-id/2_file-1_notes.txt",
                    "extraction_status": "extracted",
                    "extracted_text": "hello world",
                }
            ]
        )

        response = self.client.get("/api/v1/documents?group_id=group-id")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 1)
        self.assertEqual(response.json()["documents"][0]["file_id"], "file-1")

    def test_search_documents_returns_matching_documents(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings
        self._store.upsert_documents(
            [
                {
                    "document_id": "group-id:2:file-2",
                    "group_id": "group-id",
                    "topic_id": "2",
                    "file_id": "file-2",
                    "name": "alpha.txt",
                    "download_url": "https://example.com/alpha.txt",
                    "content_type": "text/plain",
                    "size": 30,
                    "local_path": f"{self.docs_path}/group-id/2_file-2_alpha.txt",
                    "extraction_status": "extracted",
                    "extracted_text": "这里有 AlphaTerm 关键字，方便测试搜索。",
                }
            ]
        )

        response = self.client.get("/api/v1/search_documents?q=AlphaTerm&group_id=group-id")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 1)
        self.assertIn("AlphaTerm", response.json()["documents"][0]["match_preview"])

    def test_sync_status_returns_group_states(self) -> None:
        app.dependency_overrides.clear()
        from app.config import get_settings

        app.dependency_overrides[get_settings] = self.override_settings
        self._store.update_group_sync_state(
            group_id="group-id",
            latest_topic_id=1,
            latest_create_time="2024-01-01T10:00:00.000+0800",
            latest_create_time_iso="2024-01-01T02:00:00+00:00",
        )

        response = self.client.get("/api/v1/sync_status")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["groups"]), 1)
        self.assertEqual(response.json()["groups"][0]["group_id"], "group-id")


if __name__ == "__main__":
    unittest.main()
