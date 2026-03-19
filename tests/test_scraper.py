import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

import requests

from app.services.document_ingestor import DocumentIngestor
from app.services.sqlite_store import SQLiteStore
from app.services.zsxq_scraper import ZsxqScraper


class ZsxqScraperTests(unittest.TestCase):
    def test_clean_topics_response_strips_html_and_normalizes_fields(self) -> None:
        scraper = ZsxqScraper("token", "group", request_delay=0)
        payload = {
            "resp_data": {
                "topics": [
                    {
                        "topic_id": 1,
                        "type": "talk",
                        "create_time": "2024-01-01T10:00:00.000+0800",
                        "talk": {
                            "text": "hello&lt;br&gt;<b>world</b>",
                            "images": [
                                {
                                    "image_id": 2,
                                    "type": "jpg",
                                    "large": {"url": "L", "width": 100, "height": 80},
                                }
                            ],
                        },
                        "owner": {"user_id": 9, "name": "alice"},
                        "show_comments": [
                            {
                                "comment_id": 3,
                                "create_time": "2024-01-01T11:00:00.000+0800",
                                "text": "ok<br/>fine",
                                "owner": {"user_id": 10, "name": "bob"},
                            }
                        ],
                    }
                ]
            }
        }

        cleaned = scraper.clean_topics_response(payload, requested_count=20)

        self.assertEqual(cleaned["count"], 1)
        self.assertFalse(cleaned["has_more"])
        self.assertEqual(cleaned["topics"][0]["text"], "hello\nworld")
        self.assertEqual(cleaned["topics"][0]["comments"][0]["text"], "ok\nfine")
        self.assertEqual(cleaned["topics"][0]["images"][0]["large"], "L")

    def test_fetch_all_posts_paginates_until_has_more_is_false(self) -> None:
        scraper = ZsxqScraper("token", "group", request_delay=0)
        pages = [
            {
                "group_id": "group",
                "count": 1,
                "has_more": True,
                "next_end_time": "2024-01-01T10:00:00.000+0800",
                "topics": [{"topic_id": 1}],
            },
            {
                "group_id": "group",
                "count": 1,
                "has_more": False,
                "next_end_time": "2024-01-01T09:00:00.000+0800",
                "topics": [{"topic_id": 2}],
            },
        ]

        with patch.object(scraper, "fetch_posts", side_effect=pages) as mock_fetch_posts:
            result = scraper.fetch_all_posts(page_size=1, max_pages=5)

        self.assertEqual(result["count"], 2)
        self.assertEqual(result["pages_fetched"], 2)
        self.assertEqual([item["topic_id"] for item in result["topics"]], [1, 2])
        self.assertEqual(mock_fetch_posts.call_count, 2)

    def test_fetch_all_posts_deduplicates_topics_across_pages(self) -> None:
        scraper = ZsxqScraper("token", "group", request_delay=0)
        pages = [
            {
                "group_id": "group",
                "count": 2,
                "has_more": True,
                "next_end_time": "2024-01-01T10:00:00.000+0800",
                "topics": [{"topic_id": 1}, {"topic_id": 2}],
            },
            {
                "group_id": "group",
                "count": 2,
                "has_more": False,
                "next_end_time": "2024-01-01T09:00:00.000+0800",
                "topics": [{"topic_id": 2}, {"topic_id": 3}],
            },
        ]

        with patch.object(scraper, "fetch_posts", side_effect=pages):
            result = scraper.fetch_all_posts(page_size=2, max_pages=5)

        self.assertEqual([item["topic_id"] for item in result["topics"]], [1, 2, 3])
        self.assertEqual(result["count"], 3)

    def test_clean_groups_response_normalizes_fields(self) -> None:
        scraper = ZsxqScraper("token", request_delay=0)
        payload = {
            "resp_data": {
                "groups": [
                    {
                        "group_id": 1,
                        "name": "Alpha",
                        "description": "hello&lt;br&gt;<b>world</b>",
                        "create_time": "2024-01-01T10:00:00.000+0800",
                        "owner": {"user_id": 9, "name": "alice"},
                        "members_count": 120,
                        "topics_count": 33,
                        "user_specific": {"joined": True},
                    }
                ]
            }
        }

        cleaned = scraper.clean_groups_response(payload, requested_count=20)

        self.assertEqual(cleaned["count"], 1)
        self.assertFalse(cleaned["has_more"])
        self.assertEqual(cleaned["groups"][0]["description"], "hello\nworld")
        self.assertEqual(cleaned["groups"][0]["statistics"]["members_count"], 120)

    def test_filter_promotional_topics_removes_event_ads(self) -> None:
        scraper = ZsxqScraper("token", "group", request_delay=0)
        kept_topics, filtered_topics = scraper.filter_promotional_topics(
            [
                {
                    "topic_id": 1,
                    "text": "今天分享一下最近的模型评测结论",
                    "answer_text": "",
                    "question": None,
                    "answer": None,
                    "images": [],
                    "files": [],
                },
                {
                    "topic_id": 2,
                    "text": "活动预告：本周直播预约已开启，扫码报名，限时优惠，海报见下方 https://example.com",
                    "answer_text": "",
                    "question": None,
                    "answer": None,
                    "images": [{"image_id": 1}, {"image_id": 2}],
                    "files": [],
                },
            ]
        )

        self.assertEqual([item["topic_id"] for item in kept_topics], [1])
        self.assertEqual([item["topic_id"] for item in filtered_topics], [2])

    def test_fetch_all_groups_paginates_until_has_more_is_false(self) -> None:
        scraper = ZsxqScraper("token", request_delay=0)
        pages = [
            {
                "count": 1,
                "has_more": True,
                "next_end_time": "2024-01-01T10:00:00.000+0800",
                "groups": [{"group_id": 1}],
            },
            {
                "count": 1,
                "has_more": False,
                "next_end_time": "2024-01-01T09:00:00.000+0800",
                "groups": [{"group_id": 2}],
            },
        ]

        with patch.object(scraper, "list_groups", side_effect=pages) as mock_list_groups:
            result = scraper.fetch_all_groups(page_size=1, max_pages=5)

        self.assertEqual(result["count"], 2)
        self.assertEqual(result["pages_fetched"], 2)
        self.assertEqual([item["group_id"] for item in result["groups"]], [1, 2])
        self.assertEqual(mock_list_groups.call_count, 2)

    def test_list_groups_falls_back_to_legacy_endpoint(self) -> None:
        scraper = ZsxqScraper("token", request_delay=0)

        class FakeResponse:
            def __init__(self, payload: dict, status_code: int) -> None:
                self._payload = payload
                self.status_code = status_code
                self.text = ""

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise requests.HTTPError(response=self)

            def json(self) -> dict:
                return self._payload

        responses = [
            FakeResponse({}, 404),
            FakeResponse({"resp_data": {"groups": [{"group_id": 1, "name": "Alpha"}]}}, 200),
        ]

        with patch("app.services.zsxq_scraper.requests.get", side_effect=responses) as mock_get:
            result = scraper.list_groups(count=10)

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["groups"][0]["group_id"], 1)
        self.assertEqual(mock_get.call_count, 2)

    def test_sync_group_posts_stops_after_known_topic(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = SQLiteStore(f"{temp_dir}/openclaw.db")
            store.upsert_topics(
                [
                    {
                        "topic_id": 2,
                        "type": "talk",
                        "create_time": "2024-01-01T09:00:00.000+0800",
                        "create_time_iso": "2024-01-01T01:00:00+00:00",
                        "text": "known",
                        "answer_text": "",
                        "owner": {"name": "alice"},
                        "like_count": 0,
                        "comment_count": 0,
                        "liked": False,
                    }
                ],
                group_id="group-1",
            )
            store.update_group_sync_state(
                group_id="group-1",
                latest_topic_id=2,
                latest_create_time="2024-01-01T09:00:00.000+0800",
                latest_create_time_iso="2024-01-01T01:00:00+00:00",
            )
            scraper = ZsxqScraper("token", "group-1", request_delay=0)
            pages = [
                {
                    "group_id": "group-1",
                    "count": 2,
                    "has_more": True,
                    "next_end_time": "2024-01-01T08:00:00.000+0800",
                    "topics": [
                        {
                            "topic_id": 3,
                            "create_time": "2024-01-01T10:00:00.000+0800",
                            "create_time_iso": "2024-01-01T02:00:00+00:00",
                            "text": "new",
                        },
                        {
                            "topic_id": 2,
                            "create_time": "2024-01-01T09:00:00.000+0800",
                            "create_time_iso": "2024-01-01T01:00:00+00:00",
                            "text": "known",
                        },
                    ],
                }
            ]

            with patch.object(scraper, "fetch_posts", side_effect=pages) as mock_fetch_posts:
                result = scraper.sync_group_posts(store=store, page_size=20, max_pages=5)

            topics = store.list_topics(limit=10, offset=0, group_id="group-1")

        self.assertEqual(result["new_topics_count"], 1)
        self.assertEqual(result["saved_count"], 1)
        self.assertEqual(result["documents_saved_count"], 0)
        self.assertTrue(result["stopped_on_known_topic"])
        self.assertEqual(mock_fetch_posts.call_count, 1)
        self.assertEqual([item["topic_id"] for item in topics], [3, 2])

    def test_sync_group_posts_filters_promotional_topics_but_updates_marker(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = SQLiteStore(f"{temp_dir}/openclaw.db")
            scraper = ZsxqScraper("token", "group-1", request_delay=0)
            pages = [
                {
                    "group_id": "group-1",
                    "count": 2,
                    "has_more": False,
                    "next_end_time": "2024-01-01T08:00:00.000+0800",
                    "topics": [
                        {
                            "topic_id": 5,
                            "create_time": "2024-01-01T11:00:00.000+0800",
                            "create_time_iso": "2024-01-01T03:00:00+00:00",
                            "text": "活动预告：直播预约开启，扫码报名，限时优惠 https://example.com",
                            "images": [{"image_id": 1}, {"image_id": 2}],
                            "files": [],
                        },
                        {
                            "topic_id": 4,
                            "create_time": "2024-01-01T10:00:00.000+0800",
                            "create_time_iso": "2024-01-01T02:00:00+00:00",
                            "text": "正常内容",
                            "images": [],
                            "files": [],
                        },
                    ],
                }
            ]

            with patch.object(scraper, "fetch_posts", side_effect=pages):
                result = scraper.sync_group_posts(store=store, page_size=20, max_pages=5)

            topics = store.list_topics(limit=10, offset=0, group_id="group-1")
            marker = store.get_latest_topic_marker("group-1")

        self.assertEqual(result["new_topics_count"], 1)
        self.assertEqual(result["filtered_topics_count"], 1)
        self.assertEqual(result["documents_saved_count"], 0)
        self.assertEqual([item["topic_id"] for item in topics], [4])
        self.assertEqual(marker["latest_topic_id"], "5")

    def test_document_ingestor_downloads_text_file_and_extracts_content(self) -> None:
        with TemporaryDirectory() as temp_dir:
            ingestor = DocumentIngestor(temp_dir, {"Cookie": "zsxq_access_token=token"})

            class FakeResponse:
                def __init__(self) -> None:
                    self.content = b"hello from doc"
                    self.headers = {"Content-Type": "text/plain"}

                def raise_for_status(self) -> None:
                    return None

            with patch("app.services.document_ingestor.requests.get", return_value=FakeResponse()):
                documents = ingestor.ingest_topic_documents(
                    "group-1",
                    {
                        "topic_id": 1,
                        "files": [
                            {
                                "file_id": "file-1",
                                "name": "notes.txt",
                                "size": 14,
                                "download_url": "https://example.com/notes.txt",
                            }
                        ],
                    },
                )

        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]["extraction_status"], "extracted")
        self.assertEqual(documents[0]["extracted_text"], "hello from doc")

    def test_document_ingestor_extracts_pdf_text_when_parser_available(self) -> None:
        with TemporaryDirectory() as temp_dir:
            ingestor = DocumentIngestor(temp_dir, {"Cookie": "zsxq_access_token=token"})

            class FakeResponse:
                def __init__(self) -> None:
                    self.content = b"%PDF-1.4 fake"
                    self.headers = {"Content-Type": "application/pdf"}

                def raise_for_status(self) -> None:
                    return None

            with patch("app.services.document_ingestor.requests.get", return_value=FakeResponse()):
                with patch.object(DocumentIngestor, "_extract_pdf_text", return_value="pdf body text"):
                    documents = ingestor.ingest_topic_documents(
                        "group-1",
                        {
                            "topic_id": 1,
                            "files": [
                                {
                                    "file_id": "file-pdf",
                                    "name": "report.pdf",
                                    "size": 20,
                                    "download_url": "https://example.com/report.pdf",
                                }
                            ],
                        },
                    )

        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]["extraction_status"], "extracted")
        self.assertEqual(documents[0]["extracted_text"], "pdf body text")

    def test_document_ingestor_marks_pdf_as_downloaded_only_without_parser(self) -> None:
        with TemporaryDirectory() as temp_dir:
            ingestor = DocumentIngestor(temp_dir, {"Cookie": "zsxq_access_token=token"})

            class FakeResponse:
                def __init__(self) -> None:
                    self.content = b"%PDF-1.4 fake"
                    self.headers = {"Content-Type": "application/pdf"}

                def raise_for_status(self) -> None:
                    return None

            with patch("app.services.document_ingestor.requests.get", return_value=FakeResponse()):
                with patch.object(DocumentIngestor, "_extract_pdf_text", return_value=None):
                    with patch("app.services.document_ingestor.PdfReader", None):
                        documents = ingestor.ingest_topic_documents(
                            "group-1",
                            {
                                "topic_id": 1,
                                "files": [
                                    {
                                        "file_id": "file-pdf",
                                        "name": "report.pdf",
                                        "size": 20,
                                        "download_url": "https://example.com/report.pdf",
                                    }
                                ],
                            },
                        )

        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]["extraction_status"], "downloaded_only_pdf_missing_parser")
        self.assertIsNone(documents[0]["extracted_text"])

    def test_sync_all_groups_posts_aggregates_results(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = SQLiteStore(f"{temp_dir}/openclaw.db")
            scraper = ZsxqScraper("token", request_delay=0)

            with patch.object(
                scraper,
                "fetch_all_groups",
                return_value={
                    "count": 2,
                    "pages_fetched": 1,
                    "groups": [
                        {"group_id": "group-1", "name": "Alpha"},
                        {"group_id": "group-2", "name": "Beta"},
                    ],
                },
            ):
                with patch.object(
                    ZsxqScraper,
                    "sync_group_posts",
                    side_effect=[
                        {
                            "group_id": "group-1",
                            "new_topics_count": 1,
                            "filtered_topics_count": 0,
                            "saved_count": 1,
                            "documents_saved_count": 1,
                            "pages_fetched": 1,
                            "page_size": 20,
                            "stopped_on_known_topic": False,
                            "latest_marker_before_sync": None,
                            "latest_marker_after_sync": {"latest_topic_id": 10},
                        },
                        {
                            "group_id": "group-2",
                            "new_topics_count": 2,
                            "filtered_topics_count": 1,
                            "saved_count": 2,
                            "documents_saved_count": 2,
                            "pages_fetched": 1,
                            "page_size": 20,
                            "stopped_on_known_topic": True,
                            "latest_marker_before_sync": {"latest_topic_id": 9},
                            "latest_marker_after_sync": {"latest_topic_id": 11},
                        },
                    ],
                ):
                    result = scraper.sync_all_groups_posts(store=store)

        self.assertEqual(result["groups_count"], 2)
        self.assertEqual(result["new_topics_count"], 3)
        self.assertEqual(result["filtered_topics_count"], 1)
        self.assertEqual(result["saved_count"], 3)
        self.assertEqual(result["documents_saved_count"], 3)

    def test_batch_topic_ids_exist(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = SQLiteStore(f"{temp_dir}/openclaw.db")
            store.upsert_topics(
                [
                    {
                        "topic_id": 1,
                        "type": "talk",
                        "create_time": "2024-01-01T10:00:00.000+0800",
                        "create_time_iso": "2024-01-01T02:00:00+00:00",
                        "text": "existing",
                        "answer_text": "",
                        "owner": {"name": "alice"},
                        "like_count": 0,
                        "comment_count": 0,
                        "liked": False,
                    }
                ],
                group_id="group-1",
            )

            existing = store.topic_ids_exist(["1", "2", "3"], "group-1")

        self.assertEqual(existing, {"1"})

    def test_filtered_topics_saved_to_filtered_table(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = SQLiteStore(f"{temp_dir}/openclaw.db")
            scraper = ZsxqScraper("token", "group-1", request_delay=0)
            pages = [
                {
                    "group_id": "group-1",
                    "count": 1,
                    "has_more": False,
                    "next_end_time": "2024-01-01T08:00:00.000+0800",
                    "topics": [
                        {
                            "topic_id": 5,
                            "create_time": "2024-01-01T11:00:00.000+0800",
                            "create_time_iso": "2024-01-01T03:00:00+00:00",
                            "text": "活动预告：直播预约开启，扫码报名，限时优惠 https://example.com",
                            "images": [{"image_id": 1}, {"image_id": 2}],
                            "files": [],
                        },
                    ],
                }
            ]

            with patch.object(scraper, "fetch_posts", side_effect=pages):
                scraper.sync_group_posts(store=store, page_size=20, max_pages=5)

            conn = store._connect()
            row = conn.execute(
                "SELECT topic_id FROM filtered_topics WHERE group_id = ?", ("group-1",)
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["topic_id"], "5")


class SQLiteStoreTests(unittest.TestCase):
    def test_upsert_topics_updates_existing_topic(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = SQLiteStore(f"{temp_dir}/openclaw.db")
            store.upsert_topics(
                [
                    {
                        "topic_id": 1,
                        "type": "talk",
                        "create_time": "2024-01-01T10:00:00.000+0800",
                        "create_time_iso": "2024-01-01T02:00:00+00:00",
                        "text": "v1",
                        "answer_text": "",
                        "owner": {"name": "alice"},
                        "like_count": 1,
                        "comment_count": 0,
                        "liked": False,
                    }
                ],
                group_id="group-1",
            )
            store.upsert_topics(
                [
                    {
                        "topic_id": 1,
                        "type": "talk",
                        "create_time": "2024-01-01T10:00:00.000+0800",
                        "create_time_iso": "2024-01-01T02:00:00+00:00",
                        "text": "v2",
                        "answer_text": "",
                        "owner": {"name": "alice"},
                        "like_count": 5,
                        "comment_count": 2,
                        "liked": True,
                    }
                ],
                group_id="group-1",
            )

            topics = store.list_topics(limit=10, offset=0, group_id="group-1")

        self.assertEqual(len(topics), 1)
        self.assertEqual(topics[0]["text"], "v2")
        self.assertEqual(topics[0]["like_count"], 5)

    def test_search_documents_returns_match_preview(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = SQLiteStore(f"{temp_dir}/openclaw.db")
            store.upsert_documents(
                [
                    {
                        "document_id": "group-1:1:file-1",
                        "group_id": "group-1",
                        "topic_id": "1",
                        "file_id": "file-1",
                        "name": "notes.txt",
                        "download_url": "https://example.com/notes.txt",
                        "content_type": "text/plain",
                        "size": 20,
                        "local_path": f"{temp_dir}/notes.txt",
                        "extraction_status": "extracted",
                        "extracted_text": "这是第一段内容，其中包含关键字 AlphaTerm，后面还有更多内容。",
                    }
                ]
            )

            documents = store.search_documents("AlphaTerm", limit=10, offset=0, group_id="group-1")

        self.assertEqual(len(documents), 1)
        self.assertIn("AlphaTerm", documents[0]["match_preview"])

    def test_get_all_sync_states(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = SQLiteStore(f"{temp_dir}/openclaw.db")
            store.update_group_sync_state(
                group_id="group-1",
                latest_topic_id=1,
                latest_create_time="2024-01-01T10:00:00.000+0800",
                latest_create_time_iso="2024-01-01T02:00:00+00:00",
            )
            store.update_group_sync_state(
                group_id="group-2",
                latest_topic_id=2,
                latest_create_time="2024-01-02T10:00:00.000+0800",
                latest_create_time_iso="2024-01-02T02:00:00+00:00",
            )

            states = store.get_all_sync_states()

        self.assertEqual(len(states), 2)
        group_ids = {s["group_id"] for s in states}
        self.assertEqual(group_ids, {"group-1", "group-2"})


if __name__ == "__main__":
    unittest.main()
