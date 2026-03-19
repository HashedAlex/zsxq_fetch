from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
import logging
import re
import time
from typing import Any

import requests

from app.services.document_ingestor import DocumentIngestor
from app.services.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class ZsxqScraper:
    STRONG_PROMO_PATTERNS = (
        r"活动预告",
        r"限时优惠",
        r"扫码",
        r"加微信",
        r"添加微信",
        r"报名链接",
        r"购买链接",
        r"课程优惠",
        r"海报",
        r"推广",
        r"赞助",
    )
    PROMO_PATTERNS = (
        r"报名",
        r"优惠",
        r"折扣",
        r"福利",
        r"名额",
        r"训练营",
        r"购票",
        r"早鸟",
        r"私聊",
        r"咨询",
        r"二维码",
    )
    URL_PATTERNS = (
        r"https?://",
        r"www\.",
    )

    def __init__(
        self,
        access_token: str,
        group_id: str | None = None,
        request_delay: float = 0.5,
    ) -> None:
        self.access_token = access_token
        self.group_id = group_id
        self.base_url = "https://api.zsxq.com/v2"
        self.legacy_base_url = "https://api.zsxq.com/v1"
        self.request_delay = request_delay

    def _throttled_get(self, url: str, **kwargs: Any) -> requests.Response:
        response = requests.get(url, **kwargs)
        if self.request_delay > 0:
            time.sleep(self.request_delay)
        return response

    def fetch_posts(
        self,
        count: int = 20,
        end_time: str | None = None,
        scope: str = "all",
    ) -> dict[str, Any]:
        if not self.group_id:
            raise ValueError("Missing group_id")
        params = {
            "scope": scope,
            "count": max(1, min(count, 100)),
        }
        if end_time:
            params["end_time"] = end_time

        logger.info("Fetching posts for group %s (count=%s, end_time=%s)", self.group_id, params["count"], end_time)
        response = self._throttled_get(
            f"{self.base_url}/groups/{self.group_id}/topics",
            headers=self._build_headers(),
            params=params,
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()

        return self.clean_topics_response(payload, requested_count=params["count"])

    def list_groups(
        self,
        count: int = 20,
        end_time: str | None = None,
    ) -> dict[str, Any]:
        params = {
            "count": max(1, min(count, 100)),
        }
        if end_time:
            params["end_time"] = end_time

        response: requests.Response | None = None
        last_error: requests.HTTPError | None = None
        for url in (f"{self.base_url}/groups", f"{self.legacy_base_url}/groups"):
            response = self._throttled_get(
                url,
                headers=self._build_headers(),
                params=params,
                timeout=20,
            )
            try:
                response.raise_for_status()
                payload = response.json()
                return self.clean_groups_response(payload, requested_count=params["count"])
            except requests.HTTPError as exc:
                logger.warning("Groups endpoint %s failed: %s", url, exc)
                last_error = exc
                continue

        if last_error is not None:
            raise last_error
        raise requests.HTTPError("Failed to fetch groups", response=response)

    def fetch_all_groups(
        self,
        page_size: int = 20,
        max_pages: int = 10,
    ) -> dict[str, Any]:
        all_groups: list[dict[str, Any]] = []
        seen_group_ids: set[Any] = set()
        next_end_time: str | None = None
        previous_end_time: str | None = None
        fetched_pages = 0
        requested_page_size = max(1, min(page_size, 100))

        for _ in range(max(1, max_pages)):
            page = self.list_groups(
                count=requested_page_size,
                end_time=next_end_time,
            )
            fetched_pages += 1
            groups = page.get("groups") or []

            for group in groups:
                group_id = group.get("group_id")
                if group_id in seen_group_ids:
                    continue
                seen_group_ids.add(group_id)
                all_groups.append(group)

            next_end_time = page.get("next_end_time")
            if (
                not page.get("has_more")
                or not groups
                or not next_end_time
                or next_end_time == previous_end_time
            ):
                break
            previous_end_time = next_end_time

        return {
            "count": len(all_groups),
            "page_size": requested_page_size,
            "pages_fetched": fetched_pages,
            "next_end_time": next_end_time,
            "groups": all_groups,
        }

    def fetch_all_posts(
        self,
        page_size: int = 20,
        scope: str = "all",
        max_pages: int = 10,
    ) -> dict[str, Any]:
        all_topics: list[dict[str, Any]] = []
        seen_topic_ids: set[Any] = set()
        next_end_time: str | None = None
        previous_end_time: str | None = None
        fetched_pages = 0
        requested_page_size = max(1, min(page_size, 100))

        for _ in range(max(1, max_pages)):
            page = self.fetch_posts(
                count=requested_page_size,
                end_time=next_end_time,
                scope=scope,
            )
            fetched_pages += 1
            topics = page.get("topics") or []

            for topic in topics:
                topic_id = topic.get("topic_id")
                dedupe_key = topic_id if topic_id is not None else (
                    topic.get("create_time"),
                    topic.get("text"),
                )
                if dedupe_key in seen_topic_ids:
                    continue
                seen_topic_ids.add(dedupe_key)
                all_topics.append(topic)

            next_end_time = page.get("next_end_time")
            if (
                not page.get("has_more")
                or not topics
                or not next_end_time
                or next_end_time == previous_end_time
            ):
                break
            previous_end_time = next_end_time

        return {
            "group_id": self.group_id,
            "count": len(all_topics),
            "page_size": requested_page_size,
            "pages_fetched": fetched_pages,
            "next_end_time": next_end_time,
            "topics": all_topics,
        }

    def sync_group_posts(
        self,
        store: SQLiteStore,
        docs_storage_path: str | None = None,
        page_size: int = 20,
        scope: str = "all",
        max_pages: int = 10,
    ) -> dict[str, Any]:
        if not self.group_id:
            raise ValueError("Missing group_id")

        latest_marker = store.get_latest_topic_marker(self.group_id)
        all_new_topics: list[dict[str, Any]] = []
        next_end_time: str | None = None
        previous_end_time: str | None = None
        fetched_pages = 0
        requested_page_size = max(1, min(page_size, 100))
        hit_known_topic = False
        latest_seen_topic: dict[str, Any] | None = None
        filtered_topics: list[dict[str, Any]] = []
        documents_saved = 0

        for _ in range(max(1, max_pages)):
            page = self.fetch_posts(
                count=requested_page_size,
                end_time=next_end_time,
                scope=scope,
            )
            fetched_pages += 1
            topics = page.get("topics") or []

            # Batch check which topic_ids already exist
            page_topic_ids = [str(t["topic_id"]) for t in topics if t.get("topic_id") is not None]
            known_ids = store.topic_ids_exist(page_topic_ids, self.group_id)

            for topic in topics:
                if latest_seen_topic is None:
                    latest_seen_topic = topic
                if self._is_known_topic(topic, latest_marker, known_ids):
                    hit_known_topic = True
                    break
                if self._is_promotional_topic(topic):
                    filtered_topics.append(topic)
                    continue
                all_new_topics.append(topic)

            next_end_time = page.get("next_end_time")
            if (
                hit_known_topic
                or not page.get("has_more")
                or not topics
                or not next_end_time
                or next_end_time == previous_end_time
            ):
                break
            previous_end_time = next_end_time

        saved_count = store.upsert_topics(all_new_topics, self.group_id) if all_new_topics else 0
        if filtered_topics:
            store.upsert_filtered_topics(filtered_topics, self.group_id)
        if docs_storage_path and all_new_topics:
            ingestor = DocumentIngestor(docs_storage_path, self._build_headers())
            documents: list[dict[str, Any]] = []
            for topic in all_new_topics:
                documents.extend(ingestor.ingest_topic_documents(self.group_id, topic))
            documents_saved = store.upsert_documents(documents) if documents else 0
        newest_topic = all_new_topics[0] if all_new_topics else None
        latest_marker_topic = latest_seen_topic or newest_topic

        if latest_marker_topic is not None:
            store.update_group_sync_state(
                group_id=self.group_id,
                latest_topic_id=latest_marker_topic.get("topic_id"),
                latest_create_time=latest_marker_topic.get("create_time"),
                latest_create_time_iso=latest_marker_topic.get("create_time_iso"),
            )
        elif latest_marker is not None:
            store.update_group_sync_state(
                group_id=self.group_id,
                latest_topic_id=latest_marker.get("latest_topic_id"),
                latest_create_time=latest_marker.get("latest_create_time"),
                latest_create_time_iso=latest_marker.get("latest_create_time_iso"),
            )

        logger.info(
            "Sync group %s: %d new, %d filtered, %d saved, %d docs",
            self.group_id, len(all_new_topics), len(filtered_topics), saved_count, documents_saved,
        )
        return {
            "group_id": self.group_id,
            "new_topics_count": len(all_new_topics),
            "filtered_topics_count": len(filtered_topics),
            "saved_count": saved_count,
            "documents_saved_count": documents_saved,
            "pages_fetched": fetched_pages,
            "page_size": requested_page_size,
            "stopped_on_known_topic": hit_known_topic,
            "latest_marker_before_sync": latest_marker,
            "latest_marker_after_sync": (
                {
                    "latest_topic_id": latest_marker_topic.get("topic_id"),
                    "latest_create_time": latest_marker_topic.get("create_time"),
                    "latest_create_time_iso": latest_marker_topic.get("create_time_iso"),
                }
                if latest_marker_topic is not None
                else latest_marker
            ),
        }

    def sync_all_groups_posts(
        self,
        store: SQLiteStore,
        docs_storage_path: str | None = None,
        group_page_size: int = 20,
        max_group_pages: int = 10,
        topic_page_size: int = 20,
        topic_max_pages: int = 10,
        scope: str = "all",
        max_workers: int = 3,
    ) -> dict[str, Any]:
        groups_payload = self.fetch_all_groups(page_size=group_page_size, max_pages=max_group_pages)
        groups = [g for g in (groups_payload.get("groups") or []) if g.get("group_id") is not None]

        def _sync_one(group: dict[str, Any]) -> dict[str, Any]:
            group_id = str(group["group_id"])
            group_scraper = ZsxqScraper(self.access_token, group_id, request_delay=self.request_delay)
            sync_result = group_scraper.sync_group_posts(
                store=store,
                docs_storage_path=docs_storage_path,
                page_size=topic_page_size,
                scope=scope,
                max_pages=topic_max_pages,
            )
            sync_result["group"] = group
            return sync_result

        results: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_sync_one, g): g for g in groups}
            for future in as_completed(futures):
                group = futures[future]
                try:
                    results.append(future.result())
                except Exception:
                    logger.exception("Failed to sync group %s", group.get("group_id"))

        return {
            "groups_count": len(results),
            "group_page_size": group_page_size,
            "group_pages_fetched": groups_payload.get("pages_fetched"),
            "topic_page_size": topic_page_size,
            "topic_max_pages": topic_max_pages,
            "new_topics_count": sum(item["new_topics_count"] for item in results),
            "filtered_topics_count": sum(item["filtered_topics_count"] for item in results),
            "saved_count": sum(item["saved_count"] for item in results),
            "documents_saved_count": sum(item.get("documents_saved_count", 0) for item in results),
            "results": results,
        }

    def filter_promotional_topics(self, topics: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        kept_topics: list[dict[str, Any]] = []
        filtered_topics: list[dict[str, Any]] = []
        for topic in topics:
            if self._is_promotional_topic(topic):
                filtered_topics.append(topic)
                continue
            kept_topics.append(topic)
        return kept_topics, filtered_topics

    def _build_headers(self) -> dict[str, str]:
        return {
            "Cookie": f"zsxq_access_token={self.access_token}",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://wx.zsxq.com/",
        }

    def clean_topics_response(
        self,
        payload: dict[str, Any],
        requested_count: int | None = None,
    ) -> dict[str, Any]:
        resp_data = payload.get("resp_data") or {}
        topics = resp_data.get("topics") or []

        cleaned_topics = [self._normalize_topic(topic) for topic in topics]
        end_time = cleaned_topics[-1]["create_time"] if cleaned_topics else None
        has_more = resp_data.get("has_more")
        if has_more is None:
            has_more = bool(requested_count and len(cleaned_topics) >= requested_count)

        return {
            "group_id": self.group_id,
            "count": len(cleaned_topics),
            "has_more": has_more,
            "next_end_time": end_time,
            "topics": cleaned_topics,
        }

    def clean_groups_response(
        self,
        payload: dict[str, Any],
        requested_count: int | None = None,
    ) -> dict[str, Any]:
        resp_data = payload.get("resp_data") or {}
        groups = (
            resp_data.get("groups")
            or resp_data.get("joined_groups")
            or resp_data.get("list")
            or []
        )

        cleaned_groups = [self._normalize_group(group) for group in groups]
        end_time = self._extract_group_end_time(cleaned_groups)
        has_more = resp_data.get("has_more")
        if has_more is None:
            has_more = bool(requested_count and len(cleaned_groups) >= requested_count)

        return {
            "count": len(cleaned_groups),
            "has_more": has_more,
            "next_end_time": end_time,
            "groups": cleaned_groups,
        }

    def _normalize_topic(self, topic: dict[str, Any]) -> dict[str, Any]:
        talk = topic.get("talk") or {}
        question = topic.get("question") or {}
        answer = question.get("answer") or {}

        images = talk.get("images") or topic.get("images") or []
        files = topic.get("files") or talk.get("files") or []

        owner = topic.get("owner") or {}
        user_specific = topic.get("user_specific") or {}

        return {
            "topic_id": topic.get("topic_id"),
            "type": topic.get("type"),
            "create_time": topic.get("create_time"),
            "create_time_iso": self._to_iso8601(topic.get("create_time")),
            "text": self._clean_text(talk.get("text") or question.get("text") or ""),
            "answer_text": self._clean_text(answer.get("text") or ""),
            "images": [self._normalize_image(item) for item in images],
            "files": [self._normalize_file(item) for item in files],
            "owner": {
                "user_id": owner.get("user_id"),
                "name": owner.get("name"),
                "avatar_url": owner.get("avatar_url"),
                "location": owner.get("location"),
            },
            "question": {
                "owner": self._normalize_user(question.get("owner")),
                "text": self._clean_text(question.get("text") or ""),
                "images": [self._normalize_image(item) for item in question.get("images") or []],
            }
            if question
            else None,
            "answer": {
                "owner": self._normalize_user(answer.get("owner")),
                "text": self._clean_text(answer.get("text") or ""),
                "images": [self._normalize_image(item) for item in answer.get("images") or []],
            }
            if answer
            else None,
            "like_count": topic.get("likes_count", 0),
            "comment_count": topic.get("comments_count", 0),
            "liked": bool(user_specific.get("liked")),
            "comments": [self._normalize_comment(item) for item in topic.get("show_comments") or []],
            "raw": {
                "group": topic.get("group"),
                "digested": topic.get("digested"),
                "sticky": topic.get("sticky"),
            },
        }

    def _normalize_comment(self, comment: dict[str, Any]) -> dict[str, Any]:
        return {
            "comment_id": comment.get("comment_id"),
            "create_time": comment.get("create_time"),
            "create_time_iso": self._to_iso8601(comment.get("create_time")),
            "text": self._clean_text(comment.get("text") or ""),
            "owner": self._normalize_user(comment.get("owner")),
            "replied_comment_id": comment.get("replied_comment_id"),
        }

    def _normalize_group(self, group: dict[str, Any]) -> dict[str, Any]:
        user_specific = group.get("user_specific") or {}
        return {
            "group_id": group.get("group_id"),
            "name": group.get("name"),
            "description": self._clean_text(
                group.get("description") or group.get("summary") or group.get("intro") or ""
            ),
            "create_time": group.get("create_time"),
            "create_time_iso": self._to_iso8601(group.get("create_time")),
            "owner": self._normalize_user(group.get("owner")),
            "statistics": {
                "members_count": (
                    group.get("members_count")
                    or group.get("users_count")
                    or group.get("member_num")
                ),
                "topics_count": group.get("topics_count"),
            },
            "joined": bool(
                user_specific.get("joined")
                if user_specific
                else group.get("joined", True)
            ),
            "raw": group,
        }

    def _extract_group_end_time(self, groups: list[dict[str, Any]]) -> str | None:
        for group in reversed(groups):
            if group.get("create_time"):
                return group["create_time"]
        return None

    def _is_known_topic(
        self,
        topic: dict[str, Any],
        latest_marker: dict[str, Any] | None,
        known_ids: set[str],
    ) -> bool:
        topic_id = topic.get("topic_id")
        if topic_id is not None and str(topic_id) in known_ids:
            return True

        if latest_marker is None:
            return False

        marker_iso = latest_marker.get("latest_create_time_iso")
        topic_iso = topic.get("create_time_iso")
        if marker_iso and topic_iso and topic_iso < marker_iso:
            return True

        marker_topic_id = latest_marker.get("latest_topic_id")
        return bool(
            marker_iso
            and topic_iso
            and topic_iso == marker_iso
            and marker_topic_id is not None
            and str(topic_id) == str(marker_topic_id)
        )

    def _is_promotional_topic(self, topic: dict[str, Any]) -> bool:
        text_parts = [
            topic.get("text") or "",
            topic.get("answer_text") or "",
            ((topic.get("question") or {}).get("text")) or "",
            ((topic.get("answer") or {}).get("text")) or "",
        ]
        text = "\n".join(part for part in text_parts if part).lower()
        if not text:
            return False

        strong_hits = sum(1 for pattern in self.STRONG_PROMO_PATTERNS if re.search(pattern, text, re.IGNORECASE))
        promo_hits = sum(1 for pattern in self.PROMO_PATTERNS if re.search(pattern, text, re.IGNORECASE))
        url_hits = sum(1 for pattern in self.URL_PATTERNS if re.search(pattern, text, re.IGNORECASE))

        has_marketing_images = len(topic.get("images") or []) >= 2
        has_files = bool(topic.get("files"))

        if strong_hits >= 2 and (promo_hits >= 2 or url_hits >= 1 or has_marketing_images or has_files):
            return True
        if promo_hits >= 4 and (url_hits >= 1 or has_marketing_images):
            return True
        if promo_hits >= 6:
            return True
        return False

    def _normalize_image(self, image: dict[str, Any]) -> dict[str, Any]:
        large = image.get("large") or {}
        original = image.get("original") or {}
        thumbnail = image.get("thumbnail") or {}
        return {
            "image_id": image.get("image_id"),
            "type": image.get("type"),
            "large": large.get("url"),
            "original": original.get("url"),
            "thumbnail": thumbnail.get("url"),
            "width": large.get("width") or original.get("width"),
            "height": large.get("height") or original.get("height"),
        }

    def _normalize_file(self, file_item: dict[str, Any]) -> dict[str, Any]:
        return {
            "file_id": file_item.get("file_id"),
            "name": file_item.get("name"),
            "hash": file_item.get("hash"),
            "size": file_item.get("size"),
            "download_url": file_item.get("download_url"),
        }

    def _normalize_user(self, user: dict[str, Any] | None) -> dict[str, Any] | None:
        if not user:
            return None
        return {
            "user_id": user.get("user_id"),
            "name": user.get("name"),
            "avatar_url": user.get("avatar_url"),
            "location": user.get("location"),
        }

    def _clean_text(self, value: str) -> str:
        text = unescape(value)
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _to_iso8601(self, value: str | None) -> str | None:
        if not value:
            return None

        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized).astimezone(timezone.utc).isoformat()
        except ValueError:
            return value
