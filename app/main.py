from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import threading
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
import requests

from app.config import Settings, get_settings
from app.services.document_ingestor import DocumentIngestor
from app.services.sqlite_store import SQLiteStore
from app.services.zsxq_scraper import ZsxqScraper

logger = logging.getLogger(__name__)

_store: SQLiteStore | None = None
_sync_task: asyncio.Task[None] | None = None
_last_auto_sync: dict[str, Any] | None = None
_sync_lock = threading.Lock()


def get_store() -> SQLiteStore:
    global _store
    if _store is None:
        settings = get_settings()
        _store = SQLiteStore(settings.sqlite_db_path)
    return _store


def _run_background_sync() -> dict[str, Any] | None:
    global _last_auto_sync
    settings = get_settings()
    if not settings.zsxq_access_token:
        logger.warning("Background sync skipped: missing ZSXQ_ACCESS_TOKEN")
        return None

    if not _sync_lock.acquire(blocking=False):
        logger.info("Background sync skipped: another sync is already running")
        return None

    try:
        logger.info("Background sync started")
        store = get_store()
        scraper = ZsxqScraper(
            access_token=settings.zsxq_access_token,
            request_delay=settings.request_delay_seconds,
        )
        result = scraper.sync_all_groups_posts(
            store=store,
            docs_storage_path=settings.docs_storage_path,
        )
        _last_auto_sync = {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "groups_count": result.get("groups_count", 0),
            "new_topics_count": result.get("new_topics_count", 0),
            "filtered_topics_count": result.get("filtered_topics_count", 0),
            "saved_count": result.get("saved_count", 0),
            "documents_saved_count": result.get("documents_saved_count", 0),
        }
        logger.info(
            "Background sync completed: %d groups, %d new topics, %d saved",
            result.get("groups_count", 0),
            result.get("new_topics_count", 0),
            result.get("saved_count", 0),
        )
        return _last_auto_sync
    except Exception:
        logger.exception("Background sync failed")
        _last_auto_sync = {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error": True,
        }
        return None
    finally:
        _sync_lock.release()


async def _periodic_sync(interval_seconds: int) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _run_background_sync)


@asynccontextmanager
async def lifespan(application: FastAPI):  # type: ignore[override]
    global _sync_task
    settings = get_settings()
    # Initialize global store
    get_store()
    # Start periodic sync
    if settings.sync_interval_seconds > 0:
        _sync_task = asyncio.create_task(_periodic_sync(settings.sync_interval_seconds))
        logger.info("Periodic sync enabled: every %d seconds", settings.sync_interval_seconds)
    yield
    # Shutdown
    if _sync_task is not None:
        _sync_task.cancel()
        try:
            await _sync_task
        except asyncio.CancelledError:
            pass
    store = get_store()
    store.close()


app = FastAPI(
    title="OpenClaw ZSXQ Tool API",
    version="0.2.0",
    description="A lightweight local API service for fetching and cleaning ZSXQ data.",
    lifespan=lifespan,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def _extract_error_detail(response: requests.Response | None) -> str:
    if response is None:
        return "Failed to fetch ZSXQ topics"

    try:
        payload = response.json()
    except ValueError:
        return response.text or "Failed to fetch ZSXQ topics"

    if isinstance(payload, dict):
        error = payload.get("error") or {}
        if isinstance(error, dict):
            for key in ("message", "msg", "detail"):
                value = error.get(key)
                if value:
                    return str(value)

        for key in ("message", "msg", "detail"):
            value = payload.get(key)
            if value:
                return str(value)

    return response.text or "Failed to fetch ZSXQ topics"


def _handle_zsxq_request(func, *args: Any, **kwargs: Any) -> Any:
    try:
        return func(*args, **kwargs)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 502
        detail = _extract_error_detail(exc.response)
        raise HTTPException(status_code=status_code, detail=detail) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Network error: {exc}") from exc


def _persist_topics(
    scraper: ZsxqScraper,
    store: SQLiteStore,
    topics: list[dict[str, Any]],
    group_id: str,
    settings: Settings,
) -> dict[str, Any]:
    kept_topics, filtered_topics = scraper.filter_promotional_topics(topics)
    saved = store.upsert_topics(kept_topics, group_id)
    if filtered_topics:
        store.upsert_filtered_topics(filtered_topics, group_id)
    ingestor = DocumentIngestor(settings.docs_storage_path, scraper._build_headers())
    documents: list[dict[str, Any]] = []
    for topic in kept_topics:
        documents.extend(ingestor.ingest_topic_documents(group_id, topic))
    documents_saved = store.upsert_documents(documents) if documents else 0
    return {
        "enabled": True,
        "saved_count": saved,
        "filtered_topics_count": len(filtered_topics),
        "documents_saved_count": documents_saved,
        "db_path": settings.sqlite_db_path,
        "docs_storage_path": settings.docs_storage_path,
    }


def _require_token_and_group(
    settings: Settings, group_id: str | None = None
) -> tuple[str, str]:
    if not settings.zsxq_access_token:
        raise HTTPException(status_code=400, detail="Missing ZSXQ_ACCESS_TOKEN")
    resolved = group_id or settings.group_id
    if not resolved:
        raise HTTPException(status_code=400, detail="Missing GROUP_ID")
    return settings.zsxq_access_token, resolved


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "service": "openclaw-zsxq-api",
    }


@app.get("/api/v1/sync_status")
def sync_status(
    settings: Settings = Depends(get_settings),
) -> dict:
    store = get_store()
    states = store.get_all_sync_states()
    return {
        "auto_sync_interval_seconds": settings.sync_interval_seconds,
        "last_auto_sync": _last_auto_sync,
        "groups": states,
    }


@app.post("/api/v1/trigger_sync")
def trigger_sync() -> dict:
    result = _run_background_sync()
    if result is None:
        return {"status": "skipped", "reason": "sync already running or missing token"}
    return {"status": "completed", **result}


@app.post("/api/v1/fetch_posts")
def fetch_posts(
    count: int = Query(default=20, ge=1, le=100),
    end_time: str | None = Query(default=None),
    scope: str = Query(default="all"),
    persist: bool = Query(default=False),
    group_id: str | None = Query(default=None),
    settings: Settings = Depends(get_settings),
) -> dict:
    token, resolved_group_id = _require_token_and_group(settings, group_id)
    scraper = ZsxqScraper(
        access_token=token,
        group_id=resolved_group_id,
        request_delay=settings.request_delay_seconds,
    )
    store = get_store()

    result = _handle_zsxq_request(scraper.fetch_posts, count=count, end_time=end_time, scope=scope)
    if persist:
        result["persisted"] = _persist_topics(scraper, store, result["topics"], resolved_group_id, settings)
    return result


@app.post("/api/v1/fetch_all_posts")
def fetch_all_posts(
    page_size: int = Query(default=20, ge=1, le=100),
    scope: str = Query(default="all"),
    max_pages: int = Query(default=10, ge=1, le=100),
    persist: bool = Query(default=False),
    group_id: str | None = Query(default=None),
    settings: Settings = Depends(get_settings),
) -> dict:
    token, resolved_group_id = _require_token_and_group(settings, group_id)
    scraper = ZsxqScraper(
        access_token=token,
        group_id=resolved_group_id,
        request_delay=settings.request_delay_seconds,
    )
    store = get_store()

    result = _handle_zsxq_request(
        scraper.fetch_all_posts, page_size=page_size, scope=scope, max_pages=max_pages,
    )
    if persist:
        result["persisted"] = _persist_topics(scraper, store, result["topics"], resolved_group_id, settings)
    return result


@app.get("/api/v1/groups")
def list_groups(
    count: int = Query(default=20, ge=1, le=100),
    end_time: str | None = Query(default=None),
    settings: Settings = Depends(get_settings),
) -> dict:
    if not settings.zsxq_access_token:
        raise HTTPException(status_code=400, detail="Missing ZSXQ_ACCESS_TOKEN")

    scraper = ZsxqScraper(
        access_token=settings.zsxq_access_token,
        request_delay=settings.request_delay_seconds,
    )
    return _handle_zsxq_request(scraper.list_groups, count=count, end_time=end_time)


@app.get("/api/v1/groups/all")
def list_all_groups(
    page_size: int = Query(default=20, ge=1, le=100),
    max_pages: int = Query(default=10, ge=1, le=100),
    settings: Settings = Depends(get_settings),
) -> dict:
    if not settings.zsxq_access_token:
        raise HTTPException(status_code=400, detail="Missing ZSXQ_ACCESS_TOKEN")

    scraper = ZsxqScraper(
        access_token=settings.zsxq_access_token,
        request_delay=settings.request_delay_seconds,
    )
    return _handle_zsxq_request(scraper.fetch_all_groups, page_size=page_size, max_pages=max_pages)


@app.post("/api/v1/sync_group_posts")
def sync_group_posts(
    page_size: int = Query(default=20, ge=1, le=100),
    scope: str = Query(default="all"),
    max_pages: int = Query(default=10, ge=1, le=100),
    group_id: str | None = Query(default=None),
    settings: Settings = Depends(get_settings),
) -> dict:
    token, resolved_group_id = _require_token_and_group(settings, group_id)
    scraper = ZsxqScraper(
        access_token=token,
        group_id=resolved_group_id,
        request_delay=settings.request_delay_seconds,
    )
    store = get_store()

    result = _handle_zsxq_request(
        scraper.sync_group_posts,
        store=store,
        docs_storage_path=settings.docs_storage_path,
        page_size=page_size,
        scope=scope,
        max_pages=max_pages,
    )
    result["db_path"] = settings.sqlite_db_path
    result["docs_storage_path"] = settings.docs_storage_path
    return result


@app.post("/api/v1/sync_all_groups_posts")
def sync_all_groups_posts(
    group_page_size: int = Query(default=20, ge=1, le=100),
    max_group_pages: int = Query(default=10, ge=1, le=100),
    topic_page_size: int = Query(default=20, ge=1, le=100),
    topic_max_pages: int = Query(default=10, ge=1, le=100),
    scope: str = Query(default="all"),
    settings: Settings = Depends(get_settings),
) -> dict:
    if not settings.zsxq_access_token:
        raise HTTPException(status_code=400, detail="Missing ZSXQ_ACCESS_TOKEN")

    scraper = ZsxqScraper(
        access_token=settings.zsxq_access_token,
        request_delay=settings.request_delay_seconds,
    )
    store = get_store()

    result = _handle_zsxq_request(
        scraper.sync_all_groups_posts,
        store=store,
        docs_storage_path=settings.docs_storage_path,
        group_page_size=group_page_size,
        max_group_pages=max_group_pages,
        topic_page_size=topic_page_size,
        topic_max_pages=topic_max_pages,
        scope=scope,
    )
    result["db_path"] = settings.sqlite_db_path
    result["docs_storage_path"] = settings.docs_storage_path
    return result


@app.get("/api/v1/topics")
def list_topics(
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    group_id: str | None = Query(default=None),
    settings: Settings = Depends(get_settings),
) -> dict:
    store = get_store()
    topics = store.list_topics(limit=limit, offset=offset, group_id=group_id or settings.group_id or None)
    return {
        "count": len(topics),
        "limit": limit,
        "offset": offset,
        "group_id": group_id or settings.group_id or None,
        "db_path": settings.sqlite_db_path,
        "topics": topics,
    }


@app.get("/api/v1/documents")
def list_documents(
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    group_id: str | None = Query(default=None),
    topic_id: str | None = Query(default=None),
    settings: Settings = Depends(get_settings),
) -> dict:
    store = get_store()
    documents = store.list_documents(limit=limit, offset=offset, group_id=group_id, topic_id=topic_id)
    return {
        "count": len(documents),
        "limit": limit,
        "offset": offset,
        "group_id": group_id,
        "topic_id": topic_id,
        "db_path": settings.sqlite_db_path,
        "docs_storage_path": settings.docs_storage_path,
        "documents": documents,
    }


@app.get("/api/v1/search_documents")
def search_documents(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    group_id: str | None = Query(default=None),
    settings: Settings = Depends(get_settings),
) -> dict:
    store = get_store()
    documents = store.search_documents(
        query_text=q,
        limit=limit,
        offset=offset,
        group_id=group_id,
    )
    return {
        "count": len(documents),
        "query": q,
        "limit": limit,
        "offset": offset,
        "group_id": group_id,
        "db_path": settings.sqlite_db_path,
        "docs_storage_path": settings.docs_storage_path,
        "documents": documents,
    }
