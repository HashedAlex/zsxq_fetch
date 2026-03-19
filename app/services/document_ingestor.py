from __future__ import annotations

from html import unescape
import json
import logging
from pathlib import Path
import re
from typing import Any
from xml.etree import ElementTree
from zipfile import ZipFile

import requests

try:
    from pypdf import PdfReader  # type: ignore
except ImportError:
    try:
        from PyPDF2 import PdfReader  # type: ignore
    except ImportError:
        PdfReader = None

logger = logging.getLogger(__name__)


class DocumentIngestor:
    def __init__(self, storage_path: str, headers: dict[str, str]) -> None:
        self.storage_root = Path(storage_path)
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.headers = headers

    def ingest_topic_documents(self, group_id: str, topic: dict[str, Any]) -> list[dict[str, Any]]:
        documents: list[dict[str, Any]] = []
        for file_item in topic.get("files") or []:
            try:
                document = self._download_and_extract(group_id=group_id, topic=topic, file_item=file_item)
                if document is not None:
                    documents.append(document)
            except Exception:
                logger.exception(
                    "Failed to ingest document %s for topic %s in group %s",
                    file_item.get("file_id"), topic.get("topic_id"), group_id,
                )
        return documents

    def _download_and_extract(
        self,
        group_id: str,
        topic: dict[str, Any],
        file_item: dict[str, Any],
    ) -> dict[str, Any] | None:
        download_url = file_item.get("download_url")
        if not download_url:
            return None

        logger.info("Downloading document %s from group %s", file_item.get("name"), group_id)
        response = requests.get(
            download_url,
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()
        content = response.content

        topic_id = str(topic.get("topic_id") or "unknown-topic")
        file_id = str(file_item.get("file_id") or "unknown-file")
        original_name = file_item.get("name") or f"{file_id}.bin"
        target_path = self._build_path(group_id, topic_id, file_id, original_name)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(content)

        extracted_text, extraction_status = self._extract_text(target_path, content)
        logger.info("Extracted document %s: status=%s", original_name, extraction_status)
        return {
            "document_id": f"{group_id}:{topic_id}:{file_id}",
            "group_id": group_id,
            "topic_id": topic_id,
            "file_id": file_id,
            "name": original_name,
            "download_url": download_url,
            "content_type": response.headers.get("Content-Type"),
            "size": file_item.get("size") or len(content),
            "local_path": str(target_path),
            "extracted_text": extracted_text,
            "extraction_status": extraction_status,
        }

    def _build_path(self, group_id: str, topic_id: str, file_id: str, name: str) -> Path:
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._") or "document.bin"
        return self.storage_root / group_id / f"{topic_id}_{file_id}_{safe_name}"

    def _extract_text(self, path: Path, content: bytes) -> tuple[str | None, str]:
        suffix = path.suffix.lower()
        if suffix in {".txt", ".md", ".csv", ".json", ".log"}:
            return self._decode_text(content), "extracted"
        if suffix in {".html", ".htm"}:
            text = self._decode_text(content)
            return self._html_to_text(text), "extracted"
        if suffix == ".docx":
            extracted = self._extract_docx_text(path)
            return extracted, "extracted" if extracted else "unsupported_empty"
        if suffix == ".pdf":
            extracted = self._extract_pdf_text(path)
            if extracted:
                return extracted, "extracted"
            if PdfReader is None:
                return None, "downloaded_only_pdf_missing_parser"
            return None, "downloaded_only_pdf"
        return None, f"downloaded_only_{suffix.lstrip('.') or 'binary'}"

    def _decode_text(self, content: bytes) -> str:
        for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk"):
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                continue
        return content.decode("utf-8", errors="replace")

    def _html_to_text(self, html: str) -> str:
        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
        text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        return unescape(text).strip()

    def _extract_docx_text(self, path: Path) -> str | None:
        try:
            with ZipFile(path) as docx:
                with docx.open("word/document.xml") as document_xml:
                    tree = ElementTree.parse(document_xml)
        except Exception:
            logger.warning("Failed to parse DOCX: %s", path)
            return None

        namespaces = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        lines: list[str] = []
        for paragraph in tree.findall(".//w:p", namespaces):
            texts = [node.text for node in paragraph.findall(".//w:t", namespaces) if node.text]
            if texts:
                lines.append("".join(texts))
        return "\n".join(lines).strip() or None

    def _extract_pdf_text(self, path: Path) -> str | None:
        if PdfReader is None:
            return None

        try:
            reader = PdfReader(str(path))
        except Exception:
            logger.warning("Failed to parse PDF: %s", path)
            return None

        pages: list[str] = []
        for page in getattr(reader, "pages", []):
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""
            text = text.strip()
            if text:
                pages.append(text)

        return "\n\n".join(pages).strip() or None

    def build_document_response(self, document: dict[str, Any]) -> dict[str, Any]:
        preview = document.get("extracted_text")
        if preview:
            preview = preview[:500]
        return {
            **document,
            "preview_text": preview,
        }
