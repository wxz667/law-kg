from __future__ import annotations

import base64
import re
from urllib.parse import parse_qs, quote, urlparse
from typing import Any

from .client import FlkClient, FlkClientError
from .models import CATEGORY_ID_MAP, LawListItem, LawMetadata, PageResult

CATEGORY_CODE_ID_MAP: dict[str, list[int]] = {
    "宪法": [100],
    "法律": [101, 102, 110, 120, 130, 140, 150, 155, 160, 170, 180, 190, 195, 200],
    "行政法规": [201, 210, 215],
    "监察法规": [220],
    "地方法规": [221, 222, 230, 260, 270, 290, 295, 300, 305, 310],
    "司法解释": [311, 320, 330, 340, 350],
}


class FlkApi:
    def __init__(self, client: FlkClient) -> None:
        self.client = client

    async def fetch_page(self, category: str, page_num: int, page_size: int) -> PageResult:
        code_ids = await self._get_category_code_ids(category)
        payload = {
            "searchContent": "",
            "searchType": 2,
            "searchRange": 1,
            "flfgCodeId": code_ids,
            "zdjgCodeId": [],
            "gbrqYear": [],
            "gbrq": [],
            "sxrq": [],
            "sxx": [],
            "orderByParam": {"order": "-1", "sort": ""},
            "pageNum": page_num,
            "pageSize": page_size,
        }
        response = await self.client.post_json("/law-search/search/list", payload=payload)
        return self._normalize_page_result(response, category)

    async def fetch_metadata(self, source_id: str, category: str) -> LawMetadata:
        payloads = [
            {"bbbs": source_id},
            {"flfgBbbs": source_id},
            {"id": source_id},
        ]
        last_error: Exception | None = None
        for params in payloads:
            try:
                response = await self.client.get_json("/law-search/search/flfgDetails", params=params)
                return self._normalize_metadata(response, source_id=source_id, category=category)
            except Exception as exc:
                last_error = exc
        raise FlkClientError(f"Unable to fetch metadata for {source_id}: {last_error}")

    async def fetch_metadata_by_title(self, title: str, category_hint: str = "法律") -> LawMetadata:
        payload = {
            "searchContent": title,
            "searchType": 1,
            "searchRange": 1,
            "flfgCodeId": [],
            "zdjgCodeId": [],
            "gbrqYear": [],
            "gbrq": [],
            "sxrq": [],
            "sxx": [],
            "orderByParam": {"order": "-1", "sort": ""},
            "pageNum": 1,
            "pageSize": 5,
        }
        response = await self.client.post_json("/law-search/search/list", payload=payload)
        page = self._normalize_page_result(response, category_hint)
        for item in page.items:
            if item.title == title:
                return self.metadata_from_list_item(item)
        if page.items:
            return self.metadata_from_list_item(page.items[0])
        raise FlkClientError(f"Unable to resolve metadata by title: {title}")

    async def download_docx(self, source_id: str, source_url: str | None = None) -> bytes:
        response = await self.client.post_json(
            "/law-search/download/batch",
            payload=[{"bbbs": source_id, "format": "docx"}],
        )
        url = self._extract_batch_download_url(response)
        if not url and source_url:
            detail_id = parse_source_id_from_url(source_url)
            if detail_id and detail_id != source_id:
                return await self.download_docx(detail_id, None)
        if not url:
            raise FlkClientError(f"Unable to resolve batch download url for {source_id}")
        return await self.client.get_external_bytes(url)

    def build_source_url(self, source_id: str, title: str | None = None) -> str:
        if title:
            return f"{self.client.base_url}/detail?id={quote(source_id)}&title={quote(title)}"
        return f"{self.client.base_url}/detail?id={quote(source_id)}"

    def _normalize_page_result(self, payload: Any, category: str) -> PageResult:
        data = payload
        if isinstance(payload, dict) and isinstance(payload.get("data"), (dict, list)):
            data = payload.get("data")
        rows, total = _extract_rows_and_total(data)
        items: list[LawListItem] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            source_id = _pick_first(row, "bbbs", "flfgBbbs", "id", "source_id")
            title = _clean_title(_pick_first(row, "title", "bt", "flfgMc", "name"))
            if not source_id or not title:
                continue
            items.append(
                LawListItem(
                    source_id=str(source_id),
                    title=str(title),
                    category=category,
                    issuer=_string_or_none(_pick_first(row, "zdjgName", "issuer")),
                    publish_date=_string_or_none(_pick_first(row, "gbrq", "publishDate")),
                    effective_date=_string_or_none(_pick_first(row, "sxrq", "effectiveDate")),
                    status=_normalize_status(_pick_first(row, "sxx", "status")),
                )
            )
        return PageResult(items=items, total=total)

    def metadata_from_list_item(self, item: LawListItem) -> LawMetadata:
        return LawMetadata(
            source_id=item.source_id,
            title=item.title,
            issuer=item.issuer,
            publish_date=item.publish_date,
            effective_date=item.effective_date,
            category=item.category,
            status=item.status,
            source_url=self.build_source_url(item.source_id, item.title),
            source_format="docx",
        )

    def _normalize_metadata(self, payload: Any, *, source_id: str, category: str) -> LawMetadata:
        data = payload.get("data") if isinstance(payload, dict) else payload
        detail = _extract_detail_dict(data)
        title = _clean_title(_pick_first(detail, "title", "bt", "flfgMc", "name")) or source_id
        issuer = _pick_first(detail, "ssr", "fbr", "issuer", "publishOrg", "fbjg")
        publish_date = _pick_first(detail, "gbrq", "publishDate", "fbrq")
        effective_date = _pick_first(detail, "sxrq", "effectiveDate")
        status = _normalize_status(_pick_first(detail, "sxx", "sxzt", "status", "flzt"))
        source_url = self.build_source_url(source_id, str(title))
        return LawMetadata(
            source_id=source_id,
            title=str(title),
            issuer=_string_or_none(issuer),
            publish_date=_string_or_none(publish_date),
            effective_date=_string_or_none(effective_date),
            category=category,
            status=_string_or_none(status),
            source_url=source_url,
            source_format="docx",
        )

    def _extract_download_url(self, payload: Any) -> str | None:
        if isinstance(payload, str) and payload.startswith("http"):
            return payload
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, str) and data.startswith("http"):
                return data
            if isinstance(data, dict):
                for key in ("url", "previewUrl", "downloadUrl", "link", "src"):
                    value = data.get(key)
                    if isinstance(value, str) and value.startswith("http"):
                        return value
        return None

    def _extract_batch_download_url(self, payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            return None
        first = data[0]
        if not isinstance(first, dict):
            return None
        value = first.get("url")
        if isinstance(value, str) and value.startswith("http"):
            return value
        return None

    async def _get_category_code_ids(self, category: str) -> list[int]:
        code_ids = CATEGORY_CODE_ID_MAP.get(category, [])
        if not code_ids:
            raise FlkClientError(f"Unable to resolve category code ids for {category}")
        return code_ids


def parse_source_id_from_url(source_url: str) -> str | None:
    parsed = urlparse(source_url)
    query = parse_qs(parsed.query)
    if query.get("id"):
        return query["id"][0]
    if "?" not in source_url:
        return None
    encoded = source_url.split("?", 1)[1]
    try:
        return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        return None


def _extract_rows_and_total(data: Any) -> tuple[list[dict[str, Any]], int | None]:
    candidates: list[Any] = []
    if isinstance(data, dict):
        candidates.extend([data.get("rows"), data.get("list"), data.get("records"), data.get("content")])
        page = data.get("page")
        if isinstance(page, dict):
            candidates.extend([page.get("rows"), page.get("list"), page.get("records"), page.get("content")])
    elif isinstance(data, list):
        candidates.append(data)

    for candidate in candidates:
        if isinstance(candidate, list):
            total = None
            if isinstance(data, dict):
                total_value = _pick_first(data, "total", "count", "totalRows", "totalNum")
                if total_value is not None:
                    try:
                        total = int(total_value)
                    except (TypeError, ValueError):
                        total = None
            return [row for row in candidate if isinstance(row, dict)], total
    raise FlkClientError("Unable to normalize list response payload.")


def _extract_detail_dict(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        for key in ("detail", "data", "flfg", "record"):
            value = data.get(key)
            if isinstance(value, dict):
                return value
        return data
    raise FlkClientError("Unable to normalize detail response payload.")


def _pick_first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_title(value: Any) -> str | None:
    text = _string_or_none(value)
    if text is None:
        return None
    return re.sub(r"<[^>]+>", "", text).strip() or None


def _normalize_status(value: Any) -> str | None:
    if value is None:
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return _string_or_none(value)
    mapping = {
        1: "已废止",
        2: "已修改",
        3: "现行有效",
        4: "尚未生效",
    }
    return mapping.get(numeric, str(numeric))
