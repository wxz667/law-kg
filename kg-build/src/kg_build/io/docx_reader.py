from __future__ import annotations

import re
import zipfile
from pathlib import Path

from docx import Document

from ..utils.ids import checksum_text, slugify
from ..contracts import SourceDocumentRecord

PART_RE = re.compile(r"^第[一二三四五六七八九十百零]+编\s+.+$")
CHAPTER_RE = re.compile(r"^第[一二三四五六七八九十百零]+章\s+.+$")
SECTION_RE = re.compile(r"^第[一二三四五六七八九十百零]+节\s+.+$")
ARTICLE_RE = re.compile(r"^第[一二三四五六七八九十百千万零两〇0-9]+条(?:之[一二三四五六七八九十百千万零两〇0-9]+)?\s*.*$")
ARTICLE_TITLE_SUFFIX_RE = re.compile(
    r"^第[一二三四五六七八九十百千万零两〇0-9]+条"
    r"(?:、第[一二三四五六七八九十百千万零两〇0-9]+条)*的解释$"
)
APPENDIX_RE = re.compile(r"^附件[一二三四五六七八九十百千万零两〇0-9]+$")
DATE_RE = re.compile(r"(\d{4}年\d{1,2}月\d{1,2}日)([^、；。）]+)")
LOCAL_PCG_RE = re.compile(
    r"(.+?)(?:省|自治区|维吾尔自治区|壮族自治区|回族自治区|特别行政区|市|州|盟|地区|县|区)人民代表大会常务委员会"
)
LOCAL_GOV_RE = re.compile(
    r"(.+?)(?:省|自治区|维吾尔自治区|壮族自治区|回族自治区|特别行政区|市|州|盟|地区|县|区)人民政府"
)

REGION_NAMES = [
    "北京市",
    "天津市",
    "上海市",
    "重庆市",
    "河北省",
    "山西省",
    "辽宁省",
    "吉林省",
    "黑龙江省",
    "江苏省",
    "浙江省",
    "安徽省",
    "福建省",
    "江西省",
    "山东省",
    "河南省",
    "湖北省",
    "湖南省",
    "广东省",
    "海南省",
    "四川省",
    "贵州省",
    "云南省",
    "陕西省",
    "甘肃省",
    "青海省",
    "台湾省",
    "内蒙古自治区",
    "广西壮族自治区",
    "西藏自治区",
    "宁夏回族自治区",
    "新疆维吾尔自治区",
    "香港特别行政区",
    "澳门特别行政区",
]

SOURCE_TYPE_ALIASES = {
    "constitution": "constitution",
    "law": "law",
    "statutes": "law",
    "amendment": "law",
    "regulation": "regulation",
    "decision": "regulation",
    "interpretation": "interpretation",
    "judicial-interpretations": "interpretation",
    "case": "case",
    "cases": "case",
}


class SourceDocumentReadError(ValueError):
    def __init__(self, message: str, *, error_type: str = "source_read_error") -> None:
        super().__init__(message)
        self.error_type = error_type


def read_source_document(source_path: Path) -> SourceDocumentRecord:
    validate_source_document(source_path)
    try:
        document = Document(str(source_path))
    except Exception as exc:
        raise SourceDocumentReadError(
            f"Failed to read DOCX package: {exc}",
            error_type="docx_parse_error",
        ) from exc
    paragraphs = [paragraph.text.strip() for paragraph in document.paragraphs if paragraph.text.strip()]
    title_lines = extract_title_lines(paragraphs) if paragraphs else [source_path.stem]
    title = "".join(title_lines).strip() if title_lines else source_path.stem
    preface_text, toc_lines, body_lines, appendix_lines = split_document_sections(paragraphs)
    revision_events = parse_revision_events(preface_text)
    if not revision_events:
        revision_events = parse_revision_events(normalize_text("\n".join(paragraphs[:12])).strip())
    source_type = normalize_source_type(source_path.parent.name)
    issuer = infer_issuer(title, preface_text)
    issuer_type = infer_issuer_type(title, preface_text)
    region = infer_region(title, preface_text)
    document_subtype = infer_document_subtype(source_type, title, issuer_type, region)
    return SourceDocumentRecord(
        source_id=f"{source_type}:{slugify(source_path.stem)}",
        title=title,
        source_path=str(source_path.resolve()),
        source_type=source_type,
        checksum=checksum_text(normalize_text("\n".join(paragraphs))),
        preface_text=preface_text,
        toc_lines=toc_lines,
        body_lines=body_lines,
        appendix_lines=appendix_lines,
        metadata={
            "file_name": source_path.name,
            "paragraph_count": len(paragraphs),
            "revision_events": revision_events,
            "document_type": source_type,
            "document_subtype": document_subtype,
            "issuer": issuer,
            "issuer_type": issuer_type,
            "publish_date": revision_events[0]["date"] if revision_events else "",
            "effective_date": revision_events[-1]["date"] if revision_events else "",
            "status": infer_status(preface_text),
            "doc_no": "",
            "region": region,
            "preface_text": preface_text,
        },
    )


def validate_source_document(source_path: Path) -> None:
    if not source_path.exists():
        raise SourceDocumentReadError(
            f"Source file not found: {source_path}",
            error_type="missing_source_file",
        )
    if source_path.suffix.lower() != ".docx":
        raise SourceDocumentReadError(
            f"Unsupported source format: {source_path.suffix}",
            error_type="unsupported_source_format",
        )
    if not zipfile.is_zipfile(source_path):
        raise SourceDocumentReadError(
            f"Source file is not a valid DOCX zip package: {source_path}",
            error_type="invalid_docx_package",
        )

    try:
        with zipfile.ZipFile(source_path) as archive:
            names = set(archive.namelist())
    except zipfile.BadZipFile as exc:
        raise SourceDocumentReadError(
            f"Broken DOCX zip package: {exc}",
            error_type="invalid_docx_package",
        ) from exc

    required_entries = {
        "[Content_Types].xml",
        "_rels/.rels",
        "word/document.xml",
    }
    missing_entries = sorted(required_entries - names)
    if missing_entries:
        raise SourceDocumentReadError(
            "DOCX package is missing required entries: " + ", ".join(missing_entries),
            error_type="incomplete_docx_package",
        )


def normalize_source_type(raw_type: str) -> str:
    return SOURCE_TYPE_ALIASES.get(raw_type, slugify(raw_type))


def infer_issuer_type(title: str, preface_text: str) -> str:
    text = f"{title}\n{preface_text}".strip()
    if "最高人民法院" in text and "最高人民检察院" in text:
        return "joint_judicial"
    if "最高人民法院" in text:
        return "supreme_court"
    if "最高人民检察院" in text:
        return "supreme_procuratorate"
    if "国家监察委员会" in text:
        return "supervisory_commission"
    if "全国人民代表大会常务委员会" in text:
        return "npcsc"
    if "全国人民代表大会" in text:
        return "npc"
    if "国务院" in text:
        return "state_council"
    if LOCAL_PCG_RE.search(text):
        return "local_people_congress"
    if LOCAL_GOV_RE.search(text):
        return "local_government"
    return ""


def infer_issuer(title: str, preface_text: str) -> str:
    text = f"{title}\n{preface_text}".strip()
    for candidate in (
        "全国人民代表大会常务委员会",
        "全国人民代表大会",
        "最高人民法院、最高人民检察院",
        "最高人民法院",
        "最高人民检察院",
        "国家监察委员会",
        "国务院",
    ):
        if candidate in text:
            return candidate
    local_match = LOCAL_PCG_RE.search(text) or LOCAL_GOV_RE.search(text)
    if local_match:
        return local_match.group(0)
    return ""


def infer_region(title: str, preface_text: str) -> str:
    text = f"{title}\n{preface_text}".strip()
    for region_name in REGION_NAMES:
        if region_name in text:
            return region_name
    return ""


def infer_document_subtype(source_type: str, title: str, issuer_type: str, region: str) -> str:
    if source_type == "constitution":
        if "修正案" in title or "修正文本" in title or "修正" in title:
            return "amendment"
        if "决定" in title:
            return "decision"
        return ""
    if source_type == "law":
        if "修正案" in title:
            return "amendment"
        if "决定" in title:
            return "decision"
        return ""
    if source_type == "regulation":
        if "决定" in title:
            return "decision"
        if issuer_type == "state_council":
            return "administrative"
        if issuer_type == "supervisory_commission":
            return "supervisory"
        if region or issuer_type in {"local_people_congress", "local_government"}:
            return "local"
        return ""
    if source_type == "interpretation":
        if issuer_type in {"npc", "npcsc"}:
            return "legislative"
        if issuer_type in {"supreme_court", "supreme_procuratorate", "joint_judicial"}:
            return "judicial"
        if "决定" in title:
            return "decision"
        return ""
    return ""


def infer_status(preface_text: str) -> str:
    text = preface_text or ""
    if "废止" in text:
        return "repealed"
    if "修正" in text or "修订" in text:
        return "amended"
    return "effective"


def normalize_text(text: str) -> str:
    lines = [line.strip() for line in text.replace("\r\n", "\n").split("\n")]
    normalized = "\n".join(line for line in lines if line)
    return normalized.strip() + ("\n" if normalized else "")


def extract_title(paragraphs: list[str]) -> str:
    return "".join(extract_title_lines(paragraphs))


def extract_title_lines(paragraphs: list[str]) -> list[str]:
    if not paragraphs:
        return []
    title_lines: list[str] = []
    for line in paragraphs:
        stripped = line.strip()
        if not stripped:
            if title_lines:
                break
            continue
        if title_lines and _is_title_terminator(stripped):
            break
        if not title_lines and normalize_heading(stripped) == "目录":
            break
        title_lines.append(stripped)
    if not title_lines:
        return [paragraphs[0].strip()]
    return title_lines


def _is_title_terminator(line: str) -> bool:
    if normalize_heading(line) == "目录":
        return True
    if ARTICLE_TITLE_SUFFIX_RE.match(line):
        return False
    if is_structural_body_start(line):
        return True
    if re.match(r"^[（(]\d{4}年\d{1,2}月\d{1,2}日.*[）)]$", line):
        return True
    if re.match(r"^\d{4}年\d{1,2}月\d{1,2}日", line):
        return True
    return False


def split_document_sections(
    paragraphs: list[str],
) -> tuple[str, list[str], list[str], list[str]]:
    if not paragraphs:
        return "", [], [], []
    title_lines = extract_title_lines(paragraphs)
    title_line_count = len(title_lines)

    toc_index = next((index for index, line in enumerate(paragraphs) if normalize_heading(line) == "目录"), None)
    toc_lines: list[str] = []
    content_lines: list[str] = paragraphs[1:]
    body_start = 1
    if toc_index is not None:
        body_start = find_body_start_index(paragraphs, toc_index)
        toc_lines = paragraphs[toc_index + 1 : body_start]
        content_lines = paragraphs[body_start:]
        preface_lines = paragraphs[1:toc_index]
    else:
        body_start = find_body_start_without_toc(paragraphs, title_line_count)
        preface_lines = paragraphs[title_line_count:body_start]
        content_lines = paragraphs[body_start:]

    preface_text = normalize_text("\n".join(preface_lines)).strip()

    appendix_start = next(
        (index for index, line in enumerate(content_lines) if APPENDIX_RE.match(line)),
        None,
    )
    if appendix_start is None:
        return preface_text, toc_lines, content_lines, []
    return (
        preface_text,
        toc_lines,
        content_lines[:appendix_start],
        content_lines[appendix_start:],
    )


def find_body_start_index(paragraphs: list[str], toc_index: int) -> int:
    repeated_title_index = find_repeated_title_index(paragraphs)
    if repeated_title_index is not None and repeated_title_index > toc_index:
        return repeated_title_index
    part_indices = [index for index, line in enumerate(paragraphs) if PART_RE.match(line)]
    if len(part_indices) >= 2:
        first_part_line = paragraphs[part_indices[0]]
        for later_index in part_indices[1:]:
            if paragraphs[later_index] == first_part_line and later_index > toc_index:
                return later_index
    first_structural = next(
        (
            index
            for index, line in enumerate(paragraphs[toc_index + 1 :], start=toc_index + 1)
            if is_structural_body_start(line)
        ),
        None,
    )
    if first_structural is not None:
        return first_structural
    return toc_index + 1


def find_body_start_without_toc(paragraphs: list[str], title_line_count: int) -> int:
    repeated_title_index = find_repeated_title_index(paragraphs)
    if repeated_title_index is not None:
        return repeated_title_index
    first_structural = next(
        (
            index
            for index, line in enumerate(paragraphs[title_line_count:], start=title_line_count)
            if is_structural_body_start(line)
        ),
        None,
    )
    if first_structural is not None:
        return first_structural

    index = title_line_count
    while index < len(paragraphs):
        line = paragraphs[index].strip()
        if not line:
            index += 1
            continue
        if re.match(r"^[（(]\d{4}年\d{1,2}月\d{1,2}日.*[）)]$", line):
            index += 1
            continue
        if re.match(r"^\d{4}年\d{1,2}月\d{1,2}日", line):
            index += 1
            continue
        break
    return index if index < len(paragraphs) else title_line_count


def find_repeated_title_index(paragraphs: list[str]) -> int | None:
    if not paragraphs:
        return None
    title = normalize_heading(paragraphs[0])
    for index, line in enumerate(paragraphs[1:], start=1):
        if normalize_heading(line) == title:
            return index
    return None


def is_structural_body_start(line: str) -> bool:
    return bool(
        PART_RE.match(line)
        or CHAPTER_RE.match(line)
        or SECTION_RE.match(line)
        or ARTICLE_RE.match(line)
    )


def normalize_heading(text: str) -> str:
    return text.replace(" ", "").replace("　", "")


def parse_revision_events(preface_text: str) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    if not preface_text:
        return events
    for date_text, description in DATE_RE.findall(preface_text):
        cleaned_description = description.strip("　 ")
        events.append(
            {
                "date": date_text,
                "event_type": classify_revision_event(cleaned_description),
                "description": cleaned_description,
            }
        )
    return events


def classify_revision_event(description: str) -> str:
    if "修订" in description:
        return "revision"
    if "修正" in description or "修正案" in description:
        return "amendment"
    if "通过" in description:
        return "adoption"
    return "other"
