from __future__ import annotations

import re

from ...utils.document_layout import (
    APPENDIX_RE,
    ARTICLE_RE,
    CHAPTER_RE,
    INVISIBLE_RE,
    ITEM_MARKER_RE,
    NUMBERED_LIST_RE,
    PARAGRAPH_HEADING_RE,
    PART_RE,
    SECTION_RE,
    SEGMENT_HEADING_RE,
    SPACE_RE,
    SUB_ITEM_MARKER_RE,
)
from ...contracts.graph import load_graph_schema
NUMERIC_ITEM_HEADING_RE = re.compile(r"^(?:[0-9０-９]+[.．、]|(?:（|\()[0-9０-９]+(?:）|\))).+$")
PURE_INTEGER_RE = re.compile(r"^[0-9０-９]+$")

SCHEMA = load_graph_schema()
STRUCTURAL_EDGES = {
    (item["parent_level"], item["child_level"]): item["edge_type"]
    for item in SCHEMA.get("structural_edges", [])
}
LEVEL_ORDER = SCHEMA.get("levels", [])
LEVEL_TO_NODE_TYPE = SCHEMA.get("level_to_node_type", {})
