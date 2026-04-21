from __future__ import annotations

import re

from ...utils.layout import (
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
from ...contracts import contains_edge_by_levels, graph_level_order, level_to_node_type
NUMERIC_ITEM_HEADING_RE = re.compile(r"^(?:[0-9０-９]+[.．、]|(?:（|\()[0-9０-９]+(?:）|\))).+$")
PURE_INTEGER_RE = re.compile(r"^[0-9０-９]+$")

STRUCTURAL_EDGES = contains_edge_by_levels()
LEVEL_ORDER = graph_level_order()
LEVEL_TO_NODE_TYPE = level_to_node_type()
