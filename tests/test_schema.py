from __future__ import annotations

from builder.contracts import EdgeRecord, GraphBundle, NodeRecord


def test_schema_rejects_removed_logic_node() -> None:
    bundle = GraphBundle(
        bundle_id="test:0001",
        document_id="law:test",
        nodes=[
            NodeRecord(id="document:test", type="DocumentNode", name="测试法", level="document"),
            NodeRecord(id="concept:test", type="ConceptNode", name="交通事故", level="concept"),
        ],
        edges=[
            EdgeRecord(id="edge:contains", source="document:test", target="concept:test", type="CONTAINS"),
        ],
    )
    try:
        bundle.validate_edge_references()
    except ValueError as exc:
        assert "violates schema rule" in str(exc)
    else:
        raise AssertionError("Expected structural schema validation to fail for invalid CONTAINS edge.")
