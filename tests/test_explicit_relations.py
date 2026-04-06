from __future__ import annotations

from types import SimpleNamespace

from builder.contracts import EdgeRecord, GraphBundle, NodeRecord, build_edge_id
from builder.stages import run_explicit_relations


class FakeRuntime:
    def predict_relations(self, sentences: list[str]) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(relation_type="REFERENCES", score=0.91, model="fake-relation-classifier")
            for _ in sentences
        ]


def build_bundle() -> GraphBundle:
    nodes = [
        NodeRecord(
            id="document:law:sample-law",
            type="DocumentNode",
            name="示例法",
            level="document",
            source_id="law:sample-law",
            document_type="law",
        ),
        NodeRecord(
            id="article:law:sample-law:0001",
            type="ProvisionNode",
            name="第一条",
            level="article",
            text="为了规范活动，制定本法。",
            metadata={"order": 1},
        ),
        NodeRecord(
            id="article:law:sample-law:0002",
            type="ProvisionNode",
            name="第二条",
            level="article",
            text="依照《示例法》第一条和本法第一条执行。",
            metadata={"order": 2},
        ),
        NodeRecord(
            id="paragraph:law:sample-law:0002:01",
            type="ProvisionNode",
            name="第二条第一款",
            level="paragraph",
            text="本款规定如下。",
            metadata={"order": 1},
        ),
        NodeRecord(
            id="paragraph:law:sample-law:0002:02",
            type="ProvisionNode",
            name="第二条第二款",
            level="paragraph",
            text="前款和本条共同适用。",
            metadata={"order": 2},
        ),
        NodeRecord(
            id="segment:law:sample-law:0001",
            type="ProvisionNode",
            name="正文",
            level="segment",
            text="依照《不存在的法》第一条处理。",
            metadata={"order": 1},
        ),
    ]
    edges = [
        EdgeRecord(
            id=build_edge_id("document:law:sample-law", "article:law:sample-law:0001", "CONTAINS"),
            source="document:law:sample-law",
            target="article:law:sample-law:0001",
            type="CONTAINS",
        ),
        EdgeRecord(
            id=build_edge_id("document:law:sample-law", "article:law:sample-law:0002", "CONTAINS"),
            source="document:law:sample-law",
            target="article:law:sample-law:0002",
            type="CONTAINS",
        ),
        EdgeRecord(
            id=build_edge_id("article:law:sample-law:0002", "paragraph:law:sample-law:0002:01", "CONTAINS"),
            source="article:law:sample-law:0002",
            target="paragraph:law:sample-law:0002:01",
            type="CONTAINS",
        ),
        EdgeRecord(
            id=build_edge_id("article:law:sample-law:0002", "paragraph:law:sample-law:0002:02", "CONTAINS"),
            source="article:law:sample-law:0002",
            target="paragraph:law:sample-law:0002:02",
            type="CONTAINS",
        ),
        EdgeRecord(
            id=build_edge_id("document:law:sample-law", "segment:law:sample-law:0001", "CONTAINS"),
            source="document:law:sample-law",
            target="segment:law:sample-law:0001",
            type="CONTAINS",
        ),
    ]
    return GraphBundle(
        bundle_id="test:explicit-relations",
        document_id="law:sample-law",
        nodes=nodes,
        edges=edges,
        metadata={"stage": "normalize"},
    )


def test_explicit_relations_resolves_absolute_and_local_article_references() -> None:
    graph_bundle = run_explicit_relations(build_bundle(), FakeRuntime())

    reference_edges = [edge for edge in graph_bundle.edges if edge.type == "REFERENCES"]

    assert any(
        edge.source == "article:law:sample-law:0002" and edge.target == "article:law:sample-law:0001"
        for edge in reference_edges
    )
    assert graph_bundle.metadata["stage"] == "explicit_relations"


def test_explicit_relations_resolves_this_article_and_paragraph_navigation() -> None:
    graph_bundle = run_explicit_relations(build_bundle(), FakeRuntime())

    reference_edges = {(edge.source, edge.target) for edge in graph_bundle.edges if edge.type == "REFERENCES"}

    assert ("paragraph:law:sample-law:0002:01", "paragraph:law:sample-law:0002:01") in reference_edges
    assert ("paragraph:law:sample-law:0002:02", "paragraph:law:sample-law:0002:01") in reference_edges
    assert ("paragraph:law:sample-law:0002:02", "article:law:sample-law:0002") in reference_edges


def test_explicit_relations_reports_unresolved_targets() -> None:
    graph_bundle = run_explicit_relations(build_bundle(), FakeRuntime())

    unresolved = graph_bundle.metadata["reports"]["explicit_relations"]["unresolved_references"]

    assert len(unresolved) == 1
    assert unresolved[0]["target_ref_text"] == "《不存在的法》第一条"


def test_stages_package_exports_refactored_stage_entrypoints() -> None:
    from builder.stages import (
        run_entity_alignment,
        run_entity_extraction,
        run_explicit_relations as exported_explicit_relations,
        run_implicit_reasoning,
        run_normalize,
    )

    assert callable(run_normalize)
    assert callable(exported_explicit_relations)
    assert callable(run_entity_extraction)
    assert callable(run_entity_alignment)
    assert callable(run_implicit_reasoning)
