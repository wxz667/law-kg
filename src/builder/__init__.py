"""Legal knowledge graph builder package."""

from .pipeline.orchestrator import build_batch_knowledge_graph, build_knowledge_graph

__all__ = ["build_batch_knowledge_graph", "build_knowledge_graph"]
