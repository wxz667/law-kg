"""Structure-first legal knowledge base builder."""

from .pipeline.builder import build_batch_knowledge_graph, build_knowledge_graph

__all__ = ["build_batch_knowledge_graph", "build_knowledge_graph"]
