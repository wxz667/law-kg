from __future__ import annotations

from functools import lru_cache
from typing import Any

from neo4j import GraphDatabase

from kg_api.config import settings


@lru_cache(maxsize=1)
def get_driver():
    return GraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD))


def run_query(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    driver = get_driver()
    try:
        with driver.session() as session:
            res = session.run(query, params or {})
            return [dict(r) for r in res]
    except Exception:
        return []
