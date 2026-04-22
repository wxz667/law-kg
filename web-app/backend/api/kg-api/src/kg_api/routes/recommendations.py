"""Provision search and recommendation routes"""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import Optional
from pydantic import BaseModel

from neo4j import GraphDatabase
from kg_api.config import settings

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])


def get_neo4j_driver():
    """Get Neo4j driver instance"""
    driver = GraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    )
    try:
        yield driver
    finally:
        driver.close()


class SmartRecommendIn(BaseModel):
    document_id: str | None = None
    content: str
    current_paragraph: str | None = None
    case_type: str | None = "criminal"
    top_k: int | None = 10


@router.post("/smart")
async def smart_recommend_api(payload: SmartRecommendIn):
    try:
        from kg_api.services.recommendation.hybrid_recommender import smart_recommend

        return await smart_recommend(
            content=payload.content,
            case_type=payload.case_type or "criminal",
            current_paragraph=payload.current_paragraph,
            top_k=int(payload.top_k or 10),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Smart recommendation failed: {str(e)}",
        )


@router.get("/search")
def search_provisions(
    q: str = Query(..., description="Search query keyword"),
    field: str = Query(
        "name", description="Field to search: name, text, or full_name"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    driver=Depends(get_neo4j_driver)
):
    """
    Search provisions by keyword in Neo4j knowledge graph.
    Always searches both name and text fields for better recall.
    Prioritizes article-level provisions for better context.
    """
    try:
        with driver.session() as session:
            # Always search both name and text fields to ensure we find matches
            # This ensures we return results regardless of which field the frontend requests
            # Also join with DocumentNode to get the law name
            query = """
            MATCH (n:Node)
            WHERE n.type = 'ProvisionNode'
              AND (
                toLower(n.name) CONTAINS toLower($keyword)
                OR toLower(n.text) CONTAINS toLower($keyword)
                OR toLower(coalesce(n.full_name, '')) CONTAINS toLower($keyword)
              )
            OPTIONAL MATCH (law:Node {type: 'DocumentNode'})-[:CONTAINS*]->(n)
            RETURN n, law.name AS law_name
            ORDER BY 
                CASE n.level
                    WHEN 'article' THEN 1
                    WHEN 'paragraph' THEN 2
                    WHEN 'item' THEN 3
                    WHEN 'sub_item' THEN 4
                    ELSE 5
                END,
                size(coalesce(n.text, n.name, '')) ASC
            SKIP $offset
            LIMIT $limit
            """

            result = session.run(query, keyword=q, offset=offset, limit=limit)
            provisions = []

            for record in result:
                node = record["n"]
                law_name = record.get("law_name", "")

                # Build full_name with law name for better display
                display_name = f"{law_name} {node.get('name', '')}" if law_name else node.get(
                    'name', '')

                provision_data = {
                    "id": str(node.element_id),
                    "type": node.get("type", "ProvisionNode"),
                    "name": node.get("name", ""),
                    "full_name": display_name,
                    "law_name": law_name,
                    "text": node.get("text", ""),
                    "level": node.get("level", ""),
                    "properties": dict(node)
                }
                provisions.append(provision_data)

            # Get total count
            count_query = """
            MATCH (n:Node)
            WHERE n.type = 'ProvisionNode'
              AND (
                toLower(n.name) CONTAINS toLower($keyword)
                OR toLower(n.text) CONTAINS toLower($keyword)
                OR toLower(coalesce(n.full_name, '')) CONTAINS toLower($keyword)
              )
            RETURN count(n) as total
            """
            count_result = session.run(count_query, keyword=q)
            total = count_result.single()["total"]

            return {
                "results": provisions,
                "total": total,
                "limit": limit,
                "offset": offset
            }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )


@router.get("/node/{node_id}")
def get_provision_node(
    node_id: str,
    driver=Depends(get_neo4j_driver)
):
    """
    获取单个法条节点的详细信息
    前端插入法条时需要调用此API获取法条内容
    """
    try:
        with driver.session() as session:
            query = """
            MATCH (n:Node)
            WHERE n.type = 'ProvisionNode' AND n.id = $node_id
            OPTIONAL MATCH (law:Node {type: 'DocumentNode'})-[:CONTAINS*]->(n)
            RETURN n, law.name AS law_name
            """
            result = session.run(query, node_id=node_id)
            record = result.single()

            if not record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Provision node not found: {node_id}"
                )

            node = record["n"]
            law_name = record.get("law_name", "")

            # Build full_name with law name for better display
            display_name = f"{law_name} {node.get('name', '')}" if law_name else node.get(
                'name', '')

            return {
                "id": str(node.element_id),
                "labels": ["ProvisionNode"],
                "type": node.get("type", "ProvisionNode"),
                "name": node.get("name", ""),
                "full_name": display_name,
                "law_name": law_name,
                "text": node.get("text", ""),
                "level": node.get("level", ""),
                "properties": dict(node)
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get provision node: {str(e)}"
        )
