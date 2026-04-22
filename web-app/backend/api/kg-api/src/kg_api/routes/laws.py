"""法律图谱路由 - 提供法律搜索、法律详情、法条查询等API"""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional
from neo4j import GraphDatabase

from kg_api.config import settings
from kg_api.routes.auth import get_current_user
from kg_api.models import User

router = APIRouter(prefix="/laws", tags=["Legal Provisions"])


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


@router.get("/search")
async def search_laws(
    q: str = Query(..., description="搜索关键词"),
    limit: int = Query(10, ge=1, le=50, description="返回结果数量限制"),
    driver=Depends(get_neo4j_driver)
):
    """
    搜索法律文档

    根据关键词搜索法律名称，返回匹配的法律列表
    """
    try:
        with driver.session() as session:
            # 搜索法律文档（使用id属性，而非element_id）
            query = """
            MATCH (n:Node {type: 'DocumentNode'})
            WHERE toLower(n.name) CONTAINS toLower($keyword)
            RETURN n
            ORDER BY n.name
            LIMIT $limit
            """
            result = session.run(query, keyword=q, limit=limit)

            laws = []
            for record in result:
                node = record["n"]
                laws.append({
                    # 使用id属性作为source_id
                    "source_id": node.get("id", str(node.element_id)),
                    "title": node.get("name", ""),
                    "issuer": node.get("issuer", None),
                    "publish_date": node.get("publish_date", None),
                    "effective_date": node.get("effective_date", None),
                    "category": node.get("category", None),
                    "status": node.get("status", None),
                    "has_structured_data": True
                })

            return laws
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"搜索失败: {str(e)}"
        )


@router.get("/{source_id}")
async def get_law_detail(
    source_id: str,
    title: Optional[str] = Query(None, description="法律标题（可选，用于验证）"),
    driver=Depends(get_neo4j_driver)
):
    """
    获取法律详情（含完整层级结构和法条内容）

    返回指定法律的所有节点，包括：
    - DocumentNode: 法律文档节点
    - TocNode: 目录节点（编、章、节）
    - ProvisionNode: 法条节点（条、款、项）
    """
    try:
        with driver.session() as session:
            # 获取法律文档节点（支持两种ID格式）
            # 前端可能传递element_id（如4:uuid:index）或id属性（如document:uuid）
            # 使用elementId()函数支持element_id格式查询
            doc_query = """
            MATCH (n:Node {type: 'DocumentNode'})
            WHERE n.id = $source_id OR elementId(n) = $source_id
            RETURN n
            """
            doc_result = session.run(doc_query, source_id=source_id)
            doc_record = doc_result.single()

            # 如果通过id属性或element_id没找到，尝试通过title查找
            if not doc_record and title:
                title_query = """
                MATCH (n:Node {type: 'DocumentNode'})
                WHERE n.name = $title
                RETURN n
                """
                title_result = session.run(title_query, title=title)
                doc_record = title_result.single()

            if not doc_record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"法律文档未找到: {source_id}"
                )

            doc_node = doc_record["n"]

            # 验证标题（如果提供了title参数）
            if title and doc_node.get("name") != title:
                # 尝试通过标题查找
                title_query = """
                MATCH (n:Node {type: 'DocumentNode'})
                WHERE n.name = $title
                RETURN n
                """
                title_result = session.run(title_query, title=title)
                title_record = title_result.single()

                if title_record:
                    doc_node = title_record["n"]
                    source_id = doc_node.get("id", str(doc_node.element_id))

            # 获取该法律下的所有子节点
            nodes_query = """
            MATCH (doc:Node {type: 'DocumentNode'})
            WHERE doc.id = $source_id
            MATCH path = (doc)-[:CONTAINS*]->(n:Node)
            WHERE n.type IN ['TocNode', 'ProvisionNode']
            RETURN DISTINCT n
            ORDER BY n.id
            """
            nodes_result = session.run(nodes_query, source_id=source_id)

            nodes = []
            for record in nodes_result:
                node = record["n"]
                nodes.append({
                    "id": node.get("id", str(node.element_id)),  # 使用id属性
                    "name": node.get("name", ""),
                    "level": node.get("level", ""),
                    "type": node.get("type", ""),
                    "text": node.get("text", None),
                    "metadata": {
                        k: v for k, v in dict(node).items()
                        if k not in ['element_id', 'id', 'name', 'level', 'type', 'text']
                    }
                })

            # 添加文档节点本身
            nodes.insert(0, {
                "id": doc_node.get("id", str(doc_node.element_id)),  # 使用id属性
                "name": doc_node.get("name", ""),
                "level": "document",
                "type": "DocumentNode",
                "text": doc_node.get("text", None),
                "metadata": {
                    k: v for k, v in dict(doc_node).items()
                    if k not in ['element_id', 'id', 'name', 'level', 'type', 'text']
                }
            })

            return {
                "source_id": source_id,
                "title": doc_node.get("name", ""),
                "issuer": doc_node.get("issuer", None),
                "publish_date": doc_node.get("publish_date", None),
                "effective_date": doc_node.get("effective_date", None),
                "category": doc_node.get("category", None),
                "status": doc_node.get("status", None),
                "nodes": nodes
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取法律详情失败: {str(e)}"
        )


@router.get("/{source_id}/outline")
async def get_law_outline(
    source_id: str,
    driver=Depends(get_neo4j_driver)
):
    """
    获取法律大纲（仅层级结构，不含详细法条内容）
    """
    try:
        with driver.session() as session:
            # 获取法律文档节点
            doc_query = """
            MATCH (n:Node {type: 'DocumentNode'})
            WHERE n.element_id = $source_id OR n.id = $source_id
            RETURN n
            """
            doc_result = session.run(doc_query, source_id=source_id)
            doc_record = doc_result.single()

            if not doc_record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"法律文档未找到: {source_id}"
                )

            doc_node = doc_record["n"]

            # 获取该法律下的目录节点（不含法条内容）
            nodes_query = """
            MATCH (doc:Node {type: 'DocumentNode'})
            WHERE doc.element_id = $source_id OR doc.id = $source_id
            MATCH path = (doc)-[:CONTAINS*]->(n:Node)
            WHERE n.type = 'TocNode'
            RETURN DISTINCT n
            ORDER BY n.id
            """
            nodes_result = session.run(nodes_query, source_id=source_id)

            nodes = []
            for record in nodes_result:
                node = record["n"]
                nodes.append({
                    "id": str(node.element_id),
                    "name": node.get("name", ""),
                    "level": node.get("level", ""),
                    "type": "TocNode",
                    "text": None,  # 大纲不包含法条内容
                    "metadata": {}
                })

            # 添加文档节点本身
            nodes.insert(0, {
                "id": str(doc_node.element_id),
                "name": doc_node.get("name", ""),
                "level": "document",
                "type": "DocumentNode",
                "text": None,
                "metadata": {}
            })

            return {
                "source_id": source_id,
                "title": doc_node.get("name", ""),
                "nodes": nodes
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取法律大纲失败: {str(e)}"
        )


@router.post("/search-provision")
async def search_provision_by_number(
    law_source_id: str,
    provision_number: str,
    driver=Depends(get_neo4j_driver)
):
    """
    根据法条编号查询法条（如 "第一条"）
    """
    try:
        with driver.session() as session:
            query = """
            MATCH (doc:Node {type: 'DocumentNode'})
            WHERE doc.element_id = $source_id OR doc.id = $source_id
            MATCH path = (doc)-[:CONTAINS*]->(n:Node {type: 'ProvisionNode'})
            WHERE n.name CONTAINS $provision_number
            RETURN n, doc.name AS law_name
            LIMIT 1
            """
            result = session.run(
                query,
                source_id=law_source_id,
                provision_number=provision_number
            )
            record = result.single()

            if not record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"法条未找到: {provision_number}"
                )

            node = record["n"]
            law_name = record.get("law_name", "")

            return {
                "id": str(node.element_id),
                "law_source_id": law_source_id,
                "law_name": law_name,
                "provision_number": node.get("name", ""),
                "content": node.get("text", ""),
                "level": node.get("level", ""),
                "metadata": {
                    k: v for k, v in dict(node).items()
                    if k not in ['element_id', 'id', 'name', 'level', 'type', 'text']
                }
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"查询法条失败: {str(e)}"
        )


@router.get("/{source_id}/provisions/{provision_id}")
async def get_provision_detail(
    source_id: str,
    provision_id: str,
    driver=Depends(get_neo4j_driver)
):
    """
    获取具体法条详情
    """
    try:
        with driver.session() as session:
            query = """
            MATCH (n:Node)
            WHERE (n.element_id = $provision_id OR n.id = $provision_id)
            AND n.type = 'ProvisionNode'
            OPTIONAL MATCH (doc:Node {type: 'DocumentNode'})-[:CONTAINS*]->(n)
            RETURN n, doc.name AS law_name, doc.element_id AS law_source_id
            """
            result = session.run(
                query,
                provision_id=provision_id
            )
            record = result.single()

            if not record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"法条未找到: {provision_id}"
                )

            node = record["n"]
            law_name = record.get("law_name", "")
            law_source_id = record.get("law_source_id", source_id)

            return {
                "id": str(node.element_id),
                "source_id": str(law_source_id),
                "law_name": law_name,
                "provision_number": node.get("name", ""),
                "content": node.get("text", ""),
                "level": node.get("level", ""),
                "metadata": {
                    k: v for k, v in dict(node).items()
                    if k not in ['element_id', 'id', 'name', 'level', 'type', 'text']
                }
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取法条详情失败: {str(e)}"
        )
