"""Graph node routes - Neo4j node queries"""
from fastapi import APIRouter, Depends, HTTPException, status
from neo4j import GraphDatabase
from kg_api.config import settings

router = APIRouter(prefix="/graph", tags=["Graph"])


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


@router.get("/node/{node_id}")
def get_node(
    node_id: str,
    driver=Depends(get_neo4j_driver)
):
    """
    获取单个节点的详细信息
    用于前端获取法条节点数据进行插入
    """
    try:
        with driver.session() as session:
            query = """
            MATCH (n:Node)
            WHERE n.id = $node_id
            OPTIONAL MATCH (law:Node {type: 'DocumentNode'})-[:CONTAINS*]->(n)
            RETURN n, law.name AS law_name
            """
            result = session.run(query, node_id=node_id)
            record = result.single()

            if not record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Node not found: {node_id}"
                )

            node = record["n"]
            law_name = record.get("law_name", "")

            # Build full_name with law name for better display
            display_name = f"{law_name} {node.get('name', '')}" if law_name else node.get(
                'name', '')

            return {
                "id": str(node.element_id),
                "labels": [node.get("type", "Node")],
                "type": node.get("type", "Node"),
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
            detail=f"Failed to get node: {str(e)}"
        )


@router.get("/search")
def search_nodes(
    q: str,
    field: str = "name",
    limit: int = 20,
    offset: int = 0,
    driver=Depends(get_neo4j_driver)
):
    """
    搜索节点
    """
    try:
        with driver.session() as session:
            query = """
            MATCH (n:Node)
            WHERE toLower(n.name) CONTAINS toLower($keyword)
               OR toLower(coalesce(n.text, '')) CONTAINS toLower($keyword)
               OR toLower(coalesce(n.full_name, '')) CONTAINS toLower($keyword)
            OPTIONAL MATCH (law:Node {type: 'DocumentNode'})-[:CONTAINS*]->(n)
            RETURN n, law.name AS law_name
            SKIP $offset
            LIMIT $limit
            """

            result = session.run(query, keyword=q, offset=offset, limit=limit)
            nodes = []

            for record in result:
                node = record["n"]
                law_name = record.get("law_name", "")
                display_name = f"{law_name} {node.get('name', '')}" if law_name else node.get(
                    'name', '')

                node_data = {
                    "id": str(node.element_id),
                    "labels": [node.get("type", "Node")],
                    "type": node.get("type", "Node"),
                    "name": node.get("name", ""),
                    "full_name": display_name,
                    "law_name": law_name,
                    "text": node.get("text", ""),
                    "level": node.get("level", ""),
                    "properties": dict(node)
                }
                nodes.append(node_data)

            return nodes

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )


@router.get("/neighbors/{node_id}")
def get_neighbors(
    node_id: str,
    limit: int = 50,
    driver=Depends(get_neo4j_driver)
):
    """
    获取节点的邻居节点和关系
    """
    try:
        with driver.session() as session:
            query = """
            MATCH (n:Node {id: $node_id})-[r]-(m:Node)
            RETURN n, r, m
            LIMIT $limit
            """

            result = session.run(query, node_id=node_id, limit=limit)
            neighbors = []

            for record in result:
                neighbors.append({
                    "node": {
                        "id": str(record["n"].element_id),
                        "labels": [record["n"].get("type", "Node")],
                        "properties": dict(record["n"])
                    },
                    "rel": {
                        "type": record["r"].type,
                        "start": str(record["r"].start_node.element_id),
                        "end": str(record["r"].end_node.element_id),
                        "properties": dict(record["r"])
                    },
                    "other": {
                        "id": str(record["m"].element_id),
                        "labels": [record["m"].get("type", "Node")],
                        "properties": dict(record["m"])
                    }
                })

            return neighbors

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get neighbors: {str(e)}"
        )


@router.get("/subgraph/{node_id}")
def get_subgraph(
    node_id: str,
    depth: int = 2,
    limit: int = 50,
    driver=Depends(get_neo4j_driver)
):
    """
    获取以指定节点为中心的子图
    用于知识图谱可视化
    
    Args:
        node_id: 节点ID（支持id属性或elementId）
        depth: 查询深度（几跳关系）
        limit: 返回节点数量限制
    """
    try:
        with driver.session() as session:
            # 分步查询：先找到节点，再查询关系
            # 第一步：查找起始节点
            start_query = """
            MATCH (start:Node)
            WHERE start.id = $node_id OR elementId(start) = $node_id
            RETURN start
            """
            start_result = session.run(start_query, node_id=node_id)
            start_record = start_result.single()
            
            if not start_record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Node not found: {node_id}"
                )
            
            start_node = start_record["start"]
            
            # 第二步：查询子图关系
            # Neo4j不允许在[*1..$depth]中使用参数，需要动态构建查询
            subgraph_query = f"""
            MATCH (start:Node)
            WHERE start.id = $node_id OR elementId(start) = $node_id
            
            // 查询所有相关节点和关系
            OPTIONAL MATCH p = (start)-[*1..{depth}]-(neighbor:Node)
            
            WITH start,
                 COLLECT(DISTINCT neighbor) AS neighbors,
                 COLLECT(relationships(p)) AS relLists
            
            // 展平关系列表并去重
            WITH [start] + neighbors AS allNodes,
                 REDUCE(acc = [], relList IN relLists | 
                     acc + [r IN relList WHERE NOT r IN acc | r]
                 ) AS uniqueRels
            
            RETURN allNodes[..$limit] AS nodes, uniqueRels AS edges
            """
            
            result = session.run(subgraph_query, node_id=node_id, limit=limit)
            record = result.single()
            
            if not record or not record["nodes"]:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Node not found: {node_id}"
                )
            
            nodes = []
            for node in record["nodes"]:
                nodes.append({
                    "id": node.get("id", str(node.element_id)),
                    "element_id": str(node.element_id),
                    "name": node.get("name", ""),
                    "type": node.get("type", "Node"),
                    "level": node.get("level", ""),
                    "text": node.get("text", ""),
                    "properties": dict(node)
                })
            
            # 处理关系列表
            relationships = []
            all_rels = record.get("edges", [])
            if all_rels:
                for rel in all_rels:
                    try:
                        relationships.append({
                            "type": rel.type,
                            "start": rel.start_node.get("id", str(rel.start_node.element_id)),
                            "end": rel.end_node.get("id", str(rel.end_node.element_id)),
                            "properties": dict(rel)
                        })
                    except Exception as e:
                        print(f"Error processing relationship: {e}", file=sys.stderr)
            
            # 去重关系
            seen_rels = set()
            unique_rels = []
            for rel in relationships:
                rel_key = f"{rel['start']}-{rel['type']}-{rel['end']}"
                if rel_key not in seen_rels:
                    seen_rels.add(rel_key)
                    unique_rels.append(rel)
            
            return {
                "nodes": nodes,
                "edges": unique_rels,
                "center_node_id": node_id
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get subgraph: {str(e)}"
        )
