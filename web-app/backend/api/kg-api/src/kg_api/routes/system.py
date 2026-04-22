"""System routes - System statistics and health information"""
from fastapi import APIRouter, HTTPException, status
from neo4j import GraphDatabase
from kg_api.config import settings
from datetime import datetime

router = APIRouter(prefix="/system", tags=["System"])


@router.get("/stats")
def get_system_stats():
    """
    获取系统统计数据
    包括：收录法律数量、图谱节点数量、法条总量、最后更新时间等
    """
    try:
        stats = {
            "total_laws": 0,
            "total_nodes": 0,
            "total_provisions": 0,
            "last_updated": datetime.utcnow().isoformat(),
        }

        # 从 Neo4j 获取统计数据
        try:
            driver = GraphDatabase.driver(
                settings.NEO4J_URI,
                auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
            )

            with driver.session() as session:
                # 获取 DocumentNode 类型节点数量（法律文档）
                result = session.run("""
                    MATCH (n:Node)
                    WHERE n.type = 'DocumentNode'
                    RETURN count(n) as count
                """)
                record = result.single()
                if record:
                    stats["total_laws"] = record["count"]

                # 获取所有节点数量
                result = session.run("""
                    MATCH (n:Node)
                    RETURN count(n) as count
                """)
                record = result.single()
                if record:
                    stats["total_nodes"] = record["count"]

                # 获取 ProvisionNode 类型节点数量（法条）
                result = session.run("""
                    MATCH (n:Node)
                    WHERE n.type = 'ProvisionNode'
                    RETURN count(n) as count
                """)
                record = result.single()
                if record:
                    stats["total_provisions"] = record["count"]

                # 获取最后更新时间（从某个节点的 updated_at 属性获取）
                result = session.run("""
                    MATCH (n:Node)
                    WHERE n.updated_at IS NOT NULL
                    RETURN n.updated_at as last_update
                    ORDER BY n.updated_at DESC
                    LIMIT 1
                """)
                record = result.single()
                if record and record["last_update"]:
                    stats["last_updated"] = record["last_update"]

            driver.close()

        except Exception as neo4j_error:
            print(f"Neo4j stats query error: {neo4j_error}")
            # 如果 Neo4j 查询失败，使用默认值

        return stats

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system stats: {str(e)}"
        )
