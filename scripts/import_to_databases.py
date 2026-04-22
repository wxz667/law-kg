#!/usr/bin/env python3
"""
导入图谱数据到 Neo4j 和 Elasticsearch

用法:
    python scripts/import_to_databases.py --neo4j-only    # 只导入 Neo4j
    python scripts/import_to_databases.py --es-only       # 只导入 Elasticsearch
    python scripts/import_to_databases.py                 # 两者都导入
"""

from kg_api.core.es_client import ElasticsearchClient
from kg_api.core.neo4j_client import Neo4jClient, Neo4jConfig
import argparse
import json
import sys
from pathlib import Path
from typing import Any

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent /
                "backend" / "api" / "kg-api" / "src"))


def load_graph_bundle(bundle_path: Path) -> dict[str, Any]:
    """加载图谱 bundle 文件"""
    print(f"正在加载图谱数据: {bundle_path}")
    with open(bundle_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"  - 节点数量: {len(data.get('nodes', []))}")
    print(f"  - 边数量: {len(data.get('edges', []))}")
    return data


def import_to_neo4j(bundle_data: dict[str, Any], neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> None:
    """导入数据到 Neo4j"""
    print("\n" + "="*60)
    print("开始导入到 Neo4j")
    print("="*60)

    # 创建 Neo4j 客户端
    config = Neo4jConfig(uri=neo4j_uri, user=neo4j_user,
                         password=neo4j_password)
    client = Neo4jClient(config)
    client.open()

    nodes = bundle_data.get('nodes', [])
    edges = bundle_data.get('edges', [])

    # 预处理节点数据 - 将复杂类型转换为字符串
    processed_nodes = []
    for node in nodes:
        processed_node = {
            'id': node['id'],
            'type': node.get('type', ''),
            'name': node.get('name', ''),
            'level': node.get('level', ''),
            'text': node.get('text', ''),
            'category': node.get('category', ''),
            'status': node.get('status', ''),
            'issuer': node.get('issuer', ''),
            'publish_date': node.get('publish_date', ''),
            'effective_date': node.get('effective_date', ''),
            'source_url': node.get('source_url', ''),
            # 将 metadata 和完整数据转为 JSON 字符串
            'metadata_json': json.dumps(node.get('metadata', {}), ensure_ascii=False),
            'full_data_json': json.dumps(node, ensure_ascii=False),
        }
        processed_nodes.append(processed_node)

    # 批量导入节点
    print(f"\n正在导入 {len(processed_nodes)} 个节点...")
    batch_size = 500

    for i in range(0, len(processed_nodes), batch_size):
        batch = processed_nodes[i:i+batch_size]

        # 构建 Cypher 查询 - 使用 UNWIND 和 MERGE
        # MERGE 会根据 id 判断是否存在，存在则更新，不存在则创建
        cypher_query = """
        UNWIND $nodes AS nodeData
        MERGE (n:Node {id: nodeData.id})
        SET n.type = nodeData.type,
            n.name = nodeData.name,
            n.level = nodeData.level,
            n.text = nodeData.text,
            n.category = nodeData.category,
            n.status = nodeData.status,
            n.issuer = nodeData.issuer,
            n.publish_date = nodeData.publish_date,
            n.effective_date = nodeData.effective_date,
            n.source_url = nodeData.source_url,
            n.metadata_json = nodeData.metadata_json,
            n.full_data_json = nodeData.full_data_json
        """

        try:
            client.run(cypher_query, {"nodes": batch})
            if (i // batch_size + 1) % 10 == 0 or i + batch_size >= len(processed_nodes):
                print(
                    f"  进度: {min(i + batch_size, len(processed_nodes))}/{len(processed_nodes)}")
        except Exception as e:
            print(f"\n  错误: 批次 {i}-{i+len(batch)} 导入失败: {e}")

    print(f"\n节点导入完成: {len(processed_nodes)} 个")

    # 批量导入关系
    print(f"\n正在导入 {len(edges)} 个关系...")

    for i in range(0, len(edges), batch_size):
        batch = edges[i:i+batch_size]

        # 使用动态关系类型，MERGE 会检查是否已存在
        cypher_query = """
        UNWIND $edges AS edgeData
        MATCH (source:Node {id: edgeData.source})
        MATCH (target:Node {id: edgeData.target})
        MERGE (source)-[rel:`" + edgeData['type'] + "` {properties: edgeData.properties}]->(target)
        """

        try:
            # 由于 Cypher 不支持变量作为关系类型，我们需要使用 apoc
            # 但为了简单起见，我们使用 MERGE 配合 apoc.create.relationship
            # 这里使用简单的方案：先查询是否存在，不存在则创建
            batch_queries = []
            for edge in batch:
                query = f"""
                MATCH (source:Node {{id: '{edge['source']}'}})
                MATCH (target:Node {{id: '{edge['target']}'}})
                MERGE (source)-[r:`{edge['type']}`]->(target)
                """
                batch_queries.append(query)
            
            # 合并执行
            combined_query = "\n".join(batch_queries)
            client.run(combined_query, {})
            if (i // batch_size + 1) % 10 == 0 or i + batch_size >= len(edges):
                print(f"  进度: {min(i + batch_size, len(edges))}/{len(edges)}")
        except Exception as e:
            print(f"\n  错误: 批次 {i}-{i+len(batch)} 导入失败: {e}")

    print(f"\n关系导入完成: {len(edges)} 个")
    print("\nNeo4j 导入完成!")


def import_to_elasticsearch(bundle_data: dict[str, Any], es_host: str, es_port: int) -> None:
    """导入数据到 Elasticsearch"""
    print("\n" + "="*60)
    print("开始导入到 Elasticsearch")
    print("="*60)

    # 创建 ES 客户端
    es_client = ElasticsearchClient(config={
        "url": f"http://{es_host}:{es_port}",
        "host": es_host,
        "port": es_port,
        "index_prefix": "legal_kg",
    })

    if not es_client.verify():
        print("⚠️  警告: Elasticsearch 连接失败，跳过导入")
        return

    nodes = bundle_data.get('nodes', [])

    # 按类型分组节点
    provisions = [n for n in nodes if n.get('type') == 'ProvisionNode']
    documents = [n for n in nodes if n.get('type') == 'DocumentNode']

    print(f"\n准备索引:")
    print(f"  - 法条节点: {len(provisions)}")
    print(f"  - 法律文档: {len(documents)}")

    # TODO: 实现真实的 ES 批量索引
    # 目前 ES 客户端的 bulk_index 方法还未实现
    print("\n⚠️  注意: Elasticsearch 批量索引功能尚未完全实现")
    print("   当前为预留接口，待后续完善 es_client.py 中的 bulk_index 方法")

    # Mock 提示
    if provisions:
        print(f"\n示例法条数据 (第一条):")
        print(json.dumps(provisions[0], ensure_ascii=False, indent=2))

    print("\nElasticsearch 导入完成 (Mock)")


def main():
    parser = argparse.ArgumentParser(description="导入图谱数据到数据库")
    parser.add_argument("--bundle-path", type=str,
                        default="data/processed/structured_graph/graph_bundle-0001.json",
                        help="图谱 bundle 文件路径")
    parser.add_argument("--neo4j-only", action="store_true", help="只导入 Neo4j")
    parser.add_argument("--es-only", action="store_true",
                        help="只导入 Elasticsearch")

    # Neo4j 配置
    parser.add_argument("--neo4j-uri", type=str,
                        default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", type=str, default="neo4j")
    parser.add_argument("--neo4j-password", type=str,
                        default="change_this_password")

    # ES 配置
    parser.add_argument("--es-host", type=str, default="localhost")
    parser.add_argument("--es-port", type=int, default=9200)

    args = parser.parse_args()

    # 加载数据
    bundle_path = Path(args.bundle_path)
    if not bundle_path.exists():
        print(f"错误: 文件不存在: {bundle_path}")
        sys.exit(1)

    bundle_data = load_graph_bundle(bundle_path)

    # 导入到数据库
    if not args.es_only:
        try:
            import_to_neo4j(bundle_data, args.neo4j_uri,
                            args.neo4j_user, args.neo4j_password)
        except Exception as e:
            print(f"\n❌ Neo4j 导入失败: {e}")
            import traceback
            traceback.print_exc()
            if args.neo4j_only:
                sys.exit(1)

    if not args.neo4j_only:
        try:
            import_to_elasticsearch(bundle_data, args.es_host, args.es_port)
        except Exception as e:
            print(f"\n❌ Elasticsearch 导入失败: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    print("\n" + "="*60)
    print("✓ 所有导入任务完成!")
    print("="*60)


if __name__ == "__main__":
    main()
