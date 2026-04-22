#!/usr/bin/env python3
"""
从 raw_data 导入完整的知识图谱数据到 Neo4j 和 Elasticsearch

数据格式：
- nodes.jsonl: 节点数据（每行一个JSON对象）
- edges.jsonl: 边数据（每行一个JSON对象）
- documents/: 法律文档JSON文件

用法:
    python scripts/import_kg_data.py --neo4j-only    # 只导入 Neo4j
    python scripts/import_kg_data.py --es-only       # 只导入 ES
    python scripts/import_kg_data.py                 # 两者都导入
"""

from kg_api.core.neo4j_client import Neo4jClient, Neo4jConfig
from kg_api.core.es_client import ElasticsearchClient
import json
import sys
from pathlib import Path
from typing import Any

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent /
                "backend" / "api" / "kg-api" / "src"))


def clear_neo4j_data(neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> None:
    """清空 Neo4j 中的所有数据"""
    print("\n" + "="*60)
    print("清空 Neo4j 数据")
    print("="*60)

    config = Neo4jConfig(uri=neo4j_uri, user=neo4j_user,
                         password=neo4j_password)
    client = Neo4jClient(config)
    client.open()

    try:
        print("正在删除所有节点和关系...")
        client.run("MATCH (n) DETACH DELETE n")
        print("✅ Neo4j 数据已清空")
    finally:
        client.close()


def import_nodes_to_neo4j(nodes_file: Path, neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> int:
    """导入节点到 Neo4j"""
    print("\n" + "="*60)
    print("导入节点到 Neo4j")
    print("="*60)

    config = Neo4jConfig(uri=neo4j_uri, user=neo4j_user,
                         password=neo4j_password)
    client = Neo4jClient(config)
    client.open()

    try:
        batch_size = 500
        batch = []
        total_count = 0

        with open(nodes_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                node = json.loads(line)
                batch.append(node)

                if len(batch) >= batch_size:
                    _import_node_batch(client, batch)
                    total_count += len(batch)
                    print(f"  进度: {total_count:,} 个节点")
                    batch = []

        # 导入剩余的节点
        if batch:
            _import_node_batch(client, batch)
            total_count += len(batch)
            print(f"  进度: {total_count:,} 个节点")

        print(f"\n✅ 节点导入完成: {total_count:,} 个")
        return total_count

    finally:
        client.close()


def _import_node_batch(client: Neo4jClient, batch: list[dict]) -> None:
    """批量导入节点"""
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
        n.order = nodeData.order
    """

    client.run(cypher_query, {"nodes": batch})


def import_edges_to_neo4j(edges_file: Path, neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> int:
    """导入边到 Neo4j"""
    print("\n" + "="*60)
    print("导入关系到 Neo4j")
    print("="*60)

    config = Neo4jConfig(uri=neo4j_uri, user=neo4j_user,
                         password=neo4j_password)
    client = Neo4jClient(config)
    client.open()

    try:
        batch_size = 500
        batch = []
        total_count = 0

        with open(edges_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                edge = json.loads(line)
                batch.append(edge)

                if len(batch) >= batch_size:
                    _import_edge_batch(client, batch)
                    total_count += len(batch)
                    print(f"  进度: {total_count:,} 条关系")
                    batch = []

        # 导入剩余的关系
        if batch:
            _import_edge_batch(client, batch)
            total_count += len(batch)
            print(f"  进度: {total_count:,} 条关系")

        print(f"\n✅ 关系导入完成: {total_count:,} 条")
        return total_count

    finally:
        client.close()


def _import_edge_batch(client: Neo4jClient, batch: list[dict]) -> None:
    """批量导入关系 - 使用动态 Cypher 类型（更高效）"""
    # 按关系类型分组
    edges_by_type = {}
    for edge in batch:
        rel_type = edge.get('type', 'CONTAINS')
        if rel_type not in edges_by_type:
            edges_by_type[rel_type] = []
        edges_by_type[rel_type].append(edge)

    # 为每种关系类型执行查询
    for rel_type, edges in edges_by_type.items():
        # 使用动态关系类型语法
        cypher_query = f"""
        UNWIND $edges AS edgeData
        MATCH (source:Node {{id: edgeData.source}})
        MATCH (target:Node {{id: edgeData.target}})
        CREATE (source)-[:`{rel_type}`]->(target)
        """

        client.run(cypher_query, {"edges": edges})


def _import_edge_batch_fallback(client: Neo4jClient, batch: list[dict]) -> None:
    """备用方案：不使用 APOC 导入关系"""
    # 按关系类型分组
    edges_by_type = {}
    for edge in batch:
        rel_type = edge.get('type', 'CONTAINS')
        if rel_type not in edges_by_type:
            edges_by_type[rel_type] = []
        edges_by_type[rel_type].append(edge)

    # 为每种关系类型执行查询
    for rel_type, edges in edges_by_type.items():
        # 确保关系类型是合法的 Cypher 标识符
        safe_rel_type = rel_type.replace(' ', '_').replace('-', '_')

        queries = []
        for edge in edges:
            query = f"""
            MATCH (source:Node {{id: '{edge['source']}'}})
            MATCH (target:Node {{id: '{edge['target']}'}})
            MERGE (source)-[:`{safe_rel_type}`]->(target)
            """
            queries.append(query)

        combined_query = "\n".join(queries)
        client.run(combined_query, {})


def import_documents_to_es(documents_dir: Path, es_host: str, es_port: int) -> int:
    """导入法律文档到 Elasticsearch"""
    print("\n" + "="*60)
    print("导入法律文档到 Elasticsearch")
    print("="*60)

    es_client = ElasticsearchClient(config={
        "url": f"http://{es_host}:{es_port}",
        "host": es_host,
        "port": es_port,
        "index_prefix": "legal_kg",
    })

    if not es_client.verify():
        print("⚠️  警告: Elasticsearch 连接失败，跳过导入")
        return 0

    # 加载所有文档
    doc_files = list(documents_dir.glob("*.json"))
    laws_data = []

    print(f"正在加载 {len(doc_files)} 个法律文档...")
    for doc_file in doc_files:
        try:
            with open(doc_file, 'r', encoding='utf-8') as f:
                doc = json.load(f)
                laws_data.append(doc)
        except Exception as e:
            print(f"  警告: 读取 {doc_file.name} 失败: {e}")

    # 批量索引
    print(f"正在索引 {len(laws_data)} 个文档...")
    success = es_client.bulk_index_laws(laws_data)

    if success:
        print(f"✅ Elasticsearch 导入完成: {len(laws_data)} 个文档")
        return len(laws_data)
    else:
        print(f"⚠️  Elasticsearch 导入失败")
        return 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="从 raw_data 导入知识图谱数据")
    parser.add_argument("--raw-data-dir", type=str,
                        default="backend/api/kg-api/raw_data",
                        help="raw_data 目录路径")
    parser.add_argument("--neo4j-only", action="store_true", help="只导入 Neo4j")
    parser.add_argument("--es-only", action="store_true", help="只导入 ES")
    parser.add_argument("--skip-clear", action="store_true", help="跳过清空数据步骤")

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

    # 路径设置
    raw_data_dir = Path(args.raw_data_dir)
    nodes_file = raw_data_dir / "nodes.jsonl"
    edges_file = raw_data_dir / "edges.jsonl"
    documents_dir = raw_data_dir / "documents"

    if not nodes_file.exists():
        print(f"错误: 文件不存在: {nodes_file}")
        sys.exit(1)

    if not edges_file.exists():
        print(f"错误: 文件不存在: {edges_file}")
        sys.exit(1)

    if not documents_dir.exists():
        print(f"错误: 目录不存在: {documents_dir}")
        sys.exit(1)

    # 清空 Neo4j 数据
    if not args.es_only and not args.skip_clear:
        clear_neo4j_data(args.neo4j_uri, args.neo4j_user, args.neo4j_password)

    # 导入到 Neo4j
    if not args.es_only:
        try:
            node_count = import_nodes_to_neo4j(
                nodes_file, args.neo4j_uri, args.neo4j_user, args.neo4j_password)
            edge_count = import_edges_to_neo4j(
                edges_file, args.neo4j_uri, args.neo4j_user, args.neo4j_password)
            print(f"\n✅ Neo4j 导入完成: {node_count:,} 个节点, {edge_count:,} 条关系")
        except Exception as e:
            print(f"\n❌ Neo4j 导入失败: {e}")
            import traceback
            traceback.print_exc()
            if args.neo4j_only:
                sys.exit(1)

    # 导入到 Elasticsearch
    if not args.neo4j_only:
        try:
            doc_count = import_documents_to_es(
                documents_dir, args.es_host, args.es_port)
            print(f"\n✅ Elasticsearch 导入完成: {doc_count:,} 个文档")
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
