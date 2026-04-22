#!/usr/bin/env python3
"""
从 raw_data 导入法律文档到 Neo4j 和 Elasticsearch

用法:
    python scripts/import_raw_data.py --neo4j-only    # 只导入 Neo4j
    python scripts/import_raw_data.py --es-only       # 只导入 ES
    python scripts/import_raw_data.py                 # 两者都导入
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


def load_metadata(metadata_dir: Path) -> dict[str, dict[str, Any]]:
    """加载所有元数据文件"""
    print(f"正在加载元数据...")
    metadata_map = {}

    for meta_file in sorted(metadata_dir.glob("metadata-*.json")):
        print(f"  读取: {meta_file.name}")
        with open(meta_file, 'r', encoding='utf-8') as f:
            items = json.load(f)
            for item in items:
                source_id = item.get('source_id', '')
                if source_id:
                    metadata_map[source_id] = item

    print(f"  共加载 {len(metadata_map)} 条元数据")
    return metadata_map


def load_documents(documents_dir: Path) -> list[dict[str, Any]]:
    """加载所有法律文档"""
    print(f"\n正在加载法律文档...")
    documents = []

    doc_files = list(documents_dir.glob("*.json"))
    total = len(doc_files)

    for i, doc_file in enumerate(doc_files, 1):
        try:
            with open(doc_file, 'r', encoding='utf-8') as f:
                doc = json.load(f)
                documents.append(doc)

            if i % 500 == 0 or i == total:
                print(f"  进度: {i}/{total}")
        except Exception as e:
            print(f"  警告: 读取 {doc_file.name} 失败: {e}")

    print(f"  共加载 {len(documents)} 个文档")
    return documents


def import_to_neo4j(documents: list[dict], metadata_map: dict,
                    neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> None:
    """导入数据到 Neo4j"""
    print("\n" + "="*60)
    print("开始导入到 Neo4j")
    print("="*60)

    config = Neo4jConfig(uri=neo4j_uri, user=neo4j_user,
                         password=neo4j_password)
    client = Neo4jClient(config)
    client.open()

    try:
        # 清空现有数据（可选）
        print("\n清空现有 Document 节点...")
        client.run("MATCH (d:Document) DETACH DELETE d")

        # 批量导入文档节点
        print(f"\n正在导入 {len(documents)} 个法律文档...")
        batch_size = 100

        for i in range(0, len(documents), batch_size):
            batch = documents[i:i+batch_size]

            nodes_data = []
            for doc in batch:
                source_id = doc.get('source_id', '')
                meta = metadata_map.get(source_id, {})

                node_data = {
                    'id': source_id,
                    'title': doc.get('title', ''),
                    'content': doc.get('content', ''),
                    'issuer': meta.get('issuer', ''),
                    'publish_date': meta.get('publish_date', ''),
                    'effective_date': meta.get('effective_date', ''),
                    'category': meta.get('category', ''),
                    'status': meta.get('status', ''),
                    'source_url': meta.get('source_url', ''),
                    'source_format': meta.get('source_format', ''),
                }
                nodes_data.append(node_data)

            cypher_query = """
            UNWIND $nodes AS nodeData
            MERGE (d:Document {id: nodeData.id})
            SET d.title = nodeData.title,
                d.content = nodeData.content,
                d.issuer = nodeData.issuer,
                d.publish_date = nodeData.publish_date,
                d.effective_date = nodeData.effective_date,
                d.category = nodeData.category,
                d.status = nodeData.status,
                d.source_url = nodeData.source_url,
                d.source_format = nodeData.source_format
            """

            try:
                client.run(cypher_query, {"nodes": nodes_data})
                completed = min(i + batch_size, len(documents))
                print(f"  进度: {completed}/{len(documents)}")
            except Exception as e:
                print(f"  错误: 批次 {i}-{i+len(batch)} 导入失败: {e}")

        print(f"\nNeo4j 导入完成: {len(documents)} 个文档")

    finally:
        client.close()


def import_to_elasticsearch(documents: list[dict], metadata_map: dict,
                            es_host: str, es_port: int) -> None:
    """导入数据到 Elasticsearch"""
    print("\n" + "="*60)
    print("开始导入到 Elasticsearch")
    print("="*60)

    es_client = ElasticsearchClient(config={
        "url": f"http://{es_host}:{es_port}",
        "host": es_host,
        "port": es_port,
        "index_prefix": "legal_kg",
    })

    if not es_client.verify():
        print("⚠️  警告: Elasticsearch 连接失败，跳过导入")
        return

    # 准备法律数据
    print(f"\n准备索引 {len(documents)} 个法律文档...")
    laws_data = []

    for doc in documents:
        source_id = doc.get('source_id', '')
        meta = metadata_map.get(source_id, {})

        law_data = {
            "source_id": source_id,
            "title": doc.get('title', ''),
            "content": doc.get('content', ''),
            "issuer": meta.get('issuer', ''),
            "category": meta.get('category', ''),
            "status": meta.get('status', ''),
            "source_url": meta.get('source_url', ''),
            "source_format": meta.get('source_format', ''),
        }

        # 只添加非空的日期字段
        publish_date = meta.get('publish_date', '')
        if publish_date:
            law_data["publish_date"] = publish_date

        effective_date = meta.get('effective_date', '')
        if effective_date:
            law_data["effective_date"] = effective_date

        laws_data.append(law_data)

    # 使用 bulk_index_laws 方法批量导入
    print("\n正在批量索引...")
    success = es_client.bulk_index_laws(laws_data)

    if success:
        print(f"\nElasticsearch 导入完成: {len(laws_data)} 个文档")
    else:
        print(f"\n⚠️  Elasticsearch 导入失败")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="从 raw_data 导入法律文档")
    parser.add_argument("--raw-data-dir", type=str,
                        default="data/raw",
                        help="raw_data 目录路径")
    parser.add_argument("--neo4j-only", action="store_true", help="只导入 Neo4j")
    parser.add_argument("--es-only", action="store_true", help="只导入 ES")

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
    documents_dir = raw_data_dir / "documents"
    metadata_dir = raw_data_dir / "metadata"

    if not documents_dir.exists():
        print(f"错误: 目录不存在: {documents_dir}")
        sys.exit(1)

    if not metadata_dir.exists():
        print(f"错误: 目录不存在: {metadata_dir}")
        sys.exit(1)

    # 加载数据
    metadata_map = load_metadata(metadata_dir)
    documents = load_documents(documents_dir)

    # 导入到数据库
    if not args.es_only:
        try:
            import_to_neo4j(documents, metadata_map,
                            args.neo4j_uri, args.neo4j_user, args.neo4j_password)
        except Exception as e:
            print(f"\n❌ Neo4j 导入失败: {e}")
            import traceback
            traceback.print_exc()
            if args.neo4j_only:
                sys.exit(1)

    if not args.neo4j_only:
        try:
            import_to_elasticsearch(documents, metadata_map,
                                    args.es_host, args.es_port)
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
