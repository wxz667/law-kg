#!/usr/bin/env python3
"""
从 raw_data 导入数据到 Elasticsearch

支持导入:
- nodes.jsonl: 节点数据（DocumentNode类型）
- documents/: 法律文档JSON文件

用法:
    python scripts/import_raw_data_to_es.py --nodes-only    # 只导入nodes
    python scripts/import_raw_data_to_es.py --docs-only     # 只导入documents
    python scripts/import_raw_data_to_es.py                 # 两者都导入
"""

from kg_api.core.es_client import ElasticsearchClient
import json
import sys
from pathlib import Path
from typing import Any

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent /
                "backend" / "api" / "kg-api" / "src"))


def create_es_client() -> ElasticsearchClient:
    """创建ES客户端"""
    config = {
        "url": "http://localhost:9200",
        "index_prefix": "law",
    }

    client = ElasticsearchClient(config)
    client.open()

    if not client.verify():
        raise RuntimeError("无法连接到Elasticsearch")

    return client


def import_nodes_to_es(nodes_file: Path, es_client: ElasticsearchClient) -> int:
    """导入nodes.jsonl中的DocumentNode到ES"""
    print("\n" + "=" * 60)
    print("导入 nodes.jsonl 到 Elasticsearch")
    print("=" * 60)

    if not nodes_file.exists():
        print(f"❌ 文件不存在: {nodes_file}")
        return 0

    # 读取所有DocumentNode
    document_nodes = []
    total_lines = 0

    print(f"读取文件: {nodes_file}")
    with open(nodes_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                node = json.loads(line)
                # 只处理DocumentNode类型
                if node.get('type') == 'DocumentNode':
                    document_nodes.append(node)
                total_lines += 1

                if total_lines % 5000 == 0:
                    print(
                        f"  已读取 {total_lines:,} 行, 找到 {len(document_nodes):,} 个DocumentNode")
            except Exception as e:
                print(f"⚠️ 解析错误 (行 {total_lines}): {e}")

    print(
        f"\n✅ 共读取 {total_lines:,} 行, 找到 {len(document_nodes):,} 个DocumentNode\n")

    if not document_nodes:
        print("⚠️ 没有找到DocumentNode数据")
        return 0

    # 转换为ES需要的格式
    laws_for_es = []
    for node in document_nodes:
        law_data = {
            "source_id": node.get("id", ""),
            "title": node.get("name", ""),
            "category": node.get("category", ""),
            "issuer": node.get("issuer", ""),
            "publish_date": node.get("publish_date"),
            "effective_date": node.get("effective_date"),
            "status": node.get("status", ""),
            "level": node.get("level", ""),
        }

        # 过滤掉None值
        law_data = {k: v for k, v in law_data.items() if v is not None}
        laws_for_es.append(law_data)

    # 批量索引
    print(f"开始导入 {len(laws_for_es):,} 条数据到ES...")
    success = es_client.bulk_index_laws(laws_for_es)

    if success:
        print(f"✅ 成功导入 {len(laws_for_es):,} 条DocumentNode到ES")
        return len(laws_for_es)
    else:
        print("❌ 导入失败")
        return 0


def import_documents_to_es(documents_dir: Path, es_client: ElasticsearchClient) -> int:
    """导入documents目录下的法律文档到ES"""
    print("\n" + "=" * 60)
    print("导入 documents/ 到 Elasticsearch")
    print("=" * 60)

    if not documents_dir.exists():
        print(f"❌ 目录不存在: {documents_dir}")
        return 0

    # 获取所有JSON文件
    json_files = list(documents_dir.glob("*.json"))
    print(f"找到 {len(json_files):,} 个JSON文件\n")

    if not json_files:
        print("⚠️ 没有找到JSON文件")
        return 0

    # 读取所有文档
    laws_for_es = []
    failed_files = []

    for i, json_file in enumerate(json_files, 1):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                doc_data = json.load(f)

                # 提取关键字段
                law_data = {
                    "source_id": doc_data.get("source_id", doc_data.get("id", "")),
                    "title": doc_data.get("title", doc_data.get("name", "")),
                    "category": doc_data.get("category", ""),
                    "issuer": doc_data.get("issuer", ""),
                    "publish_date": doc_data.get("publish_date"),
                    "effective_date": doc_data.get("effective_date"),
                    "status": doc_data.get("status", ""),
                    "level": doc_data.get("level", ""),
                }

                # 过滤掉None值和空字符串
                law_data = {k: v for k, v in law_data.items()
                            if v is not None and v != ""}

                if law_data.get("source_id"):
                    laws_for_es.append(law_data)

            if i % 500 == 0:
                print(f"  进度: {i:,}/{len(json_files):,} 文件")

        except Exception as e:
            failed_files.append((json_file.name, str(e)))
            if len(failed_files) <= 5:
                print(f"⚠️ 读取失败 {json_file.name}: {e}")

    print(f"\n✅ 成功读取 {len(laws_for_es):,} 个文档")
    if failed_files:
        print(f"⚠️ 失败 {len(failed_files):,} 个文件")

    if not laws_for_es:
        print("⚠️ 没有可导入的数据")
        return 0

    # 批量索引
    print(f"\n开始导入 {len(laws_for_es):,} 条数据到ES...")
    success = es_client.bulk_index_laws(laws_for_es)

    if success:
        print(f"✅ 成功导入 {len(laws_for_es):,} 条文档到ES")
        return len(laws_for_es)
    else:
        print("❌ 导入失败")
        return 0


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="从 raw_data 导入数据到 Elasticsearch")
    parser.add_argument("--nodes-only", action="store_true",
                        help="只导入nodes.jsonl")
    parser.add_argument("--docs-only", action="store_true",
                        help="只导入documents/")
    parser.add_argument("--raw-data-dir", type=str,
                        default="backend/api/kg-api/raw_data",
                        help="raw_data目录路径")

    args = parser.parse_args()

    # 确定导入模式
    import_nodes = not args.docs_only
    import_docs = not args.nodes_only

    if args.nodes_only:
        import_docs = False
    if args.docs_only:
        import_nodes = False

    # 设置路径
    raw_data_dir = Path(args.raw_data_dir)
    nodes_file = raw_data_dir / "nodes.jsonl"
    documents_dir = raw_data_dir / "documents"

    print("=" * 60)
    print("从 raw_data 导入数据到 Elasticsearch")
    print("=" * 60)
    print(f"raw_data目录: {raw_data_dir.absolute()}")
    print(f"导入nodes: {import_nodes}")
    print(f"导入documents: {import_docs}")
    print("=" * 60)

    try:
        # 创建ES客户端
        print("\n连接Elasticsearch...")
        es_client = create_es_client()
        print("✅ ES连接成功\n")

        total_imported = 0

        # 导入nodes
        if import_nodes:
            count = import_nodes_to_es(nodes_file, es_client)
            total_imported += count

        # 导入documents
        if import_docs:
            count = import_documents_to_es(documents_dir, es_client)
            total_imported += count

        print("\n" + "=" * 60)
        print(f"✅ 导入完成! 共导入 {total_imported:,} 条数据")
        print("=" * 60)

        # 验证导入结果
        print("\n验证导入结果...")
        try:
            import requests
            r = requests.get('http://localhost:9200/law_laws/_count')
            count = r.json()['count']
            print(f"📊 law_laws 索引当前文档数: {count:,}")
        except Exception as e:
            print(f"⚠️ 验证失败: {e}")

    except Exception as e:
        print(f"\n❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        try:
            es_client.close()
            print("\n✅ ES连接已关闭")
        except:
            pass


if __name__ == "__main__":
    main()
