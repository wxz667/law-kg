#!/usr/bin/env python3
"""验证导入的数据"""

from kg_api.core.es_client import ElasticsearchClient
from kg_api.core.neo4j_client import Neo4jClient, Neo4jConfig
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent /
                "backend" / "api" / "kg-api" / "src"))


# 验证 Neo4j
print("="*60)
print("验证 Neo4j 数据")
print("="*60)

neo4j_config = Neo4jConfig(uri='bolt://localhost:7687',
                           user='neo4j', password='change_this_password')
neo4j_client = Neo4jClient(neo4j_config)
neo4j_client.open()

try:
    result = neo4j_client.run('MATCH (d:Document) RETURN count(d) as count')
    # run 方法返回的是列表
    if result and len(result) > 0:
        count = result[0]['count']
        print(f"✅ Document 节点数: {count}")

    # 查看示例数据
    sample = neo4j_client.run(
        'MATCH (d:Document) RETURN d.title, d.id LIMIT 3')
    print("\n示例文档:")
    for record in sample:
        title = record.get('d.title', 'N/A')
        doc_id = record.get('d.id', 'N/A')
        print(f"  - {title} (ID: {doc_id[:20]}...)")
finally:
    neo4j_client.close()

# 验证 Elasticsearch
print("\n" + "="*60)
print("验证 Elasticsearch 数据")
print("="*60)

es_client = ElasticsearchClient(config={
    "url": "http://localhost:9200",
    "index_prefix": "legal_kg",
})

if es_client.verify():
    stats = es_client.get_stats("legal_kg_laws")
    if stats and 'indices' in stats:
        doc_count = stats['indices']['legal_kg_laws']['primaries']['docs']['count']
        print(f"✅ legal_kg_laws 索引文档数: {doc_count}")

        # 测试搜索
        results = es_client.search_laws("网络安全法", limit=2)
        print(f"\n搜索测试 ('网络安全法'):")
        for r in results:
            print(f"  - {r.get('title')} (score: {r.get('_score', 0):.2f})")
else:
    print("⚠️  ES 连接失败")

print("\n" + "="*60)
print("✓ 验证完成!")
print("="*60)
