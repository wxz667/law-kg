#!/usr/bin/env python3
"""检查原始数据和导入数据的一致性"""

from kg_api.core.es_client import ElasticsearchClient
from kg_api.core.neo4j_client import Neo4jClient, Neo4jConfig
import sys
import json
from pathlib import Path
import glob

print("="*60)
print("检查原始数据")
print("="*60)

# 统计原始文件
docs_dir = Path("data/raw/documents")
doc_files = list(docs_dir.glob("*.json"))
print(f"📁 原始文档文件数: {len(doc_files)}")

# 统计元数据
meta_count = 0
for meta_file in glob.glob("data/raw/metadata/*.json"):
    with open(meta_file, 'r', encoding='utf-8') as f:
        metas = json.load(f)
        meta_count += len(metas)

print(f"📋 元数据条目数: {meta_count}")

# 检查是否有缺失的元数据
doc_ids = {f.stem for f in doc_files}
meta_ids = set()
for meta_file in glob.glob("data/raw/metadata/*.json"):
    with open(meta_file, 'r', encoding='utf-8') as f:
        metas = json.load(f)
        for m in metas:
            meta_ids.add(m.get('source_id', ''))

missing_meta = doc_ids - meta_ids
print(f"\n⚠️  缺少元数据的文档: {len(missing_meta)} 个")
if missing_meta:
    print(f"   示例: {list(missing_meta)[:5]}")

print("\n" + "="*60)
print("检查导入后的数据")
print("="*60)

sys.path.insert(0, str(Path(__file__).parent.parent /
                "backend" / "api" / "kg-api" / "src"))


# 检查 Neo4j
neo4j_config = Neo4jConfig(uri='bolt://localhost:7687',
                           user='neo4j', password='change_this_password')
neo4j_client = Neo4jClient(neo4j_config)
neo4j_client.open()

try:
    result = neo4j_client.run('MATCH (d:Document) RETURN count(d) as count')
    if result and len(result) > 0:
        neo4j_count = result[0]['count']
        print(f"✅ Neo4j Document 节点数: {neo4j_count}")

        # 检查是否有文档没有元数据
        no_meta = neo4j_client.run('''
            MATCH (d:Document)
            WHERE d.issuer IS NULL OR d.issuer = ''
            RETURN count(d) as count
        ''')
        if no_meta and len(no_meta) > 0:
            print(f"⚠️  Neo4j 中缺少元数据的文档: {no_meta[0]['count']} 个")
finally:
    neo4j_client.close()

# 检查 Elasticsearch
es_client = ElasticsearchClient(config={
    "url": "http://localhost:9200",
    "index_prefix": "legal_kg",
})

if es_client.verify():
    stats = es_client.get_stats("legal_kg_laws")
    if stats and 'indices' in stats:
        es_count = stats['indices']['legal_kg_laws']['primaries']['docs']['count']
        print(f"✅ ES legal_kg_laws 索引文档数: {es_count}")

print("\n" + "="*60)
print("对比结果")
print("="*60)
print(f"原始文档文件: {len(doc_files)}")
print(f"Neo4j 导入:   {neo4j_count}")
print(f"ES 导入:      {es_count}")

if len(doc_files) == neo4j_count == es_count:
    print("\n✅ 所有数据完整导入！")
else:
    print(f"\n⚠️  数据不一致！")
    if len(doc_files) != neo4j_count:
        print(f"   Neo4j 缺失: {len(doc_files) - neo4j_count} 个")
    if len(doc_files) != es_count:
        print(f"   ES 缺失: {len(doc_files) - es_count} 个")
