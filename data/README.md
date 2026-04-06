# data

运行期数据目录按以下约定组织：

- `source/docs/`: 原始 DOCX
- `source/metadata/`: 元数据清单
- `intermediate/01_normalize/`: 逐文档清洗 JSON 与阶段索引
- `intermediate/02_structure_graph/`: 结构图 graph bundle
- `intermediate/03_explicit_relations/`
- `intermediate/04_entity_extraction/`
- `intermediate/05_entity_alignment/`
- `intermediate/06_implicit_reasoning/`
- `../logs/builder/`: builder manifest、阶段日志与 normalize 报告
- `exports/json/`: 最终 graph bundle
- `exports/import/neo4j/`: Neo4j 导入 JSONL
- `exports/import/elasticsearch/`: Elasticsearch 导入 JSONL
- `models/relation_classifier/`
- `models/ner/`
- `models/rgcn/`

构建时不直接写数据库，所有阶段正式产物都先写入本目录。
