# 《中华人民共和国刑法》Tree-KG 实施文档

## 1. 项目目标

本项目的目标是基于《中华人民共和国刑法》原始法条文本构建法律知识图谱，并将图谱以**独立于图数据库的序列化 JSON 形式**作为主产物存储。

项目采用 Tree-KG 的核心思想：先构建法律文本的显式层级结构，再通过语义算子逐步扩展出实体、语义描述、去重结果和逻辑关系，最终形成可审计、可回放、可版本化的图谱产物。

本项目中的法条细粒度层级定义如下：

- `article`
  对应“第…条”，如“第三百八十五条”
- `paragraph`
  对应条下自然段语义上的“款”，仅在条文确实存在多个自然段时建立该层级
- `item`
  对应“（一）”“（二）”等列举项
- `sub_item`
  对应“1.”“2.”等更细列举目
- `appendix`
  对应法典正文后的“附件一”“附件二”等文档级附录
- `appendix_item`
  对应附件中的阿拉伯数字清单项，如“1.关于禁毒的决定”

## 2. 输入规范

唯一规范原始输入格式：

- `.docx`

唯一主数据源路径：

- [data/raw/statutes/中华人民共和国刑法.docx](/home/zephyr/law-kg/data/raw/statutes/中华人民共和国刑法.docx)

输入要求：

- 文件必须包含可提取的文字层
- 文件应尽量保留“编、章、节、条、款、项”等法典结构
- 原始数据只放置在 `data/raw/` 下，不在源码目录内混放

## 3. 方法总览

项目方法依据 Tree-KG 论文进行法律场景改写，核心 pipeline 为：

`ingest -> segment -> summarize -> extract -> aggr -> conv -> embed -> dedup -> pred -> serialize`

方法约束：

- `ingest` 负责读取原始 `docx` 并生成标准化源文档
- `segment` 负责构建法律层级树和显式垂直边
- `summarize` 负责生成法条与目录节点的摘要
- `extract` 负责抽取法律实体和显式关系
- `aggr` 负责核心/非核心实体聚合
- `conv` 负责上下文卷积与实体描述增强
- `embed` 负责实体向量化
- `dedup` 负责实体对齐与去重
- `pred` 负责逻辑边预测
- `serialize` 负责输出标准图谱文件

Tree-KG 在本项目中的适配原则：

- 教材结构映射为法律结构：`编/章/节/条/款/项/目`
- 对单段条文不强制增设“款”；仅在存在真实多自然段结构时建立 `paragraph`
- 附件属于文档级附录结构，不属于第四百五十二条等正文条文的“款”
- 显式 KG 对应法律文本的物理层级和基础实体挂载
- 隐式 KG 对应条件、处罚、引用、例外等逻辑关系
- `summarize` 作为显式结构与语义算子之间的上下文桥梁
- `merge` 预留给未来的司法解释、案例与其他法律来源的增量融合，不属于当前核心构建命令

## 4. 图谱主存储策略

本项目采用 `JSON-first` 策略。

规范主产物：

- `data/graph/graph.bundle.json`

该策略适用于《中华人民共和国刑法》这一单部法典规模，原因如下：

- 序列化 JSON 可以完整保存节点与边
- 图谱构建过程无需与 Neo4j 强耦合，便于审计、重放、版本管理和后续算法迭代
- 后续只需增加独立导入脚本，即可将 `graph.bundle.json` 中的 `nodes` 与 `edges` 导入 Neo4j 用于展示和产品交付

因此，Neo4j 不是本项目当前核心 pipeline 的主存储，而是后续可选下游。

## 5. 数据目录规范

### 5.1 原始数据

- `data/raw/statutes/`
- `data/raw/judicial-interpretations/`
- `data/raw/cases/`
- `data/raw/annotations/`

### 5.2 中间产物

- `data/intermediate/01_ingest/`
- `data/intermediate/02_segment/`
- `data/intermediate/03_summarize/`
- `data/intermediate/04_extract/`
- `data/intermediate/05_aggr/`
- `data/intermediate/06_conv/`
- `data/intermediate/07_embed/`
- `data/intermediate/08_dedup/`
- `data/intermediate/09_pred/`

### 5.3 最终产物

- `data/graph/graph.bundle.json`

### 5.4 构建记录

- `data/manifest/build_manifest.json`

## 6. 图谱数据契约

### 6.1 `GraphBundle`

字段：

- `graph_id`
- `nodes`
- `edges`

职责：

- 作为唯一事实来源描述最终交付图谱
- 仅记录图节点与图边，不包含中间产物引用

### 6.2 `NodeRecord`

字段：

- `id`
- `type`
- `name`
- `level`
- `source_id`
- `text`
- `summary`
- `description`
- `embedding_ref`
- `address`
- `metadata`

正文存储规则：

- `article` 仅在未形成真实 `paragraph` 层级时承载条文正文或项前导语
- `paragraph` 仅在条文存在真实多自然段时创建，并承载该自然段正文或项前导语
- `item` 承载本项正文或目前导语
- `sub_item` 承载本目正文
- `appendix` 承载附件说明段导语
- `appendix_item` 承载附件中的单条清单正文
- 不对所有单段条文统一补出 `paragraph`

源文档结构规则：

- `source_document.json` 作为 ingest/segment 输入产物，不进入最终图谱
- `preface_text` 保存法名下方的通过、修订、修正说明
- `toc_lines` 保存目录行
- `body_lines` 保存正文与附则正文
- `appendix_lines` 保存附件原始行
- 立法沿革和时间说明通过 `preface_text` 与 `metadata.revision_events` 表达
- 最终图谱中的文档级背景信息仅保留在 `DocumentNode.metadata`

### 6.3 `EdgeRecord`

字段：

- `id`
- `source`
- `target`
- `type`
- `weight`
- `evidence`
- `metadata`

### 6.4 `BuildManifest`

字段：

- `build_id`
- `source_path`
- `status`
- `started_at`
- `finished_at`
- `stages`
- `config_snapshot`
- `artifact_paths`

职责：

- 记录单次构建的配置快照、阶段状态、产物路径和错误信息
- 作为唯一允许保留中间产物路径引用的记录文件

## 7. 节点与边规范

### 7.1 节点类型

- `DocumentNode`
- `TocNode`
- `ProvisionNode`
- `EntityNode`
- `AppendixNode`
- `AppendixItemNode`

### 7.2 垂直边

- `HAS_PART`
- `HAS_CHAPTER`
- `HAS_SECTION`
- `HAS_ARTICLE`
- `HAS_APPENDIX`
- `HAS_PARAGRAPH`
- `HAS_ITEM`
- `HAS_APPENDIX_ITEM`
- `HAS_SUB_ITEM`
- `HAS_ENTITY`
- `HAS_SUBORDINATE`

### 7.3 水平边

- `SECTION_RELATED`
- `ENTITY_RELATED`
- `CONDITION_OF`
- `PENALTY_OF`
- `EXCEPTION_TO`
- `REFERENCE_TO`
- `SAME_AS`

附件建模规则：

- `附件一/附件二` 作为 `appendix` 节点挂在 `document` 根节点下
- 附件中的 `1.`、`2.` 清单作为 `appendix_item` 节点挂在对应附件下
- 第四百五十二条正文中的附件引用保留在条文节点文本中，并通过 `REFERENCE_TO` 指向对应附件
- 附件不复用 `paragraph/item/sub_item` 层级，避免与正文条款结构混淆

## 8. 源码结构规范

核心源码位于：

- [kg-build/src/kg_build](/home/zephyr/law-kg/kg-build/src/kg_build)

源码分层：

- `contracts/`
  稳定数据契约
- `config/`
  配置加载
- `io/`
  `docx` 读取、JSON 存取、manifest 存取
- `pipeline/`
  单入口构建编排
- `stages/`
  各阶段实现与 TODO 接口
- `cli.py`
  唯一公开命令入口

静态资源位于：

- [kg-build/resources](/home/zephyr/law-kg/kg-build/resources)

节点标识规范：

- `id` 只用于结构定位，不包含原文内容
- `id` 采用最小结构地址形式，例如：
  - `article:<source_id>:0385`
  - `appendix:<source_id>:01`
  - `paragraph:<source_id>:0385:01`
  - `item:<source_id>:0385:01:01`
  - `sub_item:<source_id>:0385:01:01:01`
  - `appendix_item:<source_id>:01:01`
- `name` 负责保存人类可读法条引用
- 原始正文只存储在最小正文承担节点，不在上层节点重复存储全文

## 9. TODO 模块规范

以下模块在当前框架中保留为稳定接口，但具体算法实现使用 `TODO` 占位：

- `summarize`
- `extract`
- `aggr`
- `conv`
- `embed`
- `dedup`
- `pred`

约束：

- `TODO` 模块必须保留明确职责、输入输出契约和稳定文件路径
- `TODO` 模块不得使用演示性质、启发式性质或样例专用逻辑冒充正式实现
- `TODO` 模块仍必须参与完整 pipeline，使构建记录和产物链路保持闭合

向量存储规范：

- 节点主记录只保存 `embedding_ref`
- 实际向量独立存放在 `data/intermediate/07_embed/embeddings.jsonl`
- `graph.bundle.json` 不直接内嵌高维向量数组

## 10. 构建入口

唯一公开构建命令：

```bash
cd /home/zephyr/law-kg/kg-build
PYTHONPATH=src /home/zephyr/law-kg/.venv/bin/python -m kg_build.cli build \
  --source /home/zephyr/law-kg/data/raw/statutes/中华人民共和国刑法.docx \
  --data-root /home/zephyr/law-kg/data
```

该命令应完成：

- 原始输入读取
- 显式层级图构建
- 各语义阶段顺序落盘
- 图谱 JSON 主产物生成
- 构建 manifest 生成

## 11. 外部配置清单

图谱框架的外部配置项包括：

- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_MODEL_SUMMARIZE`
- `OPENAI_MODEL_EXTRACT`
- `OPENAI_MODEL_JUDGE`
- `OPENAI_EMBEDDING_MODEL`
- `NEO4J_URI`
- `NEO4J_USER`
- `NEO4J_PASSWORD`

配置模板：

- [.env.example](/home/zephyr/law-kg/.env.example)

说明：

- LLM 与 embedding 配置用于后续接入 `TODO` 模块
- Neo4j 配置用于后续导入与展示，不参与当前核心构建流程

## 12. 可选下游

`infra/neo4j/` 属于可选基础设施目录，仅用于未来导入、展示和产品交付。

核心 pipeline 不依赖 Neo4j：

- 图谱生成以 JSON 为主
- 可视化与图库导入属于独立下游
- 下游导入脚本与展示层不包含在当前核心构建实施范围内
