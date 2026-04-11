# 项目指南：六阶段法律知识图谱构建流水线

## 当前目标

当前项目以 `src/builder/` 为唯一构建主包，负责把 `data/source/docs/*.docx` 与 `data/source/metadata/*.json` 中维护的法律文本和元数据，转换为可持续扩充的知识图谱 JSON。

当前构建期不接数据库。正式产物统一以 JSON 为准：

- `normalize` 先输出逐文档清洗结果
- `sturctre` 起输出标准 `graph_bundle-*.json`
- 后续阶段只在图上做增量处理

## 目录约定

- Python 源码统一放在 `src/`
- 主构建包固定为 `src/builder/`
- crawler 采集模块固定为 `src/crawler/`
- 可训练模型模块固定为：
  - `src/interprets_filter/`
  - `src/ner/`
  - `src/rgcn/`
- 图谱 schema 固定为 `configs/schema.json`
- builder 输入固定来自：
  - `data/source/docs/`
  - `data/source/metadata/`
- 阶段工件固定落在 `data/intermediate/`
- builder 日志与 manifest 固定落在 `logs/builder/`
- 最终图谱固定落在 `data/exports/json/`
- 导入拆分文件固定落在 `data/exports/import/`

## 六阶段职责

### 阶段一：`normalize`

职责：

- 扫描 `data/source/metadata/*.json`
- 以 metadata 中的 `source_id` 和 `title` 为主驱动匹配 `data/source/docs/*.docx`
- 匹配规则固定为：
  - 先按 `title == docx stem` 精确匹配
  - 再按空白与全角空白归一化后的精确匹配
  - 不做模糊匹配
- 读取 DOCX 正文、表格和自动编号
- 清洗不可见字符、异常空白、目录残留、封面残留和尾部形式化落款
- 表格和列表在 normalize 阶段转成线性正文表达
- 一个物理 DOCX 可以拆成多个逻辑文书，但每个 metadata 最终只保留一个有效主文书
- 对通知、印发、转发、请示等壳文默认过滤，仅保留真正有用的正文文书
- 输出逐文档清洗 JSON，不在本阶段构图
- 缺失文档、损坏文档和清洗失败写入 `logs/builder/normalize-report.json`，不中断批处理

正式产物：

- `data/intermediate/01_normalize/documents/{source_id}.json`
- `data/intermediate/01_normalize/normalize_index.json`

其中单文档 JSON 最少包含：

- `source_id`
- `title`
- `content`
- `appendix_lines`
- metadata 中除 `source_format` 外的其余字段

### 阶段二：`sturctre`

职责：

- 消费 `normalize` 阶段产物
- 每个清洗后的文档生成一个 `DocumentNode`
- `DocumentNode.id` 直接使用 `source_id`
- `DocumentNode.name` 对应清洗后的 `title`
- 不再在本阶段修改 metadata
- 以正文 `content` 为核心，解析：
  - 编
  - 章
  - 节
  - 条
  - 款
  - 项
  - 目
  - 附件
- 解析规则固定为：
  - 自上而下优先匹配标准法条结构
  - 非标准结构时，以 `一、二、三、`、`（一）（二）`、`1. 2. 3.` 等候补结构回推层级
  - 标题型短标题可回推为候补 `chapter` / `section`
  - 冒号引导后连续项列举应优先挂为 `item`
  - 完全无法结构化时，退化为单个 `segment`，名称固定为 `正文`
- 附件解析继续复用现有实现

正式产物：

- `data/intermediate/02_sturctre/graph_bundle-0001.json`

### 阶段三：`reference_filter`

职责：

- 从结构节点文本中抽取显式交叉引用候选
- 识别 `《法名》第X条`、`本法第X条`、`本条`、`本款`、`前款` 等引用
- 对 target 做完整展开、规范化重写和 `[T][/T]` 标记
- 输出候选工件，不在本阶段直接构边

正式产物：

- `data/intermediate/03_reference_filter/candidates.jsonl`

### 阶段四：`relation_classify`

职责：

- 对 `reference_filter` 候选做关系判别
- 非司法解释来源一律输出 `REFERENCES`
- 司法解释来源走 `interprets_filter` 阈值判别与可选 LLM 仲裁
- 输出关系计划工件，最终导出时再统一物化为图边

关系类型固定为：

- `REFERENCES`
- `INTERPRETS`

正式产物：

- `data/intermediate/04_relation_classify/relation_plans.jsonl`

### 阶段五：`entity_extraction`

职责：

- 调用 `ner` 模块抽取实体
- 为实体 mention 生成概念候选节点
- 补充 `MENTIONS` 边

正式产物：

- `data/intermediate/05_entity_extraction/graph_bundle-0001.json`

### 阶段六：`entity_alignment`

职责：

- 使用向量召回与内部判别逻辑对概念候选做对齐
- 将候选概念合并为规范化 `ConceptNode`
- 重写 mention 边，使图中仅保留对齐后的概念节点

注意：

- 该阶段不单独训练模型
- 不单独建新的训练子项目

正式产物：

- `data/intermediate/05_entity_alignment/graph_bundle-0001.json`

### 阶段七：`implicit_reasoning`

职责：

- 基于结构边、显式关系边、概念节点和 mention 信息构造图特征
- 调用 `rgcn` 模块预测隐式关系
- 将预测边增量补入图
- 输出最终 graph bundle

正式产物：

- `data/intermediate/06_implicit_reasoning/graph_bundle-0001.json`
- `data/exports/json/graph_bundle-0001.json`

## 图谱范围

当前 schema 正式节点包含：

- `DocumentNode`
- `TocNode`
- `ProvisionNode`
- `ConceptNode`

其中 `appendix` 只是 `ProvisionNode` 的一种 `level`，不是独立节点类型。

当前 `DocumentNode` 顶层字段以 `configs/schema.json` 为准，当前稳定字段为：

- `id`
- `type`
- `name`
- `level`
- `category`
- `status`
- `issuer`
- `publish_date`
- `effective_date`
- `source_url`
- `metadata`

补充约束：

- `id` 对应文档 `source_id`
- 不再额外暴露 `source_id` 顶层字段
- 不再额外暴露 `document_type` / `document_subtype` 顶层字段
- 稀疏或辅助信息进入 `metadata`

当前 schema 正式关系包含：

- `CONTAINS`
- `REFERENCES`
- `INTERPRETS`
- `AMENDS`
- `REPEALS`
- `MENTIONS`

## 模型与训练边界

只有需要训练的小模型拆成独立同级模块：

- `interprets_filter`
- `ner`
- `rgcn`

每个模块都必须包含：

- 数据集构建入口
- 本地训练入口
- 推理入口
- 模型工件目录约定

## 存储边界

当前构建阶段不直接写入 Neo4j 或 Elasticsearch。

正式产物以 JSON 为准：

- normalize 文档产物写入 `data/intermediate/01_normalize/`
- 阶段图写入 `data/intermediate/`
- 最终图写入 `data/exports/json/`

阶段目录中的文件形式固定为：

- `01_normalize/documents/{source_id}.json`
- `01_normalize/normalize_index.json`
- `02_sturctre/graph_bundle-0001.json`
- `03_reference_filter/candidates.jsonl`
- `04_relation_classify/relation_plans.jsonl`
- `05_entity_extraction/graph_bundle-0001.json`
- `06_entity_alignment/graph_bundle-0001.json`
- `07_implicit_reasoning/graph_bundle-0001.json`

后续导入流程由拆分脚本负责：

- 从最终 graph bundle 中拆出 Neo4j 节点、边 JSONL
- 从最终 graph bundle 中拆出 Elasticsearch 文档 JSONL

## CLI 与脚本约定

保留以下 builder 命令：

- `build`
- `build-batch`
- `split-export`

阶段参数固定使用：

- `--start`
- `--end`
- `--rebuild`

CLI 语义固定为：

- `build` 按单个 `source_id` 构建
- `build-batch` 默认扫描全部 metadata
- 默认复用已有阶段产物
- `--rebuild` 强制重建选定阶段及其后续阶段

阶段名固定为：

- `normalize`
- `sturctre`
- `reference_filter`
- `relation_classify`
- `entity_extraction`
- `entity_alignment`
- `implicit_reasoning`

辅助脚本固定为：

- `scripts/builder`
- `scripts/crawl_fetch`
- `scripts/crawl_materialize_docx`
- `scripts/build_graph`
- `scripts/build_batch`
- `scripts/split_export`

## 原始数据契约

builder 当前唯一原始输入契约为：

- `data/source/docs/*.docx`
- `data/source/metadata/*.json`

metadata 文件约定：

- 每个文件内容是一个 JSON array
- array 中每个元素是一条文档 metadata
- 同一物理 docx 可被多个 metadata 引用，但每个 `source_id` 仍独立产出 normalize 文档

补充约束：

- `.docx` 必须是真实的 DOCX zip 包
- 若源文件实际是老 `.doc`/WPS 复合文档，只是误用了 `.docx` 后缀，必须先转换后再进入 builder
- metadata 与 docx 的匹配只允许精确匹配和空白归一化后的精确匹配

当前 metadata 常见字段包括：

- `source_id`
- `title`
- `issuer`
- `publish_date`
- `effective_date`
- `document_type`
- `document_subtype`
- `status`
- `category`
- `source_url`
- `download_link_word`
- `download_link_html`
- `download_link_pdf`
- `crawler_job_id`
- `source_format`
