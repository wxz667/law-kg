# 项目指南：法律知识图谱构建流水线

## 项目目标

项目以 `src/builder/` 为唯一构建主包，负责将 `data/source/docs/*.docx` 与 `data/source/metadata/*.json` 中维护的法律文本和元数据，转换为可持续扩充的知识图谱中间产物与图输出。

当前构建主链路固定为：

- `normalize`
- `structure`
- `detect`
- `classify`
- `extract`
- `aggregate`
- `align`

构建过程以 JSON / JSONL 中间产物为准，不直接依赖数据库写入。

## 强制约束

以下约束为项目实施规约。涉及阶段接口、增量替换、manifest、中间产物、日志和复用逻辑的代码修改，均以本规约为准。

### manifest 规约

- `manifest` 表示当前阶段累计正式产物的清单与状态描述。
- `manifest` 的结构固定为：
  - `stage`
  - `inputs`
  - `artifacts`
  - `updated_at`
  - `unit`
  - `stats`
  - `metadata`
  - `processed_units`
  - `substages`
- `manifest` 的语义对象是当前磁盘上的正式中间产物，而不是单次构建、单次 checkpoint 或单轮增量执行快照。
- `manifest` 的唯一职责是描述当前磁盘上的正式产物，以及恢复构建所需的最小状态。
- `manifest.stats` 只允许记录当前累计正式产物可直接反推的统计，不允许运行态统计。
- `manifest.metadata` 只允许记录复用判定等辅助信息，不得写入运行结果摘要。
- `manifest.processed_units` 表示已经成功处理并纳入当前累计状态的处理单元集合，它是状态字段，不是统计字段。
- `substages` 仅表示阶段内部嵌套流程，结构与顶层 manifest 一致。
- 阶段产物发生替换、过滤、清理或收敛时，`manifest` 同步反映替换后的累计状态。
- 以下字段不得写入 stage manifest 或 substage manifest 的 `stats`：
  - `source_count`
  - `succeeded_sources`
  - `failed_sources`
  - `reused_sources`
  - `processed_source_count`
  - `skipped_source_count`
  - `work_units_total`
  - `work_units_completed`
  - `work_units_failed`
  - `work_units_skipped`
  - `work_units_attempted`
  - `llm_request_count`
  - `llm_error_count`
  - `retry_count`

### 日志规约

- `job log` 与 `stage log` 记录单次构建过程状态。
- 运行中的 checkpoint、错误、失败摘要、耗时、请求统计等过程信息归属于日志。
- 单次构建状态、失败、重试、跳过、请求次数等运行态统计全部归属 `logs/builder/{job_id}.json`。
- graph stage 完成后，job log 顶层统计需要同时记录：
  - 最终图规模：`node_count`、`edge_count`、`node_type_counts`、`edge_type_counts`
  - 本次构建相对于构建前正式图的真实更新量：`updated_nodes`、`updated_edges`
- `manifest` 不承担单次运行过程快照职责。

### 阶段边界规约

- 每个阶段读取上一阶段已经落盘的正式产物作为输入。
- orchestrator 负责调度、复用判定、阶段输出替换、累计状态写回、manifest 写回和日志写回。
- orchestrator 不承担阶段业务过滤、业务补数、业务修正职责。
- 某类过滤、补齐、聚合、物化规则若属于阶段语义，实现在该阶段目录内。
- 下一阶段的输入集合由上一阶段正式产物决定；上一阶段正式产物中不存在的记录，不进入下一阶段输入。

### 工作单元规约

- 所有阶段都只记录工作单元，不区分额外的 manifest 类型。
- `unit` 字段只描述当前阶段工作单元的对象类型，例如 `source`、`candidate`、`node`。
- `processed_units` 是唯一的处理完成状态字段。
- 构建作用域、来源映射、阶段内部辅助状态不进入 manifest 主结构；若必须为复用保存辅助信息，只能进入 `metadata`。

### 中间产物规约

- 中间产物文件只记录当前阶段需要向后传递的正式结果。
- 空结果视为“已处理但无正式产物”，记录在处理状态中，不写入正式产物文件。
- 单个阶段的正式产物文件与 manifest 清单保持一致，不保留长期脏残留。

## 目录约定

- Python 源码位于 `src/`
- 主构建包位于 `src/builder/`
- crawler 模块位于 `src/crawler/`
- 图谱 schema 位于 `configs/schema.json`
- builder 输入来自：
  - `data/source/docs/`
  - `data/source/metadata/`
- builder 中间产物位于 `data/intermediate/builder/`
- builder manifest 位于 `data/manifest/builder/`
- builder 日志位于 `logs/builder/`
- 最终导出位于 `data/exports/json/`
- 导入拆分文件位于 `data/exports/import/`

## 当前阶段链路

### 阶段一：`normalize`

职责：

- 扫描 `data/source/metadata/*.json`
- 以 metadata 中的 `source_id` 和 `title` 匹配 `data/source/docs/*.docx`
- 读取 DOCX 正文、表格和自动编号
- 清洗不可见字符、异常空白、目录残留、封面残留和尾部形式化落款
- 将表格和列表转换为线性正文表达
- 输出逐文档清洗 JSON 与 normalize 索引

处理单元：

- `source_id`

正式产物：

- `data/intermediate/builder/01_normalize/documents/{source_id}.json`
- `data/intermediate/builder/01_normalize/normalize_index.json`

manifest 规约：

- `processed_units` 表示已完成 normalize 的文档集合
- `stats` 仅保留：
  - `document_count`

### 阶段二：`structure`

职责：

- 消费 `normalize` 产物
- 为每个文档构建 `DocumentNode`
- 解析正文层级结构，生成结构图节点与 `CONTAINS` 边
- 处理章节、条、款、项、目、附件等结构节点

处理单元：

- `source_id`

正式产物：

- `data/intermediate/builder/02_structure/nodes.jsonl`
- `data/intermediate/builder/02_structure/edges.jsonl`

manifest 规约：

- `processed_units` 表示当前累计结构图中已经完成替换的文档集合
- `stats` 仅保留当前图产物统计：
  - `node_count`
  - `edge_count`
  - `node_type_counts`
  - `edge_type_counts`

### 阶段三：`detect`

职责：

- 消费 `structure` 图
- 从结构节点文本中抽取显式交叉引用候选
- 识别法名、简称、自引用与相对条款引用
- 输出引用候选，不在本阶段直接物化关系边

处理单元：

- 结构图中的引用扫描单元 `unit_id`

正式产物：

- `data/intermediate/builder/03_detect/candidates.jsonl`

manifest 规约：

- `processed_units` 表示已完成扫描并纳入当前累计候选结果的 detect 单元
- `stats` 仅保留：
  - `candidate_count`

### 阶段四：`classify`

职责：

- 消费 `detect` 候选与 `structure` 图
- 对引用候选做关系判别
- 输出分类结果、待仲裁结果和最终关系图边

处理单元：

- detect 候选单元 `unit_id`

正式产物：

- `data/intermediate/builder/04_classify/results.jsonl`
- `data/intermediate/builder/04_classify/pending.jsonl`
- `data/intermediate/builder/04_classify/llm_judgments.jsonl`
- `data/intermediate/builder/04_classify/edges.jsonl`

manifest 规约：

- stage 顶层不使用 `processed_units`
- `substages` 中记录 `model` 与 `llm_judge` 的阶段内状态
- `substages.model.processed_units` 表示已完成模型阶段的 candidate 单元
- `substages.llm_judge.processed_units` 表示已完成仲裁阶段的 candidate 单元
- stage 级 `stats` 仅保留当前结果产物统计，例如：
  - `result_count`
  - `edge_count`
  - `edge_type_counts`
  - `interprets_count`
  - `references_count`
  - `ordinary_reference_count`
  - `judicial_interprets_count`
  - `judicial_references_count`
- `edge_count` 与 `edge_type_counts` 直接描述当前 `04_classify/edges.jsonl`
- `substages.stats` 同样只保留对应子阶段正式产物的统计，不记录运行态指标

### 阶段五：`extract`

职责：

- 消费 `classify` 后的图快照
- 拆分为 `input` 与 `extract` 两个子阶段
- `input` 根据图结构构建概念抽取输入
- `extract` 基于已落盘输入执行概念抽取

处理单元：

- 概念抽取输入单元 `unit_id`

正式产物：

- `data/intermediate/builder/05_extract/inputs.jsonl`
- `data/intermediate/builder/05_extract/concepts.jsonl`

输入产物规约：

- `inputs.jsonl` 每行包含：
  - `id`
  - `hierarchy`
  - `content`

概念产物规约：

- `concepts.jsonl` 每行包含：
  - `id`
  - `concepts`
- `concepts` 为结构化概念数组
- 空 `concepts` 不写入正式产物

manifest 规约：

- stage 顶层不使用 `processed_units`
- `substages.input` 记录输入构建完成状态
- `substages.extract` 记录概念抽取完成状态
- 仅完成 `input` 不代表完成整个 `extract` stage
- `substages.input.processed_units` 表示已完成输入构建的 source 单元
- `substages.extract.processed_units` 表示已完成概念抽取的输入单元
- stage 级 `stats` 仅保留：
  - `result_count`
  - `concept_count`
- `substages.input.stats` 仅保留输入产物统计，例如：
  - `output_source_count`
  - `result_count`
- `substages.extract.stats` 仅保留：
  - `result_count`
  - `concept_count`

### 阶段六：`aggregate`

职责：

- 消费 `05_extract` 的正式产物
- 基于 `extract_concepts` 构造 aggregate 输入
- 使用 `extract_inputs` 为同一 `id` 补充上下文信息，例如 `hierarchy`
- 对抽取得到的概念进行合并、消歧、主附概念划分和描述收敛
- 将聚合结果展平为单概念正式产物
- 保留核心概念与附属概念的阶段统计，但本阶段不落图

处理单元：

- aggregate 输入单元 `unit_id`

正式产物：

- `data/intermediate/builder/06_aggregate/concepts.jsonl`

输入规约：

- aggregate 输入以 `05_extract/concepts.jsonl` 为驱动集合
- `05_extract/inputs.jsonl` 仅补充相同 `id` 的上下文

概念产物规约：

- `concepts.jsonl` 每行包含：
  - `id`
  - `name`
  - `description`
  - `parent`
  - `root`
- `root` 是原章节节点 `id`
- `parent` 为直属上级概念 `id`，顶层概念写空字符串
- 空概念结果不写入正式产物，但该输入单元仍记为 processed
- checkpoint 与最终写盘都直接写当前累计正式产物，不等待全量完成后一次性落盘

manifest 规约：

- `processed_units` 表示已完成 aggregate 并纳入当前累计产物状态的输入单元
- `stats` 仅保留：
  - `result_count`
  - `concept_count`
  - `core_concept_count`
  - `subordinate_concept_count`

### 阶段七：`align`

职责：

- 消费 `06_aggregate/concepts.jsonl`
- 按章节 `root` 执行增量概念对齐
- 拆分为 `embed`、`recall`、`judge` 三个子阶段
- 基于 `equivalence` 维护当前 canonical 概念集合
- 基于对齐结果落 `ConceptNode`、`MENTIONS`、`HAS_SUBORDINATE`、`RELATED_TO`

处理单元：

- 章节根节点 `root`

正式产物：

- `data/intermediate/builder/07_align/concepts.jsonl`
- `data/intermediate/builder/07_align/vectors.jsonl`
- `data/intermediate/builder/07_align/pairs.jsonl`
- `data/intermediate/builder/07_align/relations.jsonl`
- `data/intermediate/builder/07_align/nodes.jsonl`
- `data/intermediate/builder/07_align/edges.jsonl`

产物规约：

- `concepts.jsonl` 每行包含：
  - `id`
  - `name`
  - `description`
  - `member_ids`
  - `root_ids`
- `vectors.jsonl` 每行包含：
  - `id`
  - `vector`
- `pairs.jsonl` 每行包含：
  - `left_id`
  - `right_id`
  - `relation`
  - `similarity`
- `relations.jsonl` 每行包含：
  - `left_id`
  - `right_id`
  - `relation`

manifest 规约：

- stage 顶层不使用 `processed_units`
- `substages.embed.processed_units` 表示已完成 embedding 的 raw concept `id`
- `substages.recall.processed_units` 表示已完成 recall 的 raw concept `id`
- `substages.judge.processed_units` 表示已完成判别的 pair key
- stage 级 `stats` 仅保留：
  - `concept_count`
  - `vector_count`
  - `pair_count`
  - `relation_count`
  - `node_count`
  - `edge_count`
  - `node_type_counts`
  - `edge_type_counts`
  - 以上图统计直接描述当前 `nodes.jsonl` / `edges.jsonl`
- `substages.embed.stats` 仅保留：
  - `vector_count`
  - `result_count`
- `substages.recall.stats` 仅保留：
  - `pair_count`
  - `result_count`
- `substages.judge.stats` 仅保留：
  - `pair_count`
  - `result_count`
  - `equivalent_count`
  - `is_subordinate_count`
  - `has_subordinate_count`
  - `related_count`
  - `none_count`

## 中间产物与接口规约

### 阶段输入接口

- `normalize -> structure`
  - `normalize_index.json`
- `structure -> detect`
  - `02_structure` 图快照
- `detect -> classify`
  - `03_detect/candidates.jsonl`
  - `02_structure` 图快照
- `classify -> extract`
  - `04_classify` 图快照
- `extract -> aggregate`
  - `05_extract/inputs.jsonl`
  - `05_extract/concepts.jsonl`
- `aggregate -> align`
  - `06_aggregate/concepts.jsonl`
  - `04_classify` 图快照

### 产物替换规约

- 单文档或单作用域 rebuild 时，阶段写回的是该作用域在累计产物中的替换结果。
- 当前作用域内旧产物由本轮有效结果替换。
- 当前作用域外累计产物保留。
- 空结果导致该单元不再出现在正式产物中，但该单元仍可被记录为 processed。

### 复用规约

- 复用以阶段 manifest 与正式产物共同判定。
- 复用判断基于工作单元集合和正式产物存在性。
- 被 `--rebuild` 命中的处理单元不复用旧结果。
- 复用后的进度可体现在阶段总进度中，但复用不改变正式产物语义。

## 公共组件职责

### orchestrator

职责：

- 阶段调度
- 作用域解析
- 复用判定
- checkpoint 写盘
- 正式产物替换与累计状态写回
- manifest 写回
- job log / stage log 写回

边界：

- 不负责阶段业务规则
- 不负责从辅助输入补造业务结果
- 不负责阶段内概念过滤、候选修正、聚合策略等业务逻辑

### incremental

职责：

- 提供阶段累计产物替换、筛选、图子图替换等通用增量能力

边界：

- 优先保留通用图操作和正式产物替换能力
- 阶段特有的输入构造、统计口径、业务过滤优先放回阶段目录

### manifest contract

职责：

- 定义 stage manifest 与 substage manifest 的数据结构和序列化规则

边界：

- schema 支持 source-stage 与 unit-stage 共存
- 每个阶段只写入与自身主工作单元语义一致的字段

## CLI 约定

保留以下 builder 命令：

- `build`
- `build-batch`
- `split-export`

阶段参数：

- `--start`
- `--end`
- `--rebuild`

CLI 语义：

- `build` 按单个或指定 `source_id` 构建
- `build-batch` 按 metadata 批量构建
- 默认复用已有阶段累计产物
- `--rebuild` 强制重建命中作用域内的目标阶段累计状态

阶段名固定为：

- `normalize`
- `structure`
- `detect`
- `classify`
- `extract`
- `aggregate`
- `align`

辅助脚本：

- `scripts/build`
- `scripts/build-batch`
- `scripts/builder`
- `scripts/split_export`

## 原始数据契约

builder 的原始输入契约为：

- `data/source/docs/*.docx`
- `data/source/metadata/*.json`

metadata 约定：

- 每个文件内容是 JSON array
- array 中每个元素是一条文档 metadata
- 同一物理 docx 可被多个 metadata 引用，但每个 `source_id` 独立产出 normalize 文档

补充约束：

- `.docx` 必须是真实 DOCX zip 包
- 源文件若为旧 `.doc` / WPS 复合文档，先转换为合法 `.docx`
- metadata 与 docx 采用精确匹配和空白归一化后的精确匹配

常见 metadata 字段：

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

## 图谱范围

当前 schema 正式节点包含：

- `DocumentNode`
- `TocNode`
- `ProvisionNode`
- `ConceptNode`

其中 `appendix` 是 `ProvisionNode.level` 的一种取值。

当前正式关系包含：

- `CONTAINS`
- `REFERENCES`
- `INTERPRETS`
- `MENTIONS`
- `HAS_SUBORDINATE`

`DocumentNode` 顶层字段以 `configs/schema.json` 为准，稀疏或辅助信息进入 `metadata`。
