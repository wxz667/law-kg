# law-kg 项目实施指南与规格

本文是 `law-kg` 的项目实施文档和规格文档。凡涉及 builder 阶段接口、产物格式、manifest、增量复用、日志、schema、目录布局和公共组件职责的修改，均以本文为约束；若本文与代码不一致，应按代码实现修正文档。

## 文档编写计划

文档按三层维护：

- `README.md`: 面向使用者，说明项目目标、架构、运行方式、主要产物和当前限制
- `guideline.md`: 面向实现者，定义阶段契约、边界、manifest 语义、产物字段和开发规约
- `data/README.md`: 面向数据维护者，说明 `data/` 下源数据、中间产物、manifest、训练数据和导出的目录职责

每次改变阶段接口、文件路径、schema、CLI 或运行配置，都必须同步检查这三份文档。不要在最终文档中保留讨论过程、废弃方案或“本来打算如何”的叙述，只呈现当前实现应如何使用和扩展。

## 项目目标

项目目标是把 `data/source/metadata/*.json` 与 `data/source/docs/*.docx` 中维护的中文规范性文件转换为稳定、可增量构建、可导入下游系统的法律知识图谱 JSONL 产物。

构建主链路固定为：

1. `normalize`
2. `structure`
3. `detect`
4. `classify`
5. `extract`
6. `aggregate`
7. `align`
8. `infer`

构建阶段不直接写入数据库。所有阶段以磁盘上的 JSON/JSONL 正式产物作为阶段边界。

## 顶层目录职责

- `src/crawler/`: 国家法律法规数据库采集模块，负责元数据抓取、文档下载、去重、命名和 crawler 日志
- `src/builder/`: 知识图谱构建主包
- `src/builder/contracts/`: 图结构、阶段产物、manifest、job log 的数据契约
- `src/builder/io/`: 目录布局、JSON/JSONL 读写、产物读写函数
- `src/builder/pipeline/`: orchestrator、阶段 handler、增量复用、进度展示、运行时配置
- `src/builder/stages/`: 各阶段业务实现
- `src/interprets_filter/`: 解释关系分类模型的数据集、蒸馏、训练和预测
- `utils/llm/`: LLM 与 embedding 供应商适配层
- `utils/model_assets/`: 模型与数据集资产下载、发布
- `configs/config.json`: 运行时参数、LLM 参数、训练参数
- `configs/schema.json`: 图谱节点、边和层级 schema
- `data/`: 源数据、中间产物、manifest、训练数据和最终导出
- `logs/`: 单次运行日志

## 源数据契约

builder 原始输入为：

- `data/source/metadata/*.json`
- `data/source/docs/*.docx`

metadata 文件契约：

- 文件内容必须是 JSON array
- array 中每个元素是一条文档 metadata
- 必填语义字段为 `source_id`、`title`、`category`、`source_url`
- 常用字段包括 `issuer`、`publish_date`、`effective_date`、`status`、`source_format`
- 同一物理文档可被多个 metadata 引用，但 builder 以 `source_id` 为逻辑文档单位

DOCX 契约：

- 文件必须是合法 `DOCX` zip 包
- 旧 `.doc` 或 WPS 复合文档必须先转换为合法 `.docx`
- `normalize` 通过 metadata 的 `title` 匹配文档文件名，并兼容 crawler 的标题去重后缀形式

crawler 采集契约：

- 分类范围由 `src/crawler/models.py` 的 `CATEGORY_ID_MAP` 决定：`宪法`、`法律`、`行政法规`、`监察法规`、`地方法规`、`司法解释`
- metadata 分片命名为 `metadata-{shard_no:04d}.json`
- 文档文件名由标题清洗而来，非法路径字符替换为 `_`，UTF-8 字节长度不超过文件系统限制
- 标题重复时追加 `__{source_id 后 12 位}` 后缀
- metadata 去重按标题聚合，保留 `publish_date`、`effective_date`、`source_id` 排序更靠后的记录

## 配置契约

项目运行配置集中在 `configs/config.json`。

builder 配置：

- `builder.{stage}.checkpoint_every` 控制无子阶段阶段的 checkpoint 频率
- `classify`、`extract`、`align`、`infer` 是子阶段驱动阶段，checkpoint 配置在子阶段内
- LLM 子阶段使用 `provider`、`model`、`batch_size`、`concurrent_requests`、`request_timeout_seconds`、`max_retries`、`rate_limit`、`params`
- `params` 只能包含供应商 API 参数，不能包含本地控制参数，例如 `timeout_seconds`、`max_retries`、`rpm`、`tpm`

供应商适配：

- `provider=deepseek` 需要 `DEEPSEEK_API_KEY` 和 `DEEPSEEK_BASE_URL`
- `provider=siliconflow` 需要 `SILICONFLOW_API_KEY` 和 `SILICONFLOW_BASE_URL`
- `provider=bigmodel` 需要 `BIGMODEL_API_KEY`
- 适配层会读取环境变量，也会读取仓库根目录 `.env`

`interprets_filter` 配置：

- `dataset`: 样本量、类别权重、自适应采样、数据集切分和随机种子
- `distill`: LLM 蒸馏标签参数
- `train`: backbone、训练轮数、batch size、阈值选择等训练参数
- `predict`: 默认模型目录、最大长度和预测阈值
- `hub`: Hugging Face 模型和数据集仓库配置

## CLI 规格

### crawler

入口：

```bash
scripts/crawler --category 法律 --data-root data
```

行为：

- 默认同时运行 metadata 和 docs 两个阶段
- `--metadata` 仅运行 metadata
- `--docs` 仅运行 docs
- `--overwrite` 同时覆盖 metadata 和 docs
- `--limit` 限制每个分类处理数量

### builder

当前公开命令只有 `build`。

```bash
scripts/build --data-root data --source-id <source_id>
scripts/build --data-root data --category 法律
scripts/build --data-root data --all
scripts/builder build --data-root data --all
```

作用域参数三选一：

- `--source-id`: 一个或多个 metadata `source_id`
- `--category`: 一个或多个 metadata `category`
- `--all`: 全部 metadata

阶段参数：

- `--start` / `--start-stage`: 起始阶段
- `--end` / `--through-stage`: 结束阶段，默认 `infer`
- `--rebuild`: 强制重建选中作用域内的阶段，执行前要求输入 `yes`
- `--incremental`: 显式使用默认增量合并与跳过行为；当前代码中不附加额外行为

当前限制：

- `builder.cli` 未公开 `split-export` 命令
- `scripts/split_export` 当前会调用未接入的 CLI 命令，不应作为可用入口记录在使用文档中
- `split_graph_export()` 函数存在于 `src/builder/cli.py`，但需要后续接入 CLI 后才可作为正式功能使用

### interprets_filter

`scripts/interprets_filter` 固定调用 `interprets_filter.cli run`：

```bash
scripts/interprets_filter --stage all --data-root data --model-dir models/interprets_filter
scripts/interprets_filter --stage dataset --sample-size 1500 --data-root data
scripts/interprets_filter --stage train --data-root data --model-dir models/interprets_filter
```

单条预测必须直接调用 `predict` 子命令：

```bash
uv run --no-sync python -m interprets_filter.cli predict --text "依据[T]某法[T]制定。"
```

### model_assets

当前支持资产名 `interprets_filter`。

```bash
scripts/model_asset --download interprets_filter --model
scripts/model_asset --publish interprets_filter --dataset
```

## Builder 阶段总览

| 阶段 | 工作单元 | 主要输入 | 主要输出 |
| --- | --- | --- | --- |
| `normalize` | `source` | metadata、docs | `documents/{source_id}.json`、`normalize_index.json` |
| `structure` | `source` | normalize documents/index | `nodes.jsonl`、`edges.jsonl` |
| `detect` | `node` | structure graph | `candidates.jsonl` |
| `classify` | `candidate` | detect candidates、structure graph | `results.jsonl`、`pending.jsonl`、`llm_judgments.jsonl`、`edges.jsonl` |
| `extract` | `node` | classify graph | `inputs.jsonl`、`concepts.jsonl` |
| `aggregate` | `node` | extract concepts、extract inputs、classify graph | `concepts.jsonl` |
| `align` | `concept` | aggregate concepts、classify graph | `vectors.jsonl`、`pairs.jsonl`、`concepts.jsonl`、`relations.jsonl`、graph |
| `infer` | `concept` | align concepts/vectors/relations/graph | `pairs_*.jsonl`、`relations.jsonl`、graph |

阶段序列定义在 `src/builder/pipeline/orchestrator.py::STAGE_SEQUENCE`，目录映射定义在 `src/builder/io/paths.py::STAGE_OUTPUT_DIRS`。

## manifest 规格

### manifest 的职责

`data/manifest/builder/{stage}.json` 表示当前阶段累计正式产物的状态描述，而不是单次构建日志、单次 checkpoint 或运行摘要。

manifest 的职责：

- 描述当前磁盘上的正式产物路径
- 记录可复用的最小处理状态
- 记录可从当前正式产物直接反推的累计统计
- 为增量构建提供已处理工作单元集合

manifest 不负责：

- 记录单次运行耗时
- 记录单次失败摘要
- 记录 LLM 请求次数、错误次数、重试次数
- 记录本轮处理了多少 source、跳过了多少 source
- 记录 job 级图更新量

这些运行态信息必须写入 `logs/builder/{job_id}.json`。

### manifest 顶层字段

stage manifest 固定包含：

- `stage`: 阶段名
- `inputs`: 当前阶段声明的输入路径
- `artifacts`: 当前阶段声明的产物路径
- `updated_at`: UTC 更新时间
- `unit`: 当前阶段工作单元类型
- `stats`: 当前累计正式产物统计
- `metadata`: 复用判定、上游签名等辅助信息，可省略
- `processed_units`: 已完成并纳入累计状态的工作单元，可省略
- `substages`: 子阶段状态，可省略

有 `substages` 的阶段顶层不写 `processed_units`；处理状态写入对应子阶段。

### 工作单元

当前阶段工作单元映射：

- `normalize`: `source`
- `structure`: `source`
- `detect`: `node`
- `classify`: `candidate`
- `extract`: `node`
- `aggregate`: `node`
- `align`: `concept`
- `infer`: `concept`

当前子阶段工作单元映射：

- `classify.model`: `candidate`
- `classify.judge`: `candidate`
- `extract.input`: `source`
- `extract.extract`: `node`
- `align.embed`: `concept`
- `align.recall`: `concept`
- `align.judge`: `pair`
- `infer.pass_{n}`: `pass`
- `infer.pass_{n}.recall`: `concept`
- `infer.pass_{n}.judge`: `pair`

### stats 白名单

manifest `stats` 只能记录正式产物统计。以下运行态字段不得进入 stage 或 substage manifest `stats`：

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
- `input_count`

各阶段允许字段：

- `normalize`: `document_count`
- `structure`: `node_count`、`edge_count`、`node_type_counts`、`edge_type_counts`
- `detect`: `candidate_count`
- `classify`: `result_count`、`edge_count`、`edge_type_counts`、`interprets_count`、`references_count`、`ordinary_reference_count`、`judicial_interprets_count`、`judicial_references_count`
- `extract`: `result_count`、`concept_count`
- `aggregate`: `result_count`、`concept_count`、`core_concept_count`、`subordinate_concept_count`
- `align`: `concept_count`、`vector_count`、`pair_count`、`relation_count`、`node_count`、`edge_count`、`node_type_counts`、`edge_type_counts`
- `infer`: `pair_count`、`judgment_count`、`relation_count`、`accepted_count`、`node_count`、`edge_count`、`node_type_counts`、`edge_type_counts`

子阶段允许字段以 `src/builder/contracts/manifest.py::MANIFEST_SUBSTAGE_STAT_KEYS` 为准。`infer.pass_{n}` 动态子阶段允许 `pair_count`、`judgment_count`、`result_count`、`accepted_count`。

## job log 规格

job log 路径为 `logs/builder/{job_id}.json`。

job log 记录：

- `job_id`
- `build_target`
- `data_root`
- `status`
- `started_at` / `finished_at`
- `start_stage` / `end_stage`
- `source_count`
- `stages`
- `final_artifact_paths`
- `stats`

stage log 记录：

- 阶段运行状态
- artifact path
- failures
- error
- 运行态 stats
- 图阶段的本轮更新量和最终图规模

graph stage 完成后，job log 或 stage log 可记录：

- `node_count`
- `edge_count`
- `node_type_counts`
- `edge_type_counts`
- `updated_nodes`
- `updated_edges`

`updated_nodes` / `updated_edges` 表示本轮选中作用域相对于构建前正式图的真实变化量。

## 阶段边界规约

- 每个阶段只读取上一阶段已经落盘的正式产物
- 阶段业务规则必须放在 `src/builder/stages/{stage}/`
- `src/builder/pipeline/orchestrator.py` 负责阶段调度、作用域解析、日志写回、最终导出写回
- `src/builder/pipeline/handlers/` 负责阶段与通用 pipeline 能力之间的适配
- orchestrator 和 handler 不应承载阶段业务判断、业务补数或业务过滤
- 某阶段产物缺失时，下一阶段不能自行构造业务替代结果
- 空结果表示该工作单元已处理但无正式产物，状态进入 manifest，正式产物文件不写空业务记录

## 增量与复用规约

- 默认行为是复用已有 manifest 和正式产物，按选中作用域增量替换
- `--rebuild` 命中的处理单元不复用旧结果
- 单文档或单作用域 rebuild 时，只替换当前作用域内旧产物，作用域外累计产物保留
- 当前作用域内的新结果为空时，旧结果必须被删除，该单元仍可记录为 processed
- 复用判断必须同时考虑 manifest 状态和正式产物存在性
- 复用不改变正式产物语义，只影响运行进度和跳过逻辑
- 上游签名等复用辅助信息只能写入 `manifest.metadata`

## 图谱 schema 规约

图谱 schema 位于 `configs/schema.json`。

层级顺序：

1. `document`
2. `part`
3. `chapter`
4. `section`
5. `article`
6. `paragraph`
7. `item`
8. `sub_item`
9. `segment`
10. `appendix`
11. `concept`

节点类型：

- `DocumentNode`: `document`
- `TocNode`: `part`、`chapter`、`section`
- `ProvisionNode`: `article`、`paragraph`、`item`、`sub_item`、`segment`、`appendix`
- `ConceptNode`: `concept`

关系类型：

- `CONTAINS`
- `REFERENCES`
- `INTERPRETS`
- `MENTIONS`
- `RELATED_TO`
- `HAS_SUBORDINATE`

字段规约：

- `NodeRecord.to_dict()` 会根据节点类型过滤字段
- `DocumentNode` 保留 `id/type/name/level/category/status/issuer/publish_date/effective_date/source_url`
- `TocNode` 只保留 `id/type/name/level`
- `ProvisionNode` 只保留 `id/type/name/level/text`
- `ConceptNode` 只保留 `id/type/name/level/description`
- `EdgeRecord` 只保留 `id/source/target/type`
- 图写出前必须通过 `GraphBundle.validate_edge_references()`

新增节点层级、关系或字段时，必须同步：

- `configs/schema.json`
- `src/builder/contracts/graph.py` 的数据契约或字段处理
- 阶段物化逻辑
- README、guideline、data README

## 阶段详细规格

### 1. normalize

职责：

- 扫描 `data/source/metadata/*.json`
- 根据 metadata 查找对应 `DOCX`
- 读取段落、表格和自动编号文本
- 清理不可见字符、异常空白、目录残留、封面残留和尾部形式化落款
- 将每个 `source_id` 产出为独立逻辑文档

正式产物：

- `data/intermediate/builder/01_normalize/documents/{source_id}.json`
- `data/intermediate/builder/01_normalize/normalize_index.json`

`documents/{source_id}.json` 字段：

- `source_id`
- `title`
- `content`
- `appendix_lines`
- metadata 透传字段

manifest：

- `unit=source`
- `processed_units` 为完成 normalize 的 `source_id`
- `stats.document_count` 为当前累计文档数

### 2. structure

职责：

- 消费 normalize 文档和索引
- 构建 `DocumentNode`
- 解析目录、章节、条款、段落、附件等结构
- 生成 `CONTAINS` 边

正式产物：

- `data/intermediate/builder/02_structure/nodes.jsonl`
- `data/intermediate/builder/02_structure/edges.jsonl`

manifest：

- `unit=source`
- `processed_units` 为已纳入结构图的 `source_id`
- `stats` 只描述当前结构图规模和类型计数

### 3. detect

职责：

- 消费 structure 图
- 从节点文本中识别显式引用候选
- 识别法名、简称、自引用、相对条款引用和目标节点集合
- 本阶段不物化图关系边

正式产物：

- `data/intermediate/builder/03_detect/candidates.jsonl`

候选字段：

- `id`
- `source_node_id`
- `text`
- `target_node_ids`
- `target_categories`

manifest：

- `unit=node`
- `processed_units` 为已扫描结构节点 ID
- `stats.candidate_count` 为当前候选数量

### 4. classify

职责：

- 消费 detect 候选和 structure 图
- 使用 `interprets_filter` 对候选做解释关系二分类
- 根据高低置信阈值直接落判或进入待仲裁
- 对不确定样本调用 LLM 仲裁
- 物化 `REFERENCES` / `INTERPRETS` 边

子阶段：

- `model`: 模型与规则阶段
- `judge`: LLM 仲裁阶段

正式产物：

- `data/intermediate/builder/04_classify/results.jsonl`
- `data/intermediate/builder/04_classify/pending.jsonl`
- `data/intermediate/builder/04_classify/llm_judgments.jsonl`
- `data/intermediate/builder/04_classify/edges.jsonl`

`results.jsonl` 字段：

- `id`
- `source_node_id`
- `text`
- `target_node_ids`
- `target_categories`
- `label`
- `score`
- `source`

`pending.jsonl` 字段：

- `id`
- `source_node_id`
- `text`
- `target_node_ids`
- `target_categories`
- `source_category`
- `prediction_is_interprets`
- `prediction_score`
- `is_legislative_interpretation`

`llm_judgments.jsonl` 字段：

- `id`
- `source_id`
- `text`
- `label`
- `reason`

manifest：

- 顶层 `unit=candidate`
- 顶层不使用 `processed_units`
- `substages.model.processed_units` 为模型阶段完成的 candidate ID
- `substages.judge.processed_units` 为仲裁阶段完成的 candidate ID
- 顶层 `stats.edge_count` 和 `edge_type_counts` 描述当前 `04_classify/edges.jsonl`

### 5. extract

职责：

- 消费 classify 图快照
- 构建概念抽取输入
- 调用 LLM 抽取章节级概念数组

子阶段：

- `input`: 从图快照构建抽取输入
- `extract`: 对输入执行 LLM 概念抽取

正式产物：

- `data/intermediate/builder/05_extract/inputs.jsonl`
- `data/intermediate/builder/05_extract/concepts.jsonl`

`inputs.jsonl` 字段：

- `id`
- `hierarchy`
- `content`

`concepts.jsonl` 字段：

- `id`
- `concepts`

`concepts` item 字段：

- `name`
- `description`

规约：

- 空 `concepts` 不写入正式产物
- 仅完成 `input` 不表示完成整个 `extract` 阶段
- 后续阶段以已落盘的 `inputs.jsonl` 和 `concepts.jsonl` 为准

manifest：

- 顶层 `unit=node`
- 顶层不使用 `processed_units`
- `substages.input.unit=source`
- `substages.extract.unit=node`

### 6. aggregate

职责：

- 消费 `05_extract/concepts.jsonl`
- 用 `05_extract/inputs.jsonl` 为同 ID 补充 `hierarchy`、`content` 等上下文
- 合并同章节概念
- 区分核心概念和附属概念
- 收敛描述并展平为单概念记录

正式产物：

- `data/intermediate/builder/06_aggregate/concepts.jsonl`

字段：

- `id`
- `name`
- `description`
- `parent`
- `root`

规约：

- `root` 是原章节或结构节点 ID
- `parent` 是直属上级概念 ID，顶层概念为空字符串
- aggregate 输入以 `05_extract/concepts.jsonl` 为驱动集合，`05_extract/inputs.jsonl` 只补充上下文
- 空概念不写入正式产物，但对应输入单元可记为 processed
- checkpoint 和最终写盘都写当前累计正式产物

manifest：

- `unit=node`
- `processed_units` 为已完成 aggregate 的输入节点 ID
- `stats` 记录 result、concept、core concept、subordinate concept 数量

### 7. align

职责：

- 消费 `06_aggregate/concepts.jsonl` 与 classify 图快照
- 为 raw concept 生成 embedding
- 基于向量相似度召回候选概念对
- 使用 LLM 判断 `equivalent`、`is_subordinate`、`has_subordinate`、`related`、`none`
- 基于 equivalence 生成 canonical concept
- 物化 canonical `ConceptNode`、`MENTIONS`、`HAS_SUBORDINATE`、`RELATED_TO`

子阶段：

- `embed`
- `recall`
- `judge`

正式产物：

- `data/intermediate/builder/07_align/vectors.jsonl`
- `data/intermediate/builder/07_align/pairs.jsonl`
- `data/intermediate/builder/07_align/concepts.jsonl`
- `data/intermediate/builder/07_align/relations.jsonl`
- `data/intermediate/builder/07_align/nodes.jsonl`
- `data/intermediate/builder/07_align/edges.jsonl`

`vectors.jsonl` 字段：

- `id`
- `vector`

`pairs.jsonl` 字段：

- `left_id`
- `right_id`
- `relation`
- `similarity`

`concepts.jsonl` canonical 字段：

- `id`
- `name`
- `description`
- `member_ids`
- `root_ids`
- `representative_member_id`

`relations.jsonl` 字段：

- `left_id`
- `right_id`
- `relation`

manifest：

- 顶层 `unit=concept`
- 顶层不使用 `processed_units`
- `substages.embed.processed_units` 为完成 embedding 的 raw concept ID
- `substages.recall.processed_units` 为完成候选召回的 raw concept ID
- `substages.judge.processed_units` 为完成判别的 pair key
- 顶层图统计描述当前 `07_align/nodes.jsonl` 和 `07_align/edges.jsonl`

实现细节：

- `stage_artifacts()` 当前会把 `06_aggregate/concepts.jsonl` 也列入 align manifest `artifacts`，它是对齐基础输入；业务正式输出仍以 `07_align/` 目录下文件为准

### 8. infer

职责：

- 仅消费 `07_align` 正式产物
- 以 canonical concept 为预测对象
- 复用 `representative_member_id` 对应的 raw concept vector
- 按配置执行多轮 pass 召回
- 对候选 pair 调用 LLM 判别关系和强度
- 按 `min_strength` 接受关系，最终物化 infer 图快照

子阶段结构：

- `pass_1`
  - `recall`
  - `judge`
- `pass_2`
  - `recall`
  - `judge`
- pass 数量来自 `configs/config.json` 的 `builder.infer.recall.pass`

正式产物：

- `data/intermediate/builder/08_infer/pairs_1.jsonl`
- `data/intermediate/builder/08_infer/pairs_2.jsonl`
- `data/intermediate/builder/08_infer/relations.jsonl`
- `data/intermediate/builder/08_infer/nodes.jsonl`
- `data/intermediate/builder/08_infer/edges.jsonl`

`pairs_{n}.jsonl` 字段：

- `left_id`
- `right_id`
- `pass_index`
- `semantic_score`
- `aa_score`
- `ca_score`
- `bridge_score`
- `score`
- `relation`
- `strength`

`relations.jsonl` 字段：

- `left_id`
- `right_id`
- `relation`

规约：

- infer 当前没有 `candidates.jsonl` 或 `judgments.jsonl`
- 召回候选和判别结果合并保存在 `pairs_{n}.jsonl`
- `relation=none` 或 `strength < min_strength` 的 pair 不进入 `relations.jsonl`
- `related` 物化为 `RELATED_TO`
- `is_subordinate` / `has_subordinate` 物化为方向正确的 `HAS_SUBORDINATE`

manifest：

- 顶层 `unit=concept`
- 顶层不使用 `processed_units`
- `substages.pass_{n}.unit=pass`
- `substages.pass_{n}.substages.recall.unit=concept`
- `substages.pass_{n}.substages.judge.unit=pair`
- 顶层 `stats.pair_count` 是所有 pass pair 合计
- 顶层 `stats.judgment_count` 是已判别 pair 合计

## 中间产物接口矩阵

- `normalize -> structure`: `01_normalize/documents/`、`01_normalize/normalize_index.json`
- `structure -> detect`: `02_structure/nodes.jsonl`、`02_structure/edges.jsonl`
- `detect -> classify`: `03_detect/candidates.jsonl`、`02_structure` 图
- `classify -> extract`: `04_classify` 图快照
- `extract -> aggregate`: `05_extract/inputs.jsonl`、`05_extract/concepts.jsonl`、`04_classify` 图
- `aggregate -> align`: `06_aggregate/concepts.jsonl`、`04_classify` 图
- `align -> infer`: `07_align/concepts.jsonl`、`07_align/vectors.jsonl`、`07_align/relations.jsonl`、`07_align` 图

## 最终导出规约

当 `through_stage` 是图阶段时，orchestrator 会把当前图快照写入：

- `data/exports/json/nodes.jsonl`
- `data/exports/json/edges.jsonl`

图阶段包括：

- `structure`
- `classify`
- `align`
- `infer`

非图阶段结束时不应期望最终图导出更新。

## 公共组件边界

### orchestrator

职责：

- 构建 job id
- 解析阶段范围
- 解析 source scope
- 创建目录
- 调用阶段 handler
- 写 job log
- 写最终图导出
- 计算 job 级图更新量

不得承担：

- 阶段业务规则
- 业务候选过滤
- 概念聚合策略
- LLM prompt 或判别逻辑

### handler

职责：

- 把 pipeline 上下文转换为阶段输入
- 执行复用判断
- 执行增量替换
- 写阶段正式产物
- 构建并写入 manifest
- 更新 stage log

不得承担：

- 阶段核心算法
- 领域规则本身

### stages

职责：

- 实现阶段业务语义
- 定义 prompt、规则、召回、判别、物化等核心逻辑
- 返回可被 handler 写盘的 records、graph bundle 和 stats

### contracts

职责：

- 定义所有跨阶段 records
- 定义图 schema 加载与校验
- 定义 manifest、job log 的序列化规则
- 提供 stats 白名单过滤

### io

职责：

- 维护路径布局
- 读写 JSON/JSONL
- 将 dataclass record 与磁盘格式互转

## 文件写入规约

- JSON 使用 `ensure_ascii=False` 和缩进写入
- JSONL 每行一个 JSON object
- 写入正式产物时使用临时文件替换，避免半写入
- 正式产物文件只保存当前累计有效结果
- 长期脏残留不得作为阶段状态来源
- 中间产物可重建，源数据不可由 builder 随意删除

## 命名与 ID 规约

- `source_id` 来自 metadata
- 图节点 ID 应包含可反推 source 的 locator 语义
- `source_id_from_node_id()` 是从图节点回溯 source 的公共入口
- `ConceptNode` ID 使用 `concept:` 前缀
- edge ID 当前由 `build_edge_id()` 生成 UUID 风格 `edge:{uuid}`，不得依赖可读性或稳定派生语义
- pair key 在实现中应由左右 ID 和 pass/关系上下文稳定构造，避免重复判别

## 质量与验证规约

文档或实现改动后至少检查：

- README、guideline、data README 是否与 CLI、产物路径、阶段名一致
- `configs/schema.json` 是否与 `NodeRecord` / `EdgeRecord` 写出字段一致
- manifest stats 是否只包含白名单字段
- 新增阶段或产物是否加入 `BuildLayout`、`stage_inputs()`、`stage_artifacts()`
- 图阶段写出前是否能通过 schema 和边端点校验
- CLI 示例是否能被当前 argparse 接受

代码层建议验证命令：

```bash
uv run python -m builder.cli --help
uv run python -m crawler.cli --help
uv run python -m interprets_filter.cli --help
```

涉及运行阶段的改动应使用小作用域 source 或 `--limit` 先验证，再执行全量任务。
