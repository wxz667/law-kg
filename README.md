# law-kg

`law-kg` 是一个面向中文规范性文件的法律知识图谱构建项目。项目从国家法律法规数据库抓取元数据与 `DOCX` 文档，经过标准化、结构解析、显式关系识别、概念抽取、概念对齐和隐式关系推理，最终输出可供图数据库、搜索系统或下游分析使用的 JSONL 图谱产物。

当前代码主线以 `src/builder/` 为核心，`src/crawler/` 负责数据采集，`src/interprets_filter/` 负责解释关系判别模型的数据集、训练和预测，`utils/` 承载 LLM 供应商与模型资产等公共适配层。

## 功能范围

- 从国家法律法规数据库抓取法律、行政法规、监察法规、地方法规、司法解释等分类的元数据与 `DOCX` 文档
- 将源文档标准化为稳定的逻辑文档记录，并清理目录、封面、异常空白、尾部形式化落款等噪声
- 解析文档、编、章、节、条、款、项、目、段落、附件等结构层级，输出结构图
- 检测显式法条引用候选，并结合规则、解释关系分类模型与可选 LLM 仲裁生成 `REFERENCES` / `INTERPRETS` 边
- 基于图快照构建章节级概念抽取输入，使用 LLM 抽取并聚合概念
- 对 raw 概念执行 embedding、召回、LLM 判别和 canonical 对齐，生成 `ConceptNode`、`MENTIONS`、`RELATED_TO`、`HAS_SUBORDINATE`
- 在 canonical 概念图上执行多轮隐式关系召回和判别，补充最终概念关系
- 为 `interprets_filter` 模型构建训练数据、蒸馏标签、训练分类器，并管理 Hugging Face 模型与数据集资产

## 架构概览

```text
src/
  crawler/             # FLK 元数据与 DOCX 文档采集
  builder/             # 八阶段知识图谱构建主流水线
    contracts/         # 图、manifest、阶段产物的数据契约
    io/                # 路径布局、JSON/JSONL 读写、产物 store
    pipeline/          # orchestrator、增量复用、阶段 handler、进度输出
    stages/            # normalize/structure/detect/classify/extract/aggregate/align/infer
    utils/             # ID、locator、数字、引用与布局等 builder 内部工具
  interprets_filter/   # 解释关系分类模型的数据集、训练、预测
utils/
  llm/                 # deepseek / siliconflow / bigmodel 统一适配
  model_assets/        # 模型与训练数据资产下载、发布
configs/
  config.json          # 运行配置、LLM 参数、训练参数
  schema.json          # 图谱节点、边、层级 schema
data/
  source/              # 源元数据与 DOCX
  intermediate/        # 中间产物
  manifest/            # 阶段累计状态
  train/               # 训练数据
  exports/             # 导出的图谱 JSONL
logs/
  builder/             # builder 单次运行日志
  crawler/             # crawler 运行日志
  interprets_filter/   # 训练/蒸馏摘要
```

更细的实施规范见 [guideline.md](guideline.md)，数据目录规范见 [data/README.md](data/README.md)。

## 环境准备

项目使用 Python 3.10+，依赖由 `uv` 管理。

```bash
uv sync
```

涉及 LLM 或 embedding 的阶段需要在环境变量或仓库根目录 `.env` 中配置供应商凭据。当前适配层支持：

```bash
SILICONFLOW_API_KEY=...
SILICONFLOW_BASE_URL=...

DEEPSEEK_API_KEY=...
DEEPSEEK_BASE_URL=...

BIGMODEL_API_KEY=...
```

`configs/config.json` 当前默认在 `classify.judge`、`extract.extract`、`aggregate`、`align.judge`、`infer.judge` 使用 `siliconflow` 的 `Pro/deepseek-ai/DeepSeek-V3.2`，在 `align.embed` 使用 `BAAI/bge-m3`。

## 数据采集

采集入口为 `scripts/crawler`，默认同时执行元数据抓取与文档下载。

```bash
scripts/crawler --category 法律 --data-root data
```

常用参数：

- `--category`: 一个或多个分类名，或 `all`，支持 `宪法`、`法律`、`行政法规`、`监察法规`、`地方法规`、`司法解释`
- `--category-except`: 从选中分类中排除一个或多个分类，metadata 和 document 阶段都生效
- `--metadata`: 仅抓取元数据
- `--document`: 仅下载文档
- `--status`: document 阶段只处理指定状态，支持 `现行有效`、`已修改`、`已废止`、`尚未生效`
- `--status-except`: document 阶段排除指定状态
- `--overwrite`: 覆盖已存在的元数据和文档
- `--limit`: 每个分类最多处理的记录数
- `--data-root`: 覆盖 `crawler.data_root`，并在未显式传目录参数时使用 `{data-root}/source/metadata` 与 `{data-root}/source/documents`
- `--base-url`: 覆盖 `crawler.base_url`
- `--metadata-dir`、`--document-dir`: 覆盖 `configs/config.json` 中的 crawler 输出目录
- `--metadata-shard-size`: 每个 metadata 分片最多存储的记录数
- `--concurrency`、`--retries`、`--timeout`、`--request-delay`、`--request-jitter`、`--warmup-timeout`: 覆盖 `configs/config.json` 中的网络并发、节流与重试参数

采集产物写入：

- `crawler.metadata_dir` 指向的 `metadata-*.json`
- `crawler.document_dir` 指向的 `*.docx`
- `data/manifest/crawler/documents.json` 记录已下载 DOCX 的 `source_id` 清单、分类统计和状态统计
- `logs/crawler/`

`crawler` 会按标题生成合法文件名；标题重复时追加 `source_id` 后缀。元数据按标题去重，保留发布日期、生效日期和 `source_id` 排序更靠后的记录。文档下载阶段通过 `documents.json` 复用已有 DOCX；进度条总量仍按当前分类 metadata 候选总数显示，已在 manifest 中的文档计入 skipped。

## Builder 流水线

builder 的输入/产物路径由 `configs/config.json` 的 `builder` 顶层配置控制：

- `data`: builder 中间产物、manifest 和默认导出的数据根目录
- `metadata`: normalize 阶段读取的 metadata JSON 目录
- `document`: normalize 阶段读取的原始 DOCX 目录

`data` 与原始输入目录相互独立；CLI 的 `--data-root` 只覆盖 `builder.data`，不会改写 `metadata` 或 `document`。

builder 当前固定为八个阶段：

1. `normalize`
2. `structure`
3. `detect`
4. `classify`
5. `extract`
6. `aggregate`
7. `align`
8. `infer`

阶段职责：

- `normalize`: 读取 `builder.metadata` 与 `builder.document` 指向的原始输入，生成清洗后的逻辑文档和 normalize 索引
- `structure`: 构建 `DocumentNode`、`TocNode`、`ProvisionNode` 与 `CONTAINS` 结构边
- `detect`: 从结构节点文本中扫描显式引用候选
- `classify`: 对候选引用进行模型判别、规则修正和可选 LLM 仲裁，生成显式关系边
- `extract`: 基于 `classify` 图快照构建概念抽取输入，并调用 LLM 抽取概念
- `aggregate`: 合并章节内概念，区分核心概念与附属概念，展平为单概念记录
- `align`: 对 raw 概念进行 embedding、候选召回、LLM 判别和 canonical 对齐，生成概念图快照
- `infer`: 在 canonical 概念图上两轮召回与判别，补充隐式 `RELATED_TO` / `HAS_SUBORDINATE`

## 构建用法

统一入口是 `builder.cli` 的 `build` 命令，脚本 `scripts/build` 会自动传入该命令。

按单个或多个 `source_id` 构建：

```bash
scripts/build \
  --data-root data \
  --source-id 021e7d7684474107b8f3febbb1c4f8b5
```

按分类构建：

```bash
scripts/build \
  --data-root data \
  --category 法律
```

按状态构建：

```bash
scripts/build \
  --data-root data \
  --status 现行有效
```

排除指定类别或状态后构建：

```bash
scripts/build \
  --data-root data \
  --category-except 地方法规 \
  --status-except 已废止
```

构建全部已发现元数据：

```bash
scripts/build \
  --data-root data \
  --all
```

指定阶段范围：

```bash
scripts/build \
  --data-root data \
  --source-id 021e7d7684474107b8f3febbb1c4f8b5 \
  --start normalize \
  --end classify
```

强制重建选中作用域内的阶段：

```bash
scripts/build \
  --data-root data \
  --category 法律 \
  --start extract \
  --end infer \
  --rebuild
```

`--rebuild` 会要求输入 `yes` 后继续。`--incremental` 是当前默认增量合并与复用行为的显式开关，代码中不附加额外语义。

构建目标有三种互斥模式：`--source-id` 精确指定文档、`--all` 全量构建、或使用 metadata 过滤参数。过滤参数包括 `--category`、`--category-except`、`--status`、`--status-except`，其中 `--category` 和 `--status` 是包含范围，`--category-except` 和 `--status-except` 是排除范围。`--status` 可选值为 `现行有效`、`已修改`、`已废止`、`尚未生效`；`status: null` 或非标准状态不会被 `--status` 命中，但会在 `--status-except` 未明确排除时保留。

也可以使用底层入口：

```bash
scripts/builder build --data-root data --all
```

从已有阶段产物导出当前可用图谱，不进入构建流程：

```bash
scripts/export --stage infer --target data/exports
scripts/export --target data/exports
```

未传 `--stage` 时会按 `infer -> align -> classify -> structure` 选择磁盘上最新可用图产物。导出结果写入 `{target}/nodes.jsonl` 与 `{target}/edges.jsonl`。

脚本入口的完整说明见 [scripts/README.md](scripts/README.md)。

## 图谱 schema

图谱 schema 位于 `configs/schema.json`，当前节点类型为：

- `DocumentNode`: `level=document`，保留 `category`、`status`、`issuer`、`publish_date`、`effective_date`、`source_url`
- `TocNode`: `level=part/chapter/section`
- `ProvisionNode`: `level=article/paragraph/item/sub_item/segment/appendix`，保留正文 `text`
- `ConceptNode`: `level=concept`，保留 `description`

当前关系类型为：

- `CONTAINS`
- `REFERENCES`
- `INTERPRETS`
- `MENTIONS`
- `RELATED_TO`
- `HAS_SUBORDINATE`

所有节点和边在写出时都会按 schema 校验。稀疏字段不会落盘；不在该节点类型 `fields` 中的字段即使对象上存在也不会写入 JSONL。

## 主要产物

builder 不在构建阶段写数据库，正式产物均为 JSON 或 JSONL。

```text
data/intermediate/builder/01_normalize/
  documents/{source_id}.json
  index.json

data/intermediate/builder/02_structure/
  nodes.jsonl
  edges.jsonl

data/intermediate/builder/03_detect/
  candidates.jsonl

data/intermediate/builder/04_classify/
  results.jsonl
  pending.jsonl
  llm_judgments.jsonl
  edges.jsonl

data/intermediate/builder/05_extract/
  inputs.jsonl
  concepts.jsonl

data/intermediate/builder/06_aggregate/
  concepts.jsonl

data/intermediate/builder/07_align/
  concepts.jsonl
  vectors.jsonl
  pairs.jsonl
  relations.jsonl
  nodes.jsonl
  edges.jsonl

data/intermediate/builder/08_infer/
  pairs_1.jsonl
  pairs_2.jsonl
  relations.jsonl
  nodes.jsonl
  edges.jsonl

data/exports/
  nodes.jsonl
  edges.jsonl
```

`data/manifest/builder/{stage}.json` 描述阶段累计正式产物与可复用状态；`logs/builder/{job_id}.json` 记录单次运行过程、失败、跳过、请求次数和最终图统计。

## interprets_filter

`interprets_filter` 是 `classify.model` 使用的解释关系分类器。它从 `03_detect/candidates.jsonl` 构建候选样本，使用 LLM 蒸馏标签，训练二分类模型，并在 builder 中通过 `interprets_filter.api:predict_interprets` 调用。

构建数据集并训练：

```bash
scripts/interprets-filter \
  --stage all \
  --data-root data \
  --model-dir models/interprets_filter
```

仅构建数据集：

```bash
scripts/interprets-filter --stage dataset --sample-size 1500 --data-root data
```

仅训练：

```bash
scripts/interprets-filter --stage train --data-root data --model-dir models/interprets_filter
```

单条预测需要直接调用 CLI 的 `predict` 子命令：

```bash
uv run --no-sync python -m interprets_filter.cli predict \
  --text "本规定依据[T]中华人民共和国网络安全法[T]制定。"
```

训练数据写入 `data/train/interprets_filter/`，中间蒸馏状态写入 `data/intermediate/interprets_filter/`，模型默认写入 `models/interprets_filter/`。

## 模型资产

模型和训练数据资产入口为 `scripts/model-asset`，当前支持资产名 `interprets_filter`。

```bash
scripts/model-asset --download interprets_filter --model
scripts/model-asset --publish interprets_filter --dataset
```

远端仓库、revision、默认本地目录来自 `configs/config.json` 的 `interprets_filter.hub` 与 `interprets_filter.predict.default_model_dir`。

## 开发约定

- 文档、规范与代码不一致时，以代码实现为准更新文档
- 阶段业务逻辑放在 `src/builder/stages/{stage}/`，调度、复用、checkpoint 和 manifest 写回放在 `src/builder/pipeline/`
- `manifest` 只描述当前磁盘累计正式产物和最小恢复状态，单次运行统计属于 `logs/builder/{job_id}.json`
- 图谱 schema 的新增节点、边或字段必须同步更新 `configs/schema.json`、contracts 和文档
- 中间产物默认允许重建与覆盖，源数据和训练产物需要按任务明确保留策略
