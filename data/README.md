# data

`data/` 是项目运行期数据根目录，存放源数据、builder 中间产物、阶段 manifest、`interprets_filter` 训练数据、最终 JSONL 图谱导出以及历史归档。

除 `source/` 外，本目录下大多数内容都可以按阶段重新生成。删除或覆盖任何数据前，应先确认当前任务是否依赖已有增量状态。

## 目录结构

```text
data/
  source/
    documents/
    metadata/
  intermediate/
    builder/
      01_normalize/
      02_structure/
      03_detect/
      04_classify/
      05_extract/
      06_aggregate/
      07_align/
      08_infer/
    interprets_filter/
  manifest/
    builder/
  train/
    interprets_filter/
  exports/
    json/
  archive/
```

## source

`source/` 是 crawler 的默认输出目录，也是本项目默认配置下的 builder 原始输入目录。crawler 实际写入的 metadata 和 DOCX 目录以 `configs/config.json` 中 `crawler.metadata_dir`、`crawler.document_dir` 为准，CLI 的 `--metadata-dir`、`--document-dir` 可覆盖它们。builder 实际读取的 metadata 和 DOCX 目录以 `builder.metadata`、`builder.document` 为准，它们可以独立于 `builder.data`。

### `source/metadata/`

存放 crawler 抓取或人工维护的 metadata 分片：

- 文件名通常为 `metadata-0001.json`、`metadata-0002.json`
- 每个文件内容是 JSON array
- 每个元素是一条文档 metadata
- 每个分片最多记录数由 `crawler.metadata_shard_size` 控制

常用字段：

- `source_id`
- `title`
- `issuer`
- `publish_date`
- `effective_date`
- `category`
- `status`
- `source_url`
- `source_format`

### `source/documents/`

存放原始规范性文件 `DOCX` 文档。

约定：

- 文件必须是合法 `.docx`
- 文件名通常来自文档标题
- 标题重复时 crawler 会追加 `__{source_id 后 12 位}` 后缀
- builder 以 metadata 的 `source_id` 为逻辑处理单位，不以文件名作为主键

## intermediate/builder

`intermediate/builder/` 是 builder 八阶段中间产物目录。阶段目录名由代码中的 `STAGE_OUTPUT_DIRS` 决定。

### `01_normalize/`

标准化阶段产物。

```text
01_normalize/
  documents/{source_id}.json
  index.json
```

`documents/{source_id}.json` 保存清洗后的逻辑文档正文、附件行和 metadata 透传字段。`index.json` 是 JSON array，数组项只包含 `source_id`、`title` 和生成文档文件名 `document`。

### `02_structure/`

结构图阶段产物。

```text
02_structure/
  nodes.jsonl
  edges.jsonl
```

包含 `DocumentNode`、`TocNode`、`ProvisionNode` 与 `CONTAINS` 边。

### `03_detect/`

显式引用候选阶段产物。

```text
03_detect/
  candidates.jsonl
```

每条候选包含来源节点、引用文本、候选目标节点和目标分类。

### `04_classify/`

显式关系分类阶段产物。

```text
04_classify/
  results.jsonl
  pending.jsonl
  llm_judgments.jsonl
  edges.jsonl
```

- `results.jsonl`: 模型、规则或 LLM 已完成判别的候选结果
- `pending.jsonl`: 需要 LLM 仲裁或仍待处理的候选
- `llm_judgments.jsonl`: LLM 仲裁明细
- `edges.jsonl`: 物化后的 `REFERENCES` / `INTERPRETS` 边

`classify` 图快照的节点会从上游结构图继承；该阶段自身只写关系边。

### `05_extract/`

概念抽取阶段产物。

```text
05_extract/
  inputs.jsonl
  concepts.jsonl
```

- `inputs.jsonl`: LLM 概念抽取输入，字段为 `id`、`hierarchy`、`content`
- `concepts.jsonl`: 抽取结果，字段为 `id`、`concepts`

空概念结果不写入 `concepts.jsonl`。

### `06_aggregate/`

章节内概念聚合阶段产物。

```text
06_aggregate/
  concepts.jsonl
```

字段：

- `id`
- `name`
- `description`
- `parent`
- `root`

`root` 指向原章节或结构节点，`parent` 指向直属上级概念；顶层概念的 `parent` 为空字符串。

### `07_align/`

跨章节 canonical 概念对齐阶段产物。

```text
07_align/
  vectors.jsonl
  pairs.jsonl
  concepts.jsonl
  relations.jsonl
  nodes.jsonl
  edges.jsonl
```

- `vectors.jsonl`: raw concept 向量
- `pairs.jsonl`: 对齐候选及判别关系
- `concepts.jsonl`: canonical concept 集合
- `relations.jsonl`: canonical 概念间关系
- `nodes.jsonl` / `edges.jsonl`: align 后图快照

align 会生成 `ConceptNode`、`MENTIONS`、`RELATED_TO`、`HAS_SUBORDINATE`，并保留上游结构和显式关系。

### `08_infer/`

canonical 概念层隐式关系推理阶段产物。

```text
08_infer/
  pairs_1.jsonl
  pairs_2.jsonl
  relations.jsonl
  nodes.jsonl
  edges.jsonl
```

- `pairs_{n}.jsonl`: 第 n 轮召回候选和 LLM 判别结果，包含 `relation` 与 `strength`
- `relations.jsonl`: 被接受的隐式关系
- `nodes.jsonl` / `edges.jsonl`: infer 后图快照

当前 infer 没有 `candidates.jsonl` 或 `judgments.jsonl`；候选与判别结果合并在 `pairs_{n}.jsonl`。

## intermediate/interprets_filter

`intermediate/interprets_filter/` 保存解释关系分类器训练前的数据蒸馏与采样中间状态。

常见文件：

- `adaptive_state.json`
- `distilled_detailed.jsonl`
- `quality_report.json`
- `review_samples.jsonl`

该目录可由 `scripts/interprets-filter --stage dataset --rebuild` 重建。

## manifest/builder

`manifest/builder/` 保存 builder 各阶段累计正式产物状态：

```text
manifest/builder/
  normalize.json
  structure.json
  detect.json
  classify.json
  extract.json
  aggregate.json
  align.json
  infer.json
```

manifest 用于阶段复用、增量构建和断点恢复。它只描述当前磁盘上的累计正式产物，不保存单次运行过程统计。单次运行日志在 `../logs/builder/{job_id}.json`。

## train/interprets_filter

`train/interprets_filter/` 保存解释关系分类器的数据集切分和训练输入，通常由 `scripts/interprets-filter --stage dataset` 生成。

该目录与 `models/interprets_filter/` 配合使用：

- `data/train/interprets_filter/`: 训练数据
- `models/interprets_filter/`: 训练后的模型

## exports

`exports/` 是脚本导出的当前图谱目录。

```text
exports/
  nodes.jsonl
  edges.jsonl
```

使用方式：

```bash
scripts/export --target data/exports
scripts/export --stage classify --target data/exports
```

未指定 `--stage` 时，`scripts/export` 会按 `infer -> align -> classify -> structure` 查找磁盘上最新可用图产物。图阶段包括：

- `structure`
- `classify`
- `align`
- `infer`

`detect` 使用 `structure` 图视角，`extract` 和 `aggregate` 使用 `classify` 图视角，`normalize` 不支持图导出。

## archive

`archive/` 存放历史中间产物、旧 manifest 或实验归档，不参与当前 builder 默认读取。

使用约定：

- 不要让当前阶段逻辑依赖 `archive/`
- 若需要恢复归档数据，应明确复制回对应正式目录
- 归档文件命名应能看出来源阶段或迁移背景

## 维护约定

- `source/` 是原始输入，应谨慎删除
- `intermediate/builder/` 是阶段正式中间产物，可按阶段重建
- `manifest/builder/` 与 `intermediate/builder/` 必须保持同一构建状态
- `exports/` 是当前导出的图快照，不是历史版本库
- `logs/` 不在 `data/` 内，builder 日志位于 `../logs/builder/`
- 文档中记录的文件名和阶段目录必须以 `src/builder/io/paths.py` 为准
