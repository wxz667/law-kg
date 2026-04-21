# scripts

`scripts/` 放置项目常用命令入口。脚本会通过 `uv` 调用对应 Python module，建议从仓库根目录运行。

## `build`

构建法律知识图谱。该脚本固定调用 `builder.cli build`。

```bash
scripts/build --data-root data --source-id <source_id>
scripts/build --data-root data --category 法律
scripts/build --data-root data --all
scripts/build --data-root data --all --start structure --end infer
```

常用参数：

- `--source-id`: 构建一个或多个 metadata source_id
- `--category`: 构建一个或多个 metadata category
- `--all`: 构建全部 metadata
- `--start` / `--end`: 指定阶段范围
- `--rebuild`: 对选中作用域强制重建

## `builder`

builder 原始 CLI 入口。需要直接访问子命令时使用。

```bash
scripts/builder build --data-root data --all
scripts/builder export --stage infer --target data/exports
```

## `export`

从已有 builder 阶段产物导出图 JSONL，不运行构建流程。

```bash
scripts/export --target data/exports
scripts/export --stage classify --target data/exports
```

参数：

- `--data-root`: 数据根目录，默认 `data`
- `--stage`: 指定阶段视角；未传时按 `infer -> align -> classify -> structure` 选择最新可用图
- `--target`: 导出目录，默认 `data/exports`

输出：

- `{target}/nodes.jsonl`
- `{target}/edges.jsonl`

阶段视角：

- `structure`: 导出结构图
- `detect`: 导出 `structure` 图
- `classify`: 导出显式关系图
- `extract` / `aggregate`: 导出 `classify` 图
- `align`: 导出概念对齐图
- `infer`: 导出隐式关系补全图

`normalize` 不产生图，不能导出。

## `crawler`

抓取国家法律法规数据库元数据和 DOCX 文档。

```bash
scripts/crawler --category 法律 --data-root data
scripts/crawler --category all --metadata
scripts/crawler --category 法律 --document --limit 100
```

常用参数：

- `--category`: 分类名或 `all`
- `--metadata`: 只抓取 metadata
- `--document`: 只下载文档
- `--overwrite`: 覆盖已有 metadata 和文档
- `--limit`: 限制每个分类处理数量
- `--data-root`: 覆盖 `crawler.data_root` 和默认 source 输出目录
- `--base-url`: 覆盖 `crawler.base_url`
- `--metadata-dir` / `--document-dir`: 覆盖 `configs/config.json` 的 crawler 输出目录
- `--metadata-shard-size`: 每个 metadata 分片最多存储的记录数
- `--concurrency` / `--retries` / `--timeout`: 覆盖 crawler 网络参数
- `--request-delay` / `--request-jitter` / `--warmup-timeout`: 覆盖 crawler 请求节流和预热参数

## `interprets-filter`

运行解释关系分类器的数据集构建与训练流程。该脚本固定调用 `interprets_filter.cli run`。

```bash
scripts/interprets-filter --stage all --data-root data --model-dir models/interprets_filter
scripts/interprets-filter --stage dataset --sample-size 1500 --data-root data
scripts/interprets-filter --stage train --data-root data --model-dir models/interprets_filter
```

单条预测使用底层 CLI：

```bash
uv run --no-sync python -m interprets_filter.cli predict --text "依据[T]某法[T]制定。"
```

## `model-asset`

下载或发布模型与训练数据资产。当前支持资产名 `interprets_filter`。

```bash
scripts/model-asset --download interprets_filter --model
scripts/model-asset --download interprets_filter --dataset
scripts/model-asset --publish interprets_filter --model
```

远端仓库配置来自 `configs/config.json` 的 `interprets_filter.hub`。
