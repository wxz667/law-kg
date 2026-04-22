# kg-build

`kg-build` 是统一主流水线项目，负责：

1. 文档导入 `ingest`
2. 结构切分 `segment`
3. 关系链接阶段 `link`

当前阶段不再承担 TreeKG/LLM 语义抽取逻辑，只保留法律文本结构化底座，并在 `link` 阶段调用外部 `kg-link` 子项目进行关系检测。最终产物导出由 pipeline 内部统一完成，不再作为独立阶段暴露。

## 当前流水线

```text
ingest -> segment -> link
```

## 运行方式

```bash
cd /home/zephyr/law-kg
PYTHONPATH=kg-build/src .venv/bin/python -m kg_build.cli build \
  --data-root data \
  --source law/中华人民共和国刑法.docx
```

也可以只跑到某一阶段：

```bash
PYTHONPATH=kg-build/src .venv/bin/python -m kg_build.cli build \
  --data-root data \
  --source law/中华人民共和国刑法.docx \
  --start segment \
  --end link
```

批量构建：

```bash
PYTHONPATH=kg-build/src .venv/bin/python -m kg_build.cli build-batch \
  --data-root data \
  --category law \
  --end segment
```

全量重跑且不复用缓存：

```bash
PYTHONPATH=kg-build/src .venv/bin/python -m kg_build.cli build-batch \
  --data-root data \
  --end segment \
  --rebuild
```

## 产物

- `data/intermediate/01_ingest/<scope>.source_document.json`
- `data/intermediate/02_segment/graph.bundle-0001.json`
- `data/intermediate/02_segment/graph.index.json`
- `data/intermediate/03_link/graph.bundle-0001.json`
- `data/intermediate/03_link/graph.index.json`
- `data/exports/json/graph.bundle-0001.json`
- `data/exports/json/graph.index.json`

## 说明

- `segment` 只负责结构节点和 `HAS_CHILD` 结构边。
- `DocumentNode` 顶层保留 `document_type / document_subtype / status`，其余文档说明字段放在 `metadata`。
- `document_subtype` 只在能明确判定细分类时填写；无法可靠判定时留空，不使用 `general` 之类的兜底值。
- `link` 当前已接入 `kg-link` 的占位预测接口，后续可替换为 DeepKE 推理。
- CLI 运行时只显示进度条和错误摘要，不再打印详细阶段日志。
- 单文档构建会显示当前阶段名；批处理按阶段推进，每个阶段只显示一条纯进度条。
- 导出不再作为独立阶段暴露，而是在 pipeline 收尾时统一完成。
- `build-batch` 默认从 `data/raw` 递归发现文档，也可以通过 `--category` 限定子目录。
- `build-batch` 按阶段批量执行：先全量跑完当前阶段，再进入下一阶段。
- 批处理结束时终端只输出总览统计；具体失败条目写入 `data/logs/kg-build/`。
- `segment` 与 `link` 的公开阶段产物会聚合为分片 bundle；单文档缓存与 manifests 迁入 `data/.cache/kg-build/`。
- 单文档从 `link` 起跑时，会优先从 `02_segment` 的聚合图索引中回查对应文档子图。
- `--start` / `--end` 是对外的阶段参数名称。
- `--rebuild` 会强制从 `ingest` 重跑，不复用旧缓存。
- 规范关系类型预留为：
  - `REFERS_TO`
  - `INTERPRETS`
  - `AMENDS`
  - `REPEALS`
