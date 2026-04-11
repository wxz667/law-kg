# law-kg

## 项目简介

`law-kg` 是一个面向中文规范性文件的知识图谱构建项目。项目从结构化元数据与原始 `DOCX` 文档中提取文书结构、显式关系、实体与隐式关系特征，并以 JSONL 图产物形式输出，供后续检索、分析和图数据库导入使用。

项目采用统一 `src/` 源码目录组织，`src/builder/` 为主构建流水线，`src/interprets_filter/`、`src/ner/`、`src/rgcn/` 为相关训练与推理模块。

## 功能范围

项目当前包含以下功能：

- 规范性文件标准化清洗与逻辑文书切分
- 文书目录层级、条款层级与正文片段的结构图构建
- 显式引用候选筛选、目标定位与关系分类
- 实体抽取、实体对齐与概念节点构建
- 隐式关系特征生成与推理
- 图谱产物拆分导出，用于 Neo4j 与 Elasticsearch 等下游系统
- `interprets_filter` 模型的数据集构建、训练、预测与模型资产管理

## Builder 流水线

主构建流程按以下七个阶段执行：

1. `normalize`
2. `structure`
3. `reference_filter`
4. `relation_classify`
5. `entity_extraction`
6. `entity_alignment`
7. `implicit_reasoning`

各阶段职责如下：

- `normalize`
  读取 `data/source/metadata/*.json` 与 `data/source/docs/*.docx`，完成逻辑文书切分、正文清洗与标准化索引生成。
- `structure`
  基于标准化结果构建文档节点、目录节点、条款节点与结构边。
- `reference_filter`
  在文档文本中识别显式引用，生成候选引用与目标节点映射。
- `relation_classify`
  对候选引用执行规则修正、模型判别与可选的 LLM 仲裁，输出 `REFERENCES` 与 `INTERPRETS` 边。
- `entity_extraction`
  从文本节点中抽取实体与概念候选。
- `entity_alignment`
  对概念候选进行归并和对齐，形成统一概念节点。
- `implicit_reasoning`
  生成隐式关系推理特征并补充推理边。

## 图谱产物

项目图谱产物采用分阶段 JSONL 形式输出，不直接在构建阶段写入数据库。

### 节点

- 文档节点类型：`DocumentNode`
- 目录节点类型：`TocNode`
- 条款节点类型：`ProvisionNode`
- 概念节点类型：`ConceptNode`

### 边

- 结构边：`CONTAINS`
- 语义边：`REFERENCES`、`INTERPRETS`、`MENTIONS`

### 正式产物形式

图阶段目录内按需输出：

- `nodes.jsonl`
- `edges.jsonl`

阶段状态与增量构建信息写入：

- `data/manifest/builder/{stage_name}.json`

运行日志写入：

- `logs/builder/{job_id}.json`

## 目录结构

### 源码目录

- `src/builder/`：主构建流水线
- `src/interprets_filter/`：解释关系分类数据集、训练与预测模块
- `src/ner/`：NER 数据集、训练与预测模块
- `src/rgcn/`：隐式关系推理数据集、训练与预测模块
- `src/crawler/`：采集与原始数据整理模块
- `utils/`：仓库级公共组件与外部接口适配层

### 配置与说明

- `configs/config.json`：项目运行配置
- `configs/schema.json`：图谱结构 schema
- `guideline.md`：项目实施规范

### 数据目录

- `data/source/docs/`：原始 `DOCX` 文档
- `data/source/metadata/`：元数据清单
- `data/intermediate/builder/`：builder 分阶段中间产物
- `data/manifest/builder/`：builder 阶段状态快照
- `data/train/interprets_filter/`：`interprets_filter` 训练数据集
- `data/exports/json/`：最终图谱导出

## 运行方式

### 单文档构建

```bash
scripts/build \
  --data-root data \
  --source-id 2c909fdd678bf17901678bf5aba10073
```

### 指定阶段范围构建

```bash
scripts/build \
  --data-root data \
  --source-id 2c909fdd678bf17901678bf5aba10073 \
  --start normalize \
  --end relation_classify
```

### 批量构建

```bash
scripts/build-batch \
  --data-root data
```

### 统一 CLI 入口

```bash
scripts/builder build-batch --data-root data
```

### 图谱拆分导出

```bash
scripts/split_export \
  --graph data/exports/json \
  --output-root data/exports/import
```

