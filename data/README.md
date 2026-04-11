# data

## 目录说明

`data/` 为项目运行期数据根目录，存放源数据、中间产物、阶段状态、训练数据与最终导出结果。

## 目录结构

### 源数据

- `source/docs/`
  原始规范性文件 `DOCX` 文档。
- `source/metadata/`
  与源文档对应的元数据清单。

### Builder 中间产物

- `intermediate/builder/01_normalize/`
  标准化阶段产物，包括逻辑文书清洗结果与阶段索引。
- `intermediate/builder/02_structure/`
  结构图阶段产物，包括结构节点与结构边。
- `intermediate/builder/03_reference_filter/`
  显式引用候选筛选阶段产物。
- `intermediate/builder/04_relation_classify/`
  显式关系分类阶段产物，包括关系分类结果、LLM 仲裁记录与关系边。
- `intermediate/builder/05_entity_extraction/`
  实体抽取阶段产物。
- `intermediate/builder/06_entity_alignment/`
  实体对齐阶段产物。
- `intermediate/builder/07_implicit_reasoning/`
  隐式关系推理阶段产物。

图相关阶段正式产物采用 JSONL 文件组织，按阶段目录写入：

- `nodes.jsonl`
- `edges.jsonl`

### 阶段状态

- `manifest/builder/`
  builder 各阶段的固定覆盖式状态文件，用于阶段复用与增量构建判断。

### 训练数据

- `train/interprets_filter/`
  `interprets_filter` 模型的数据集切分与训练输入。

### 最终导出

- `exports/json/`
  最终图谱导出目录。

## 使用约定

- 构建阶段不直接写数据库，所有正式产物先写入本目录
- `manifest/builder/` 中的文件为阶段状态快照，不保留历史版本
- `intermediate/builder/` 为运行期中间产物目录，支持重建与覆盖
