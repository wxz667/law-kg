# law-kg

`law-kg` 现在采用统一 `src/` 源码目录，并以 `src/builder/` 作为唯一构建主包。

## 当前架构

构建期从 `data/source/metadata/*.json` 与 `data/source/docs/*.docx` 读取输入，不直接写数据库。主流水线按 7 个阶段推进：

1. `normalize`
2. `sturctre`
3. `reference_filter`
4. `relation_classify`
5. `entity_extraction`
6. `entity_alignment`
7. `implicit_reasoning`

`normalize` 阶段产出逐文档清洗结果与阶段索引；`sturctre` 产出统一 `graph_bundle-*.json`。`reference_filter` 与 `relation_classify` 输出候选工件和关系计划工件，不直接写图；最终导出时再将关系计划物化回图中。结构边当前只保留 `CONTAINS`。

注意：第一阶段要求输入是真实的 Office Open XML `.docx` 包。如果文件只是后缀名为 `.docx`、实际内容仍是老 `.doc` 复合文档，builder 会明确报错并要求先转换。第一阶段现在先把一个物理 `.docx` 拆成一个或多个逻辑文书，再分别做正文结构解析。逻辑文书标题优先来自正文显式标题块，而不是引用书名号；通知、公告、请示等壳文默认不进图，批复、答复、复函正文保留。`第一条` 仍然优先按正式条文解析；当正文只有 `一、`、`（一）`、`1.` 这类特殊层级时，builder 会先寻找候补 `article` 锚点，再向上回推候补 `chapter` / `section`，而不是默认把 `一、` 直接当作条文。只有完全未命中模板时才退化为单个 `正文` 节点。

## 目录结构

- `src/builder/`: 七阶段构建流水线
- `src/interprets_filter/`: 司法解释解释关系筛选数据集、训练与推理模块
- `src/ner/`: NER 数据集、训练与推理模块
- `src/rgcn/`: 隐式关系推理数据集、训练与推理模块
- `src/crawler/`: 采集与原始数据整理模块
- `configs/schema.json`: 图谱 schema
- `guideline.md`: 项目实施指南
- `data/raw/`: 原始 `DOCX + metadata`
- `data/intermediate/01_normalize/`: 逐文档清洗结果与阶段索引
- `data/intermediate/02_sturctre/`: 结构图谱 graph bundle
- `data/intermediate/03_reference_filter/`
- `data/intermediate/04_relation_classify/`
- `data/intermediate/05_entity_extraction/`
- `data/intermediate/06_entity_alignment/`
- `data/intermediate/07_implicit_reasoning/`
- `logs/builder/`: builder 运行日志、manifest 与 normalize 报告
- `data/exports/json/`: 最终 graph bundle 导出
- `data/exports/import/`: 供 Neo4j / Elasticsearch 导入的 JSONL
- `data/models/`: 本地模型工件目录

## 模型与数据集发布

仓库不再跟踪 `models/interprets_filter/` 下的训练模型与 checkpoint。本地训练产物建议发布到 Hugging Face：

- 模型仓库：保存最终推理所需文件，如 `model.safetensors`、`config.json`、`tokenizer.json`、`tokenizer_config.json`、`label_map.json`、`metrics.json`
- 数据集仓库：保存 `data/train/interprets_filter/` 下的数据集切分与说明文件

在 [configs/config.json](/home/zephyr/law-kg/configs/config.json) 的 `interprets_filter.hub` 中填写：

```json
{
  "model_repo_id": "your-name/your-interprets-filter-model",
  "model_revision": "main",
  "dataset_repo_id": "your-name/your-interprets-filter-dataset",
  "dataset_revision": "main"
}
```

下载模型和数据集：

```bash
scripts/interprets_filter_download
```

也可以只下载模型：

```bash
scripts/interprets_filter_download --skip-dataset
```

发布到 Hugging Face 的推荐流程：

```bash
hf auth login
hf repo create your-name/your-interprets-filter-model
hf repo create your-name/your-interprets-filter-dataset --repo-type dataset

hf upload your-name/your-interprets-filter-model models/interprets_filter . \
  --exclude "checkpoints/**" \
  --exclude "optimizer.pt" \
  --exclude "scheduler.pt" \
  --exclude "rng_state.pth" \
  --exclude "trainer_state.json" \
  --exclude "training_args.bin"

hf upload your-name/your-interprets-filter-dataset data/train/interprets_filter . --repo-type dataset
```

## 运行方式

单文件构建：

```bash
scripts/build_graph \
  --data-root data \
  --source-id 2c909fdd678bf17901678bf5aba10073
```

只跑到某一阶段：

```bash
scripts/build_graph \
  --data-root data \
  --source-id 2c909fdd678bf17901678bf5aba10073 \
  --start normalize \
  --end entity_alignment
```

批量构建：

```bash
scripts/build_batch \
  --data-root data \
  --category law
```

将最终图谱拆分为导入文件：

```bash
scripts/split_export \
  --graph data/exports/json/law__中华人民共和国刑法/graph_bundle-0001.json \
  --output-root data/exports/import/law__中华人民共和国刑法
```

统一 CLI 入口也可直接使用：

```bash
scripts/builder build-batch --data-root data --category law
```

## crawler 到 builder 的数据路径

## 当前实现边界

- `builder` 只负责构图，不负责训练，不负责数据库导入
- `entity_alignment` 不单独训练模型，直接使用预训练向量召回 + 内部判别逻辑
- `interprets_filter`、`ner`、`rgcn` 提供独立的数据集构建、训练和推理入口
- 所有阶段最终以 JSON graph bundle 为准，数据库导入通过拆分脚本完成
