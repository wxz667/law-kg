# 项目指南：法律文本结构化底座

## 当前目标

当前项目只聚焦于法律文本的结构化处理，不做前件/后件语义拆分，不做 LLM 驱动的摘要、实体抽取或语义增强。

当前保留的主流程为：

1. `ingest`
2. `segment`
3. `link`

目标产物是可供后续 DeepKE 三元组关系抽取与关系回写消费的稳定结构图。最终导出由 pipeline 内部统一完成，不再视作独立阶段。

## CLI 约定

- `kg-build` 统一以 `data/` 作为根目录输入
- 原始文档统一从 `data/raw/` 下发现
- 单文件构建时，`--source` 使用相对于 `data/raw/` 的路径
- 批量构建时，不再传入 `source-root`，而是通过：
  - `--data-root`
  - `--category`
  - `--glob`
  控制发现范围
- 可以通过：
  - `--start`
  - `--end`
  - `--rebuild`
  指定处理区间
- 导出在 pipeline 收尾统一执行，不作为单独阶段暴露
- 单文档构建显示阶段名；批量构建按阶段推进，每个阶段只显示一条纯进度条
- 命令行只显示进度条和总览统计；批处理错误详情写入 `data/logs/kg-build/`

## 文档分类

原始数据当前主要按以下类别组织：

- `constitution`
- `law`
- `regulation`
- `interpretation`

解释类文件不在顶层继续区分“法律解释/司法解释”，其性质通过 `document_subtype` 细分；没有可明确判定的细分类时，`document_subtype` 置空。

- `constitution`: `amendment / decision`
- `law`: `amendment / decision`
- `regulation`: `administrative / supervisory / local / decision`
- `interpretation`: `legislative / judicial / decision`

## 结构切分规则

### 标准结构优先

优先识别并切分：

- 编
- 章
- 节
- 条
- 款
- 项
- 目
- 附件

### 前言与公告前置

- `preface` 统一保存在 `DocumentNode.metadata.preface_text`
- 不为 `preface` 单独建节点
- 对“公告开头 + 解释正文”的解释类文件：
  - 公告部分进入 `preface`
  - 正文从第一个 `第X条` 开始正常切分

### 非标准结构退化

当文本不能可靠识别为标准条文结构时，统一切分为 `segment` 类型的 `ProvisionNode`。

规则：

- 不强求所有文档都具备完整的编章节目层级
- 有则建，无则跳过
- 无法细分时按连续段落合并成 `segment`
- 公文式文本（通知、公告、函、答复、批复、意见等）正文整体作为单个 `segment`
- `segment` 仅在标准正文回落场景下继续承接 `item/sub_item`
- 公文式正文与附件 `segment` 不再继续向下拆 `item/sub_item`

## 当前图谱范围

当前 schema 只保留：

- `DocumentNode`
- `TocNode`
- `ProvisionNode`
- `AppendixNode`

以及结构边：

- `HAS_CHILD`

同时为未来关系抽取预留规范关系类型：

- `REFERS_TO`
- `INTERPRETS`
- `AMENDS`
- `REPEALS`

这些关系当前阶段只在 schema 中保留，不在本轮自动构建。

## 文档元数据

`DocumentNode` 当前正式属性：

- `document_type`
- `document_subtype`
- `status`

`DocumentNode.metadata` 当前最小集合：

- `issuer`
- `publish_date`
- `effective_date`
- `issuer_type`
- `doc_no`
- `region`
- `preface_text`

当前阶段不自动做废止、修订、解释关系解析。

## Link 阶段与外部子项目

`kg-build` 仍然是统一主流水线，`link` 作为第三阶段加入：

1. 从结构图中生成关系抽取样本
2. 调用外部 `kg-link` 子项目的接口
3. 读取关系预测结果
4. 将未来的 `REFERS_TO / INTERPRETS / AMENDS / REPEALS` 回写到图谱中

`segment` 与 `link` 的公开阶段结果统一聚合为：

- `graph.bundle-0001.json`
- `graph.bundle-0002.json`
- ...
- `graph.index.json`
- Internal build caches and manifests live under `data/.cache/kg-build/`, not in public stage output directories.

单文档目录只保留为内部缓存，以支持增量重跑。

`kg-link` 是单独子项目，负责：

- 训练数据构建
- 高阶 LLM 辅助标注
- DeepKE 训练与微调
- DeepKE 推理接口

## 后续路线

后续引用关系的主路线为：

1. 基于结构图做文档级 / 条文级候选范围定位
2. 通过 `kg-link` 对接 DeepKE / 预训练关系抽取模型生成三元组
3. 将 `(条文/文档, 关系, 条文/文档)` 映射为：
   - `REFERS_TO`
   - `INTERPRETS`
   - `AMENDS`
   - `REPEALS`

当前阶段不复用旧的 TreeKG、前件后件、conv/aggr、embedding 路线。
