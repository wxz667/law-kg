# 《中华人民共和国刑法》Tree-KG 项目实施方案与技术规范

## 1. 文档定位

本文档是本项目的正式实施方案与技术规范，用于统一以下内容：

- Tree-KG 论文事实在法律场景中的落地方式
- 图谱节点、边、字段与产物的数据契约
- 各阶段 pipeline 的职责、输入输出和禁止项
- 面向法律文本的检索、推理与扩展安全边界

本文档是后续实现 `summarize`、`extract`、`aggr`、`conv`、`embed`、`dedup`、`pred` 的上位规范。若当前工程实现与本文档不一致，以本文档为准逐步收敛。

## 2. 项目目标与设计原则

### 2.1 项目目标

本项目基于《中华人民共和国刑法》原始法条文本构建法律知识图谱，并将图谱以独立于图数据库的序列化 JSON 形式作为主产物存储。

规范主产物：

- [data/graph/graph.bundle.json](/home/zephyr/law-kg/data/graph/graph.bundle.json)

项目目标不是直接生成“自动解释法条”的系统，而是构建一个可审计、可回放、可版本化、可逐步扩展的法律知识图谱底座。

### 2.2 设计原则

- 原文优先：法条原文是唯一规范依据，任何派生字段不得替代原文
- 结构先行：先构建显式层级树，再扩展语义实体与隐藏关系
- 弱侵入扩展：语义增强只补充辅助信息，不改写结构事实
- 可追溯：新增实体、边、摘要、描述都必须能追溯到来源节点或证据
- 保守预测：预测边只表达潜在相关，不默认等同于法条明文规定
- 删除无前景字段：明确禁用且无合理使用前景的字段，不再保留为“预留字段”

## 3. Tree-KG 论文事实与法律场景映射

### 3.1 论文事实

Tree-KG 的核心方法分为两个阶段：

- Phase 1：从结构化文本构建显式 KG
- Phase 2：通过一组预定义算子迭代扩展 hidden KG

论文中的关键机制包括：

- 基于层级结构构建 tree-like hierarchical graph
- 对底层文本做摘要，并自底向上聚合为高层 TOC 摘要
- 抽取实体与显式关系
- 对实体做上下文卷积、聚合、向量化、去重、边预测

### 3.2 法律场景适配

本项目将论文中的“教材目录树”映射为“法律结构树”，将显式 KG 和 hidden KG 解释为：

- 显式 KG：法律文本的物理层级、附件结构、文本可直接举证的引用关系
- hidden KG：围绕法律实体形成的语义关系、聚合关系、候选预测关系

适配原则如下：

- 教材结构映射为法律结构：`编/章/节/条/款/项/目/附件/附件项`
- 对单段条文不强制补 `paragraph`
- 仅当条文存在真实多自然段结构时创建 `paragraph`
- 附件属于文档级附录结构，不并入正文 `paragraph/item/sub_item`
- `conv` 只增强实体语义，不增强法条规范效力
- `pred` 只补充候选语义连接，不得破坏法条边界

## 4. 输入、目录与交付规范

### 4.1 输入规范

唯一规范原始输入格式：

- `.docx`

唯一当前主数据源路径：

- [data/raw/statutes/中华人民共和国刑法.docx](/home/zephyr/law-kg/data/raw/statutes/中华人民共和国刑法.docx)

输入要求：

- 文件必须包含可提取的文字层
- 文件应尽量保留“编、章、节、条、款、项”等法典结构
- 原始数据只放置在 `data/raw/` 下

### 4.2 数据目录规范

原始数据目录：

- `data/raw/statutes/`
- `data/raw/judicial-interpretations/`
- `data/raw/cases/`
- `data/raw/annotations/`

中间产物目录：

- `data/intermediate/01_ingest/`
- `data/intermediate/02_segment/`
- `data/intermediate/03_summarize/`
- `data/intermediate/04_extract/`
- `data/intermediate/05_aggr/`
- `data/intermediate/06_conv/`
- `data/intermediate/07_embed/`
- `data/intermediate/08_dedup/`
- `data/intermediate/09_pred/`

阶段执行 sidecar：

- `checkpoint.json`
- `task_results.jsonl`

最终产物目录：

- `data/graph/graph.bundle.json`

构建记录：

- `data/manifest/build_manifest.json`

说明：

- `checkpoint.json` 与 `task_results.jsonl` 仅用于高成本 LLM 阶段的阶段内断点续跑
- 这两类文件不是规范主产物，阶段成功完成后应清理

### 4.3 主存储策略

本项目采用 `JSON-first` 策略。

原因如下：

- 单部法典规模适合以 JSON 完整承载节点与边
- 便于审计、重放、版本管理与阶段性回归
- 避免核心 pipeline 与 Neo4j 强耦合

Neo4j 属于后续可选下游，不属于当前核心构建流程主存储。

## 5. 图谱分层模型

### 5.1 图谱层次

本项目图谱分为三层：

- 结构层：文档、目录、法条、款项、附件等显式层级结构
- 语义层：法律实体、实体描述、显式语义边
- 候选层：预测边与弱语义连接

### 5.2 层间边界

- 结构层是主骨架，必须稳定、可定位、可引用
- 语义层只能附着在结构层之上，不得覆盖结构层
- 候选层默认不进入规范事实集合

### 5.3 规范事实集合

本项目中“规范事实集合”默认包含：

- 层级结构边 `HAS_*`
- 文本可直接举证的 `REFERENCE_TO`
- 经文本明确支持的 `CONDITION_OF`、`PENALTY_OF`、`EXCEPTION_TO`
- 经严格去重确认的 `SAME_AS`

不默认纳入规范事实集合的内容：

- `SECTION_RELATED`
- `ENTITY_RELATED`
- `pred` 阶段输出的预测边

## 6. 数据契约

### 6.1 `GraphBundle`

字段：

- `graph_id`
- `nodes`
- `edges`

职责：

- 描述最终交付图谱
- 不保留中间阶段上下文与路径引用

### 6.2 `EdgeRecord`

字段：

- `id`
- `source`
- `target`
- `type`
- `weight`
- `evidence`
- `metadata`

约束：

- 每条边必须引用现存节点
- 显式语义边必须带可回溯 `evidence`
- 预测边必须在 `metadata` 中显式标识预测属性与分数

### 6.3 `BuildManifest`

字段：

- `build_id`
- `source_path`
- `status`
- `started_at`
- `finished_at`
- `stages`
- `config_snapshot`
- `artifact_paths`

职责：

- 记录一次构建的配置、阶段状态、产物路径与错误信息
- 作为唯一允许保留中间产物路径引用的记录文件

## 7. 节点类型与字段规范

### 7.1 规范说明

本项目区分两层概念：

- 逻辑模型：每类节点允许出现哪些合法字段
- 工程兼容层：当前代码中若仍使用统一 dataclass，仅属于过渡实现，不构成规范许可

未被本类型允许的字段，不是“默认留空”，而是“规范上不应出现”。

被删除字段不属于当前项目的合法节点属性。若未来确有新需求，必须通过规范修订显式引入，不得在实现中自行恢复。

### 7.2 公共基础字段

所有节点共享以下基础字段：

- `id`
- `type`
- `name`
- `level`
- `source_id`
- `metadata`

字段职责：

- `id`：稳定结构定位标识，不包含原文内容
- `type`：节点类型
- `name`：人类可读引用或命名
- `level`：节点所处层级
- `source_id`：来源文档标识
- `metadata`：补充信息与实现细节，不承载主规范事实

### 7.3 类型专属字段

- `text`
  结构文本类字段，用于承载规范原文或附录正文
- `summary`
  聚合摘要类字段，用于承载非叶子结构节点的聚合语义表示
- `description`
  实体语义描述字段，用于实体定义与上下文增强
- `embedding_ref`
  向量引用字段，用于实体级向量存储定位
- `address`
  法律定位字段，用于结构节点的层级地址

唯一归属如下：

- `text`：`ProvisionNode`、`AppendixNode`、`AppendixItemNode`
- `summary`：`TocNode`、`ProvisionNode`、`AppendixNode`、可选 `EntityNode`
- `description`：仅 `EntityNode`
- `embedding_ref`：仅 `EntityNode`
- `address`：`TocNode`、`ProvisionNode`、`AppendixNode`、`AppendixItemNode`

### 7.4 节点类型定义

#### `DocumentNode`

合法字段：

- `id`
- `type`
- `name`
- `level`
- `source_id`
- `metadata`

禁止字段：

- `text`
- `summary`
- `description`
- `embedding_ref`
- `address`

用途：

- 表示源文档根节点
- 承载来源路径、校验和、立法沿革等文档级背景信息

#### `TocNode`

合法字段：

- `id`
- `type`
- `name`
- `level`
- `source_id`
- `summary`
- `address`
- `metadata`

禁止字段：

- `text`
- `description`
- `embedding_ref`

用途：

- 表示编、章、节等目录层节点
- 承载自底向上的主题聚合摘要
- 不承载正文，不参与默认卷积和向量化

#### `ProvisionNode`

合法字段：

- `id`
- `type`
- `name`
- `level`
- `source_id`
- `text`
- `summary`
- `address`
- `metadata`

禁止字段：

- `description`
- `embedding_ref`

用途：

- 表示条、款、项、目等规范单元
- `text` 是唯一规范依据
- `summary` 仅用于非叶子规范节点的聚合语义表示，不要求所有 `ProvisionNode` 都生成

正文承载规则：

- `article` 仅在未形成真实 `paragraph` 层级时承载正文或项前导语
- `paragraph` 在存在真实多自然段时承载该自然段正文或项前导语
- `item` 承载本项正文或目前导语
- `sub_item` 承载本目正文
- 不对所有单段条文统一补出 `paragraph`

#### `EntityNode`

合法字段：

- `id`
- `type`
- `name`
- `level`
- `source_id`
- `description`
- `embedding_ref`
- `metadata`

可选字段：

- `summary`

禁止字段：

- `text`
- `address`

用途：

- 表示法律概念、主体、行为、对象、条件、处罚要素等实体
- `description` 是实体语义中心字段
- `embedding_ref` 仅用于实体向量定位

#### `AppendixNode`

合法字段：

- `id`
- `type`
- `name`
- `level`
- `source_id`
- `text`
- `summary`
- `address`
- `metadata`

禁止字段：

- `description`
- `embedding_ref`

用途：

- 表示附件根节点
- 承载附件说明导语和可选摘要

#### `AppendixItemNode`

合法字段：

- `id`
- `type`
- `name`
- `level`
- `source_id`
- `text`
- `address`
- `metadata`

可选字段：

- `summary`

禁止字段：

- `description`
- `embedding_ref`

用途：

- 表示附件中的清单项或明细项
- 承载显式附录文本单元

### 7.5 非法字段规则

- 任何 stage 若输出了本类型未定义字段，视为违反数据契约
- 非法字段不得以“先写入、后忽略”的方式存在于最终图谱
- 新增字段必须先修订本文档，再修改实现

## 8. 节点标识与地址规范

### 8.1 `id` 规则

- `id` 只用于结构定位，不包含原文内容
- `id` 采用最小结构地址形式

示例：

- `article:<source_id>:0385`
- `appendix:<source_id>:01`
- `paragraph:<source_id>:0385:01`
- `item:<source_id>:0385:01:01`
- `sub_item:<source_id>:0385:01:01:01`
- `appendix_item:<source_id>:01:01`

### 8.2 `address` 规则

`address` 只属于结构节点。

正文结构节点的 `address` 应包含：

- `article_no`
- `article_suffix`
- `paragraph_no`
- `item_no`
- `sub_item_no`
- `appendix_no`
- `appendix_item_no`

目录节点的 `address` 应包含：

- `part_no`
- `chapter_no`
- `section_no`
- `level_marker`

`EntityNode` 与 `DocumentNode` 不得声明 `address`。

## 9. 边类型与语义边界

### 9.1 节点类型

- `DocumentNode`
- `TocNode`
- `ProvisionNode`
- `EntityNode`
- `AppendixNode`
- `AppendixItemNode`

### 9.2 结构边

- `HAS_PART`
- `HAS_CHAPTER`
- `HAS_SECTION`
- `HAS_ARTICLE`
- `HAS_APPENDIX`
- `HAS_PARAGRAPH`
- `HAS_ITEM`
- `HAS_APPENDIX_ITEM`
- `HAS_SUB_ITEM`

规则：

- 结构边只表达归属、层级或聚合关系
- 结构边不表达法律效果

### 9.3 语义边

- `HAS_ENTITY`
- `HAS_SUBORDINATE`
- `SECTION_RELATED`
- `ENTITY_RELATED`
- `CONDITION_OF`
- `PENALTY_OF`
- `EXCEPTION_TO`
- `REFERENCE_TO`
- `SAME_AS`

规则：

- `REFERENCE_TO` 可作为规范事实边
- `CONDITION_OF`、`PENALTY_OF`、`EXCEPTION_TO` 仅在文本证据明确时进入规范事实集合
- `SECTION_RELATED`、`ENTITY_RELATED` 属于弱语义边
- `SAME_AS` 仅用于确认同一实体，不用于上下位合并

### 9.4 预测边规则

`pred` 输出的边必须满足以下要求：

- 在 `metadata` 中标识 `is_predicted = true`
- 保留 `score`、阈值、模型信息和来源证据
- 默认不视为规范事实
- 面向用户输出时必须标识为“推测相关”或“候选相关”

### 9.5 附件建模规则

- `附件一/附件二` 作为 `AppendixNode` 挂在 `DocumentNode` 下
- 附件中的 `1.`、`2.` 等清单作为 `AppendixItemNode` 挂在对应附件下
- 第四百五十二条正文中的附件引用保留在 `ProvisionNode.text` 中，并通过 `REFERENCE_TO` 指向对应附件
- 附件不复用 `paragraph/item/sub_item` 层级

## 10. Pipeline 实施方案

核心 pipeline：

`ingest -> segment -> summarize -> extract -> aggr -> conv -> embed -> dedup -> pred -> serialize`

### 10.1 `ingest`

职责：

- 读取原始 `docx`
- 生成标准化 `source_document.json`

输入：

- 原始法律文档

输出：

- 标准化源文档记录

禁止项：

- 不做结构树构建
- 不做语义抽取
- 不做节点字段推断

### 10.2 `segment`

职责：

- 构建显式层级树
- 生成目录节点、条款节点、附件节点
- 生成结构边和附件引用边

输入：

- `source_document.json`

输出：

- 结构层 `GraphBundle`

允许写入：

- `DocumentNode.metadata`
- `TocNode.address`
- `ProvisionNode.text/address`
- `AppendixNode.text/address`
- `AppendixItemNode.text/address`

禁止项：

- 不新增 `EntityNode`
- 不写 `description`
- 不写 `embedding_ref`
- 不将推理结果写成法律效果边

### 10.3 `summarize`

职责：

- 只为语义聚合节点生成 `summary`
- 对 `TocNode`、有规范子节点的 `ProvisionNode`、有子项的 `AppendixNode` 生成聚合摘要
- 采用自底向上聚合方式生成高层目录摘要
- 叶子法条节点默认不做自由自然语言摘要

输入：

- `segment` 产物

输出：

- 带摘要的结构层 `GraphBundle`

允许写入：

- `summary`

禁止项：

- 不对叶子法条做原文改写式摘要
- 不写 `description`
- 不写 `embedding_ref`
- 不新增实体和关系

执行约束：

- 允许阶段内批处理与并发，但必须保持 bottom-up 依赖顺序
- `summarize` 只能采用分层批并发：层内并发、层间串行
- 父节点不得与其依赖的子聚合节点混在同一执行批次
- 若启用断点续跑，必须先回放已完成任务结果，再继续上层聚合

### 10.4 `extract`

职责：

- 以语义叶子节点原文为主、以上层聚合摘要为辅抽取 `EntityNode`
- 新增 `HAS_ENTITY`
- 在文本证据充分时生成显式语义边

输入：

- `summarize` 产物

输入规则：

- 语义叶子节点优先使用 `text`
- 父级 `summary` 与章节 `summary` 仅作为短文本上下文补充
- 不将聚合 `summary` 当作与叶子原文等价的主抽取语料
- `REFERENCE_TO` 优先通过规则解析当前原文中的显式引用来生成，不依赖模型自由生成目标节点

输出：

- 带实体和显式语义边的 `GraphBundle`

允许写入：

- `EntityNode.description`
- 可选 `EntityNode.summary`
- `HAS_ENTITY`
- 明确证据支持的 `REFERENCE_TO`、`CONDITION_OF`、`PENALTY_OF`、`EXCEPTION_TO`、`ENTITY_RELATED`

禁止项：

- 不给 `ProvisionNode` 增加 `description`
- 不给 `EntityNode` 写 `text/address`
- 不输出无证据强关系
- 不允许使用截断原文或推测性文本伪造 `evidence`
- `evidence` 必须是当前叶子节点原文中的精确子串；若不能精确定位，则不入图

执行约束：

- `extract` 可采用独立任务批并发，因为语义叶子节点之间默认相互独立
- 阶段执行参数如 `batch_size`、`concurrency` 属于运行参数，只能由执行器消费，不得透传给模型 SDK
- 若启用断点续跑，必须按任务粒度恢复，不得伪造半成品 `GraphBundle`

### 10.5 `aggr`

职责：

- 在实体层区分核心实体与非核心实体
- 将可聚合的实体关系转成 `HAS_SUBORDINATE`

输入：

- `extract` 产物

输出：

- 带实体层聚合结构的 `GraphBundle`

允许写入：

- `HAS_SUBORDINATE`
- 与实体角色相关的 `metadata`

禁止项：

- 不修改条文结构
- 不新增法条级语义描述字段

### 10.6 `conv`

职责：

- 结合邻域实体上下文更新 `EntityNode.description`

输入：

- `aggr` 产物

输出：

- 带实体增强描述的 `GraphBundle`

允许写入：

- `EntityNode.description`

禁止项：

- 不修改 `ProvisionNode.text`
- 不向 `TocNode` 写 `description`
- 不向 `AppendixNode` 或 `AppendixItemNode` 写 `description`
- 不把卷积结果视为法条解释或规范依据

### 10.7 `embed`

职责：

- 仅对 `EntityNode.description` 生成向量
- 写入 `embedding_ref`
- 将向量独立存放在 `data/intermediate/07_embed/embeddings.jsonl`

输入：

- `conv` 产物

输出：

- 带 `embedding_ref` 的实体节点图谱
- 独立向量 sidecar

允许写入：

- `EntityNode.embedding_ref`

禁止项：

- 不处理 `TocNode`
- 不处理 `ProvisionNode`
- 不在 `graph.bundle.json` 中内嵌高维向量数组

### 10.8 `dedup`

职责：

- 在实体层执行对齐与去重
- 优先新增 `SAME_AS` 软对齐关系

输入：

- `embed` 产物

输出：

- 带实体对齐结果的 `GraphBundle`

允许写入：

- `SAME_AS`
- 去重决策相关 `metadata`

禁止项：

- 不激进删点
- 不跨概念强行合并

### 10.9 `pred`

职责：

- 围绕实体层及弱语义边生成候选预测连接

输入：

- `dedup` 产物

输出：

- 带候选预测边的 `GraphBundle`

允许写入：

- 标注为预测的弱语义边

禁止项：

- 不直接在条文节点之间制造无文本证据的强法律效果边
- 不把预测边默认纳入规范事实集合

### 10.10 `serialize`

职责：

- 校验节点引用与边引用完整性
- 输出最终交付图谱与序列化状态文件

输入：

- 上游图谱 bundle

输出：

- `data/graph/graph.bundle.json`
- `data/graph/serialize_result.json`

禁止项：

- 不附带中间产物路径
- 不输出非法字段

## 11. 检索与问答安全约束

### 11.1 结果优先级

检索排序默认遵循以下优先级：

- 原文直接命中
- 显式语义边支持
- 弱语义边支持
- 候选预测边支持

### 11.2 派生字段边界

- `ProvisionNode.text` 是唯一规范依据
- `summary` 只用于聚合、导航、抽取辅助和检索召回，不替代叶子节点原文
- `description` 只用于实体语义增强
- `description` 不得替代法条解释

### 11.4 统一语义输入规则

- 语义叶子节点后续阶段优先使用 `text`
- 语义聚合节点后续阶段优先使用 `summary`
- 语义聚合节点可同时保留前导文本，但其主语义输入仍为 `summary`
- 后续阶段不得假定所有节点都存在 `summary`

### 11.3 用户输出边界

- 来自原文或显式证据的结论可表述为“法条规定”
- 来自弱语义边或预测边的结论只能表述为“相关”“可能相关”“候选相关”

## 12. 真实结构示例

### 12.1 多自然段条文示例：第六条

节点演进：

- `article:...:0006`
- `paragraph:...:0006:01`
- `paragraph:...:0006:02`
- `paragraph:...:0006:03`

边演进：

- `article -> paragraph` 通过 `HAS_PARAGRAPH`

示例节点形态：

```json
{
  "id": "paragraph:statutes中华人民共和国刑法:0006:01",
  "type": "ProvisionNode",
  "name": "第六条第一款",
  "level": "paragraph",
  "source_id": "statutes:中华人民共和国刑法",
  "text": "凡在中华人民共和国领域内犯罪的，除法律有特别规定的以外，都适用本法。",
  "summary": "",
  "address": {
    "article_no": 6,
    "article_suffix": null,
    "paragraph_no": 1,
    "item_no": null,
    "sub_item_no": null,
    "appendix_no": null,
    "appendix_item_no": null
  },
  "metadata": {
    "parent_article_id": "article:statutes中华人民共和国刑法:0006"
  }
}
```

说明：

- `ProvisionNode` 保留 `text/summary/address`
- 不出现 `description/embedding_ref`

### 12.2 列举型条文示例：第三十三条

节点演进：

- `article:...:0033`
- `paragraph:...:0033:01`
- `item:...:0033:01:01`
- `item:...:0033:01:02`
- `item:...:0033:01:03`

边演进：

- `article -> paragraph` 通过 `HAS_PARAGRAPH`
- `paragraph -> item` 通过 `HAS_ITEM`

示例节点形态：

```json
{
  "id": "item:statutes中华人民共和国刑法:0033:01:01",
  "type": "ProvisionNode",
  "name": "第三十三条第一款第一项",
  "level": "item",
  "source_id": "statutes:中华人民共和国刑法",
  "text": "管制；",
  "summary": "",
  "address": {
    "article_no": 33,
    "article_suffix": null,
    "paragraph_no": 1,
    "item_no": 1,
    "sub_item_no": null,
    "appendix_no": null,
    "appendix_item_no": null
  },
  "metadata": {
    "parent_paragraph_id": "paragraph:statutes中华人民共和国刑法:0033:01",
    "item_marker": "（一）"
  }
}
```

### 12.3 正文与附件联动示例：第四百五十二条

节点演进：

- `paragraph:...:0452:02`
- `appendix:...:01`
- `paragraph:...:0452:03`
- `appendix:...:02`

边演进：

- `paragraph -> appendix` 通过 `REFERENCE_TO`

示例边形态：

```json
{
  "id": "edge:reference_to:paragraphstatutes中华人民共和国刑法045202:appendixstatutes中华人民共和国刑法01",
  "source": "paragraph:statutes中华人民共和国刑法:0452:02",
  "target": "appendix:statutes中华人民共和国刑法:01",
  "type": "REFERENCE_TO",
  "weight": 1.0,
  "evidence": [
    {
      "text": "列于本法附件一的全国人民代表大会常务委员会制定的条例、补充规定和决定，已纳入本法或者已不适用，自本法施行之日起，予以废止。",
      "appendix_label": "附件一"
    }
  ],
  "metadata": {}
}
```

### 12.4 目录节点示例：`TocNode`

```json
{
  "id": "chapter:statutes中华人民共和国刑法:01",
  "type": "TocNode",
  "name": "第一章 刑法的任务、基本原则和适用范围",
  "level": "chapter",
  "source_id": "statutes:中华人民共和国刑法",
  "summary": "本章概述刑法任务、罪刑法定、适用范围等基本规则。",
  "address": {
    "part_no": 1,
    "chapter_no": 1,
    "section_no": null,
    "level_marker": "chapter"
  },
  "metadata": {
    "ordinal": 1
  }
}
```

说明：

- `TocNode` 不出现 `text/description/embedding_ref`

### 12.5 实体节点示例：`EntityNode`

```json
{
  "id": "entity:statutes中华人民共和国刑法:00385:国家工作人员",
  "type": "EntityNode",
  "name": "国家工作人员",
  "level": "entity",
  "source_id": "statutes:中华人民共和国刑法",
  "description": "刑法贪污贿赂相关条文中的主体概念，指在相关条文语境下承担特定职务身份的行为主体。",
  "embedding_ref": "embed/entity/国家工作人员",
  "metadata": {
    "source_node_ids": [
      "article:statutes中华人民共和国刑法:0385"
    ]
  }
}
```

说明：

- `EntityNode` 不出现 `text/address`
- `description` 与 `embedding_ref` 只属于实体节点

## 13. TODO 模块规范

当前保留为稳定接口但尚未完成正式实现的阶段：

- `summarize`
- `extract`
- `aggr`
- `conv`
- `embed`
- `dedup`
- `pred`

约束：

- 必须保留明确职责、输入输出契约与稳定路径
- 不得使用演示性质、样例专用逻辑冒充正式实现
- 即使为占位实现，也必须参与完整 pipeline，以保持产物链路闭合

向量存储规范：

- `graph.bundle.json` 中只保存 `EntityNode.embedding_ref`
- 实际向量独立存放在 `data/intermediate/07_embed/embeddings.jsonl`
- 不在主图中内嵌高维向量数组

## 14. 源码与构建入口

核心源码位于：

- [kg-build/src/kg_build](/home/zephyr/law-kg/kg-build/src/kg_build)

源码分层：

- `contracts/`
- `config/`
- `io/`
- `llm/`
- `pipeline/`
- `stages/`
- `cli.py`

静态资源位于：

- [kg-build/resources](/home/zephyr/law-kg/kg-build/resources)

唯一公开构建命令：

```bash
cd /home/zephyr/law-kg/kg-build
PYTHONPATH=src /home/zephyr/law-kg/.venv/bin/python -m kg_build.cli build \
  --source /home/zephyr/law-kg/data/raw/statutes/中华人民共和国刑法.docx \
  --data-root /home/zephyr/law-kg/data
```

该命令应完成：

- 原始输入读取
- 显式层级图构建
- 各语义阶段顺序落盘
- 图谱 JSON 主产物生成
- 构建 manifest 生成

## 15. 外部配置与可选下游

外部配置项包括：

- `kg-build/resources/models.json`
- `BIGMODEL_API_KEY`
- `DEEPSEEK_API_KEY`
- `DEEPSEEK_BASE_URL`
- `NEO4J_URI`
- `NEO4J_USER`
- `NEO4J_PASSWORD`

配置模板：

- [kg-build/resources/models.json](/home/zephyr/law-kg/kg-build/resources/models.json)
- [.env.example](/home/zephyr/law-kg/.env.example)

说明：

- `kg-build/resources/models.json` 只定义阶段到 `provider/model/purpose/params` 的路由
- LLM 厂商接入参数由 `.env` 提供，并且只允许 `infra/llm` 读取
- `kg_build/llm` 不得直接读取 `.env`，也不得持有 `api_key`、`base_url` 等厂商接入字段
- `kg_build/llm/` 负责阶段模型路由与算法侧调用 facade
- 与算法强相关的 prompt、任务语义、结果校验应放在对应 `stages/` 模块内
- `infra/llm/` 负责明确厂商的 SDK 或 API 适配，不承载任何阶段语义
- 厂商接入参数应由对应厂商包各自读取和校验；通用层只负责 provider 路由，不统一假定所有厂商都需要相同参数
- provider 命名必须使用明确厂商或平台语义，如 `bigmodel`、`deepseek`、`openrouter`；不得使用 `openai_compatible` 这类笼统兼容名
- 系统不提供隐式 fallback 或降级策略；阶段路由缺失、厂商接入参数缺失时必须立即报错退出
- `.env.example` 保留基础设施参数模板与厂商级 LLM 接入变量模板，不暴露阶段级变量
- Neo4j 仅用于未来导入、展示与产品交付

可选下游：

- `infra/llm/`
- `infra/neo4j/`

核心 pipeline 不依赖 Neo4j。

## 16. 验收要求

### 16.1 字段归属验收

- 每种节点都有明确字段清单
- 被删除字段不再以“预留”或“可留空”名义出现
- `description` 和 `embedding_ref` 的唯一主体是 `EntityNode`

### 16.2 Stage 权限验收

- 每个阶段只写被允许写的字段
- `conv` 只更新实体描述
- `embed` 只生成实体向量引用

### 16.3 示例一致性验收

- 文档内所有 JSON 示例遵循本规范字段集合
- 不出现旧式全字段节点样例
- 第六条、第三十三条、第四百五十二条示例与本规范一致

### 16.4 契约违规判定

以下情况视为违反规范：

- 某节点输出了其未定义的字段
- `ProvisionNode` 出现 `description` 或 `embedding_ref`
- `TocNode` 出现 `text`、`description` 或 `embedding_ref`
- `EntityNode` 出现 `text` 或 `address`
- `pred` 直接写入无证据的强法律效果边并作为规范事实使用
