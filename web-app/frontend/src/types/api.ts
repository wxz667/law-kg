/**
 * API 错误类型
 */
export class ApiError extends Error {
    constructor(
        public status: number,
        public message: string,
        public code?: string
    ) {
        super(message);
        this.name = 'ApiError';
    }
}

/**
 * 通用 API 响应类型
 */
export interface ApiResponse<T = unknown> {
    data: T;
    status: number;
    message?: string;
}

/**
 * 法条节点输出类型
 */
export interface NodeOut {
    id: string;
    labels: string[];
    properties: Record<string, any>;
}

/**
 * 关系输出类型
 */
export interface RelationshipOut {
    type: string | null;
    start: string | null;
    end: string | null;
    properties: Record<string, any>;
}

/**
 * 邻居节点输出类型
 */
export interface NeighborOut {
    node: NodeOut;
    rel: RelationshipOut;
    other: NodeOut;
}

/**
 * 文书输出类型
 */
export interface DocumentOut {
    id: string;
    title: string;
    content: string;
    created_at: string;
    updated_at: string;
}

/**
 * 文书创建输入类型
 */
export interface DocumentCreateIn {
    title: string;
    content?: string;
}

/**
 * 文书更新输入类型
 */
export interface DocumentUpdateIn {
    title?: string;
    content?: string;
}

/**
 * 插入法条输入类型
 */
export interface InsertProvisionIn {
    provision_id: string;
    mode?: 'cursor' | 'append';  // "cursor": 光标处插入, "append": 文末附件插入
    law_name?: string;  // 法律名称
    article?: string;  // 条
    paragraph?: string;  // 款
    item?: string;  // 项
    content?: string;  // 法条完整内容或文档内容
}

/**
 * 搜索参数类型
 */
export interface SearchParams {
    q: string;
    field?: 'name' | 'text';
    limit?: number;
    [key: string]: string | number | undefined;
}

/**
 * 列表参数类型
 */
export interface ListParams {
    limit?: number;
    offset?: number;
    [key: string]: string | number | undefined;
}

// ==================== Elasticsearch 搜索相关类型 ====================

/**
 * 实体搜索请求参数
 */
export interface EntitySearchRequest {
    q: string;                          // 搜索关键词
    entity_types?: string[];            // 实体类型过滤（如 ["ProvisionNode", "DocumentNode"]）
    filters?: Record<string, any>;      // 其他过滤器
    limit?: number;                     // 返回数量限制
    offset?: number;                    // 偏移量
    highlight?: boolean;                // 是否高亮匹配内容
}

/**
 * 实体搜索结果项
 */
export interface EntitySearchResult {
    id: string;                         // 实体 ID
    type: string;                       // 实体类型，如 "ProvisionNode"
    name: string;                       // 实体名称
    score: number;                      // 相关性评分
    highlight?: Record<string, string[]>; // 高亮字段
    properties: Record<string, any>;    // 实体的其他属性
}

/**
 * 实体搜索响应
 */
export interface EntitySearchResponse {
    total: number;                      // 总结果数
    hits: EntitySearchResult[];         // 结果列表
    took: number;                       // 查询耗时（毫秒）
}

/**
 * 搜索建议请求
 */
export interface SuggestionRequest {
    query: string;                      // 输入的前缀
    size?: number;                      // 返回建议数量
}

/**
 * 搜索建议响应
 */
export interface SuggestionResponse {
    suggestions: string[];              // 建议列表
}

/**
 * 搜索统计信息
 */
export interface SearchStats {
    total_entities: number;             // 实体总数
    index_names: string[];              // 索引名称列表
    index_sizes: Record<string, number>; // 各索引的文档数
    es_enabled: boolean;                // ES 是否启用
    es_available: boolean;              // ES 是否可用
}

/**
 * 搜索服务健康状态
 */
export interface SearchHealth {
    ok: boolean;                        // 总体是否可用
    es_enabled: boolean;                // ES 功能是否启用
    es_available: boolean;              // ES 服务是否可连接
}

// ==================== 法律知识图谱相关类型 ====================

/**
 * 法律大纲输出类型
 */
export interface LawOutlineOut {
    source_id: string;                  // 法律源 ID
    title: string;                      // 法律标题
    issuer?: string | null;             // 发布机关
    publish_date?: string | null;       // 发布日期
    effective_date?: string | null;     // 生效日期
    category?: string | null;           // 法律类别
    status?: string | null;             // 法律状态
    has_structured_data: boolean;       // 是否有结构化数据
}

/**
 * 法律节点输出类型
 */
export interface LawNodeOut {
    id: string;                         // 节点 ID
    name: string;                       // 节点名称
    level: string;                      // 节点层级：document, chapter, section, article, paragraph, item, sub_item
    type: string;                       // 节点类型：TocNode, ProvisionNode
    text?: string | null;               // 法条文本内容（仅法条节点有）
    metadata: Record<string, any>;      // 元数据
}

/**
 * 法律详情输出类型
 */
export interface LawDetailOut {
    source_id: string;                  // 法律源 ID
    title: string;                      // 法律标题
    issuer?: string | null;             // 发布机关
    publish_date?: string | null;       // 发布日期
    effective_date?: string | null;     // 生效日期
    category?: string | null;           // 法律类别
    status?: string | null;             // 法律状态
    nodes: LawNodeOut[];                // 法律节点列表
}

/**
 * 法条查询输入类型
 */
export interface ProvisionQueryIn {
    law_source_id: string;              // 法律源 ID
    provision_number: string;           // 法条编号（如 "第一条"）
}

/**
 * 法条输出类型
 */
export interface ProvisionOut {
    source_id: string;                  // 法律源 ID
    provision_id: string;               // 法条 ID
    number: string;                     // 法条编号
    title?: string | null;              // 法条标题
    text?: string | null;               // 法条内容
    metadata?: Record<string, any>;     // 元数据
}
