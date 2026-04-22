import { ApiClient } from './api';
import type {
    NodeOut,
    NeighborOut,
    DocumentOut,
    DocumentCreateIn,
    DocumentUpdateIn,
    InsertProvisionIn,
    SearchParams,
    ListParams,
    EntitySearchRequest,
    EntitySearchResponse,
    SuggestionRequest,
    SuggestionResponse,
    SearchStats,
    SearchHealth,
    LawOutlineOut,
    LawDetailOut,
    ProvisionQueryIn,
    ProvisionOut,
} from '@/types/api';

/**
 * 法条相关 API 服务
 */
export const provisionApi = {
    /**
     * 获取法条详情
     */
    getNode: async (client: ApiClient, nodeId: string): Promise<NodeOut> => {
        return client.get<NodeOut>(`/graph/node/${nodeId}`);
    },

    /**
     * 搜索法条
     */
    search: async (client: ApiClient, params: SearchParams): Promise<NodeOut[]> => {
        return client.get<NodeOut[]>('/graph/search', { params });
    },

    /**
     * 获取关联法条
     */
    getNeighbors: async (client: ApiClient, nodeId: string, limit: number = 50): Promise<NeighborOut[]> => {
        return client.get<NeighborOut[]>(`/graph/neighbors/${nodeId}`, { params: { limit } });
    },
};

/**
 * 法条推荐 API 服务
 */
export const recommendationApi = {
    /**
     * 为文书推荐法条（基于内容和图谱关联）
     */
    getDocumentRecommendations: async (
        client: ApiClient,
        docId: string,
        limit: number = 10
    ): Promise<any[]> => {
        return client.get<any[]>('/recommendations/document', {
            params: { doc_id: docId, limit }
        });
    },

    /**
     * 获取法条的关联法条（基于图谱关系）
     */
    getRelatedProvisions: async (
        client: ApiClient,
        provisionId: string,
        limit: number = 10
    ): Promise<any[]> => {
        return client.get<any[]>(`/recommendations/provision/${provisionId}/related`, {
            params: { limit },
        });
    },

    /**
     * 搜索法条（支持关键词搜索）
     */
    search: async (
        client: ApiClient,
        q: string,
        field: string = 'name',
        limit: number = 20,
        offset: number = 0
    ): Promise<any> => {
        return client.get<any>('/recommendations/search', {
            params: { q, field, limit, offset },
        });
    },

    smartRecommend: async (
        client: ApiClient,
        data: {
            document_id?: string;
            content: string;
            current_paragraph?: string;
            case_type?: 'criminal' | 'civil' | 'administrative' | string;
            top_k?: number;
        }
    ): Promise<{ recommendations: any[]; metadata: any }> => {
        return client.post<{ recommendations: any[]; metadata: any }>('/recommendations/smart', data);
    },
};

/**
 * 文书相关 API 服务（个人文书，带用户认证）
 */
export const userDocumentApi = {
    /**
     * 获取用户文书列表
     */
    list: async (client: ApiClient, params?: { limit?: number; offset?: number; doc_type?: string; status?: string }): Promise<any[]> => {
        return client.get<any[]>('/user-documents', {
            params,
        });
    },

    /**
     * 创建用户文书
     */
    create: async (client: ApiClient, data: { title: string; content?: string; doc_type?: string }): Promise<any> => {
        return client.post<any>('/user-documents', data);
    },

    /**
     * 获取文书详情
     */
    get: async (client: ApiClient, docId: string): Promise<any> => {
        return client.get<any>(`/user-documents/${docId}`);
    },

    /**
     * 更新文书
     */
    update: async (client: ApiClient, docId: string, data: { title?: string; content?: string; doc_type?: string }): Promise<any> => {
        return client.put<any>(`/user-documents/${docId}`, data);
    },

    /**
     * 删除文书
     */
    delete: async (client: ApiClient, docId: string): Promise<void> => {
        return client.delete<void>(`/user-documents/${docId}`);
    },
};

/**
 * 通用文书 API 服务（不带用户认证）
 */
export const documentApi = {
    /**
     * 获取文书列表
     */
    list: async (client: ApiClient, params?: ListParams): Promise<DocumentOut[]> => {
        return client.get<DocumentOut[]>('/documents', { params });
    },

    /**
     * 创建文书
     */
    create: async (client: ApiClient, data: DocumentCreateIn): Promise<DocumentOut> => {
        return client.post<DocumentOut>('/documents', data);
    },

    /**
     * 获取文书详情
     */
    get: async (client: ApiClient, docId: string): Promise<DocumentOut> => {
        return client.get<DocumentOut>(`/documents/${docId}`);
    },

    /**
     * 更新文书
     */
    update: async (client: ApiClient, docId: string, data: DocumentUpdateIn): Promise<DocumentOut> => {
        return client.put<DocumentOut>(`/documents/${docId}`, data);
    },

    /**
     * 删除文书
     */
    delete: async (client: ApiClient, docId: string): Promise<void> => {
        return client.delete<void>(`/documents/${docId}`);
    },

    /**
     * 在文书中插入法条
     */
    insertProvision: async (
        client: ApiClient,
        docId: string,
        data: InsertProvisionIn
    ): Promise<DocumentOut> => {
        return client.post<DocumentOut>(`/user-documents/${docId}/insert-provision`, data);
    },
};

/**
 * Elasticsearch 搜索相关 API 服务
 */
export const searchApi = {
    /**
     * 健康检查
     */
    health: async (client: ApiClient): Promise<SearchHealth> => {
        return client.get<SearchHealth>('/search/health');
    },

    /**
     * 实体搜索
     */
    searchEntities: async (
        client: ApiClient,
        params: EntitySearchRequest
    ): Promise<EntitySearchResponse> => {
        return client.post<EntitySearchResponse>('/search/entities', params);
    },

    /**
     * 获取搜索建议
     */
    getSuggestions: async (
        client: ApiClient,
        params: SuggestionRequest
    ): Promise<SuggestionResponse> => {
        return client.post<SuggestionResponse>('/search/entities/suggest', params);
    },

    /**
     * 获取搜索统计信息
     */
    getStats: async (client: ApiClient): Promise<SearchStats> => {
        return client.get<SearchStats>('/search/stats');
    },
};

/**
 * 法律知识图谱 API 服务
 */
export const lawApi = {
    /**
     * 获取法律列表（已废弃，请使用 searchLaws）
     */
    listLaws: async (
        client: ApiClient,
        params?: { category?: string; limit?: number; offset?: number }
    ): Promise<LawOutlineOut[]> => {
        return client.get<LawOutlineOut[]>('/laws', { params });
    },

    /**
     * 搜索法律（支持 ES 加速）
     */
    searchLaws: async (
        client: ApiClient,
        keyword: string,
        limit: number = 10
    ): Promise<LawOutlineOut[]> => {
        return client.get<LawOutlineOut[]>('/laws/search', {
            params: { q: keyword, limit }
        });
    },

    /**
     * 获取法律详情（含完整层级结构和法条内容）
     */
    getLawDetail: async (client: ApiClient, sourceId: string, title?: string): Promise<LawDetailOut> => {
        const params = title ? `?title=${encodeURIComponent(title)}` : '';
        return client.get<LawDetailOut>(`/laws/${sourceId}${params}`);
    },

    /**
     * 获取法律大纲（仅层级结构，不含详细内容）
     */
    getLawOutline: async (client: ApiClient, sourceId: string): Promise<LawDetailOut> => {
        return client.get<LawDetailOut>(`/laws/${sourceId}/outline`);
    },

    /**
     * 根据编号查询法条
     */
    searchProvision: async (
        client: ApiClient,
        query: ProvisionQueryIn
    ): Promise<ProvisionOut> => {
        return client.post<ProvisionOut>('/laws/search-provision', query);
    },

    /**
     * 获取具体法条详情
     */
    getProvisionDetail: async (
        client: ApiClient,
        sourceId: string,
        provisionId: string
    ): Promise<ProvisionOut> => {
        return client.get<ProvisionOut>(`/laws/${sourceId}/provisions/${provisionId}`);
    },
};
