/**
 * 文书类型
 */
export interface Document {
    id: string;
    title: string;
    content: string;
    type?: string;
    status?: string;
    created_at: string;
    updated_at: string;
}

/**
 * 文书创建/更新输入类型
 */
export interface DocumentUpdateIn {
    title: string;
    content: string;
    doc_type?: string;
    status?: string;
}

/**
 * 文书列表响应类型
 */
export interface DocumentListResponse {
    total: number;
    documents: Document[];
}
