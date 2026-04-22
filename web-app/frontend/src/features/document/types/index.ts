export interface Document {
    id: string;
    title: string;
    content: string;
    type?: string;
    status?: string;
    created_at: string;
    updated_at: string;
    provisions?: string[];
}

export interface DocumentCreateIn {
    title: string;
    content?: string;
    type?: string;
}

export interface DocumentUpdateIn {
    title?: string;
    content?: string;
    type?: string;
}

export interface DocumentListParams {
    limit?: number;
    offset?: number;
    search?: string;
    type?: string;
    status?: string;
}

export interface DocumentListResponse {
    total: number;
    items: Document[];
    limit: number;
    offset: number;
}
