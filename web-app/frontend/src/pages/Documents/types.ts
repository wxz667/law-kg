export interface Document {
    id: string;
    title: string;
    content: string;
    type?: string;
    status?: string;
    created_at: string;
    updated_at: string;
}

export interface DocumentListResponse {
    items: Document[];
    total: number;
    limit: number;
    offset: number;
}
