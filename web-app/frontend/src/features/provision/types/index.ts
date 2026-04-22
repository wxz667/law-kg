export type ProvisionLevel =
    | 'document'
    | 'part'
    | 'chapter'
    | 'section'
    | 'article'
    | 'paragraph'
    | 'item'
    | 'sub_item';

export type ProvisionType = 'DocumentNode' | 'TocNode' | 'ProvisionNode';

export interface TreeNode {
    id: string;
    name: string;
    level: ProvisionLevel;
    type: ProvisionType;
    children?: TreeNode[];
    hasChildren?: boolean; // 用于懒加载
    properties?: Record<string, any>;
}

export interface SearchFilters {
    q: string;
    field?: 'name' | 'text';
    legalType?: string;
    status?: string;
    dateRange?: {
        start?: string;
        end?: string;
    };
}

export interface ProvisionDetail {
    id: string;
    name: string;
    content: string;
    level: ProvisionLevel;
    type: ProvisionType;
    documentName?: string;
    publishDate?: string;
    status?: string;
    children?: ProvisionDetail[];
}

export interface RelatedProvision {
    id: string;
    name: string;
    relationship: 'REFERS_TO' | 'INTERPRETS' | 'AMENDS' | 'REPEALS';
    documentName?: string;
}
