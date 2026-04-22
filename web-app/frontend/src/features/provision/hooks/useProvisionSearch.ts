import { useState, useCallback } from 'react';
import type { TreeNode, SearchFilters } from '../types';

interface UseProvisionSearchReturn {
    results: TreeNode[];
    loading: boolean;
    error: string | null;
    total: number;
    search: (filters: SearchFilters) => Promise<void>;
    clear: () => void;
}

export function useProvisionSearch(): UseProvisionSearchReturn {
    const [results, setResults] = useState<TreeNode[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [total, setTotal] = useState(0);

    const search = useCallback(async (filters: SearchFilters) => {
        try {
            setLoading(true);
            setError(null);

            // TODO: 调用实际 API
            // const response = await apiClient.get<TreeNode[]>('/graph/search', {
            //     params: {
            //         q: filters.q,
            //         field: filters.field,
            //         limit: 20,
            //     }
            // });
            // setResults(response.data);
            // setTotal(response.total);

            // Mock 数据
            const mockResults: TreeNode[] = [
                {
                    id: 'search_result_1',
                    name: `搜索结果：${filters.q}`,
                    level: 'article',
                    type: 'ProvisionNode',
                },
            ];

            setResults(mockResults);
            setTotal(1);
        } catch (err) {
            setError(err instanceof Error ? err.message : '搜索失败');
        } finally {
            setLoading(false);
        }
    }, []);

    const clear = useCallback(() => {
        setResults([]);
        setError(null);
        setTotal(0);
    }, []);

    return {
        results,
        loading,
        error,
        total,
        search,
        clear,
    };
}
