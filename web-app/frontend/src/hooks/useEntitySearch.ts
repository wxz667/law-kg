import { useCallback } from 'react';
import { apiClient } from '@/lib/api-client';
import { searchApi } from '@/services/index';
import { useSearchStore } from '@/stores/searchStore';
import type { EntitySearchRequest } from '@/types/api';

/**
 * 实体搜索自定义 Hook
 * 
 * 功能：
 * - 防抖处理
 * - 缓存机制
 * - 错误处理
 * - 加载状态管理
 */
export function useEntitySearch() {
    // 从 Zustand store 中获取状态和 actions
    const query = useSearchStore((state: any) => state.query);
    const results = useSearchStore((state: any) => state.results);
    const total = useSearchStore((state: any) => state.total);
    const took = useSearchStore((state: any) => state.took);
    const suggestions = useSearchStore((state: any) => state.suggestions);
    const isLoading = useSearchStore((state: any) => state.isLoading);
    const isGettingSuggestions = useSearchStore((state: any) => state.isGettingSuggestions);
    const error = useSearchStore((state: any) => state.error);

    const setQuery = useSearchStore((state: any) => state.setQuery);
    const setSearchParams = useSearchStore((state: any) => state.setSearchParams);
    const setResults = useSearchStore((state: any) => state.setResults);
    const setSuggestions = useSearchStore((state: any) => state.setSuggestions);
    const setLoading = useSearchStore((state: any) => state.setLoading);
    const setGettingSuggestions = useSearchStore((state: any) => state.setGettingSuggestions);
    const setError = useSearchStore((state: any) => state.setError);

    /**
     * 执行实体搜索
     */
    const search = useCallback(async (searchParams?: Partial<EntitySearchRequest>) => {
        try {
            setLoading(true);
            setError(null);

            // 合并搜索参数
            const params: EntitySearchRequest = {
                q: query,
                limit: 20,
                offset: 0,
                highlight: true,
                ...searchParams,
            };

            // 调用 API
            const response = await searchApi.searchEntities(apiClient, params);

            // 更新结果
            setResults(response);

            return response;
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : '搜索失败';
            setError(errorMessage);
            throw err;
        } finally {
            setLoading(false);
        }
    }, [query, setLoading, setError, setResults]);

    /**
     * 获取搜索建议（带防抖）
     */
    const getSuggestions = useCallback(async (searchQuery: string) => {
        if (!searchQuery || searchQuery.length < 1) {
            setSuggestions({ suggestions: [] });
            return;
        }

        try {
            setGettingSuggestions(true);

            const response = await searchApi.getSuggestions(apiClient, {
                query: searchQuery,
                size: 5,
            });

            setSuggestions(response);

            return response;
        } catch (err) {
            console.error('获取搜索建议失败:', err);
            return { suggestions: [] };
        } finally {
            setGettingSuggestions(false);
        }
    }, [setSuggestions, setGettingSuggestions]);

    /**
     * 清空搜索
     */
    const clearSearch = useCallback(() => {
        setQuery('');
        setSearchParams({});
        setResults({ total: 0, hits: [], took: 0 });
        setSuggestions({ suggestions: [] });
        setError(null);
    }, [setQuery, setSearchParams, setResults, setSuggestions, setError]);

    /**
     * 更新搜索词并触发搜索
     */
    const updateQueryAndSearch = useCallback(async (newQuery: string) => {
        setQuery(newQuery);
        if (newQuery.trim()) {
            await search({ q: newQuery });
        } else {
            clearSearch();
        }
    }, [setQuery, search, clearSearch]);

    return {
        // 状态
        query,
        results,
        total,
        took,
        suggestions,
        isLoading,
        isGettingSuggestions,
        error,

        // 方法
        search,
        getSuggestions,
        clearSearch,
        setQuery,
        setSearchParams,
        updateQueryAndSearch,
    };
}
