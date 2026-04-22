import { create } from 'zustand';
import type { EntitySearchRequest, EntitySearchResult, EntitySearchResponse, SuggestionResponse } from '@/types/api';

interface SearchState {
    // 搜索参数
    query: string;
    searchParams: EntitySearchRequest;

    // 搜索结果
    results: EntitySearchResult[];
    total: number;
    took: number;

    // 搜索建议
    suggestions: string[];

    // 加载状态
    isLoading: boolean;
    isGettingSuggestions: boolean;

    // 错误状态
    error: string | null;

    // Actions
    setQuery: (query: string) => void;
    setSearchParams: (params: Partial<EntitySearchRequest>) => void;
    clearSearchParams: () => void;
    setResults: (response: EntitySearchResponse) => void;
    clearResults: () => void;
    setSuggestions: (response: SuggestionResponse) => void;
    clearSuggestions: () => void;
    setLoading: (loading: boolean) => void;
    setGettingSuggestions: (loading: boolean) => void;
    setError: (error: string | null) => void;
    clearError: () => void;
    reset: () => void;
}

const initialState = {
    query: '',
    searchParams: {
        q: '',
        entity_types: undefined,
        filters: undefined,
        limit: 20,
        offset: 0,
        highlight: true,
    },
    results: [],
    total: 0,
    took: 0,
    suggestions: [],
    isLoading: false,
    isGettingSuggestions: false,
    error: null,
};

export const useSearchStore = create<SearchState>()((set: any) => ({
    ...initialState,

    setQuery: (query: string) => set({ query }),

    setSearchParams: (params: Partial<EntitySearchRequest>) =>
        set((state: SearchState) => ({
            searchParams: { ...state.searchParams, ...params },
        })),

    clearSearchParams: () => set({ searchParams: initialState.searchParams }),

    setResults: (response: EntitySearchResponse) => set({
        results: response.hits,
        total: response.total,
        took: response.took,
    }),

    clearResults: () => set({ results: [], total: 0, took: 0 }),

    setSuggestions: (response: SuggestionResponse) => set({
        suggestions: response.suggestions,
    }),

    clearSuggestions: () => set({ suggestions: [] }),

    setLoading: (loading: boolean) => set({ isLoading: loading }),

    setGettingSuggestions: (loading: boolean) => set({ isGettingSuggestions: loading }),

    setError: (error: string | null) => set({ error }),

    clearError: () => set({ error: null }),

    reset: () => set(initialState),
}));
