import { Search as SearchIcon, Filter, CheckCircle2, ArrowRight, Sparkles } from 'lucide-react';
import { useCallback, useMemo, useState } from 'react';
import { apiClient } from '@/lib/api-client';

type ProvisionSearchResult = {
    id: string;
    type?: string;
    name?: string;
    full_name?: string;
    law_name?: string;
    text?: string;
    level?: string;
    properties?: Record<string, unknown>;
};

export default function Search() {
    const [query, setQuery] = useState('');
    const [scope, setScope] = useState<'all' | 'provisions'>('all');
    const [department, setDepartment] = useState('');
    const [effectLevel, setEffectLevel] = useState('');

    const [results, setResults] = useState<ProvisionSearchResult[]>([]);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [hasSearched, setHasSearched] = useState(false);

    const pageSize = 10;
    const totalPages = useMemo(() => Math.max(1, Math.ceil(total / pageSize)), [total]);

    const runSearch = useCallback(
        async (opts?: { q?: string; nextPage?: number }) => {
            const q = (opts?.q ?? query).trim();
            const nextPage = opts?.nextPage ?? page;

            try {
                setLoading(true);
                setError(null);
                setHasSearched(true);

                const offset = (nextPage - 1) * pageSize;
                const resp = await apiClient.get<{
                    results: ProvisionSearchResult[];
                    total: number;
                    limit: number;
                    offset: number;
                }>('/recommendations/advanced-search', {
                    params: {
                        q: q || undefined,
                        scope,
                        department: department || undefined,
                        effect_level: effectLevel || undefined,
                        limit: pageSize,
                        offset,
                    },
                });

                setResults(Array.isArray(resp?.results) ? resp.results : []);
                setTotal(typeof resp?.total === 'number' ? resp.total : 0);
                setPage(nextPage);
            } catch (e) {
                setResults([]);
                setTotal(0);
                setError(e instanceof Error ? e.message : '搜索失败');
            } finally {
                setLoading(false);
            }
        },
        [query, page, scope, department, effectLevel]
    );

    const handleSearchClick = useCallback(() => {
        setPage(1);
        runSearch({ nextPage: 1 });
    }, [runSearch]);

    const resetFilters = useCallback(() => {
        setScope('all');
        setDepartment('');
        setEffectLevel('');
        setResults([]);
        setTotal(0);
        setPage(1);
        setHasSearched(false);
        setError(null);
    }, []);

    return (
        <div className="min-h-screen bg-slate-50">
            {/* Header */}
            <div className="border-b border-slate-200 bg-white/80 backdrop-blur-md sticky top-0 z-50">
                <div className="max-w-6xl mx-auto px-8 py-8">
                    <div className="flex items-center gap-4 mb-2">
                        <div className="p-2 bg-blue-600 rounded-lg shadow-lg shadow-blue-600/20">
                            <Sparkles className="w-5 h-5 text-white" />
                        </div>
                        <h1 className="text-4xl font-black text-slate-900 tracking-tight">智能搜索</h1>
                    </div>
                    <p className="text-slate-500 font-medium">结合知识图谱技术，快速定位法律条文与关联信息</p>
                </div>
            </div>

            {/* Main Content */}
            <div className="max-w-6xl mx-auto px-8 py-12">
                {/* Search Box Section */}
                <div className="bg-white rounded-[3rem] p-12 border border-slate-200 shadow-2xl shadow-slate-200/50 mb-12 relative overflow-hidden">
                    {/* Background decoration */}
                    <div className="absolute top-0 right-0 w-64 h-64 bg-blue-50 rounded-full blur-3xl -mr-32 -mt-32 opacity-50" />
                    
                    <div className="relative z-10">
                        <div className="text-center mb-12">
                            <h2 className="text-3xl font-black text-slate-900 mb-4 tracking-tight">高级搜索</h2>
                            <p className="text-slate-500 text-lg font-medium">支持关键词查询，快速筛选法律条文</p>
                        </div>

                        <div className="relative max-w-4xl mx-auto mb-10">
                            <input
                                type="text"
                                placeholder="输入您想查询的法律问题或关键词..."
                                className="w-full px-8 py-6 pl-16 bg-slate-50 border-2 border-slate-100 rounded-3xl focus:border-blue-500 focus:ring-8 focus:ring-blue-500/5 focus:bg-white transition-all outline-none text-xl shadow-inner font-medium"
                                value={query}
                                onChange={(e) => setQuery(e.target.value)}
                                onKeyDown={(e) => {
                                    if (e.key === 'Enter') {
                                        handleSearchClick();
                                    }
                                }}
                            />
                            <SearchIcon className="w-8 h-8 text-slate-400 absolute left-6 top-1/2 transform -translate-y-1/2" />
                            <button
                                onClick={handleSearchClick}
                                disabled={loading}
                                className="absolute right-3 top-1/2 transform -translate-y-1/2 px-10 py-4 bg-slate-900 text-white rounded-2xl hover:bg-slate-800 transition-all duration-300 font-bold shadow-xl shadow-slate-900/20 flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
                            >
                                <span>立即搜索</span>
                                <ArrowRight className="w-5 h-5" />
                            </button>
                        </div>
                    </div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-10">
                    {/* Left Side: Advanced Filters */}
                    <div className="lg:col-span-1 space-y-8">
                        <div className="bg-white rounded-[2.5rem] p-10 border border-slate-200 shadow-sm">
                            <div className="flex items-center gap-3 mb-8">
                                <div className="p-2 bg-slate-100 rounded-lg">
                                    <Filter className="w-5 h-5 text-slate-600" />
                                </div>
                                <h2 className="text-xl font-black text-slate-900 tracking-tight">高级筛选</h2>
                            </div>
                            
                            <div className="space-y-8">
                                <div>
                                    <label className="block text-sm font-black text-slate-400 uppercase tracking-widest mb-4">
                                        搜索范围
                                    </label>
                                    <select
                                        value={scope}
                                        onChange={(e) => setScope(e.target.value as 'all' | 'provisions')}
                                        className="w-full px-6 py-4 bg-slate-50 border border-slate-100 rounded-2xl focus:ring-4 focus:ring-blue-500/5 focus:border-blue-500 transition-all outline-none font-bold text-slate-700 cursor-pointer hover:bg-slate-100/50"
                                    >
                                        <option value="all">全部资源</option>
                                        <option value="provisions">法律条文</option>
                                    </select>
                                </div>

                                <div>
                                    <label className="block text-sm font-black text-slate-400 uppercase tracking-widest mb-4">
                                        法律部门
                                    </label>
                                    <select
                                        value={department}
                                        onChange={(e) => setDepartment(e.target.value)}
                                        className="w-full px-6 py-4 bg-slate-50 border border-slate-100 rounded-2xl focus:ring-4 focus:ring-blue-500/5 focus:border-blue-500 transition-all outline-none font-bold text-slate-700 cursor-pointer hover:bg-slate-100/50"
                                    >
                                        <option value="">所有部门</option>
                                        <option value="法律">法律</option>
                                        <option value="宪法">宪法</option>
                                        <option value="行政法规">行政法规</option>
                                        <option value="司法解释">司法解释</option>
                                        <option value="监察法规">监察法规</option>
                                    </select>
                                </div>

                                <div>
                                    <label className="block text-sm font-black text-slate-400 uppercase tracking-widest mb-4">
                                        效力级别
                                    </label>
                                    <select
                                        value={effectLevel}
                                        onChange={(e) => setEffectLevel(e.target.value)}
                                        className="w-full px-6 py-4 bg-slate-50 border border-slate-100 rounded-2xl focus:ring-4 focus:ring-blue-500/5 focus:border-blue-500 transition-all outline-none font-bold text-slate-700 cursor-pointer hover:bg-slate-100/50"
                                    >
                                        <option value="">全部级别</option>
                                        <option value="现行有效">现行有效</option>
                                        <option value="已废止">已废止</option>
                                    </select>
                                </div>
                            </div>

                            <button
                                onClick={resetFilters}
                                className="w-full mt-10 py-4 border-2 border-slate-100 text-slate-600 rounded-2xl font-bold hover:bg-slate-50 transition-all flex items-center justify-center gap-2"
                            >
                                重置筛选
                            </button>
                        </div>
                    </div>

                    {/* Right Side: Results / Empty State */}
                    <div className="lg:col-span-2">
                        {!hasSearched ? (
                            <div className="bg-slate-100/50 rounded-[2.5rem] border-4 border-dashed border-slate-200 p-16 text-center h-full flex flex-col items-center justify-center">
                                <div className="w-32 h-32 bg-white rounded-full flex items-center justify-center shadow-2xl mb-10 relative">
                                    <SearchIcon className="w-12 h-12 text-slate-200" />
                                    <div className="absolute -right-2 -top-2 w-10 h-10 bg-blue-500 rounded-2xl flex items-center justify-center shadow-lg animate-bounce">
                                        <Sparkles className="w-5 h-5 text-white" />
                                    </div>
                                </div>
                                <h3 className="text-3xl font-black text-slate-900 mb-4 tracking-tight">准备好开始检索了吗？</h3>
                                <p className="text-slate-500 text-lg font-medium mb-12 max-w-md mx-auto">
                                    在上方输入关键词，系统将为您实时呈现来自知识图谱的深度关联结果
                                </p>
                                <div className="flex flex-wrap items-center justify-center gap-8">
                                    {[
                                        { label: '全文语义理解', color: 'text-blue-600' },
                                        { label: '关联知识图谱', color: 'text-indigo-600' },
                                        { label: '精准效力筛选', color: 'text-violet-600' },
                                    ].map((feature, index) => (
                                        <div key={index} className={`flex items-center gap-3 ${feature.color}`}>
                                            <CheckCircle2 className="w-6 h-6" />
                                            <span className="font-black text-sm uppercase tracking-wider">{feature.label}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        ) : (
                            <div className="bg-white rounded-[2.5rem] border border-slate-200 shadow-sm p-10">
                                <div className="flex items-center justify-between gap-4 mb-8">
                                    <div className="text-slate-900 font-black text-xl">
                                        搜索结果
                                        <span className="ml-3 text-slate-400 font-bold text-sm">
                                            共 {total} 条
                                        </span>
                                    </div>
                                    <div className="flex items-center gap-3">
                                        <button
                                            onClick={() => runSearch({ nextPage: Math.max(1, page - 1) })}
                                            disabled={loading || page <= 1}
                                            className="px-4 py-2 rounded-xl border border-slate-200 text-slate-700 font-bold disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-50"
                                        >
                                            上一页
                                        </button>
                                        <div className="text-slate-500 font-bold">
                                            {page} / {totalPages}
                                        </div>
                                        <button
                                            onClick={() => runSearch({ nextPage: Math.min(totalPages, page + 1) })}
                                            disabled={loading || page >= totalPages}
                                            className="px-4 py-2 rounded-xl border border-slate-200 text-slate-700 font-bold disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-50"
                                        >
                                            下一页
                                        </button>
                                    </div>
                                </div>

                                {error && (
                                    <div className="mb-8 p-4 rounded-2xl bg-red-50 border border-red-100 text-red-700 font-bold">
                                        {error}
                                    </div>
                                )}

                                {loading ? (
                                    <div className="py-20 text-center">
                                        <div className="mx-auto w-10 h-10 rounded-full border-4 border-slate-200 border-t-slate-900 animate-spin" />
                                        <div className="mt-4 text-slate-500 font-bold">检索中...</div>
                                    </div>
                                ) : results.length === 0 ? (
                                    <div className="py-20 text-center text-slate-500 font-bold">
                                        未找到匹配结果
                                    </div>
                                ) : (
                                    <div className="space-y-4 max-h-[600px] overflow-y-auto pr-2 custom-scrollbar">
                                        {results.map((item) => (
                                            <div
                                                key={item.id}
                                                className="group p-6 rounded-2xl border border-slate-200 hover:border-blue-500 hover:shadow-md transition-all"
                                            >
                                                <div className="flex items-start justify-between gap-4">
                                                    <div className="min-w-0">
                                                        <div className="text-slate-900 font-black text-lg truncate">
                                                            {item.full_name || item.name || '未知法条'}
                                                        </div>
                                                        <div className="mt-1 text-slate-400 font-bold text-sm">
                                                            {item.law_name || '未知法律'} · {item.level || 'unknown'}
                                                        </div>
                                                    </div>
                                                    <div className="flex-shrink-0 px-3 py-1.5 rounded-xl bg-blue-50 text-blue-700 font-black text-xs">
                                                        匹配
                                                    </div>
                                                </div>
                                                {!!(item.text || '').trim() && (
                                                    <div className="mt-4 text-slate-600 font-medium leading-relaxed line-clamp-3">
                                                        {String(item.text).trim()}
                                                    </div>
                                                )}
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
