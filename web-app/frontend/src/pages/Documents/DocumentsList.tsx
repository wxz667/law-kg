import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Plus, FileText, Search, Edit2, Trash2, Clock, ChevronRight, Gavel, Scale, FileSignature, Info } from 'lucide-react';
import type { Document } from '../types';
import { apiClient, userDocumentApi } from '@/lib/api-client';

export function DocumentsList() {
    const navigate = useNavigate();
    const [documents, setDocuments] = useState<Document[]>([]);
    const [loading, setLoading] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [typeFilter, setTypeFilter] = useState<string>('all');
    const [statusFilter, setStatusFilter] = useState<string>('all');
    const [debouncedQuery, setDebouncedQuery] = useState('');

    useEffect(() => {
        loadDocuments();
    }, [typeFilter, statusFilter, debouncedQuery]);

    // 防抖搜索
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedQuery(searchQuery);
        }, 300);
        return () => clearTimeout(timer);
    }, [searchQuery]);

    const loadDocuments = async () => {
        try {
            setLoading(true);
            const params: any = {};
            if (typeFilter !== 'all') params.doc_type = typeFilter;
            if (statusFilter !== 'all') params.status = statusFilter;
            if (debouncedQuery.trim()) params.q = debouncedQuery.trim();

            const docs = await userDocumentApi.list(apiClient, params);
            setDocuments(docs);
        } catch (error) {
            console.error('加载文书列表失败:', error);
        } finally {
            setLoading(false);
        }
    };

    const handleDelete = async (id: string) => {
        if (!confirm('确定要删除这篇文书吗？')) return;
        try {
            await userDocumentApi.delete(apiClient, id);
            await loadDocuments();
            alert('文书已删除');
        } catch (error) {
            console.error('删除失败:', error);
            alert('删除失败，请重试');
        }
    };

    const getStatusStyles = (status: string) => {
        const statusText = status || '草稿';
        switch (statusText) {
            case '已发布': return 'bg-emerald-50 text-emerald-700 border-emerald-100 shadow-emerald-100/50';
            case '草稿': return 'bg-amber-50 text-amber-700 border-amber-100 shadow-amber-100/50';
            default: return 'bg-slate-50 text-slate-700 border-slate-100 shadow-slate-100/50';
        }
    };

    const getTypeIcon = (type: string) => {
        switch (type) {
            case '起诉状': return <FileSignature className="w-6 h-6" />;
            case '判决书': return <Gavel className="w-6 h-6" />;
            case '意见书': return <Info className="w-6 h-6" />;
            default: return <FileText className="w-6 h-6" />;
        }
    };

    const getTypeColor = (type: string) => {
        switch (type) {
            case '起诉状': return 'bg-blue-50 text-blue-600';
            case '判决书': return 'bg-violet-50 text-violet-600';
            case '意见书': return 'bg-emerald-50 text-emerald-600';
            default: return 'bg-slate-50 text-slate-600';
        }
    };

    return (
        <div className="min-h-screen bg-slate-50">
            {/* Header */}
            <div className="border-b border-slate-200 bg-white/80 backdrop-blur-md sticky top-0 z-50">
                <div className="max-w-7xl mx-auto px-8 py-8">
                    <div className="flex flex-col md:flex-row md:items-center justify-between gap-6 mb-8">
                        <div>
                            <div className="flex items-center gap-3 mb-2">
                                <div className="p-2 bg-emerald-600 rounded-lg shadow-lg shadow-emerald-600/20">
                                    <Scale className="w-5 h-5 text-white" />
                                </div>
                                <h1 className="text-4xl font-black text-slate-900 tracking-tight">文书管理</h1>
                            </div>
                            <p className="text-slate-500 font-medium">构建专业的法律文书库，实现法条引用自动化</p>
                        </div>
                        <button
                            onClick={() => navigate('/documents/new')}
                            className="group inline-flex items-center gap-3 px-8 py-4 bg-slate-900 text-white rounded-[2rem] font-black hover:bg-slate-800 transition-all duration-300 shadow-xl shadow-slate-900/10 hover:-translate-y-1"
                        >
                            <Plus className="w-6 h-6" />
                            <span>创建新文书</span>
                        </button>
                    </div>

                    {/* Search Bar */}
                    <div className="relative max-w-2xl mb-6">
                        <input
                            type="text"
                            placeholder="通过标题、关键词或文书类型进行检索..."
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            className="w-full px-6 py-5 pl-16 bg-white border-2 border-slate-100 rounded-3xl focus:border-blue-500 focus:ring-4 focus:ring-blue-500/10 transition-all outline-none text-lg shadow-sm font-medium"
                        />
                        <Search className="w-6 h-6 text-slate-400 absolute left-6 top-1/2 transform -translate-y-1/2" />
                    </div>

                    {/* Filters */}
                    <div className="flex flex-wrap gap-4">
                        {/* Type Filter */}
                        <div className="flex items-center gap-2">
                            <span className="text-sm font-bold text-slate-500 uppercase tracking-wider">文书类型</span>
                            <div className="flex gap-2">
                                {['all', '通用文书', '起诉状', '判决书', '意见书'].map((type) => (
                                    <button
                                        key={type}
                                        onClick={() => setTypeFilter(type)}
                                        className={`px-4 py-2 rounded-xl text-sm font-semibold transition-all ${typeFilter === type
                                            ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20'
                                            : 'bg-white text-slate-600 border border-slate-200 hover:border-blue-500'
                                            }`}
                                    >
                                        {type === 'all' ? '全部' : type}
                                    </button>
                                ))}
                            </div>
                        </div>

                        {/* Status Filter */}
                        <div className="flex items-center gap-2 ml-8">
                            <span className="text-sm font-bold text-slate-500 uppercase tracking-wider">状态</span>
                            <div className="flex gap-2">
                                {['all', 'draft', '已发布', '草稿'].map((status) => (
                                    <button
                                        key={status}
                                        onClick={() => setStatusFilter(status)}
                                        className={`px-4 py-2 rounded-xl text-sm font-semibold transition-all ${statusFilter === status
                                            ? 'bg-emerald-600 text-white shadow-lg shadow-emerald-600/20'
                                            : 'bg-white text-slate-600 border border-slate-200 hover:border-emerald-500'
                                            }`}
                                    >
                                        {status === 'all' ? '全部' : status === 'draft' ? '草稿' : status}
                                    </button>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Main Content */}
            <div className="max-w-7xl mx-auto px-8 py-12">
                {loading ? (
                    <div className="flex flex-col items-center justify-center py-32 space-y-6">
                        <div className="relative w-20 h-20">
                            <div className="absolute inset-0 border-4 border-slate-200 rounded-full" />
                            <div className="absolute inset-0 border-4 border-blue-600 rounded-full border-t-transparent animate-spin" />
                        </div>
                        <p className="text-slate-400 font-bold uppercase tracking-widest text-sm">正在加载文书库...</p>
                    </div>
                ) : documents.length === 0 ? (
                    <div className="bg-white rounded-[3rem] border-4 border-dashed border-slate-200 p-20 text-center flex flex-col items-center">
                        <div className="w-32 h-32 bg-slate-50 rounded-full flex items-center justify-center mb-8 shadow-inner">
                            <FileText className="w-16 h-16 text-slate-200" />
                        </div>
                        <h3 className="text-3xl font-black text-slate-900 mb-4 tracking-tight">您的文书库空空如也</h3>
                        <p className="text-slate-500 text-lg font-medium mb-12 max-w-md">
                            立即开始创建您的第一篇法律文书，利用智能助手提升文书质量与效率
                        </p>
                        <button
                            onClick={() => navigate('/documents/new')}
                            className="group inline-flex items-center gap-3 px-10 py-5 bg-blue-600 text-white rounded-2xl font-black hover:bg-blue-700 transition-all duration-300 shadow-xl shadow-blue-600/20"
                        >
                            <Plus className="w-6 h-6" />
                            <span>立即创建</span>
                        </button>
                    </div>
                ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-10">
                        {documents.map((doc) => (
                            <div
                                key={doc.id}
                                className="group bg-white rounded-[2.5rem] border border-slate-200 p-10 hover:shadow-2xl hover:shadow-blue-500/10 transition-all duration-500 hover:-translate-y-2 relative overflow-hidden"
                                onClick={() => navigate(`/documents/${doc.id}`)}
                            >
                                {/* Hover background decoration */}
                                <div className="absolute -right-10 -bottom-10 w-40 h-40 bg-slate-50 rounded-full opacity-0 group-hover:opacity-100 transition-opacity duration-500 blur-3xl" />

                                <div className="relative z-10">
                                    <div className="flex items-start justify-between mb-8">
                                        <div className="flex items-center gap-4">
                                            <div className={`p-4 rounded-2xl ${getTypeColor(doc.type || '')} shadow-inner group-hover:scale-110 transition-transform duration-500`}>
                                                {getTypeIcon(doc.type || '')}
                                            </div>
                                            <div>
                                                <h3 className="font-black text-slate-900 text-xl group-hover:text-blue-600 transition-colors line-clamp-1 leading-tight">
                                                    {doc.title}
                                                </h3>
                                                <div className="flex items-center gap-2 mt-1.5">
                                                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-md text-xs font-semibold bg-slate-100 text-slate-600">
                                                        {doc.type || '通用文书'}
                                                    </span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="bg-slate-50 rounded-2xl p-6 mb-8 group-hover:bg-white group-hover:border-slate-100 border border-transparent transition-all">
                                        <p className="text-slate-600 text-[15px] font-medium leading-relaxed line-clamp-3">
                                            {/* 提取纯文本用于预览，去除 HTML 标签 */}
                                            {doc.content?.replace(/<[^>]*>/g, '').substring(0, 150) || '暂无内容'}
                                        </p>
                                    </div>

                                    <div className="flex items-center justify-between">
                                        <div className={`inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold border shadow-sm ${getStatusStyles(doc.status || '')}`}>
                                            {doc.status || '草稿'}
                                        </div>

                                        <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-all duration-300 translate-y-2 group-hover:translate-y-0">
                                            <button
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    navigate(`/documents/edit/${doc.id}`);
                                                }}
                                                className="p-3 text-slate-400 hover:text-blue-600 hover:bg-blue-50 rounded-xl transition-all"
                                                title="编辑文书"
                                            >
                                                <Edit2 className="w-5 h-5" />
                                            </button>
                                            <button
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    handleDelete(doc.id);
                                                }}
                                                className="p-3 text-slate-400 hover:text-red-600 hover:bg-red-50 rounded-xl transition-all"
                                                title="永久删除"
                                            >
                                                <Trash2 className="w-5 h-5" />
                                            </button>
                                        </div>
                                    </div>

                                    <div className="mt-8 pt-6 border-t border-slate-100 flex items-center justify-between text-xs font-bold text-slate-400 uppercase tracking-widest">
                                        <div className="flex items-center gap-2">
                                            <Clock className="w-3.5 h-3.5" />
                                            <span>{new Date(doc.updated_at).toLocaleDateString('zh-CN')}</span>
                                        </div>
                                        <div className="flex items-center gap-1 group-hover:text-blue-600 transition-colors">
                                            <span>查看详情</span>
                                            <ChevronRight className="w-3.5 h-3.5 group-hover:translate-x-1 transition-transform" />
                                        </div>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
