import { useState, useEffect, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Save, AlertCircle, FileText, History, Search, Plus, Loader, ChevronDown, FileCode } from 'lucide-react';
import { Modal } from '@/components/ui';
import type { DocumentUpdateIn } from '../types';
import { useEditor, EditorContent } from '@tiptap/react';
import StarterKit from '@tiptap/starter-kit';
import { apiClient, userDocumentApi, recommendationApi } from '@/lib/api-client';

export function DocumentEditor() {
    const { id } = useParams<{ id: string }>();
    const navigate = useNavigate();
    const isEditMode = !!id;

    const [title, setTitle] = useState('');
    const [docType, setDocType] = useState('通用文书');
    const [docStatus, setDocStatus] = useState('draft');
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [showConfirmModal, setShowConfirmModal] = useState(false);
    const [saveMessage, setSaveMessage] = useState<{ type: 'success' | 'error', text: string } | null>(null);

    // 法条推荐相关状态
    const [activeTab, setActiveTab] = useState<'search' | 'recommend'>('recommend');
    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState<any[]>([]);
    const [searchTotal, setSearchTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [recommendations, setRecommendations] = useState<any[]>([]);
    const [loadingRecommendations, setLoadingRecommendations] = useState(false);
    const [searchLoading, setSearchLoading] = useState(false);
    const [showInsertMenu, setShowInsertMenu] = useState<string | null>(null);
    const pageSize = 10;

    // 点击空白区域关闭插入菜单
    useEffect(() => {
        const handleClickOutside = () => {
            if (showInsertMenu) {
                setShowInsertMenu(null);
            }
        };
        document.addEventListener('click', handleClickOutside);
        return () => document.removeEventListener('click', handleClickOutside);
    }, [showInsertMenu]);

    const editor = useEditor({
        extensions: [StarterKit],
        content: '',
        editorProps: {
            attributes: {
                class: 'prose prose-sm sm:prose lg:prose-lg xl:prose-2xl focus:outline-none min-h-[500px] max-w-none px-8 py-10 font-medium text-slate-700 leading-relaxed',
            },
        },
    });

    useEffect(() => {
        if (isEditMode && id) {
            loadDocument(id);
        }
    }, [id, isEditMode]);

    const loadRecommendations = async (docId: string) => {
        try {
            setLoadingRecommendations(true);
            const content = editor?.getHTML() || '';
            const data = await recommendationApi.smartRecommend(apiClient, {
                document_id: docId,
                content,
                case_type: 'criminal',
                top_k: 10
            });
            setRecommendations(data?.recommendations || []);
        } catch (error) {
            console.error('加载推荐失败:', error);
            setRecommendations([]);
        } finally {
            setLoadingRecommendations(false);
        }
    };

    const debounceRecommend = useCallback(
        (() => {
            let timer: NodeJS.Timeout;
            return (docId: string) => {
                clearTimeout(timer);
                timer = setTimeout(() => {
                    loadRecommendations(docId);
                }, 500); // 从700ms缩短到500ms，响应更快
            };
        })(),
        [editor]
    );

    useEffect(() => {
        if (!editor || !id) return;
        const handler = () => {
            if (activeTab !== 'recommend') return;
            debounceRecommend(id);
        };
        editor.on('update', handler);
        return () => {
            editor.off('update', handler);
        };
    }, [editor, id, activeTab, debounceRecommend]);

    // 防抖搜索
    const debounceSearch = useCallback(
        (() => {
            let timer: NodeJS.Timeout;
            return (query: string) => {
                clearTimeout(timer);
                timer = setTimeout(() => {
                    setCurrentPage(1); // 新搜索时重置到第一页
                    handleSearch(query, 1);
                }, 500);
            };
        })(),
        []
    );

    const handleSearch = async (query: string, page: number = 1) => {
        if (!query.trim()) {
            setSearchResults([]);
            setSearchTotal(0);
            setCurrentPage(1);
            return;
        }
        try {
            setSearchLoading(true);
            console.log('🔍 开始搜索:', query.trim(), '页码:', page);
            const offset = (page - 1) * pageSize;
            const response = await recommendationApi.search(apiClient, query.trim(), 'name', pageSize, offset);
            console.log('✅ 搜索结果响应:', response);

            // 兼容两种返回格式
            const results = response.results || response;
            const total = response.total || results.length;

            console.log('✅ 解析结果:', results.length, '条，总计:', total);
            setSearchResults(Array.isArray(results) ? results : []);
            setSearchTotal(total);
            setCurrentPage(page);
        } catch (error: any) {
            console.error('❌ 搜索失败详情:', error);
            console.error('错误状态:', error?.response?.status);
            console.error('错误信息:', error?.response?.data || error?.message);
            alert(`搜索失败: ${error?.response?.data?.detail || error?.message || '未知错误'}`);
        } finally {
            setSearchLoading(false);
        }
    };

    const autoSaveCurrentContent = async () => {
        if (!id || !editor) return;
        const content = (editor.getHTML() || '').trim();
        const t = title.trim();
        const payload: { title?: string; content?: string } = { content };
        if (t) payload.title = t;
        await userDocumentApi.update(apiClient, id, payload);
        setSaveMessage({ type: 'success', text: '已自动保存' });
        setTimeout(() => setSaveMessage(null), 1500);
    };

    const handleInsertProvision = async (
        provisionId: string,
        mode: 'cursor' | 'append' = 'cursor',
        provisionData?: {
            id?: string;
            provision_id?: string;
            name?: string;
            full_name?: string;
            text?: string;
            properties?: Record<string, unknown>;
        }
    ) => {
        if (!id) return;
        try {
            let fullName: string;
            let provisionText: string;

            // 如果传入了法条数据，直接使用
            if (provisionData) {
                const props = (provisionData.properties || {}) as Record<string, unknown>;
                fullName = String(provisionData.full_name || props.full_name || provisionData.name || props.name || provisionData.provision_id || provisionData.id || '');
                provisionText = String(provisionData.text || props.text || '').trim();
            } else {
                // 如果没传入数据，从推荐列表中查找
                const rec = recommendations.find(r => (r.provision_id || r.id) === provisionId);
                if (rec) {
                    const props = (rec.properties || {}) as Record<string, unknown>;
                    fullName = String(rec.full_name || props.full_name || rec.name || props.name || provisionId);
                    provisionText = String(rec.text || props.text || '').trim();
                } else {
                    // 也尝试从搜索结果中查找
                    const searchResult = searchResults.find(r => r.id === provisionId);
                    if (searchResult) {
                        const props = (searchResult.properties || {}) as Record<string, unknown>;
                        fullName = String(searchResult.full_name || props.full_name || searchResult.name || props.name || provisionId);
                        provisionText = String(searchResult.text || props.text || '').trim();
                    } else {
                        setSaveMessage({ type: 'error', text: '未找到法条数据' });
                        setTimeout(() => setSaveMessage(null), 2000);
                        return;
                    }
                }
            }

            // 确保docId是数字类型
            const docId = parseInt(id, 10);
            if (isNaN(docId)) {
                setSaveMessage({ type: 'error', text: '文档ID无效' });
                setTimeout(() => setSaveMessage(null), 2000);
                return;
            }

            if (mode === 'cursor' && editor) {
                // 光标处插入：前端直接在编辑器中插入文本
                const snippet = `<p>依据${fullName}之规定：${provisionText}</p>`;
                editor.commands.insertContent(snippet);

                // 直接保存文档内容
                await autoSaveCurrentContent();
                setSaveMessage({ type: 'success', text: '法条已按规范格式插入并自动保存' });
                setTimeout(() => setSaveMessage(null), 2000);
            } else if (mode === 'append') {
                // 文末附件模式：智能追加到附录部分
                const currentHTML = editor.getHTML();
                const appendixTitle = '附录：引用法条';
                const newProvision = `<p><strong>${fullName}</strong></p><p>${provisionText}</p>`;

                // 检查是否已有附录部分
                if (currentHTML.includes('附录：引用法条') || currentHTML.includes('附录:引用法条')) {
                    // 已有附录，在附录标题后追加内容
                    const appendixMatch = currentHTML.match(/(<h[1-6][^>]*>.*?附录[：:].*?<\/h[1-6]>)/);
                    if (appendixMatch) {
                        // 在附录标题后插入
                        const appendixEnd = appendixMatch.index! + appendixMatch[0].length;
                        const beforeAppendixContent = currentHTML.substring(0, appendixEnd);
                        const afterAppendixContent = currentHTML.substring(appendixEnd);
                        const updatedHTML = beforeAppendixContent + newProvision + afterAppendixContent;
                        editor.commands.setContent(updatedHTML);
                    } else {
                        // 找到了文本但没找到HTML标签，直接追加
                        editor.commands.insertContentAt(editor.state.doc.content.size, newProvision);
                    }
                } else {
                    // 没有附录，创建附录标题和内容
                    const appendixHTML = `\n\n<h2>${appendixTitle}</h2>\n${newProvision}`;
                    editor.commands.insertContentAt(editor.state.doc.content.size, appendixHTML);
                }

                // 保存文档
                await autoSaveCurrentContent();
                setSaveMessage({ type: 'success', text: '法条已添加为文末附件并自动保存' });
                setTimeout(() => setSaveMessage(null), 2000);
            }

            // 刷新推荐列表
            loadRecommendations(id);
            setShowInsertMenu(null);
        } catch (error) {
            console.error('插入法条失败:', error);
            setSaveMessage({ type: 'error', text: '插入失败' });
            setTimeout(() => setSaveMessage(null), 2000);
        }
    };

    const loadDocument = async (docId: string) => {
        try {
            setLoading(true);
            const doc = await userDocumentApi.get(apiClient, docId);
            setTitle(doc.title);
            setDocType(doc.doc_type || '通用文书');
            setDocStatus(doc.status || 'draft');
            if (editor) {
                editor.commands.setContent(doc.content || '');
            }
            loadRecommendations(docId);
        } catch (error) {
            console.error('加载文书失败:', error);
            alert('加载文书失败');
        } finally {
            setLoading(false);
        }
    };

    const handleSave = async () => {
        if (!title.trim()) {
            alert('请输入文书标题');
            return;
        }

        try {
            setSaving(true);
            const content = editor?.getHTML() || '';
            const data: DocumentUpdateIn = {
                title: title.trim(),
                content: content.trim(),
                doc_type: docType,
                status: docStatus,
            };

            if (isEditMode && id) {
                // 更新现有文书
                await userDocumentApi.update(apiClient, id, data);
                setSaveMessage({ type: 'success', text: '文书保存成功' });
            } else {
                // 创建新文书
                const createData = {
                    title: title.trim(),
                    content: content.trim(),
                    doc_type: docType,
                    status: docStatus,
                };
                const newDoc = await userDocumentApi.create(apiClient, createData);
                setSaveMessage({ type: 'success', text: '文书创建成功' });
                // 如果是新建，跳转到编辑页面
                if (newDoc && newDoc.id) {
                    navigate(`/documents/${newDoc.id}`);
                }
            }

            // 3秒后自动清除提示
            setTimeout(() => setSaveMessage(null), 3000);
        } catch (error: any) {
            console.error('保存失败:', error);
            const errorMessage = error?.message || '未知错误';
            console.error('错误详情:', errorMessage);
            alert(`保存失败: ${errorMessage}`);
        } finally {
            setSaving(false);
        }
    };

    const handleCancel = () => {
        if (title || (editor && !editor.isEmpty)) {
            setShowConfirmModal(true);
        } else {
            navigate('/documents');
        }
    };

    if (loading) {
        return (
            <div className="min-h-screen bg-slate-50 flex flex-col items-center justify-center space-y-6">
                <div className="relative w-20 h-20">
                    <div className="absolute inset-0 border-4 border-slate-200 rounded-full" />
                    <div className="absolute inset-0 border-4 border-blue-600 rounded-full border-t-transparent animate-spin" />
                </div>
                <p className="text-slate-400 font-bold uppercase tracking-widest text-sm">正在载入编辑器...</p>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-slate-50 pb-20">
            {/* 保存成功提示 */}
            {saveMessage && (
                <div className={`fixed top-6 left-1/2 -translate-x-1/2 z-[100] px-6 py-3 rounded-2xl shadow-2xl transition-all animate-in fade-in slide-in-from-top-4 ${saveMessage.type === 'success' ? 'bg-emerald-500 text-white' : 'bg-red-500 text-white'}`}>
                    <p className="font-bold text-sm">{saveMessage.text}</p>
                </div>
            )}

            {/* Toolbar Header */}
            <div className="border-b border-slate-200 bg-white/80 backdrop-blur-md sticky top-0 z-50">
                <div className="max-w-7xl mx-auto px-8 py-6">
                    <div className="flex items-center justify-between gap-8">
                        <div className="flex items-center gap-6 flex-1 min-w-0">
                            <button
                                onClick={handleCancel}
                                className="p-3 bg-slate-50 text-slate-400 hover:text-slate-900 hover:bg-slate-100 rounded-2xl transition-all"
                                title="返回列表"
                            >
                                <ArrowLeft className="w-6 h-6" />
                            </button>
                            <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-3 mb-1">
                                    <div className="p-1.5 bg-blue-500 rounded-lg shadow-lg shadow-blue-500/20">
                                        <FileText className="w-4 h-4 text-white" />
                                    </div>
                                    <span className="text-xs font-black text-slate-400 uppercase tracking-widest">
                                        {isEditMode ? '编辑文书' : '创建新文书'}
                                    </span>
                                </div>
                                <input
                                    type="text"
                                    value={title}
                                    onChange={(e) => setTitle(e.target.value)}
                                    placeholder="输入文书标题..."
                                    className="w-full bg-transparent border-none p-0 text-2xl font-black text-slate-900 focus:ring-0 placeholder-slate-200 truncate"
                                />
                            </div>
                        </div>

                        {/* 文书类型和状态选择 */}
                        <div className="flex items-center gap-4">
                            <div>
                                <label className="block text-xs font-bold text-slate-500 mb-1 uppercase">文书类型</label>
                                <select
                                    value={docType}
                                    onChange={(e) => setDocType(e.target.value)}
                                    className="px-4 py-2 bg-white border-2 border-slate-200 rounded-xl font-bold text-sm focus:border-blue-500 focus:ring-2 focus:ring-blue-500/10 transition-all outline-none"
                                >
                                    <option value="通用文书">通用文书</option>
                                    <option value="起诉状">起诉状</option>
                                    <option value="判决书">判决书</option>
                                    <option value="意见书">意见书</option>
                                </select>
                            </div>
                            <div>
                                <label className="block text-xs font-bold text-slate-500 mb-1 uppercase">状态</label>
                                <select
                                    value={docStatus}
                                    onChange={(e) => setDocStatus(e.target.value)}
                                    className="px-4 py-2 bg-white border-2 border-slate-200 rounded-xl font-bold text-sm focus:border-blue-500 focus:ring-2 focus:ring-blue-500/10 transition-all outline-none"
                                >
                                    <option value="draft">草稿</option>
                                    <option value="已发布">已发布</option>
                                </select>
                            </div>
                        </div>

                        <div className="flex items-center gap-4">
                            <button
                                onClick={handleCancel}
                                className="px-6 py-3 bg-white text-slate-600 rounded-2xl font-bold border-2 border-slate-100 hover:border-slate-200 hover:bg-slate-50 transition-all"
                            >
                                取消
                            </button>
                            <button
                                onClick={handleSave}
                                disabled={saving}
                                className="group flex items-center gap-2 px-10 py-3 bg-slate-900 text-white rounded-2xl font-black hover:bg-slate-800 transition-all shadow-xl shadow-slate-900/10 disabled:opacity-50"
                            >
                                {saving ? (
                                    <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                                ) : (
                                    <Save className="w-5 h-5" />
                                )}
                                <span>{isEditMode ? '保存修改' : '立即创建'}</span>
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            {/* Main Editor Body */}
            <div className="max-w-7xl mx-auto px-8 py-12">
                <div className="grid grid-cols-1 lg:grid-cols-12 gap-10">
                    {/* Left: Editor Column */}
                    <div className="lg:col-span-8">
                        <div className="bg-white rounded-[2.5rem] border border-slate-200 shadow-2xl shadow-slate-200/50 overflow-hidden flex flex-col min-h-[800px]">
                            {/* Editor Menu Bar */}
                            <div className="px-10 py-6 border-b border-slate-100 bg-slate-50/50 flex flex-wrap gap-2 items-center">
                                <div className="flex items-center gap-1 p-1 bg-white rounded-xl border border-slate-100 shadow-sm">
                                    <button
                                        onClick={() => editor?.chain().focus().toggleBold().run()}
                                        className={`p-2 rounded-lg transition-all ${editor?.isActive('bold') ? 'bg-slate-900 text-white shadow-lg' : 'text-slate-400 hover:bg-slate-100'}`}
                                        title="加粗"
                                    >
                                        <span className="font-bold text-lg leading-none">B</span>
                                    </button>
                                    <button
                                        onClick={() => editor?.chain().focus().toggleItalic().run()}
                                        className={`p-2 rounded-lg transition-all ${editor?.isActive('italic') ? 'bg-slate-900 text-white shadow-lg' : 'text-slate-400 hover:bg-slate-100'}`}
                                        title="斜体"
                                    >
                                        <span className="italic text-lg leading-none">I</span>
                                    </button>
                                </div>

                                <div className="flex items-center gap-1 p-1 bg-white rounded-xl border border-slate-100 shadow-sm">
                                    {[1, 2, 3].map((level) => (
                                        <button
                                            key={level}
                                            onClick={() => editor?.chain().focus().toggleHeading({ level: level as any }).run()}
                                            className={`px-4 py-2 rounded-lg transition-all font-black text-sm ${editor?.isActive('heading', { level }) ? 'bg-slate-900 text-white shadow-lg' : 'text-slate-400 hover:bg-slate-100'}`}
                                            title={`标题 ${level}`}
                                        >
                                            H{level}
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Actual Editor Content */}
                            <div className="flex-1 bg-white relative">
                                <EditorContent editor={editor} />
                            </div>
                        </div>
                    </div>

                    {/* Right: Sidebar Column */}
                    <div className="lg:col-span-4 space-y-8">
                        {/* Legal Context Card */}
                        <div className="bg-white rounded-[2.5rem] border border-slate-200 p-10 shadow-sm relative overflow-hidden">
                            <div className="absolute top-0 right-0 w-40 h-40 bg-blue-50 rounded-full blur-3xl -mr-20 -mt-20 opacity-50" />
                            <div className="relative z-10">
                                <div className="flex items-center gap-3 mb-8">
                                    <div className="p-2 bg-blue-500 text-white rounded-lg shadow-lg shadow-blue-500/20">
                                        <Search className="w-4 h-4" />
                                    </div>
                                    <h2 className="text-xl font-black text-slate-900 tracking-tight">智能引用助手</h2>
                                </div>
                                <div className="space-y-6">
                                    {/* Tab 切换 */}
                                    <div className="flex gap-2">
                                        <button
                                            onClick={() => setActiveTab('recommend')}
                                            className={`flex-1 px-4 py-2 rounded-xl font-bold text-sm transition-all ${activeTab === 'recommend'
                                                ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/20'
                                                : 'bg-slate-50 text-slate-600 hover:bg-slate-100'
                                                }`}
                                        >
                                            智能推荐
                                        </button>
                                        <button
                                            onClick={() => setActiveTab('search')}
                                            className={`flex-1 px-4 py-2 rounded-xl font-bold text-sm transition-all ${activeTab === 'search'
                                                ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/20'
                                                : 'bg-slate-50 text-slate-600 hover:bg-slate-100'
                                                }`}
                                        >
                                            搜索法条
                                        </button>
                                    </div>

                                    {activeTab === 'search' ? (
                                        <>
                                            {/* 搜索框 */}
                                            <div className="relative">
                                                <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                                                <input
                                                    type="text"
                                                    value={searchQuery}
                                                    onChange={(e) => {
                                                        setSearchQuery(e.target.value);
                                                        debounceSearch(e.target.value);
                                                    }}
                                                    placeholder="输入法条名称或关键词搜索..."
                                                    className="w-full px-4 py-3 pl-10 bg-slate-50 border border-slate-200 rounded-xl text-sm font-medium focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                                                />
                                                {searchLoading && (
                                                    <Loader className="w-4 h-4 animate-spin absolute right-3 top-1/2 -translate-y-1/2 text-blue-500" />
                                                )}
                                            </div>

                                            {/* 搜索结果数量 */}
                                            {searchResults.length > 0 && (
                                                <div className="flex items-center justify-between text-xs text-slate-500 font-medium">
                                                    <div className="flex items-center gap-2">
                                                        <div className="w-1.5 h-1.5 bg-green-500 rounded-full" />
                                                        找到 {searchTotal} 条相关法条
                                                    </div>
                                                    <div className="text-slate-400">
                                                        第 {currentPage} / {Math.ceil(searchTotal / pageSize)} 页
                                                    </div>
                                                </div>
                                            )}

                                            {/* 搜索结果 */}
                                            <div className="space-y-3 max-h-[500px] overflow-y-auto">
                                                {searchResults.length === 0 && !searchLoading && (
                                                    <div className="text-center py-12 text-slate-400">
                                                        <Search className="w-16 h-16 mx-auto mb-4 opacity-20" />
                                                        <p className="font-medium">输入法条名称进行搜索</p>
                                                        <p className="text-xs mt-2">例如：合同、违约、劳动法</p>
                                                    </div>
                                                )}
                                                {searchResults.map((provision) => (
                                                    <div key={provision.id} className="group p-4 bg-slate-50 rounded-xl border border-slate-100 hover:border-blue-500 hover:shadow-md transition-all">
                                                        <div className="flex justify-between items-start gap-3">
                                                            <div className="flex-1 min-w-0">
                                                                <p className="font-bold text-sm text-slate-900 mb-1">
                                                                    {provision.full_name || provision.name || provision.properties?.name || '未知法条'}
                                                                </p>
                                                                <p className="text-xs text-slate-500 line-clamp-3 leading-relaxed">
                                                                    {provision.text || provision.properties?.text || '暂无内容'}
                                                                </p>
                                                            </div>
                                                            <div className="relative flex-shrink-0">
                                                                <button
                                                                    onClick={(e) => {
                                                                        e.stopPropagation();
                                                                        setShowInsertMenu(showInsertMenu === provision.id ? null : provision.id);
                                                                    }}
                                                                    className="p-2 bg-blue-50 text-blue-600 rounded-lg hover:bg-blue-100 transition-all flex items-center gap-1"
                                                                    title="插入法条"
                                                                >
                                                                    <Plus className="w-4 h-4" />
                                                                    <ChevronDown className="w-3 h-3" />
                                                                </button>

                                                                {showInsertMenu === provision.id && (
                                                                    <div className="absolute right-0 top-full mt-2 w-48 bg-white rounded-xl shadow-xl border border-slate-200 z-50 overflow-hidden" onClick={(e) => e.stopPropagation()}>
                                                                        <button
                                                                            onClick={() => handleInsertProvision(provision.id, 'cursor', provision)}
                                                                            className="w-full px-4 py-3 text-left text-sm font-medium text-slate-700 hover:bg-blue-50 hover:text-blue-600 transition-colors flex items-center gap-2"
                                                                        >
                                                                            <FileText className="w-4 h-4" />
                                                                            <div>
                                                                                <p className="font-bold">光标处插入</p>
                                                                                <p className="text-xs text-slate-500 font-normal">插入到编辑器光标位置</p>
                                                                            </div>
                                                                        </button>
                                                                        <button
                                                                            onClick={() => handleInsertProvision(provision.id, 'append', provision)}
                                                                            className="w-full px-4 py-3 text-left text-sm font-medium text-slate-700 hover:bg-blue-50 hover:text-blue-600 transition-colors flex items-center gap-2 border-t border-slate-100"
                                                                        >
                                                                            <FileCode className="w-4 h-4" />
                                                                            <div>
                                                                                <p className="font-bold">文末附件</p>
                                                                                <p className="text-xs text-slate-500 font-normal">添加为文末引用附件</p>
                                                                            </div>
                                                                        </button>
                                                                    </div>
                                                                )}
                                                            </div>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>

                                            {/* 分页控件 */}
                                            {searchResults.length > 0 && searchTotal > pageSize && (
                                                <div className="flex items-center justify-center gap-2 pt-4 border-t border-slate-100">
                                                    <button
                                                        onClick={() => handleSearch(searchQuery, currentPage - 1)}
                                                        disabled={currentPage === 1 || searchLoading}
                                                        className="px-3 py-1.5 text-xs font-medium text-slate-600 bg-slate-50 rounded-lg hover:bg-slate-100 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
                                                    >
                                                        上一页
                                                    </button>
                                                    <div className="flex items-center gap-1">
                                                        {Array.from({ length: Math.min(5, Math.ceil(searchTotal / pageSize)) }, (_, i) => {
                                                            let page: number;
                                                            if (Math.ceil(searchTotal / pageSize) <= 5) {
                                                                page = i + 1;
                                                            } else if (currentPage <= 3) {
                                                                page = i + 1;
                                                            } else if (currentPage >= Math.ceil(searchTotal / pageSize) - 2) {
                                                                page = Math.ceil(searchTotal / pageSize) - 4 + i;
                                                            } else {
                                                                page = currentPage - 2 + i;
                                                            }
                                                            return (
                                                                <button
                                                                    key={page}
                                                                    onClick={() => handleSearch(searchQuery, page)}
                                                                    disabled={searchLoading}
                                                                    className={`w-8 h-8 text-xs font-bold rounded-lg transition-all ${currentPage === page
                                                                        ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/20'
                                                                        : 'bg-slate-50 text-slate-600 hover:bg-slate-100'
                                                                        }`}
                                                                >
                                                                    {page}
                                                                </button>
                                                            );
                                                        })}
                                                    </div>
                                                    <button
                                                        onClick={() => handleSearch(searchQuery, currentPage + 1)}
                                                        disabled={currentPage >= Math.ceil(searchTotal / pageSize) || searchLoading}
                                                        className="px-3 py-1.5 text-xs font-medium text-slate-600 bg-slate-50 rounded-lg hover:bg-slate-100 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
                                                    >
                                                        下一页
                                                    </button>
                                                </div>
                                            )}
                                        </>
                                    ) : (
                                        <>
                                            {/* 推荐列表 */}
                                            <div className="space-y-3 max-h-[500px] overflow-y-auto">
                                                {loadingRecommendations ? (
                                                    <div className="text-center py-12">
                                                        <Loader className="w-10 h-10 animate-spin text-blue-500 mx-auto mb-3" />
                                                        <p className="text-sm text-slate-500 font-medium">分析中...</p>
                                                    </div>
                                                ) : recommendations.length === 0 ? (
                                                    <div className="text-center py-8 text-slate-400 text-sm">
                                                        <AlertCircle className="w-12 h-12 mx-auto mb-3 opacity-20" />
                                                        <p>暂无推荐，请先编写文书内容</p>
                                                    </div>
                                                ) : (
                                                    recommendations.map((rec) => (
                                                        <div key={rec.provision_id || rec.id} className="group p-4 bg-slate-50 rounded-xl border border-slate-100 hover:border-blue-500 transition-all">
                                                            <div className="flex justify-between items-start gap-3">
                                                                <div className="flex-1 min-w-0">
                                                                    <p className="font-bold text-sm text-slate-900 mb-1">
                                                                        {rec.full_name || rec.name || rec.properties?.name || '未知法条'}
                                                                    </p>
                                                                    <p className="text-xs text-slate-500 line-clamp-2 leading-relaxed mb-2">
                                                                        {rec.text || rec.properties?.text || '暂无内容'}
                                                                    </p>
                                                                    <span className="inline-block px-2 py-1 bg-blue-50 text-blue-600 text-xs rounded-lg font-bold">
                                                                        {typeof rec.score === 'number' ? `${(rec.score * 100).toFixed(1)}%` : (rec.recommend_reason || rec.reason || rec.relation_type || '相关')}
                                                                    </span>
                                                                    {(rec.reason || rec.recommend_reason) && (
                                                                        <p className="text-xs text-slate-500 mt-2 leading-relaxed">
                                                                            {rec.reason || rec.recommend_reason}
                                                                        </p>
                                                                    )}
                                                                </div>
                                                                <div className="relative">
                                                                    {(() => {
                                                                        const rid = rec.provision_id || rec.id;
                                                                        const insertModes = Array.isArray(rec.insert_modes) ? rec.insert_modes : ['cursor', 'append'];
                                                                        const canInsert = !String(rid || '').startsWith('ai::') && insertModes.length > 0;
                                                                        return (
                                                                            <>
                                                                                <button
                                                                                    onClick={(e) => {
                                                                                        if (!canInsert) return;
                                                                                        e.stopPropagation();
                                                                                        setShowInsertMenu(showInsertMenu === rid ? null : rid);
                                                                                    }}
                                                                                    className={`p-2 rounded-lg transition-all flex-shrink-0 flex items-center gap-1 ${canInsert ? 'bg-blue-50 text-blue-600 hover:bg-blue-100' : 'bg-slate-100 text-slate-400 cursor-not-allowed'}`}
                                                                                    title={canInsert ? '插入法条' : '当前仅可参考，暂不可插入（数据库未命中）'}
                                                                                >
                                                                                    <Plus className="w-4 h-4" />
                                                                                    <ChevronDown className="w-3 h-3" />
                                                                                </button>

                                                                                {showInsertMenu === rid && canInsert && (
                                                                                    <div className="absolute right-0 top-full mt-2 w-48 bg-white rounded-xl shadow-xl border border-slate-200 z-50 overflow-hidden">
                                                                                        <button
                                                                                            onClick={() => handleInsertProvision(rec.provision_id || rec.id, 'cursor', rec)}
                                                                                            className="w-full px-4 py-3 text-left text-sm font-medium text-slate-700 hover:bg-blue-50 hover:text-blue-600 transition-colors flex items-center gap-2"
                                                                                        >
                                                                                            <FileText className="w-4 h-4" />
                                                                                            <div>
                                                                                                <p className="font-bold">光标处插入</p>
                                                                                                <p className="text-xs text-slate-500 font-normal">插入到编辑器光标位置</p>
                                                                                            </div>
                                                                                        </button>
                                                                                        <button
                                                                                            onClick={() => handleInsertProvision(rec.provision_id || rec.id, 'append', rec)}
                                                                                            className="w-full px-4 py-3 text-left text-sm font-medium text-slate-700 hover:bg-blue-50 hover:text-blue-600 transition-colors flex items-center gap-2 border-t border-slate-100"
                                                                                        >
                                                                                            <FileCode className="w-4 h-4" />
                                                                                            <div>
                                                                                                <p className="font-bold">文末附件</p>
                                                                                                <p className="text-xs text-slate-500 font-normal">添加为文末引用附件</p>
                                                                                            </div>
                                                                                        </button>
                                                                                    </div>
                                                                                )}
                                                                            </>
                                                                        );
                                                                    })()}
                                                                </div>
                                                            </div>
                                                        </div>
                                                    ))
                                                )}
                                            </div>

                                            <div className="flex items-center gap-2 p-4 bg-amber-50 rounded-2xl border border-amber-100 text-amber-700">
                                                <AlertCircle className="w-5 h-5 flex-shrink-0" />
                                                <p className="text-xs font-bold leading-relaxed">
                                                    系统会根据您的文书内容智能推荐相关法条，提高编写效率。
                                                </p>
                                            </div>
                                        </>
                                    )}
                                </div>
                            </div>
                        </div>

                        {/* Metadata Card */}
                        <div className="bg-slate-900 rounded-[2.5rem] p-10 shadow-xl shadow-slate-900/20">
                            <h2 className="text-xl font-black text-white mb-8 tracking-tight flex items-center gap-3">
                                <History className="w-5 h-5 text-blue-500" />
                                文书元数据
                            </h2>
                            <div className="space-y-6">
                                <div>
                                    <p className="text-xs font-black text-slate-500 uppercase tracking-widest mb-2">最后更新时间</p>
                                    <p className="text-slate-300 font-bold">{new Date().toLocaleString()}</p>
                                </div>
                                <div>
                                    <p className="text-xs font-black text-slate-500 uppercase tracking-widest mb-2">自动保存状态</p>
                                    <div className="flex items-center gap-2 text-emerald-400 font-bold">
                                        <div className="w-2 h-2 bg-emerald-400 rounded-full animate-pulse" />
                                        <span>云端已同步</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Confirm Modal */}
            <Modal
                isOpen={showConfirmModal}
                onClose={() => setShowConfirmModal(false)}
                title="放弃修改？"
                footer={
                    <div className="flex gap-4">
                        <button
                            onClick={() => setShowConfirmModal(false)}
                            className="px-6 py-2 text-slate-600 font-bold hover:bg-slate-50 rounded-xl transition-all"
                        >
                            继续编辑
                        </button>
                        <button
                            onClick={() => {
                                setShowConfirmModal(false);
                                navigate('/documents');
                            }}
                            className="px-6 py-2 bg-red-600 text-white font-bold rounded-xl shadow-lg shadow-red-600/20 hover:bg-red-700 transition-all"
                        >
                            放弃修改
                        </button>
                    </div>
                }
            >
                <p className="text-slate-500 font-medium">
                    当前文书内容已发生变更，如果现在离开，所有未保存的修改将会永久丢失。
                </p>
            </Modal>
        </div>
    );
}
