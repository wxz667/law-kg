import { useState, useEffect, useRef, useCallback } from 'react';
import { Book, ChevronRight, Star, Search, FileText, Loader2 } from 'lucide-react';
import { useProvisionStore, type TreeNode } from '@/stores/provisionStore';
import { KnowledgeGraph } from '@/features/provision/components/KnowledgeGraph';
import { lawApi } from '@/services';
import { apiClient } from '@/lib/api-client';
import type { LawOutlineOut, LawNodeOut, LawDetailOut } from '@/types/api';

export function Provisions() {
    const { currentProvision, setCurrentProvision, toggleFavorite, favorites } = useProvisionStore();
    const [searchQuery, setSearchQuery] = useState('');
    const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
    const [treeData, setTreeData] = useState<TreeNode | null>(null);
    const [lawList, setLawList] = useState<LawOutlineOut[]>([]);
    const [selectedLaw, setSelectedLaw] = useState<string>('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // 搜索结果缓存（关键词 -> 结果）
    const searchCacheRef = useRef<Map<string, LawOutlineOut[]>>(new Map());
    // 法律详情缓存（sourceId -> 详情）
    const lawDetailCacheRef = useRef<Map<string, LawDetailOut>>(new Map());

    // 将后端节点列表转换为树形结构
    const convertNodesToTree = (nodes: LawNodeOut[]): TreeNode[] => {
        // 去重：确保每个节点 ID 唯一
        const uniqueNodes = nodes.filter((node, index, self) =>
            index === self.findIndex((n) => n.id === node.id)
        );

        // 找到法律文档节点（根节点）
        const docNode = uniqueNodes.find(n => n.level === 'document');
        if (!docNode) return [];

        // 按层级排序，确保父节点先处理
        uniqueNodes.sort((a, b) => a.id.split(':').length - b.id.split(':').length);

        // 递归构建树形结构
        const buildChildren = (parentId: string): TreeNode[] => {
            // 提取父节点的 key（去掉类型前缀）
            const parentKey = parentId.split(':').slice(1).join(':');

            return uniqueNodes
                .filter(n => {
                    if (n.id === parentId) return false;

                    // 提取子节点的 key（去掉类型前缀）
                    const nodeKey = n.id.split(':').slice(1).join(':');

                    // 子节点的 key 必须以父节点 key + ":" 开头
                    return nodeKey.startsWith(parentKey + ':');
                })
                .map(node => ({
                    id: node.id,
                    title: node.name,
                    content: node.text || undefined,
                    level: node.level,
                    nodeType: node.type as 'DocumentNode' | 'TocNode' | 'ProvisionNode',
                    children: buildChildren(node.id),
                }));
        };

        return buildChildren(docNode.id);
    };

    // 将后端 level 字符串转换为数字层级（用于缩进）
    const getNodeLevel = (level: string | number | undefined): number => {
        if (typeof level === 'number') return level;
        const levelMap: Record<string, number> = {
            'document': 0,
            'part': 1,
            'chapter': 2,
            'section': 3,
            'article': 4,
            'paragraph': 5,
            'item': 6,
            'sub_item': 7,
        };
        return levelMap[level || ''] ?? 0;
    };

    // 将后端 level 字符串转换为显示文本
    const getNodeLevelText = (level: string | number | undefined): string => {
        if (typeof level === 'number') return `第 ${level} 级`;
        const levelMap: Record<string, string> = {
            'document': '法律',
            'part': '编',
            'chapter': '章',
            'section': '节',
            'article': '条',
            'paragraph': '款',
            'item': '项',
            'sub_item': '目',
        };
        return levelMap[level || ''] || '';
    };

    // 加载法律详情（带缓存）
    const loadLawDetail = useCallback(async (sourceId: string, title?: string) => {
        // 检查缓存
        const cachedDetail = lawDetailCacheRef.current.get(sourceId);
        if (cachedDetail) {
            console.log('使用缓存的法律详情:', sourceId);
            setSelectedLaw(sourceId);
            setTreeData(null);
            setCurrentProvision(null);

            const tree = convertNodesToTree(cachedDetail.nodes);
            if (tree.length > 0) {
                setTreeData(tree[0]);
                setExpandedNodes(new Set([cachedDetail.nodes.find(n => n.level === 'document')?.id || '']));
            }
            return;
        }

        try {
            setLoading(true);
            setError(null);
            setSelectedLaw(sourceId);
            // 立即清空之前的数据，显示加载状态
            setTreeData(null);
            setCurrentProvision(null);

            console.log('🔥 新版代码已加载！开始获取法律详情:', sourceId, title ? `(${title})` : '');
            const lawDetail: LawDetailOut = await lawApi.getLawDetail(apiClient, sourceId, title);
            console.log('法律详情获取成功，节点数:', lawDetail.nodes?.length || 0);

            // 存入缓存
            lawDetailCacheRef.current.set(sourceId, lawDetail);

            // 找到法律文档节点作为根
            const docNode = lawDetail.nodes?.find(n => n.level === 'document');
            if (!docNode) {
                console.error('未找到法律文档节点');
                setTreeData(null);
                return;
            }

            // 去重：确保每个节点 ID 唯一
            const uniqueNodes = (lawDetail.nodes || []).filter((node, index, self) =>
                index === self.findIndex((n) => n.id === node.id)
            );

            // 按层级排序，确保父节点先处理
            uniqueNodes.sort((a, b) => a.id.split(':').length - b.id.split(':').length);

            // 递归构建树形结构
            const buildChildren = (parentId: string): TreeNode[] => {
                // 提取父节点的 key（去掉类型前缀）
                const parentKey = parentId.split(':').slice(1).join(':');

                return uniqueNodes
                    .filter(n => {
                        if (n.id === parentId) return false;

                        // 提取子节点的 key（去掉类型前缀）
                        const nodeKey = n.id.split(':').slice(1).join(':');

                        // 子节点的 key 必须以父节点 key + ":" 开头
                        return nodeKey.startsWith(parentKey + ':');
                    })
                    .map(node => ({
                        id: node.id,
                        title: node.name,
                        content: node.text || undefined,
                        level: node.level,
                        nodeType: node.type as 'DocumentNode' | 'TocNode' | 'ProvisionNode',
                        children: buildChildren(node.id),
                    }));
            };

            // 创建根节点
            const rootNode: TreeNode = {
                id: docNode.id,
                title: docNode.name,
                content: docNode.text || undefined,
                level: docNode.level,
                nodeType: docNode.type as 'DocumentNode',
                children: buildChildren(docNode.id),
            };

            console.log('树构建完成，根节点子项:', rootNode.children?.length || 0);
            console.log('树结构:', JSON.stringify(rootNode, null, 2).substring(0, 500));

            setTreeData(rootNode);
            setExpandedNodes(new Set([docNode.id]));
            setCurrentProvision(null);
        } catch (err) {
            console.error('加载法律详情失败:', err);
            setError(`加载法律详情失败: ${err instanceof Error ? err.message : '未知错误'}`);
        } finally {
            setLoading(false);
        }
    }, [convertNodesToTree]);

    // 初始化加载 - 不再自动加载法律列表
    useEffect(() => {
        // 清空可能残留的全局状态，确保左右两侧状态一致
        setCurrentProvision(null);
        setLoading(false);
    }, []);

    // 搜索法律（带缓存）
    const handleSearch = async () => {
        const query = searchQuery.trim();
        if (!query) {
            setLawList([]);
            setTreeData(null);
            return;
        }

        // 检查缓存
        const cachedResults = searchCacheRef.current.get(query);
        if (cachedResults) {
            console.log('使用缓存的搜索结果:', query);
            setLawList(cachedResults);

            // 如果有结果，自动加载第一个法律的详情
            if (cachedResults.length > 0) {
                await loadLawDetail(cachedResults[0].source_id, cachedResults[0].title);
            } else {
                setTreeData(null);
                setCurrentProvision(null);
            }
            return;
        }

        try {
            setLoading(true);
            setError(null);
            console.log('开始搜索法律:', query);

            const laws = await lawApi.searchLaws(apiClient, query, 10);
            console.log('搜索结果:', laws.length, '部法律');

            // 存入缓存
            searchCacheRef.current.set(query, laws);
            setLawList(laws);

            // 如果有结果，自动加载第一个法律的详情
            if (laws.length > 0) {
                console.log('加载第一个法律详情:', laws[0].source_id);
                await loadLawDetail(laws[0].source_id, laws[0].title);
            } else {
                setTreeData(null);
                setCurrentProvision(null);
            }
        } catch (err) {
            console.error('搜索失败:', err);
            setError(`搜索失败: ${err instanceof Error ? err.message : '未知错误'}`);
        } finally {
            setLoading(false);
        }
    };

    // 监听回车键
    const handleKeyPress = (e: React.KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter') {
            handleSearch();
        }
    };

    const handleNodeClick = (node: TreeNode) => {
        setCurrentProvision(node);
        if (node.children && node.children.length > 0) {
            const newExpanded = new Set(expandedNodes);
            if (newExpanded.has(node.id)) {
                newExpanded.delete(node.id);
            } else {
                newExpanded.add(node.id);
            }
            setExpandedNodes(newExpanded);
        }
    };

    const handleLawSelect = (sourceId: string, title?: string) => {
        if (sourceId !== selectedLaw) {
            loadLawDetail(sourceId, title);
        }
    };

    const renderTree = (node: TreeNode, level = 0) => {
        const isExpanded = expandedNodes.has(node.id);
        const hasChildren = node.children && node.children.length > 0;
        const isFavorite = favorites.includes(node.id);
        const isSelected = currentProvision?.id === node.id;
        const nodeLevel = getNodeLevel(node.level);
        const levelText = getNodeLevelText(node.level);

        return (
            <div key={node.id}>
                <div
                    className={`group flex items-center gap-3 py-3 px-4 rounded-xl cursor-pointer transition-all duration-300 ${isSelected
                        ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20'
                        : 'hover:bg-slate-50 text-slate-700'
                        } ${level === 0 && !isSelected
                            ? 'bg-gradient-to-r from-blue-50 to-purple-50 border border-blue-100'
                            : ''
                        }`}
                    style={{ marginLeft: `${getNodeLevel(node.level) * 20}px` }}
                    onClick={() => handleNodeClick(node)}
                >
                    {hasChildren && (
                        <ChevronRight
                            className={`w-4 h-4 transition-transform duration-300 flex-shrink-0 ${isSelected ? 'text-white/70' : 'text-slate-400'} ${isExpanded ? 'rotate-90' : ''
                                }`}
                        />
                    )}
                    {!hasChildren && <div className="w-4 flex-shrink-0" />}

                    {/* 节点类型图标 */}
                    <div className={`p-2 rounded-lg transition-all duration-300 group-hover:scale-110 shadow-sm flex-shrink-0 ${isSelected ? 'bg-white/20' :
                        nodeLevel === 0 ? 'bg-gradient-to-br from-blue-500 to-blue-600' :
                            nodeLevel === 1 || nodeLevel === 2 ? 'bg-gradient-to-br from-emerald-500 to-emerald-600' :
                                nodeLevel === 3 ? 'bg-gradient-to-br from-violet-500 to-violet-600' :
                                    'bg-gradient-to-br from-amber-500 to-amber-600'
                        }`}>
                        <Book className={`w-3.5 h-3.5 ${isSelected ? 'text-white' : 'text-white'}`} />
                    </div>

                    {/* 节点内容 */}
                    <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                            <span className={`text-sm font-bold tracking-tight transition-colors duration-300 truncate ${isSelected ? 'text-white' : 'text-slate-700 group-hover:text-blue-600'}`}>
                                {node.title}
                            </span>
                            {/* 层级标签 */}
                            {levelText && (
                                <span className={`px-2 py-0.5 text-xs font-bold rounded-full flex-shrink-0 ${isSelected ? 'bg-white/20 text-white/90' :
                                    nodeLevel === 0 ? 'bg-blue-100 text-blue-700' :
                                        nodeLevel === 2 ? 'bg-emerald-100 text-emerald-700' :
                                            nodeLevel === 3 ? 'bg-violet-100 text-violet-700' :
                                                'bg-amber-100 text-amber-700'
                                    }`}>
                                    {levelText}
                                </span>
                            )}
                        </div>
                        {/* 子节点数量提示 */}
                        {hasChildren && !isSelected && (
                            <p className="text-xs text-slate-400 mt-0.5">{node.children!.length} 个子项</p>
                        )}
                    </div>

                    <button
                        onClick={(e) => {
                            e.stopPropagation();
                            toggleFavorite(node.id);
                        }}
                        className={`p-2 rounded-lg transition-all duration-300 flex-shrink-0 ${isSelected ? 'hover:bg-white/20' : 'hover:bg-slate-200 opacity-0 group-hover:opacity-100'}`}
                    >
                        <Star
                            className={`w-4 h-4 transition-colors ${isFavorite
                                ? 'fill-amber-400 text-amber-400'
                                : isSelected ? 'text-white/50' : 'text-slate-300'
                                }`}
                        />
                    </button>
                </div>

                {hasChildren && isExpanded && (
                    <div className="mt-2 space-y-1">
                        {node.children!.map(child => renderTree(child, level + 1))}
                    </div>
                )}
            </div>
        );
    };

    return (
        <div className="min-h-screen bg-slate-50">
            {/* Header */}
            <div className="border-b border-slate-200 bg-white/80 backdrop-blur-md sticky top-0 z-50">
                <div className="max-w-7xl mx-auto px-8 py-8">
                    <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-5">
                        <div>
                            <h1 className="text-3xl font-black text-slate-900 mb-1 tracking-tight">法条数据库</h1>
                            <p className="text-slate-500 font-medium text-sm">浏览完整的法律条文层级结构与详细内容</p>
                        </div>
                    </div>

                    {/* Search Bar */}
                    <div className="relative max-w-3xl">
                        <input
                            type="text"
                            placeholder="输入法律名称或关键词搜索..."
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            onKeyPress={handleKeyPress}
                            className="w-full px-4 py-3 pl-14 bg-white border-2 border-slate-100 rounded-xl focus:border-blue-500 focus:ring-4 focus:ring-blue-500/10 transition-all outline-none shadow-sm font-medium"
                        />
                        <Search className="w-5 h-5 text-slate-400 absolute left-5 top-1/2 transform -translate-y-1/2" />
                        <button
                            onClick={handleSearch}
                            disabled={loading || !searchQuery.trim()}
                            className="absolute right-2 top-1/2 transform -translate-y-1/2 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-all duration-300 font-bold shadow-lg shadow-blue-600/20 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
                        >
                            {loading ? '搜索中...' : '检索'}
                        </button>
                    </div>
                </div>
            </div>

            {/* Main Content */}
            <div className="max-w-7xl mx-auto px-8 py-8">
                <div className="grid grid-cols-1 lg:grid-cols-12 gap-10">
                    {/* Tree View */}
                    <div className="lg:col-span-7">
                        <div className="bg-white rounded-[2.5rem] border border-slate-200 shadow-sm overflow-hidden flex flex-col h-[800px]">
                            <div className="px-10 py-8 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
                                <div className="flex items-center gap-4">
                                    <div className="p-3 bg-blue-500 text-white rounded-2xl shadow-lg shadow-blue-500/20">
                                        <Book className="w-6 h-6" />
                                    </div>
                                    <h2 className="text-2xl font-black text-slate-900 tracking-tight">法律层级结构</h2>
                                </div>
                                <div className="text-sm font-bold text-slate-400 bg-white px-4 py-2 rounded-full border border-slate-100">
                                    {lawList.length > 0 ? `${lawList.length} 条结果` : '待搜索'}
                                </div>
                            </div>

                            {/* 法律选择器 */}
                            {lawList.length > 0 && (
                                <div className="px-8 py-4 border-b border-slate-100 bg-white">
                                    <div className="flex items-center gap-2 flex-wrap max-h-32 overflow-y-auto custom-scrollbar">
                                        {lawList.map((law) => (
                                            <button
                                                key={law.source_id}
                                                onClick={() => handleLawSelect(law.source_id, law.title)}
                                                className={`px-4 py-2 rounded-xl text-sm font-bold transition-all whitespace-nowrap ${selectedLaw === law.source_id
                                                    ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20'
                                                    : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                                                    }`}
                                            >
                                                {law.title.length > 15 ? law.title.substring(0, 15) + '...' : law.title}
                                            </button>
                                        ))}
                                    </div>
                                </div>
                            )}

                            <div className="p-8 space-y-4 overflow-y-auto custom-scrollbar flex-1">
                                {loading && !treeData ? (
                                    <div className="flex items-center justify-center h-64">
                                        <Loader2 className="w-8 h-8 text-blue-500 animate-spin" />
                                        <span className="ml-3 text-slate-500 font-medium">加载中...</span>
                                    </div>
                                ) : error ? (
                                    <div className="text-center py-12">
                                        <p className="text-red-500 font-medium mb-4">{error}</p>
                                        <button
                                            onClick={handleSearch}
                                            className="px-6 py-3 bg-blue-600 text-white rounded-xl font-bold hover:bg-blue-700 transition-all"
                                        >
                                            重试
                                        </button>
                                    </div>
                                ) : treeData ? (
                                    renderTree(treeData)
                                ) : lawList.length === 0 && !searchQuery ? (
                                    <div className="text-center py-20">
                                        <div className="inline-flex items-center justify-center w-20 h-20 bg-slate-100 rounded-full mb-6">
                                            <Search className="w-10 h-10 text-slate-400" />
                                        </div>
                                        <h3 className="text-xl font-bold text-slate-700 mb-2">开始搜索法律</h3>
                                        <p className="text-slate-500 font-medium">输入法律名称或关键词，快速查找相关法律条文</p>
                                    </div>
                                ) : lawList.length === 0 ? (
                                    <div className="text-center py-20">
                                        <div className="inline-flex items-center justify-center w-20 h-20 bg-slate-100 rounded-full mb-6">
                                            <FileText className="w-10 h-10 text-slate-400" />
                                        </div>
                                        <h3 className="text-xl font-bold text-slate-700 mb-2">未找到相关法律</h3>
                                        <p className="text-slate-500 font-medium">尝试使用其他关键词进行搜索</p>
                                    </div>
                                ) : (
                                    /* 有搜索结果但法律节点为空 */
                                    <div className="text-center py-20">
                                        <div className="inline-flex items-center justify-center w-20 h-20 bg-gradient-to-br from-blue-50 to-purple-50 rounded-full mb-6">
                                            <Book className="w-10 h-10 text-blue-500" />
                                        </div>
                                        <h3 className="text-xl font-bold text-slate-700 mb-2">该法律暂无层级结构数据</h3>
                                        <p className="text-slate-500 font-medium mb-4">该法律文档尚未完成结构化处理</p>
                                        <p className="text-sm text-slate-400">数据正在整理中，敬请期待</p>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>

                    {/* Detail Panel */}
                    <div className="lg:col-span-5">
                        {currentProvision ? (
                            <div className="bg-white rounded-[2.5rem] border border-slate-200 shadow-2xl shadow-slate-200/50 p-10 sticky top-32 max-h-[calc(100vh-8rem)] overflow-y-auto custom-scrollbar">
                                {/* 法律信息头部 */}
                                {treeData && currentProvision.level === 'document' && (
                                    <div className="mb-8 pb-6 border-b border-slate-100">
                                        <div className="flex items-start justify-between mb-4">
                                            <div>
                                                <h2 className="text-xl font-black text-slate-900 tracking-tight mb-2">{treeData.title}</h2>
                                                <div className="flex flex-wrap gap-2">
                                                    {(() => {
                                                        const law = lawList.find(l => l.source_id === selectedLaw);
                                                        return (
                                                            <>
                                                                {law?.category && (
                                                                    <span className="px-3 py-1 bg-blue-50 text-blue-700 text-xs font-bold rounded-full">
                                                                        {law.category}
                                                                    </span>
                                                                )}
                                                                {law?.status && (
                                                                    <span className={`px-3 py-1 text-xs font-bold rounded-full ${law.status.includes('现行')
                                                                        ? 'bg-emerald-50 text-emerald-700'
                                                                        : 'bg-amber-50 text-amber-700'
                                                                        }`}>
                                                                        {law.status}
                                                                    </span>
                                                                )}
                                                            </>
                                                        );
                                                    })()}
                                                </div>
                                            </div>
                                            <div className="p-3 bg-gradient-to-br from-blue-500 to-purple-600 text-white rounded-2xl shadow-lg">
                                                <Book className="w-6 h-6" />
                                            </div>
                                        </div>
                                        <div className="grid grid-cols-2 gap-3 text-sm">
                                            {(() => {
                                                const law = lawList.find(l => l.source_id === selectedLaw);
                                                return (
                                                    <>
                                                        {law?.issuer && (
                                                            <div className="bg-slate-50 p-3 rounded-xl">
                                                                <p className="text-xs text-slate-400 font-bold mb-1">发布机关</p>
                                                                <p className="text-slate-700 font-medium truncate">{law.issuer}</p>
                                                            </div>
                                                        )}
                                                        {law?.publish_date && (
                                                            <div className="bg-slate-50 p-3 rounded-xl">
                                                                <p className="text-xs text-slate-400 font-bold mb-1">发布日期</p>
                                                                <p className="text-slate-700 font-medium">{law.publish_date}</p>
                                                            </div>
                                                        )}
                                                        {law?.effective_date && (
                                                            <div className="bg-slate-50 p-3 rounded-xl col-span-2">
                                                                <p className="text-xs text-slate-400 font-bold mb-1">生效日期</p>
                                                                <p className="text-slate-700 font-medium">{law.effective_date}</p>
                                                            </div>
                                                        )}
                                                    </>
                                                );
                                            })()}
                                        </div>
                                    </div>
                                )}

                                {/* 节点详情 */}
                                <div className="space-y-6">
                                    <div className="flex items-center gap-4">
                                        <div className={`p-3 rounded-xl ${currentProvision.nodeType === 'ProvisionNode' ? 'bg-emerald-50 text-emerald-600' :
                                            currentProvision.nodeType === 'TocNode' ? 'bg-violet-50 text-violet-600' :
                                                'bg-blue-50 text-blue-600'
                                            }`}>
                                            <FileText className="w-6 h-6" />
                                        </div>
                                        <div>
                                            <h3 className="text-lg font-black text-slate-900 tracking-tight">{currentProvision.title}</h3>
                                            <p className="text-slate-400 text-xs font-bold uppercase tracking-widest">{getNodeLevelText(currentProvision.level)}</p>
                                        </div>
                                    </div>

                                    {/* 法条内容区域 */}
                                    {currentProvision.nodeType === 'ProvisionNode' && currentProvision.content && (
                                        <div className="bg-gradient-to-br from-slate-50 to-blue-50 rounded-2xl p-6 border border-slate-200 relative">
                                            <div className="absolute -left-1 top-6 w-1 h-16 bg-gradient-to-b from-blue-500 to-purple-500 rounded-full" />
                                            <p className="text-slate-800 whitespace-pre-line leading-relaxed text-base font-medium">
                                                {currentProvision.content}
                                            </p>
                                        </div>
                                    )}

                                    {/* 目录节点提示 */}
                                    {currentProvision.nodeType === 'TocNode' && (
                                        <div className="bg-violet-50 rounded-2xl p-6 border border-violet-100">
                                            <div className="flex items-center gap-3 mb-3">
                                                <div className="p-2 bg-violet-500 text-white rounded-lg">
                                                    <Book className="w-4 h-4" />
                                                </div>
                                                <p className="text-violet-900 font-bold">{getNodeLevelText(currentProvision.level)}</p>
                                            </div>
                                            <p className="text-violet-700 text-sm font-medium">点击左侧箭头展开查看详细内容</p>
                                        </div>
                                    )}

                                    {/* 法律文档提示 */}
                                    {currentProvision.nodeType === 'DocumentNode' && (
                                        <div className="bg-gradient-to-br from-blue-500 to-purple-600 rounded-2xl p-6 text-white">
                                            <div className="flex items-center gap-3 mb-3">
                                                <div className="p-2 bg-white/20 rounded-lg">
                                                    <Book className="w-5 h-5" />
                                                </div>
                                                <p className="font-bold">法律文档</p>
                                            </div>
                                            <p className="text-white/90 text-sm font-medium mb-2">{currentProvision.title}</p>
                                            <p className="text-white/70 text-xs">点击展开查看完整的法律层级结构</p>
                                        </div>
                                    )}

                                    {/* 统计信息 */}
                                    <div className="grid grid-cols-3 gap-3">
                                        <div className="bg-slate-50 p-4 rounded-xl border border-slate-100 text-center">
                                            <p className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-1">层级</p>
                                            <p className="text-lg font-black text-slate-900">{getNodeLevelText(currentProvision.level)}</p>
                                        </div>
                                        <div className="bg-slate-50 p-4 rounded-xl border border-slate-100 text-center">
                                            <p className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-1">编号</p>
                                            <p className="text-sm font-black text-slate-900 truncate">{(() => {
                                                const parts = currentProvision.id.split(':');
                                                return parts.length > 2 ? parts.slice(2).join(':') : parts[parts.length - 1];
                                            })()}</p>
                                        </div>
                                        <div className="bg-slate-50 p-4 rounded-xl border border-slate-100 text-center">
                                            <p className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-1">类型</p>
                                            <p className="text-sm font-black text-slate-900">{currentProvision.nodeType === 'ProvisionNode' ? '法条' : currentProvision.nodeType === 'TocNode' ? '目录' : '文档'}</p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <div className="bg-slate-100/50 rounded-[2.5rem] border-4 border-dashed border-slate-200 p-12 text-center sticky top-32 flex flex-col items-center justify-center h-[600px]">
                                <div className="w-28 h-28 bg-white rounded-full flex items-center justify-center shadow-xl mb-8">
                                    <Book className="w-12 h-12 text-slate-300" />
                                </div>
                                <h3 className="text-2xl font-black text-slate-400 mb-3 tracking-tight">未选择法条</h3>
                                <p className="text-slate-400 font-bold max-w-xs">请从左侧列表中选择一个具体法条查看详情，或搜索法律名称开始浏览</p>
                            </div>
                        )}
                    </div>
                </div>

                {/* Knowledge Graph Section - 法律知识图谱 */}
                {currentProvision && (
                    <div className="mt-10">
                        <KnowledgeGraph
                            provisionId={currentProvision.id}
                            provisionTitle={currentProvision.title}
                            height={500}
                        />
                    </div>
                )}


            </div>
        </div>
    );
}
