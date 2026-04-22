import { useState, useEffect } from 'react';
import { Search, X } from 'lucide-react';
import { TreeNode } from './TreeNode';
import { Input } from '@/components/ui';
import type { TreeNode as TreeNodeType } from '../types';

interface ProvisionTreeProps {
    selectedId?: string;
    onSelect?: (node: TreeNodeType) => void;
}

export function ProvisionTree({ selectedId, onSelect }: ProvisionTreeProps) {
    const [treeData, setTreeData] = useState<TreeNodeType[]>([]);
    const [loading, setLoading] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [isSearching, setIsSearching] = useState(false);
    const [searchResults, setSearchResults] = useState<TreeNodeType[]>([]);

    // 加载根节点数据
    useEffect(() => {
        loadRootNodes();
    }, []);

    const loadRootNodes = async () => {
        try {
            setLoading(true);
            // TODO: 调用实际 API 获取根节点
            // const response = await apiClient.get<TreeNodeType[]>('/graph/nodes/root');

            // Mock 数据
            const mockData: TreeNodeType[] = [
                {
                    id: 'doc_1',
                    name: '中华人民共和国民法典',
                    level: 'document',
                    type: 'DocumentNode',
                    hasChildren: true,
                },
                {
                    id: 'doc_2',
                    name: '中华人民共和国刑法',
                    level: 'document',
                    type: 'DocumentNode',
                    hasChildren: true,
                },
            ];

            setTreeData(mockData);
        } catch (error) {
            console.error('加载根节点失败:', error);
        } finally {
            setLoading(false);
        }
    };

    // 懒加载子节点
    const handleExpand = async (node: TreeNodeType) => {
        try {
            // TODO: 调用实际 API 获取子节点
            // const response = await apiClient.get<TreeNodeType[]>(`/graph/node/${node.id}/children`);

            // Mock 数据
            const mockChildren: TreeNodeType[] = [
                {
                    id: `${node.id}_part_1`,
                    name: '第一编 总则',
                    level: 'part',
                    type: 'TocNode',
                    hasChildren: true,
                },
                {
                    id: `${node.id}_part_2`,
                    name: '第二编 物权',
                    level: 'part',
                    type: 'TocNode',
                    hasChildren: true,
                },
            ];

            // 更新树数据
            const updatedTree = updateNodeChildren(treeData, node.id, mockChildren);
            setTreeData(updatedTree);
        } catch (error) {
            console.error('加载子节点失败:', error);
        }
    };

    // 更新节点子节点
    const updateNodeChildren = (
        nodes: TreeNodeType[],
        nodeId: string,
        children: TreeNodeType[]
    ): TreeNodeType[] => {
        return nodes.map(node => {
            if (node.id === nodeId) {
                return { ...node, children };
            }
            if (node.children) {
                return {
                    ...node,
                    children: updateNodeChildren(node.children, nodeId, children),
                };
            }
            return node;
        });
    };

    // 搜索处理
    const handleSearch = async (query: string) => {
        setSearchQuery(query);

        if (!query.trim()) {
            setIsSearching(false);
            setSearchResults([]);
            return;
        }

        setIsSearching(true);
        try {
            // TODO: 调用实际 API 搜索
            // const response = await apiClient.get<TreeNodeType[]>('/graph/search', { 
            //     params: { q: query, field: 'name' } 
            // });

            // Mock 搜索结果
            const mockResults: TreeNodeType[] = [
                {
                    id: 'search_1',
                    name: `搜索结果：${query}`,
                    level: 'article',
                    type: 'ProvisionNode',
                },
            ];

            setSearchResults(mockResults);
        } catch (error) {
            console.error('搜索失败:', error);
        } finally {
            // 保持搜索状态
        }
    };

    const clearSearch = () => {
        setSearchQuery('');
        setIsSearching(false);
        setSearchResults([]);
    };

    const handleSelect = (node: TreeNodeType) => {
        if (onSelect) {
            onSelect(node);
        }
    };

    return (
        <div className="h-full flex flex-col bg-white border-r">
            {/* 搜索栏 */}
            <div className="p-3 border-b">
                <div className="relative">
                    <Input
                        placeholder="搜索法条..."
                        value={searchQuery}
                        onChange={(e) => handleSearch(e.target.value)}
                        leftAddon={<Search className="w-4 h-4" />}
                        rightAddon={
                            searchQuery && (
                                <button
                                    onClick={clearSearch}
                                    className="text-gray-400 hover:text-gray-600"
                                >
                                    <X className="w-4 h-4" />
                                </button>
                            )
                        }
                    />
                </div>
            </div>

            {/* 树形列表 */}
            <div className="flex-1 overflow-y-auto p-2" role="tree">
                {loading ? (
                    <div className="flex items-center justify-center py-8">
                        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
                    </div>
                ) : isSearching ? (
                    <div>
                        <div className="text-sm text-gray-500 mb-2 px-2">
                            搜索结果
                        </div>
                        {searchResults.map((node) => (
                            <TreeNode
                                key={node.id}
                                node={node}
                                selectedId={selectedId}
                                onSelect={handleSelect}
                                onExpand={handleExpand}
                            />
                        ))}
                    </div>
                ) : (
                    treeData.map((node) => (
                        <TreeNode
                            key={node.id}
                            node={node}
                            selectedId={selectedId}
                            onSelect={handleSelect}
                            onExpand={handleExpand}
                        />
                    ))
                )}
            </div>
        </div>
    );
}
