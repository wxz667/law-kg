import { useState } from 'react';
import { ChevronRight, FileText, Book, BookOpen, Layers, List, Type } from 'lucide-react';
import type { TreeNode as TreeNodeType } from '../types';

interface TreeNodeProps {
    node: TreeNodeType;
    depth?: number;
    selectedId?: string;
    onSelect: (node: TreeNodeType) => void;
    onExpand?: (node: TreeNodeType) => void;
}

const levelIcons: Record<string, React.ElementType> = {
    document: Book,
    part: BookOpen,
    chapter: Layers,
    section: Layers,
    article: FileText,
    paragraph: Type,
    item: List,
    sub_item: List,
};

const levelColors: Record<string, string> = {
    document: 'text-blue-600',
    part: 'text-purple-600',
    chapter: 'text-green-600',
    section: 'text-yellow-600',
    article: 'text-red-600',
    paragraph: 'text-gray-700',
    item: 'text-gray-600',
    sub_item: 'text-gray-500',
};

export function TreeNode({ node, depth = 0, selectedId, onSelect, onExpand }: TreeNodeProps) {
    const [isExpanded, setIsExpanded] = useState(false);
    const [isLoaded, setIsLoaded] = useState(false);

    const hasChildren = node.hasChildren || (node.children && node.children.length > 0);
    const Icon = levelIcons[node.level] || FileText;
    const colorClass = levelColors[node.level] || 'text-gray-700';
    const isSelected = selectedId === node.id;

    const handleToggle = async (e: React.MouseEvent) => {
        e.stopPropagation();

        if (hasChildren) {
            // 懒加载：第一次展开时加载子节点
            if (!isLoaded && node.hasChildren && onExpand) {
                await onExpand(node);
                setIsLoaded(true);
            }
            setIsExpanded(!isExpanded);
        }
    };

    const handleClick = () => {
        onSelect(node);
    };

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            handleToggle(e as any);
            handleClick();
        } else if (e.key === 'ArrowRight') {
            if (!isExpanded && hasChildren) {
                setIsExpanded(true);
            }
        } else if (e.key === 'ArrowLeft') {
            if (isExpanded) {
                setIsExpanded(false);
            }
        }
    };

    return (
        <div>
            <div
                className={`
                    flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer
                    ${isSelected ? 'bg-blue-100 text-blue-800' : 'hover:bg-gray-100'}
                    ${depth > 0 ? 'ml-4' : ''}
                `}
                onClick={handleClick}
                onKeyDown={handleKeyDown}
                tabIndex={0}
                role="treeitem"
                aria-selected={isSelected}
                aria-expanded={hasChildren ? isExpanded : undefined}
            >
                {/* 展开/折叠图标 */}
                {hasChildren ? (
                    <ChevronRight
                        className={`w-4 h-4 flex-shrink-0 transition-transform ${isExpanded ? 'transform rotate-90' : ''
                            }`}
                    />
                ) : (
                    <div className="w-4 h-4 flex-shrink-0" />
                )}

                {/* 节点图标 */}
                <Icon className={`w-4 h-4 flex-shrink-0 ${colorClass}`} />

                {/* 节点名称 */}
                <span className="text-sm truncate flex-1">{node.name}</span>
            </div>

            {/* 子节点 */}
            {isExpanded && node.children && (
                <div role="group">
                    {node.children.map((child) => (
                        <TreeNode
                            key={child.id}
                            node={child}
                            depth={depth + 1}
                            selectedId={selectedId}
                            onSelect={onSelect}
                            onExpand={onExpand}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}
