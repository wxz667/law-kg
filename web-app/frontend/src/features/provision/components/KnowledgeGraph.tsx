import { useEffect, useRef, useState } from 'react';
import { Graph } from '@antv/g6';
import { Network, Share2, Maximize2, ZoomIn, ZoomOut } from 'lucide-react';
import { apiClient } from '@/lib/api-client';

interface NodeData {
    id: string;
    label: string;
    type?: string;
    level?: string;
    content?: string;
}

interface EdgeData {
    source: string;
    target: string;
    label?: string;
    type?: string;
}

interface GraphData {
    nodes: NodeData[];
    edges: EdgeData[];
}

interface KnowledgeGraphProps {
    provisionId: string;
    provisionTitle: string;
    height?: number;
}

export function KnowledgeGraph({
    provisionId,
    provisionTitle,
    height = 500
}: KnowledgeGraphProps) {
    const containerRef = useRef<HTMLDivElement>(null);
    const graphRef = useRef<Graph | null>(null);
    const [fullscreen, setFullscreen] = useState(false);
    const [loading, setLoading] = useState(true);
    const [graphData, setGraphData] = useState<GraphData | null>(null);

    // 从 API 获取图谱数据
    useEffect(() => {
        const fetchGraphData = async () => {
            console.log('🔍 KnowledgeGraph useEffect triggered');
            console.log('📌 provisionId:', provisionId);
            console.log('📌 provisionTitle:', provisionTitle);

            if (!provisionId) {
                console.log('❌ provisionId is empty, skipping API call');
                return;
            }

            setLoading(true);
            try {
                const url = `/graph/subgraph/${encodeURIComponent(provisionId)}?depth=2&limit=50`;
                console.log('🚀 Fetching graph data from:', url);
                const data = await apiClient.get<any>(url);
                console.log('✅ Graph data received:', data);

                // 转换后端返回的数据格式为前端期望的格式
                const transformedData: GraphData = {
                    nodes: (data.nodes || []).map((node: any) => ({
                        id: node.id,
                        label: node.name || node.label || '',
                        type: node.type,
                        level: node.level,
                        content: node.text || node.content,
                    })),
                    edges: (data.edges || []).map((edge: any) => ({
                        source: edge.start || edge.source,
                        target: edge.end || edge.target,
                        label: edge.type || edge.label,
                        type: edge.type,
                    })),
                };

                console.log('✅ Transformed data:', transformedData);
                setGraphData(transformedData);
            } catch (error) {
                console.error('❌ 获取图谱数据失败:', error);
                setGraphData({ nodes: [], edges: [] });
            } finally {
                setLoading(false);
            }
        };
        fetchGraphData();
    }, [provisionId]);

    // 初始化 G6 Graph
    useEffect(() => {
        if (!containerRef.current || !graphData || graphData.nodes.length === 0) {
            console.log('⚠️ Graph initialization skipped: no data or container');
            console.log('  - containerRef:', !!containerRef.current);
            console.log('  - graphData:', !!graphData);
            console.log('  - nodes count:', graphData?.nodes.length);
            return;
        }

        console.log('🎨 Starting G6 initialization with', graphData.nodes.length, 'nodes and', graphData.edges.length, 'edges');

        // 销毁旧实例
        if (graphRef.current) {
            console.log('🗑️ Destroying old graph instance');
            graphRef.current.destroy();
            graphRef.current = null;
        }

        try {
            // 判断节点类型
            const getNodeType = (node: NodeData): string => {
                if (node.id === provisionId) return 'current';
                if (node.level === 'document') return 'document';
                if (node.level === 'article') return 'article';
                return 'related';
            };

            // 节点颜色映射
            const colors: Record<string, { fill: string; stroke: string }> = {
                current: { fill: '#3b82f6', stroke: '#2563eb' },
                document: { fill: '#f59e0b', stroke: '#d97706' },
                article: { fill: '#10b981', stroke: '#059669' },
                related: { fill: '#8b5cf6', stroke: '#7c3aed' },
            };

            const width = containerRef.current.clientWidth;
            const centerX = width / 2;
            const centerY = height / 2;
            const radius = Math.min(width, height) * 0.35;  // 半径为画布较小边的 35%

            // 找到中心节点（文档节点）
            const centerNode = graphData.nodes.find(n => n.level === 'document') || graphData.nodes[0];
            const leafNodes = graphData.nodes.filter(n => n.id !== centerNode.id);

            // 手动计算节点位置
            const positionedNodes = [
                // 中心节点
                {
                    id: centerNode.id,
                    data: { label: centerNode.label },
                    style: {
                        size: 40,
                        x: centerX,
                        y: centerY,
                        fill: colors['document']?.fill || '#f59e0b',
                        stroke: colors['document']?.stroke || '#d97706',
                        lineWidth: 4,
                        shadowBlur: 10,
                        shadowColor: colors['document']?.fill || '#f59e0b',
                        shadowOffsetX: 0,
                        shadowOffsetY: 2,
                        labelText: centerNode.label,
                        labelPlacement: 'bottom' as const,
                        labelOffsetY: 10,
                        labelFontSize: 14,
                        labelFontWeight: 700,
                        labelFill: '#1f2937',
                        labelBackground: true,
                        labelBackgroundFill: '#ffffff',
                        labelBackgroundPadding: [4, 8],
                        labelBackgroundRadius: 4,
                        labelBackgroundStroke: colors['document']?.stroke || '#d97706',
                        labelBackgroundLineWidth: 1.5,
                    },
                },
                // 外围节点均匀分布
                ...leafNodes.map((node, index) => {
                    const angle = (2 * Math.PI * index) / leafNodes.length - Math.PI / 2;  // 从顶部开始
                    const nodeType = getNodeType(node);
                    const isCurrent = nodeType === 'current';
                    const x = centerX + radius * Math.cos(angle);
                    const y = centerY + radius * Math.sin(angle);

                    return {
                        id: node.id,
                        data: { label: node.label },
                        style: {
                            size: isCurrent ? 40 : 35,
                            x: x,
                            y: y,
                            fill: colors[nodeType]?.fill || '#3b82f6',
                            stroke: colors[nodeType]?.stroke || '#2563eb',
                            lineWidth: isCurrent ? 4 : 2,
                            shadowBlur: 10,
                            shadowColor: colors[nodeType]?.fill || '#3b82f6',
                            shadowOffsetX: 0,
                            shadowOffsetY: 2,
                            labelText: node.label,
                            labelPlacement: 'bottom' as const,
                            labelOffsetY: 10,
                            labelFontSize: isCurrent ? 14 : 13,
                            labelFontWeight: isCurrent ? 700 : 600,
                            labelFill: '#1f2937',
                            labelBackground: true,
                            labelBackgroundFill: '#ffffff',
                            labelBackgroundPadding: [4, 8],
                            labelBackgroundRadius: 4,
                            labelBackgroundStroke: colors[nodeType]?.stroke || '#3b82f6',
                            labelBackgroundLineWidth: 1.5,
                        },
                    };
                }),
            ];

            // 根据关系类型设置不同的样式
            const edgeStyleMap: Record<string, { stroke: string; lineWidth: number; lineDash?: number[] }> = {
                'CONTAINS': { stroke: '#3b82f6', lineWidth: 2.5 },  // 包含 - 蓝色实线
                'MENTIONS': { stroke: '#f59e0b', lineWidth: 2, lineDash: [5, 5] },  // 提及 - 橙色虚线
                'CITES': { stroke: '#10b981', lineWidth: 2, lineDash: [10, 5] },  // 引用 - 绿色长虚线
                'RELATED': { stroke: '#8b5cf6', lineWidth: 2, lineDash: [2, 2] },  // 相关 - 紫色点线
                'SUBSECTION': { stroke: '#ef4444', lineWidth: 2 },  // 下级 - 红色实线
                'INTERPRETS': { stroke: '#ec4899', lineWidth: 2, lineDash: [8, 4] },  // 解释 - 粉色虚线
            };

            const graph = new Graph({
                container: containerRef.current,
                width: width,
                height: height,
                data: {
                    nodes: positionedNodes,
                    edges: graphData.edges.map(edge => {
                        const relType = edge.label || edge.type || 'CONTAINS';
                        const style = edgeStyleMap[relType] || { stroke: '#94a3b8', lineWidth: 2 };
                        return {
                            source: edge.source,
                            target: edge.target,
                            label: relType,
                            style: {
                                ...style,
                                endArrow: true,
                            },
                        };
                    }),
                },
                layout: { type: 'preset' },  // 使用预设坐标，不进行布局计算
                behaviors: ['zoom-canvas', 'drag-canvas', 'drag-element'],
            });

            console.log('🎨 Rendering graph...');
            graph.render();
            graphRef.current = graph;
            console.log('✅ Graph rendered successfully');

            // 监听窗口大小变化
            const handleResize = () => {
                if (containerRef.current && graph) {
                    graph.setSize(containerRef.current.clientWidth, height);
                }
            };

            window.addEventListener('resize', handleResize);

            return () => {
                window.removeEventListener('resize', handleResize);
                if (graphRef.current) {
                    graphRef.current.destroy();
                    graphRef.current = null;
                }
            };
        } catch (error) {
            console.error('G6 初始化失败:', error);
        }
    }, [graphData, provisionId, height]);

    // 全屏查看
    const toggleFullscreen = () => {
        setFullscreen(!fullscreen);
    };

    // 缩放控制
    const zoomIn = () => {
        if (graphRef.current) {
            const currentZoom = graphRef.current.getZoom();
            graphRef.current.zoomTo(currentZoom * 1.2);
        }
    };

    const zoomOut = () => {
        if (graphRef.current) {
            const currentZoom = graphRef.current.getZoom();
            graphRef.current.zoomTo(currentZoom * 0.8);
        }
    };

    // 重置视图
    const resetView = () => {
        if (graphRef.current) {
            graphRef.current.fitView();
        }
    };

    return (
        <div className={`bg-white rounded-[2.5rem] border border-slate-200 overflow-hidden ${fullscreen ? 'fixed inset-0 z-50 p-8' : ''}`}>
            {/* Header Toolbar */}
            <div className="px-10 py-6 border-b border-slate-100 bg-gradient-to-r from-blue-50 to-indigo-50 flex items-center justify-between">
                <div className="flex items-center gap-4">
                    <div className="p-3 bg-blue-500 text-white rounded-2xl shadow-lg shadow-blue-500/20">
                        <Network className="w-6 h-6" />
                    </div>
                    <div>
                        <h2 className="text-xl font-black text-slate-900 tracking-tight">法律知识图谱</h2>
                        <p className="text-xs font-bold text-slate-400 uppercase tracking-widest mt-1">
                            Legal Knowledge Graph
                        </p>
                    </div>
                </div>

                <div className="flex items-center gap-3">
                    <button
                        onClick={zoomIn}
                        className="p-3 bg-white text-slate-600 hover:text-blue-600 hover:bg-blue-50 rounded-xl transition-all border border-slate-100"
                        title="放大"
                    >
                        <ZoomIn className="w-4 h-4" />
                    </button>
                    <button
                        onClick={zoomOut}
                        className="p-3 bg-white text-slate-600 hover:text-blue-600 hover:bg-blue-50 rounded-xl transition-all border border-slate-100"
                        title="缩小"
                    >
                        <ZoomOut className="w-4 h-4" />
                    </button>
                    <button
                        onClick={resetView}
                        className="p-3 bg-white text-slate-600 hover:text-blue-600 hover:bg-blue-50 rounded-xl transition-all border border-slate-100"
                        title="重置视图"
                    >
                        <Maximize2 className="w-4 h-4" />
                    </button>
                    <button
                        onClick={toggleFullscreen}
                        className="flex items-center gap-2 px-5 py-3 bg-slate-900 text-white rounded-xl font-bold hover:bg-slate-800 transition-all shadow-lg shadow-slate-900/20"
                    >
                        <Share2 className="w-4 h-4" />
                        <span>{fullscreen ? '退出全屏' : '全屏查看'}</span>
                    </button>
                </div>
            </div>

            {/* Graph Container */}
            <div
                ref={containerRef}
                className="relative"
                style={{ height: fullscreen ? 'calc(100vh - 200px)' : `${height}px` }}
            >
                {/* Loading State */}
                {loading && (
                    <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                        <div className="flex flex-col items-center space-y-4">
                            <div className="relative w-16 h-16">
                                <div className="absolute inset-0 border-4 border-slate-200 rounded-full" />
                                <div className="absolute inset-0 border-4 border-blue-600 rounded-full border-t-transparent animate-spin" />
                            </div>
                            <p className="text-slate-400 font-bold uppercase tracking-widest text-sm">正在加载图谱...</p>
                        </div>
                    </div>
                )}

                {/* Empty State */}
                {!loading && graphData && graphData.nodes.length === 0 && (
                    <div className="absolute inset-0 flex items-center justify-center">
                        <div className="flex flex-col items-center space-y-4">
                            <div className="w-20 h-20 bg-slate-100 rounded-full flex items-center justify-center">
                                <Network className="w-10 h-10 text-slate-300" />
                            </div>
                            <p className="text-slate-400 font-bold text-lg">暂无图谱数据</p>
                            <p className="text-slate-300 text-sm">该法条还没有关联的知识图谱关系</p>
                        </div>
                    </div>
                )}
            </div>

            {/* Legend */}
            <div className="px-10 py-6 border-t border-slate-100 bg-slate-50">
                <div className="flex items-center justify-between">
                    <div className="flex flex-col gap-3">
                        {/* 节点类型图例 */}
                        <div className="flex items-center gap-6">
                            <div className="flex items-center gap-3">
                                <div className="w-4 h-4 rounded-full bg-blue-500 border-2 border-blue-600" />
                                <span className="text-sm font-bold text-slate-600">当前法条</span>
                            </div>
                            <div className="flex items-center gap-3">
                                <div className="w-4 h-4 rounded-full bg-amber-500 border-2 border-amber-600" />
                                <span className="text-sm font-bold text-slate-600">法律文档</span>
                            </div>
                            <div className="flex items-center gap-3">
                                <div className="w-4 h-4 rounded-full bg-emerald-500 border-2 border-emerald-600" />
                                <span className="text-sm font-bold text-slate-600">法条节点</span>
                            </div>
                            <div className="flex items-center gap-3">
                                <div className="w-4 h-4 rounded-full bg-violet-500 border-2 border-violet-600" />
                                <span className="text-sm font-bold text-slate-600">相关节点</span>
                            </div>
                        </div>

                        {/* 关系类型图例 */}
                        <div className="flex items-center gap-6 pt-2 border-t border-slate-200">
                            <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">关系类型：</span>
                            <div className="flex items-center gap-2">
                                <svg width="30" height="8"><line x1="0" y1="4" x2="30" y2="4" stroke="#3b82f6" strokeWidth="2.5" /></svg>
                                <span className="text-xs text-slate-600">包含</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <svg width="30" height="8"><line x1="0" y1="4" x2="30" y2="4" stroke="#f59e0b" strokeWidth="2" strokeDasharray="5,5" /></svg>
                                <span className="text-xs text-slate-600">提及</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <svg width="30" height="8"><line x1="0" y1="4" x2="30" y2="4" stroke="#10b981" strokeWidth="2" strokeDasharray="10,5" /></svg>
                                <span className="text-xs text-slate-600">引用</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <svg width="30" height="8"><line x1="0" y1="4" x2="30" y2="4" stroke="#8b5cf6" strokeWidth="2" strokeDasharray="2,2" /></svg>
                                <span className="text-xs text-slate-600">相关</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <svg width="30" height="8"><line x1="0" y1="4" x2="30" y2="4" stroke="#ef4444" strokeWidth="2" /></svg>
                                <span className="text-xs text-slate-600">下级</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <svg width="30" height="8"><line x1="0" y1="4" x2="30" y2="4" stroke="#ec4899" strokeWidth="2" strokeDasharray="8,4" /></svg>
                                <span className="text-xs text-slate-600">解释</span>
                            </div>
                        </div>
                    </div>

                    <div className="flex items-center gap-2 text-sm text-slate-500">
                        <span className="font-bold">提示：</span>
                        <span>滚轮缩放 · 拖拽移动 · 悬停激活</span>
                    </div>
                </div>
            </div>
        </div>
    );
}
