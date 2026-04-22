import { useEffect, useRef, useState } from 'react';
import { Graph } from '@antv/g6';
import { Network, Maximize2, ZoomIn, ZoomOut, RotateCcw } from 'lucide-react';

interface LawNode {
    id: string;
    label: string;
    type: 'civil' | 'criminal' | 'administrative' | 'commercial' | 'constitutional';
    level: number;
    articleCount?: number;
}

interface LawEdge {
    source: string;
    target: string;
    label: string;
    type: 'references' | 'amends' | 'implements' | 'relates';
}

interface LawGraphData {
    nodes: LawNode[];
    edges: LawEdge[];
}

export function LawKnowledgeGraph() {
    const containerRef = useRef<HTMLDivElement>(null);
    const graphRef = useRef<Graph | null>(null);
    const [loading, setLoading] = useState(true);

    // Mock 数据 - 展示各个法律之间的关系图谱
    const generateMockLawGraphData = (): LawGraphData => {
        const nodes: LawNode[] = [
            {
                id: 'constitution',
                label: '宪法',
                type: 'constitutional',
                level: 1,
                articleCount: 143
            },
            {
                id: 'civil_code',
                label: '民法典',
                type: 'civil',
                level: 2,
                articleCount: 1260
            },
            {
                id: 'criminal_law',
                label: '刑法',
                type: 'criminal',
                level: 2,
                articleCount: 452
            },
            {
                id: 'admin_procedure',
                label: '行政诉讼法',
                type: 'administrative',
                level: 2,
                articleCount: 103
            },
            {
                id: 'civil_procedure',
                label: '民事诉讼法',
                type: 'civil',
                level: 2,
                articleCount: 284
            },
            {
                id: 'company_law',
                label: '公司法',
                type: 'commercial',
                level: 3,
                articleCount: 218
            },
            {
                id: 'contract_law',
                label: '合同法（已废止）',
                type: 'civil',
                level: 3,
                articleCount: 428
            },
            {
                id: 'property_law',
                label: '物权法（已废止）',
                type: 'civil',
                level: 3,
                articleCount: 247
            },
            {
                id: 'marriage_law',
                label: '婚姻法（已废止）',
                type: 'civil',
                level: 3,
                articleCount: 51
            },
            {
                id: 'succession_law',
                label: '继承法（已废止）',
                type: 'civil',
                level: 3,
                articleCount: 37
            },
            {
                id: 'tort_liability',
                label: '侵权责任法（已废止）',
                type: 'civil',
                level: 3,
                articleCount: 92
            },
            {
                id: 'labor_contract',
                label: '劳动合同法',
                type: 'civil',
                level: 3,
                articleCount: 98
            },
            {
                id: 'intellectual_property',
                label: '知识产权法',
                type: 'civil',
                level: 3,
                articleCount: 156
            }
        ];

        const edges: LawEdge[] = [
            // 宪法与其他法律的关系
            { source: 'constitution', target: 'civil_code', label: '上位法', type: 'references' },
            { source: 'constitution', target: 'criminal_law', label: '上位法', type: 'references' },
            { source: 'constitution', target: 'admin_procedure', label: '上位法', type: 'references' },
            { source: 'constitution', target: 'civil_procedure', label: '上位法', type: 'references' },

            // 民法典整合了之前的单行法
            { source: 'civil_code', target: 'contract_law', label: '吸收', type: 'amends' },
            { source: 'civil_code', target: 'property_law', label: '吸收', type: 'amends' },
            { source: 'civil_code', target: 'marriage_law', label: '吸收', type: 'amends' },
            { source: 'civil_code', target: 'succession_law', label: '吸收', type: 'amends' },
            { source: 'civil_code', target: 'tort_liability', label: '吸收', type: 'amends' },

            // 程序法与实体法的关系
            { source: 'civil_procedure', target: 'civil_code', label: '程序保障', type: 'implements' },
            { source: 'admin_procedure', target: 'criminal_law', label: '程序衔接', type: 'relates' },

            // 特别法与一般法的关系
            { source: 'company_law', target: 'civil_code', label: '特别法', type: 'references' },
            { source: 'labor_contract', target: 'civil_code', label: '特别法', type: 'references' },
            { source: 'intellectual_property', target: 'civil_code', label: '特别法', type: 'references' },

            // 其他关联关系
            { source: 'criminal_law', target: 'civil_code', label: '刑民交叉', type: 'relates' },
            { source: 'company_law', target: 'criminal_law', label: '经济犯罪', type: 'relates' }
        ];

        return { nodes, edges };
    };

    useEffect(() => {
        if (!containerRef.current) return;

        const data = generateMockLawGraphData();

        // 初始化 G6 Graph
        const graph = new Graph({
            container: containerRef.current!,
            width: containerRef.current.clientWidth,
            height: 600,
            layout: {
                type: 'force',
                preventOverlap: true,
                linkDistance: (d: any) => {
                    // 根据边的类型设置不同的距离
                    if (d.label === '上位法') return 200;
                    if (d.label === '吸收') return 150;
                    return 180;
                },
                nodeStrength: -400,
                edgeStrength: 0.15,
                alpha: 0.1,
            },
            node: {
                style: (node: any) => {
                    const colors = {
                        constitutional: '#ef4444', // 红色 - 宪法
                        civil: '#3b82f6',          // 蓝色 - 民法
                        criminal: '#8b5cf6',       // 紫色 - 刑法
                        administrative: '#f59e0b', // 橙色 - 行政法
                        commercial: '#10b981'      // 绿色 - 商法
                    };

                    const sizes = {
                        1: 80,   // 宪法最大
                        2: 65,   // 基本法律
                        3: 50    // 单行法
                    };

                    return {
                        size: (sizes as Record<number, number>)[node.level] || 50,
                        fill: (colors as Record<string, string>)[node.type] || '#3b82f6',
                        stroke: '#ffffff',
                        lineWidth: 3,
                        shadowColor: (colors as Record<string, string>)[node.type] || '#3b82f6',
                        shadowBlur: 20,
                        label: node.label,
                        labelCfg: {
                            position: 'bottom',
                            offset: 10,
                            style: {
                                fontSize: 13,
                                fill: '#1e293b',
                                fontWeight: 700,
                                background: {
                                    fill: '#ffffff',
                                    stroke: '#e2e8f0',
                                    padding: [4, 8, 4, 8],
                                    radius: 6
                                }
                            }
                        }
                    };
                },
            },
            edge: {
                style: (edge: any) => {
                    const colors = {
                        references: '#94a3b8',
                        amends: '#ef4444',
                        implements: '#3b82f6',
                        relates: '#10b981'
                    };

                    return {
                        stroke: (colors as Record<string, string>)[edge.type] || '#94a3b8',
                        lineWidth: 2,
                        opacity: 0.8,
                        lineDash: edge.type === 'amends' ? [5, 5] : undefined,
                        label: edge.label,
                        labelCfg: {
                            autoRotate: true,
                            style: {
                                fontSize: 11,
                                fill: '#475569',
                                fontWeight: 600,
                                background: {
                                    fill: '#ffffff',
                                    stroke: '#e2e8f0',
                                    padding: [2, 6, 2, 6],
                                    radius: 4
                                }
                            }
                        }
                    };
                },
            },
            behaviors: [
                'zoom-canvas',
                'drag-canvas',
                'drag-element',
            ],
            plugins: [
                {
                    type: 'tooltip',
                    trigger: 'mouseenter',
                    getContent: (evt: any) => {
                        const model = evt.item?.getModel();
                        if (!model) return '';
                        return `
                            <div style="padding: 12px; min-width: 150px;">
                                <div style="font-weight: bold; margin-bottom: 8px; color: #1e293b;">${model.label}</div>
                                <div style="font-size: 12px; color: #64748b;">
                                    ${model.articleCount ? `<div>条文数量：${model.articleCount}条</div>` : ''}
                                    <div>层级：${model.level === 1 ? '根本大法' : model.level === 2 ? '基本法律' : '单行法律'}</div>
                                </div>
                            </div>
                        `;
                    }
                }
            ]
        });

        // 转换数据格式
        const graphData = {
            nodes: data.nodes.map(node => ({
                id: node.id,
                label: node.label,
                type: node.type,
                level: node.level,
                articleCount: node.articleCount
            })),
            edges: data.edges.map(edge => ({
                source: edge.source,
                target: edge.target,
                label: edge.label,
                type: edge.type
            })),
        };

        graph.setData(graphData);
        graph.render();

        // 自动适应视图
        setTimeout(() => {
            graph.fitView();
        }, 100);

        graphRef.current = graph as any;
        setLoading(false);

        // 监听窗口大小变化
        const handleResize = () => {
            if (containerRef.current && graph) {
                (graph as any).changeSize(containerRef.current.clientWidth, 600);
            }
        };

        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
            graph.destroy();
        };
    }, []);

    // 缩放控制
    const zoomIn = () => {
        if (graphRef.current) {
            (graphRef.current as any).zoom(1.2);
        }
    };

    const zoomOut = () => {
        if (graphRef.current) {
            (graphRef.current as any).zoom(0.8);
        }
    };

    const resetView = () => {
        if (graphRef.current) {
            graphRef.current.fitView();
        }
    };

    return (
        <div className="bg-white rounded-[2.5rem] border border-slate-200 overflow-hidden shadow-sm">
            {/* Header */}
            <div className="px-10 py-6 border-b border-slate-100 bg-gradient-to-r from-indigo-50 via-purple-50 to-pink-50">
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <div className="p-3 bg-gradient-to-br from-indigo-500 to-purple-600 text-white rounded-2xl shadow-lg shadow-indigo-500/30">
                            <Network className="w-6 h-6" />
                        </div>
                        <div>
                            <h2 className="text-xl font-black text-slate-900 tracking-tight">法律知识体系图谱</h2>
                            <p className="text-xs font-bold text-slate-500 uppercase tracking-widest mt-1">
                                Legal System Knowledge Graph
                            </p>
                        </div>
                    </div>

                    <div className="flex items-center gap-3">
                        <button
                            onClick={zoomIn}
                            className="p-3 bg-white text-slate-600 hover:text-indigo-600 hover:bg-indigo-50 rounded-xl transition-all border border-slate-200 shadow-sm"
                            title="放大"
                        >
                            <ZoomIn className="w-4 h-4" />
                        </button>
                        <button
                            onClick={zoomOut}
                            className="p-3 bg-white text-slate-600 hover:text-indigo-600 hover:bg-indigo-50 rounded-xl transition-all border border-slate-200 shadow-sm"
                            title="缩小"
                        >
                            <ZoomOut className="w-4 h-4" />
                        </button>
                        <button
                            onClick={resetView}
                            className="p-3 bg-white text-slate-600 hover:text-indigo-600 hover:bg-indigo-50 rounded-xl transition-all border border-slate-200 shadow-sm"
                            title="重置视图"
                        >
                            <RotateCcw className="w-4 h-4" />
                        </button>
                        <button
                            className="flex items-center gap-2 px-5 py-3 bg-slate-900 text-white rounded-xl font-bold hover:bg-slate-800 transition-all shadow-lg shadow-slate-900/20"
                        >
                            <Maximize2 className="w-4 h-4" />
                            <span>全屏查看</span>
                        </button>
                    </div>
                </div>
            </div>

            {/* Graph Container */}
            <div
                ref={containerRef}
                className="relative bg-gradient-to-br from-slate-50 to-slate-100"
                style={{ height: '600px' }}
            >
                {/* Loading State */}
                {loading && (
                    <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                        <div className="flex flex-col items-center space-y-4">
                            <div className="relative w-16 h-16">
                                <div className="absolute inset-0 border-4 border-slate-200 rounded-full" />
                                <div className="absolute inset-0 border-4 border-indigo-600 rounded-full border-t-transparent animate-spin" />
                            </div>
                            <p className="text-slate-500 font-bold uppercase tracking-widest text-sm">正在加载法律图谱...</p>
                        </div>
                    </div>
                )}

                {/* Info Overlay */}
                {!loading && (
                    <div className="absolute top-4 left-4 bg-white/90 backdrop-blur-sm rounded-2xl p-4 border border-slate-200 shadow-lg max-w-xs">
                        <h3 className="font-black text-slate-900 mb-2 text-sm">图谱说明</h3>
                        <ul className="space-y-2 text-xs text-slate-600">
                            <li className="flex items-start gap-2">
                                <span className="w-2 h-2 rounded-full bg-red-500 mt-1.5 flex-shrink-0"></span>
                                <span><strong>红色节点：</strong>宪法（根本大法）</span>
                            </li>
                            <li className="flex items-start gap-2">
                                <span className="w-2 h-2 rounded-full bg-blue-500 mt-1.5 flex-shrink-0"></span>
                                <span><strong>蓝色节点：</strong>民事法律</span>
                            </li>
                            <li className="flex items-start gap-2">
                                <span className="w-2 h-2 rounded-full bg-purple-500 mt-1.5 flex-shrink-0"></span>
                                <span><strong>紫色节点：</strong>刑事法律</span>
                            </li>
                            <li className="flex items-start gap-2">
                                <span className="w-2 h-2 rounded-full bg-orange-500 mt-1.5 flex-shrink-0"></span>
                                <span><strong>橙色节点：</strong>行政法律</span>
                            </li>
                            <li className="flex items-start gap-2">
                                <span className="w-2 h-2 rounded-full bg-green-500 mt-1.5 flex-shrink-0"></span>
                                <span><strong>绿色节点：</strong>商事法律</span>
                            </li>
                        </ul>
                    </div>
                )}
            </div>

            {/* Legend & Stats */}
            <div className="px-10 py-6 border-t border-slate-100 bg-slate-50">
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-8">
                        <div className="flex items-center gap-3">
                            <div className="w-4 h-4 rounded-full bg-red-500 border-2 border-red-600 shadow-sm" />
                            <span className="text-sm font-bold text-slate-700">宪法</span>
                        </div>
                        <div className="flex items-center gap-3">
                            <div className="w-4 h-4 rounded-full bg-blue-500 border-2 border-blue-600 shadow-sm" />
                            <span className="text-sm font-bold text-slate-700">民法</span>
                        </div>
                        <div className="flex items-center gap-3">
                            <div className="w-4 h-4 rounded-full bg-purple-500 border-2 border-purple-600 shadow-sm" />
                            <span className="text-sm font-bold text-slate-700">刑法</span>
                        </div>
                        <div className="flex items-center gap-3">
                            <div className="w-4 h-4 rounded-full bg-orange-500 border-2 border-orange-600 shadow-sm" />
                            <span className="text-sm font-bold text-slate-700">行政法</span>
                        </div>
                        <div className="flex items-center gap-3">
                            <div className="w-4 h-4 rounded-full bg-green-500 border-2 border-green-600 shadow-sm" />
                            <span className="text-sm font-bold text-slate-700">商法</span>
                        </div>
                    </div>

                    <div className="flex items-center gap-6 text-sm text-slate-600">
                        <div className="flex items-center gap-2">
                            <span className="font-bold">法律总数：</span>
                            <span className="font-black text-slate-900">13 部</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className="font-bold">关联关系：</span>
                            <span className="font-black text-slate-900">18 条</span>
                        </div>
                    </div>
                </div>

                <div className="mt-4 pt-4 border-t border-slate-200">
                    <p className="text-xs text-slate-500 font-medium text-center">
                        💡 提示：滚轮缩放 · 拖拽移动 · 悬停查看详情 · 点击节点可深入探索
                    </p>
                </div>
            </div>
        </div>
    );
}
