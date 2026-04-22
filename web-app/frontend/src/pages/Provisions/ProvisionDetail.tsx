import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { apiClient, provisionApi } from '@/lib/api-client';
import type { NodeOut } from '@/types/api';
import { KnowledgeGraph } from '@/features/provision/components/KnowledgeGraph';

export function ProvisionDetail() {
    const { id } = useParams<{ id: string }>();
    const navigate = useNavigate();
    const [provision, setProvision] = useState<NodeOut | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        async function fetchProvision() {
            if (!id) return;

            try {
                setLoading(true);
                setError(null);
                const data = await provisionApi.getNode(apiClient, id);
                setProvision(data);
            } catch (err) {
                console.error('Failed to fetch provision:', err);
                setError('无法加载法条详情');
            } finally {
                setLoading(false);
            }
        }

        fetchProvision();
    }, [id]);

    if (loading) {
        return (
            <div className="min-h-full bg-gradient-to-br from-gray-50 to-gray-100 flex items-center justify-center">
                <div className="bg-white rounded-xl shadow-md p-8 text-center border border-gray-200">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
                    <p className="text-gray-600 font-medium">加载中...</p>
                </div>
            </div>
        );
    }

    if (error || !provision) {
        return (
            <div className="min-h-full bg-gradient-to-br from-gray-50 to-gray-100 flex items-center justify-center">
                <div className="bg-white rounded-xl shadow-md p-8 max-w-md text-center border border-gray-200">
                    <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
                        <svg className="w-8 h-8 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                    </div>
                    <h3 className="text-lg font-bold text-gray-900 mb-2">
                        {error || '法条不存在'}
                    </h3>
                    <p className="text-gray-600 mb-6">
                        无法找到该法条，请检查链接是否正确
                    </p>
                    <button
                        onClick={() => navigate('/provisions')}
                        className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium shadow-md"
                    >
                        返回法条库
                    </button>
                </div>
            </div>
        );
    }

    const title = provision.properties?.name || provision.id;
    const content = provision.properties?.text || '';
    const level = provision.properties?.level || 1;

    return (
        <div className="min-h-full bg-gradient-to-br from-gray-50 to-gray-100">
            <div className="max-w-5xl mx-auto px-6 py-8">
                <div className="mb-6">
                    <button
                        onClick={() => navigate('/provisions')}
                        className="group flex items-center gap-2 text-gray-600 hover:text-blue-600 transition-colors"
                    >
                        <svg className="w-5 h-5 transform group-hover:-translate-x-1 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
                        </svg>
                        <span className="font-medium">返回法条库</span>
                    </button>
                </div>

                <div className="bg-white rounded-xl shadow-md border border-gray-200 overflow-hidden">
                    <div className="bg-gradient-to-r from-blue-500 to-blue-600 px-6 py-4">
                        <h1 className="text-xl font-bold text-white">
                            {title}
                        </h1>
                    </div>

                    <div className="p-6">
                        {content && (
                            <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl p-6 mb-6 border border-blue-200">
                                <div className="flex items-start gap-3 mb-3">
                                    <div className="bg-blue-500 text-white p-2 rounded-lg flex-shrink-0">
                                        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                                        </svg>
                                    </div>
                                    <h2 className="text-lg font-bold text-gray-900">法条内容</h2>
                                </div>
                                <p className="text-gray-800 whitespace-pre-line leading-relaxed text-base">
                                    {content}
                                </p>
                            </div>
                        )}

                        <div className="mb-6">
                            <KnowledgeGraph
                                provisionId={provision.id}
                                provisionTitle={title}
                                height={460}
                            />
                        </div>

                        <div className="border-t pt-6">
                            <h3 className="text-lg font-bold text-gray-900 mb-4 flex items-center gap-2">
                                <svg className="w-5 h-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                                </svg>
                                关联信息
                            </h3>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
                                    <div className="text-sm text-gray-500 mb-1">法条 ID</div>
                                    <div className="font-mono text-sm font-medium text-gray-900 bg-white px-3 py-2 rounded">
                                        {provision.id}
                                    </div>
                                </div>
                                <div className="bg-green-50 rounded-lg p-4 border border-green-200">
                                    <div className="text-sm text-gray-500 mb-1">层级</div>
                                    <div className="text-sm font-medium text-gray-900">
                                        第 {level} 级
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div className="mt-6 flex items-center gap-3">
                            <button
                                onClick={() => navigate('/provisions')}
                                className="px-6 py-3 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors font-medium"
                            >
                                返回
                            </button>
                            <button className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium shadow-md">
                                收藏此法条
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
