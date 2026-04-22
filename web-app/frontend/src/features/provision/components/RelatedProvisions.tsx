import { useNavigate } from 'react-router-dom';
import { ExternalLink } from 'lucide-react';
import { Card } from '@/components/ui';
import type { RelatedProvision } from '../types';

interface RelatedProvisionsProps {
    related: RelatedProvision[];
}

const relationshipLabels: Record<string, string> = {
    REFERS_TO: '引用',
    INTERPRETS: '解释',
    AMENDS: '修订',
    REPEALS: '废止',
};

const relationshipColors: Record<string, string> = {
    REFERS_TO: 'bg-blue-100 text-blue-800',
    INTERPRETS: 'bg-green-100 text-green-800',
    AMENDS: 'bg-yellow-100 text-yellow-800',
    REPEALS: 'bg-red-100 text-red-800',
};

export function RelatedProvisions({ related }: RelatedProvisionsProps) {
    const navigate = useNavigate();

    if (!related || related.length === 0) {
        return (
            <Card title="关联法条">
                <div className="text-center py-8 text-gray-500">
                    暂无关联法条
                </div>
            </Card>
        );
    }

    return (
        <div className="space-y-4">
            {/* 基本信息卡片 */}
            <Card title="基本信息">
                <div className="text-center py-4 text-gray-500">
                    基本信息已在内容区显示
                </div>
            </Card>

            {/* 关联关系卡片 */}
            <Card title="关联关系" extra={
                <span className="text-sm text-gray-500">{related.length} 条</span>
            }>
                <div className="space-y-3">
                    {related.map((item) => (
                        <div
                            key={item.id}
                            className="flex items-start gap-3 p-3 rounded hover:bg-gray-50 cursor-pointer transition-colors"
                            onClick={() => navigate(`/provisions/${item.id}`)}
                        >
                            <span className={`px-2 py-1 rounded text-xs font-medium ${relationshipColors[item.relationship] || 'bg-gray-100 text-gray-800'
                                }`}>
                                {relationshipLabels[item.relationship] || '关联'}
                            </span>
                            <div className="flex-1 min-w-0">
                                <div className="font-medium text-gray-900 truncate">
                                    {item.name}
                                </div>
                                {item.documentName && (
                                    <div className="text-xs text-gray-500 truncate">
                                        {item.documentName}
                                    </div>
                                )}
                            </div>
                            <ExternalLink className="w-4 h-4 text-gray-400 flex-shrink-0" />
                        </div>
                    ))}
                </div>
            </Card>
        </div>
    );
}
