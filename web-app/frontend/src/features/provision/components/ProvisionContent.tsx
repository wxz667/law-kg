import type { ProvisionDetail } from '../types';

interface ProvisionContentProps {
    provision: ProvisionDetail;
}

export function ProvisionContent({ provision }: ProvisionContentProps) {
    return (
        <div className="bg-white rounded-lg shadow p-6">
            {/* 法条标题 */}
            <h2 className="text-2xl font-bold mb-4 text-gray-900">
                {provision.name}
            </h2>

            {/* 基本信息 */}
            <div className="mb-6 pb-6 border-b">
                <dl className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                        <dt className="text-gray-500">所属法律</dt>
                        <dd className="text-gray-900 font-medium">{provision.documentName}</dd>
                    </div>
                    {provision.publishDate && (
                        <div>
                            <dt className="text-gray-500">发布日期</dt>
                            <dd className="text-gray-900">{provision.publishDate}</dd>
                        </div>
                    )}
                    {provision.status && (
                        <div>
                            <dt className="text-gray-500">效力状态</dt>
                            <dd className="text-green-600 font-medium">{provision.status}</dd>
                        </div>
                    )}
                </dl>
            </div>

            {/* 法条正文 */}
            <div className="prose max-w-none">
                <div className="text-gray-800 leading-relaxed whitespace-pre-line">
                    {provision.content}
                </div>
            </div>

            {/* 款/项/目（如果有） */}
            {provision.children && provision.children.length > 0 && (
                <div className="mt-6 pt-6 border-t">
                    <h3 className="font-semibold mb-3">款项明细</h3>
                    <div className="space-y-3">
                        {provision.children.map((child, index) => (
                            <div key={child.id} className="pl-4 border-l-2 border-gray-200">
                                <div className="text-sm text-gray-600 mb-1">
                                    {child.level === 'paragraph' && `第${index + 1}款`}
                                    {child.level === 'item' && `（${String.fromCharCode(0x5341 + index + 1)}）`}
                                    {child.level === 'sub_item' && `${index + 1}.`}
                                </div>
                                <div className="text-gray-800">{child.content}</div>
                            </div>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
