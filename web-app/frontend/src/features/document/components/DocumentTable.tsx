import { Empty } from '@/components/ui';
import type { Document } from '../types';

interface DocumentTableProps {
    documents: Document[];
    loading: boolean;
    selectedIds: Set<string>;
    onSelectAll: (checked: boolean) => void;
    onSelectOne: (id: string) => void;
    onEdit: (id: string) => void;
    onDelete: (id: string) => void;
    onView: (id: string) => void;
}

const typeColors: Record<string, string> = {
    '起诉状': 'bg-blue-100 text-blue-800',
    '判决书': 'bg-green-100 text-green-800',
    '意见书': 'bg-yellow-100 text-yellow-800',
    '裁定书': 'bg-purple-100 text-purple-800',
    '合同': 'bg-pink-100 text-pink-800',
    '协议': 'bg-indigo-100 text-indigo-800',
};

export function DocumentTable({
    documents,
    loading,
    selectedIds,
    onSelectAll,
    onSelectOne,
    onEdit,
    onDelete,
    onView,
}: DocumentTableProps) {
    return (
        <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                    <tr>
                        <th className="px-6 py-3 text-left">
                            <input
                                type="checkbox"
                                checked={selectedIds.size === documents.length && documents.length > 0}
                                onChange={(e) => onSelectAll(e.target.checked)}
                                className="rounded border-gray-300"
                            />
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            标题
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            类型
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            更新时间
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                            操作
                        </th>
                    </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                    {loading ? (
                        <tr>
                            <td colSpan={5} className="px-6 py-12 text-center">
                                <div className="flex items-center justify-center">
                                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
                                </div>
                            </td>
                        </tr>
                    ) : documents.length === 0 ? (
                        <tr>
                            <td colSpan={5} className="px-6 py-12">
                                <Empty description="暂无文书" />
                            </td>
                        </tr>
                    ) : (
                        documents.map((doc) => (
                            <tr key={doc.id} className="hover:bg-gray-50">
                                <td className="px-6 py-4 whitespace-nowrap">
                                    <input
                                        type="checkbox"
                                        checked={selectedIds.has(doc.id)}
                                        onChange={() => onSelectOne(doc.id)}
                                        className="rounded border-gray-300"
                                    />
                                </td>
                                <td className="px-6 py-4">
                                    <button
                                        onClick={() => onView(doc.id)}
                                        className="text-blue-600 hover:text-blue-800 font-medium"
                                    >
                                        {doc.title}
                                    </button>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap">
                                    <span className={`px-2 py-1 rounded text-xs font-medium ${typeColors[doc.type || ''] || 'bg-gray-100 text-gray-800'
                                        }`}>
                                        {doc.type || '未分类'}
                                    </span>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                    {new Date(doc.updated_at).toLocaleString('zh-CN')}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-right">
                                    <button
                                        onClick={() => onEdit(doc.id)}
                                        className="text-blue-600 hover:text-blue-800 mr-3"
                                    >
                                        编辑
                                    </button>
                                    <button
                                        onClick={() => onDelete(doc.id)}
                                        className="text-red-600 hover:text-red-800"
                                    >
                                        删除
                                    </button>
                                </td>
                            </tr>
                        ))
                    )}
                </tbody>
            </table>
        </div>
    );
}
