import { useState, useEffect } from 'react';
import { Button, Input, Modal } from '@/components/ui';
import type { Document, DocumentCreateIn, DocumentUpdateIn } from '../types';

interface DocumentFormProps {
    document?: Document;
    mode: 'create' | 'edit';
    onSubmit: (data: DocumentCreateIn | DocumentUpdateIn) => Promise<void>;
    onCancel: () => void;
    loading?: boolean;
}

export function DocumentForm({
    document,
    mode,
    onSubmit,
    onCancel,
    loading = false,
}: DocumentFormProps) {
    const [title, setTitle] = useState('');
    const [content, setContent] = useState('');
    const [showConfirmModal, setShowConfirmModal] = useState(false);

    useEffect(() => {
        if (document && mode === 'edit') {
            setTitle(document.title);
            setContent(document.content);
        }
    }, [document, mode]);

    const handleSubmit = async () => {
        // 验证
        if (!title.trim()) {
            alert('请输入文书标题');
            return;
        }

        if (title.trim().length > 200) {
            alert('标题长度不能超过 200 个字符');
            return;
        }

        const data: DocumentCreateIn | DocumentUpdateIn = {
            title: title.trim(),
            content: content.trim(),
        };

        await onSubmit(data);
    };

    const handleCancel = () => {
        // 检查是否有未保存的修改
        if (title || content) {
            setShowConfirmModal(true);
        } else {
            onCancel();
        }
    };

    const confirmCancel = () => {
        setShowConfirmModal(false);
        onCancel();
    };

    return (
        <>
            <div className="space-y-6">
                {/* 标题 */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                        标题 <span className="text-red-500">*</span>
                    </label>
                    <Input
                        value={title}
                        onChange={(e) => setTitle(e.target.value)}
                        placeholder="请输入文书标题"
                        maxLength={200}
                        error={
                            !title.trim() && title.length > 0
                                ? '标题不能为空'
                                : title.length > 200
                                    ? '标题长度不能超过 200 个字符'
                                    : undefined
                        }
                    />
                    <p className="text-xs text-gray-500 mt-1">
                        {title.length}/200
                    </p>
                </div>

                {/* 内容 */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                        内容
                    </label>
                    <textarea
                        value={content}
                        onChange={(e) => setContent(e.target.value)}
                        placeholder="请输入文书内容..."
                        rows={20}
                        className="w-full border border-gray-300 rounded-md px-3 py-2 focus:border-blue-500 focus:ring-blue-500 sm:text-sm font-mono"
                    />
                </div>

                {/* 底部按钮 */}
                <div className="flex items-center gap-4 pt-6 border-t">
                    <Button onClick={handleSubmit} loading={loading}>
                        保存
                    </Button>
                    <Button variant="secondary" onClick={handleCancel}>
                        取消
                    </Button>
                </div>
            </div>

            {/* 确认取消弹窗 */}
            <Modal
                isOpen={showConfirmModal}
                onClose={() => setShowConfirmModal(false)}
                title="提示"
                footer={
                    <div className="flex gap-2">
                        <Button variant="secondary" onClick={() => setShowConfirmModal(false)}>
                            继续编辑
                        </Button>
                        <Button variant="danger" onClick={confirmCancel}>
                            放弃修改
                        </Button>
                    </div>
                }
            >
                <p className="text-gray-600">
                    有未保存的修改，确定要放弃吗？
                </p>
            </Modal>
        </>
    );
}
