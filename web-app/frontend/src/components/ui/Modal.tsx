import { ReactNode, useEffect } from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { X } from 'lucide-react';

export interface ModalProps {
    isOpen: boolean;
    onClose: () => void;
    title?: string;
    children?: ReactNode;
    footer?: ReactNode;
    closeOnOverlayClick?: boolean;
    className?: string;
}

export function Modal({
    isOpen,
    onClose,
    title,
    children,
    footer,
    closeOnOverlayClick = true,
    className,
}: ModalProps) {
    // 阻止背景滚动
    useEffect(() => {
        if (isOpen) {
            document.body.style.overflow = 'hidden';
        } else {
            document.body.style.overflow = '';
        }
        return () => {
            document.body.style.overflow = '';
        };
    }, [isOpen]);

    // ESC 键关闭
    useEffect(() => {
        const handleEsc = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                onClose();
            }
        };
        if (isOpen) {
            window.addEventListener('keydown', handleEsc);
        }
        return () => {
            window.removeEventListener('keydown', handleEsc);
        };
    }, [isOpen, onClose]);

    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 z-[100] overflow-y-auto">
            {/* 遮罩层 */}
            <div
                className="fixed inset-0 bg-slate-900/60 backdrop-blur-sm transition-opacity animate-in fade-in duration-300"
                onClick={closeOnOverlayClick ? onClose : undefined}
            />

            {/* Modal 内容 */}
            <div className="flex min-h-full items-center justify-center p-6">
                <div
                    className={twMerge(
                        clsx(
                            'relative bg-white rounded-[2.5rem] shadow-2xl max-w-lg w-full overflow-hidden animate-in zoom-in-95 slide-in-from-bottom-10 duration-500',
                            className
                        )
                    )}
                    onClick={(e) => e.stopPropagation()}
                >
                    {/* Header */}
                    <div className="flex items-center justify-between px-10 py-8 border-b border-slate-100">
                        {title ? (
                            <h3 className="text-2xl font-black text-slate-900 tracking-tight">{title}</h3>
                        ) : <div />}
                        <button
                            onClick={onClose}
                            className="p-2 bg-slate-50 text-slate-400 hover:text-slate-900 hover:bg-slate-100 rounded-xl transition-all"
                        >
                            <X className="w-6 h-6" />
                        </button>
                    </div>

                    {/* Body */}
                    <div className="px-10 py-8">
                        {children}
                    </div>

                    {/* Footer */}
                    {footer && (
                        <div className="px-10 py-8 bg-slate-50 border-t border-slate-100">
                            {footer}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
