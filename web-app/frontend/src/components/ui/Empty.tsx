import { ReactNode } from 'react';
import { clsx } from 'clsx';
import { Inbox } from 'lucide-react';

export interface EmptyProps {
    image?: ReactNode;
    description?: string;
    action?: ReactNode;
    className?: string;
}

export function Empty({
    image,
    description = '暂无数据',
    action,
    className,
}: EmptyProps) {
    return (
        <div className={clsx('flex flex-col items-center justify-center py-20 px-10 text-center', className)}>
            <div className="w-32 h-32 bg-slate-50 rounded-full flex items-center justify-center mb-8 shadow-inner relative overflow-hidden">
                <div className="absolute inset-0 bg-gradient-to-br from-slate-100/50 to-white/50 blur-xl" />
                {image || (
                    <Inbox className="w-16 h-16 text-slate-200 relative z-10" />
                )}
            </div>
            {description && (
                <p className="text-slate-400 font-bold text-lg mb-8 tracking-tight">{description}</p>
            )}
            {action && <div className="animate-in fade-in slide-in-from-bottom-4 duration-700">{action}</div>}
        </div>
    );
}
