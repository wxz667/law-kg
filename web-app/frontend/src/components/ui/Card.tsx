import { ReactNode } from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export interface CardProps {
    title?: string;
    extra?: ReactNode;
    children?: ReactNode;
    hoverable?: boolean;
    loading?: boolean;
    className?: string;
}

export function Card({
    title,
    extra,
    children,
    hoverable = false,
    loading = false,
    className,
}: CardProps) {
    return (
        <div
            className={twMerge(
                clsx(
                    'bg-white rounded-[2.5rem] border border-slate-100 shadow-sm transition-all duration-500 overflow-hidden',
                    hoverable && 'hover:shadow-2xl hover:shadow-blue-500/10 hover:-translate-y-2 cursor-pointer',
                    className
                )
            )}
        >
            {(title || extra) && (
                <div className="px-10 py-8 border-b border-slate-50 flex items-center justify-between bg-slate-50/30">
                    {title && (
                        <h3 className="text-xl font-black text-slate-900 tracking-tight">{title}</h3>
                    )}
                    {extra && <div>{extra}</div>}
                </div>
            )}
            <div className="px-10 py-8">
                {loading ? (
                    <div className="flex flex-col items-center justify-center py-12 space-y-4">
                        <div className="relative w-12 h-12">
                            <div className="absolute inset-0 border-4 border-slate-100 rounded-full" />
                            <div className="absolute inset-0 border-4 border-blue-500 rounded-full border-t-transparent animate-spin" />
                        </div>
                        <p className="text-xs font-black text-slate-300 uppercase tracking-widest">正在载入内容</p>
                    </div>
                ) : (
                    children
                )}
            </div>
        </div>
    );
}
