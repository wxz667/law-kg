import { InputHTMLAttributes, ReactNode } from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
    error?: string;
    leftAddon?: ReactNode;
    rightAddon?: ReactNode;
    label?: string;
}

export function Input({
    error,
    leftAddon,
    rightAddon,
    label,
    className,
    ...props
}: InputProps) {
    return (
        <div className="w-full">
            {label && (
                <label className="block text-sm font-black text-slate-400 uppercase tracking-widest mb-3 ml-2">
                    {label}
                </label>
            )}
            <div className={clsx(
                'relative flex items-center group',
                (leftAddon || rightAddon) && 'flex-stretch'
            )}>
                {leftAddon && (
                    <div className="absolute left-0 pl-5 flex items-center pointer-events-none text-slate-400 group-focus-within:text-blue-500 transition-colors">
                        {leftAddon}
                    </div>
                )}
                <input
                    className={twMerge(
                        clsx(
                            'block w-full rounded-2xl border-2 border-slate-100 bg-slate-50 px-6 py-4 text-slate-700 font-medium outline-none transition-all shadow-inner',
                            'focus:bg-white focus:border-blue-500 focus:ring-4 focus:ring-blue-500/5',
                            error && 'border-red-500 focus:border-red-500 focus:ring-red-500/5 bg-red-50/30',
                            leftAddon && 'pl-14',
                            rightAddon && 'pr-14',
                            className
                        )
                    )}
                    {...props}
                />
                {rightAddon && (
                    <div className="absolute right-0 pr-5 flex items-center text-slate-400 group-focus-within:text-blue-500 transition-colors">
                        {rightAddon}
                    </div>
                )}
            </div>
            {error && (
                <p className="mt-2 ml-2 text-sm font-bold text-red-500 flex items-center gap-1">
                    <span className="w-1 h-1 bg-red-500 rounded-full" />
                    {error}
                </p>
            )}
        </div>
    );
}
