import { ButtonHTMLAttributes, ReactNode } from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

type Variant = 'primary' | 'secondary' | 'danger' | 'ghost';
type Size = 'sm' | 'md' | 'lg';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
    variant?: Variant;
    size?: Size;
    loading?: boolean;
    leftIcon?: ReactNode;
    rightIcon?: ReactNode;
    children?: ReactNode;
}

const variantStyles: Record<Variant, string> = {
    primary: 'bg-slate-900 text-white hover:bg-slate-800 shadow-lg shadow-slate-900/10 focus:ring-slate-500',
    secondary: 'bg-white text-slate-700 border-2 border-slate-100 hover:border-slate-200 hover:bg-slate-50 shadow-sm focus:ring-slate-200',
    danger: 'bg-red-600 text-white hover:bg-red-700 shadow-lg shadow-red-600/10 focus:ring-red-500',
    ghost: 'bg-transparent text-slate-500 hover:bg-slate-100 hover:text-slate-900 focus:ring-slate-100',
};

const sizeStyles: Record<Size, string> = {
    sm: 'px-4 py-2 text-xs font-black uppercase tracking-widest gap-2 rounded-xl',
    md: 'px-6 py-3 text-sm font-black uppercase tracking-widest gap-2.5 rounded-2xl',
    lg: 'px-10 py-4 text-base font-black uppercase tracking-widest gap-3 rounded-[1.5rem]',
};

export function Button({
    variant = 'primary',
    size = 'md',
    loading = false,
    leftIcon,
    rightIcon,
    children,
    className,
    disabled,
    ...props
}: ButtonProps) {
    const classes = twMerge(
        clsx(
            'inline-flex items-center justify-center transition-all duration-300 focus:outline-none focus:ring-4 active:scale-95',
            variantStyles[variant],
            sizeStyles[size],
            (disabled || loading) && 'opacity-50 cursor-not-allowed grayscale active:scale-100',
            !children && 'p-3',
            className
        )
    );

    return (
        <button className={classes} disabled={disabled || loading} {...props}>
            {loading ? (
                <svg
                    className="animate-spin h-5 w-5"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                >
                    <circle
                        className="opacity-25"
                        cx="12"
                        cy="12"
                        r="10"
                        stroke="currentColor"
                        strokeWidth="4"
                    />
                    <path
                        className="opacity-75"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    />
                </svg>
            ) : (
                <>
                    {leftIcon && <span className="flex-shrink-0">{leftIcon}</span>}
                    {children && <span>{children}</span>}
                    {rightIcon && <span className="flex-shrink-0">{rightIcon}</span>}
                </>
            )}
        </button>
    );
}
