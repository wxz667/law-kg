import { Navigate, useLocation } from 'react-router-dom';

interface ProtectedRouteProps {
    children: React.ReactNode;
}

/**
 * 检查 JWT Token 是否过期
 */
function isTokenExpired(token: string): boolean {
    try {
        const parts = token.split('.');
        if (parts.length !== 3) {
            return true;
        }
        const payload = JSON.parse(atob(parts[1]));
        const currentTime = Math.floor(Date.now() / 1000);
        return payload.exp && payload.exp < currentTime;
    } catch (error) {
        // 如果解析失败，认为 token 无效
        console.error('Token 解析失败:', error);
        return true;
    }
}

/**
 * 受保护的路由组件
 * 检查用户是否已登录，如果未登录则重定向到登录页
 */
export function ProtectedRoute({ children }: ProtectedRouteProps) {
    const location = useLocation();
    const token = localStorage.getItem('access_token');

    if (!token || isTokenExpired(token)) {
        // 清除过期的 token
        localStorage.removeItem('access_token');
        localStorage.removeItem('user_id');
        localStorage.removeItem('username');

        // 未登录或 token 已过期，重定向到登录页，并保存原始路径
        return <Navigate to="/login" state={{ from: location.pathname }} replace />;
    }

    return <>{children}</>;
}
